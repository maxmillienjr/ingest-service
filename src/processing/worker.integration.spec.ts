import { Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test } from '@nestjs/testing';
import type { MongoClient } from 'mongodb';
import {
  startMongoMemory,
  type MongoMemory,
} from '../../test/support/mongo-memory';
import type { Env } from '../config/env.schema';
import type { EventDoc } from '../events/event.types';
import { EventsRepository } from '../events/events.repository';
import { createMongoClient } from '../mongo/mongo-client';
import type { PatientDoc } from '../patients/patient.types';
import { PatientsRepository } from '../patients/patients.repository';
import { EVENT_PROCESSOR, type EventProcessor } from './processor';
import { WorkerService } from './worker.service';

interface Interval {
  id: string;
  patientId: string;
  start: number;
  end: number;
  ok: boolean;
}

/**
 * Records when each event was in the external call, for the overlap and
 * order checks, and fails the calls it is told to.
 */
class RecordingProcessor implements EventProcessor {
  intervals: Interval[] = [];
  /** Decides per call; `nth` is 1 for the first call for that event. */
  failWhen: (event: EventDoc, nth: number) => boolean = () => false;
  private readonly calls = new Map<string, number>();

  constructor(private readonly delayMs: number) {}

  async process(event: EventDoc): Promise<unknown> {
    const nth = (this.calls.get(event._id) ?? 0) + 1;
    this.calls.set(event._id, nth);
    const start = performance.now();
    await new Promise((resolve) => setTimeout(resolve, this.delayMs));
    const ok = !this.failWhen(event, nth);
    this.intervals.push({
      id: event._id,
      patientId: event.patientId,
      start,
      end: performance.now(),
      ok,
    });
    if (!ok) throw new Error(`simulated failure, call ${nth}`);
    return { eventId: event._id };
  }

  /** Ids in the order the external call saw them, failures included. */
  sequence(patientId: string): string[] {
    return this.intervals
      .filter((r) => r.patientId === patientId)
      .sort((a, b) => a.start - b.start)
      .map((r) => r.id);
  }
}

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

describe('WorkerService against MongoDB', () => {
  let mongo: MongoMemory;
  let client: MongoClient;
  let events: EventsRepository;
  let patients: PatientsRepository;
  const processor = new RecordingProcessor(15);

  beforeAll(async () => {
    jest.spyOn(Logger.prototype, 'log').mockImplementation(() => {});
    jest.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
    mongo = await startMongoMemory();
    client = createMongoClient(mongo.uri);
    await client.connect();
    events = new EventsRepository(client.db());
    await events.onModuleInit();
    patients = new PatientsRepository(client.db());
  });

  afterAll(async () => {
    jest.restoreAllMocks();
    await client.close();
    await mongo.stop();
  });

  const config: Partial<Env> = {
    ROLE: 'worker',
    WORKER_CONCURRENCY: 4,
    LEASE_TTL_MS: 5_000,
    POLL_IDLE_MS: 5,
    SHUTDOWN_GRACE_MS: 2_000,
    MAX_ATTEMPTS: 3,
    RETRY_BASE_MS: 30,
    RETRY_MAX_MS: 30,
  };

  async function worker(
    proc: EventProcessor = processor,
    cfg: Partial<Env> = config,
  ): Promise<WorkerService> {
    const moduleRef = await Test.createTestingModule({
      providers: [
        WorkerService,
        { provide: EventsRepository, useValue: events },
        { provide: PatientsRepository, useValue: patients },
        { provide: EVENT_PROCESSOR, useValue: proc },
        {
          provide: ConfigService,
          useValue: { get: (key: keyof Env) => cfg[key] },
        },
      ],
    }).compile();
    return moduleRef.get(WorkerService);
  }

  async function until(
    condition: () => boolean,
    timeoutMs = 3_000,
  ): Promise<void> {
    const started = Date.now();
    while (!condition()) {
      if (Date.now() - started > timeoutMs)
        throw new Error('condition not met');
      await sleep(5);
    }
  }

  const T0 = new Date('2026-01-01T00:00:00Z');
  const past = new Date(Date.now() - 60_000);

  const insert = (patientId: string, i: number): Promise<unknown> =>
    events.insertIfAbsent({
      _id: `${patientId}-${String(i).padStart(2, '0')}`,
      patientId,
      type: 'vitals',
      data: { i },
      ts: new Date(T0.getTime() + i * 1000),
      // Arrival order deliberately differs from ts order.
      receivedAt: new Date(past.getTime() + ((i * 7) % 10) * 100),
      status: 'pending',
      notBefore: past,
      attempts: 0,
    });

  async function untilAllDone(expected: number): Promise<void> {
    for (let i = 0; i < 400; i += 1) {
      const done = await client
        .db()
        .collection('events')
        .countDocuments({ status: 'done' });
      if (done >= expected) return;
      await sleep(10);
    }
    throw new Error('events did not all complete');
  }

  /** Waits until `patientId` has `n` events in a terminal status. */
  async function untilSettled(patientId: string, n: number): Promise<void> {
    const col = client.db().collection<EventDoc>('events');
    for (let i = 0; i < 400; i += 1) {
      const settled = await col.countDocuments({
        patientId,
        status: { $in: ['done', 'failed'] },
      });
      if (settled >= n) return;
      await sleep(10);
    }
    const docs = await col.find({ patientId }).sort({ ts: 1 }).toArray();
    throw new Error(
      `patient ${patientId} did not settle: ${docs
        .map((d) => `${d._id}:${d.status}/${d.attempts}`)
        .join(' ')}`,
    );
  }

  it('two workers never process one patient concurrently, and apply its events in ts order', async () => {
    const shuffled = [7, 2, 9, 0, 5, 1, 8, 3, 6, 4];
    for (const i of shuffled) await insert('p1', i);
    for (const i of [3, 0, 2, 1, 5, 4]) await insert('p2', i);

    const [w1, w2] = [await worker(), await worker()];
    w1.onApplicationBootstrap();
    w2.onApplicationBootstrap();
    await untilAllDone(16);
    await Promise.all([
      w1.beforeApplicationShutdown(),
      w2.beforeApplicationShutdown(),
    ]);

    for (const patientId of ['p1', 'p2']) {
      const runs = processor.intervals
        .filter((r) => r.patientId === patientId)
        .sort((a, b) => a.start - b.start);
      // Serial: each call starts after the previous one ended.
      for (let i = 1; i < runs.length; i += 1) {
        expect(runs[i]!.start).toBeGreaterThanOrEqual(runs[i - 1]!.end);
      }
      // Ordered: processing order is ts order, whatever the arrival order.
      expect(runs.map((r) => r.id)).toEqual([...runs.map((r) => r.id)].sort());
    }

    const docs = await client
      .db()
      .collection<EventDoc>('events')
      .find({})
      .toArray();
    expect(docs.every((d) => d.attempts === 1 && d.outOfOrder === false)).toBe(
      true,
    );
    // Both workers did real work; the lease shared it rather than starving one.
    expect(new Set(processor.intervals.map((r) => r.id)).size).toBe(16);
  });

  it('processes a late arrival and flags it as out of order', async () => {
    // p1's watermark is at i=9 from the previous test. i=4 arrives now.
    await events.insertIfAbsent({
      _id: 'p1-late',
      patientId: 'p1',
      type: 'vitals',
      data: {},
      ts: new Date(T0.getTime() + 4 * 1000),
      receivedAt: new Date(),
      status: 'pending',
      notBefore: past,
      attempts: 0,
    });

    const w = await worker();
    w.onApplicationBootstrap();
    await untilAllDone(17);
    await w.beforeApplicationShutdown();

    expect(await events.findById('p1-late')).toMatchObject({
      status: 'done',
      outOfOrder: true,
    });
    expect(
      await client
        .db()
        .collection<PatientDoc>('patients')
        .findOne({ _id: 'p1' }),
    ).toMatchObject({ lastAppliedTs: new Date(T0.getTime() + 9 * 1000) });
  });

  it('recovers an event whose worker died mid-call: one outcome, two attempts, no sweeper', async () => {
    // What a SIGKILLed worker leaves behind: a stale lease and an expired lock.
    const stale = new Date(Date.now() - 10_000);
    await client
      .db()
      .collection<PatientDoc>('patients')
      .updateOne(
        { _id: 'p3' },
        { $set: { leaseOwner: 'dead-worker', leaseUntil: stale } },
        { upsert: true },
      );
    await events.insertIfAbsent({
      _id: 'p3-00',
      patientId: 'p3',
      type: 'vitals',
      data: {},
      ts: T0,
      receivedAt: past,
      status: 'processing',
      notBefore: past,
      attempts: 1,
      lockToken: 'dead-token',
      lockedBy: 'dead-worker',
      lockedUntil: stale,
    });

    const w = await worker();
    w.onApplicationBootstrap();
    await untilAllDone(18);
    await w.beforeApplicationShutdown();

    expect(await events.findById('p3-00')).toMatchObject({
      status: 'done',
      attempts: 2,
      result: { eventId: 'p3-00' },
    });
  });

  it('keeps ts order across retries: a failed attempt holds the line until it succeeds', async () => {
    // Every odd event fails its first call, with two workers competing.
    processor.failWhen = (event, nth) =>
      nth === 1 && (event.data as { i: number }).i % 2 === 1;
    for (const i of [5, 1, 7, 3, 0, 6, 2, 4]) await insert('p5', i);

    const [w1, w2] = [await worker(), await worker()];
    w1.onApplicationBootstrap();
    w2.onApplicationBootstrap();
    await untilSettled('p5', 8);
    await Promise.all([
      w1.beforeApplicationShutdown(),
      w2.beforeApplicationShutdown(),
    ]);
    processor.failWhen = () => false;

    // The call sequence never goes backwards: a retry repeats an id, it
    // never lets a later id in first.
    const seq = processor.sequence('p5');
    expect(seq).toHaveLength(12);
    for (let i = 1; i < seq.length; i += 1) {
      expect(seq[i]! >= seq[i - 1]!).toBe(true);
    }
    const docs = await client
      .db()
      .collection<EventDoc>('events')
      .find({ patientId: 'p5' })
      .sort({ ts: 1 })
      .toArray();
    expect(docs.map((d) => d.attempts)).toEqual([1, 2, 1, 2, 1, 2, 1, 2]);
    expect(docs.every((d) => d.status === 'done' && !d.outOfOrder)).toBe(true);
    // The retry history survives the eventual success.
    expect(docs[1]?.error).toMatch(/simulated failure/);
  });

  it('marks a poison event failed after MAX_ATTEMPTS and lets the patient continue', async () => {
    processor.failWhen = (event) => event._id === 'p6-00';
    for (const i of [2, 0, 1]) await insert('p6', i);

    const w = await worker();
    w.onApplicationBootstrap();
    await untilSettled('p6', 3);
    await w.beforeApplicationShutdown();
    processor.failWhen = () => false;

    expect(await events.findById('p6-00')).toMatchObject({
      status: 'failed',
      attempts: 3,
      error: expect.stringMatching(/simulated failure/) as string,
    });
    // Later events waited for every attempt, then went through in order.
    expect(processor.sequence('p6')).toEqual([
      'p6-00',
      'p6-00',
      'p6-00',
      'p6-01',
      'p6-02',
    ]);
    expect(await events.findById('p6-02')).toMatchObject({
      status: 'done',
      attempts: 1,
      outOfOrder: false,
    });
  });

  it('fences out a stalled attempt after another worker took over its expired lock', async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => {
      release = r;
    });
    let stalledCalls = 0;
    const stalled: EventProcessor = {
      async process(event) {
        stalledCalls += 1;
        await gate;
        return { eventId: event._id, by: 'stalled' };
      },
    };
    const shortLease = { ...config, LEASE_TTL_MS: 300, SHUTDOWN_GRACE_MS: 100 };
    await insert('p4', 0);

    const wA = await worker(stalled, shortLease);
    wA.onApplicationBootstrap();
    await until(() => stalledCalls === 1);

    // A's lock and lease expire 300ms after its claim; B takes over.
    const wB = await worker(processor, shortLease);
    wB.onApplicationBootstrap();
    await untilSettled('p4', 1);

    // A wakes up late and tries to write its outcome with a stale token.
    release();
    await sleep(50);
    await Promise.all([
      wA.beforeApplicationShutdown(),
      wB.beforeApplicationShutdown(),
    ]);

    const doc = await events.findById('p4-00');
    expect(doc).toMatchObject({
      status: 'done',
      attempts: 2,
      result: { eventId: 'p4-00' },
    });
    expect(doc?.result).not.toMatchObject({ by: 'stalled' });
  });
});
