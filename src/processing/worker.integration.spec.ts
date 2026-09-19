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
}

/** Records when each event was in the external call, for the overlap check. */
class RecordingProcessor implements EventProcessor {
  intervals: Interval[] = [];
  constructor(private readonly delayMs: number) {}

  async process(event: EventDoc): Promise<unknown> {
    const start = performance.now();
    await new Promise((resolve) => setTimeout(resolve, this.delayMs));
    this.intervals.push({
      id: event._id,
      patientId: event.patientId,
      start,
      end: performance.now(),
    });
    return { eventId: event._id };
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
    await untilAllDone(19);

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
