import { Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test } from '@nestjs/testing';
import type { Env } from '../config/env.schema';
import type { ClaimedEvent } from '../events/event.types';
import { EventsRepository, type Outcome } from '../events/events.repository';
import { PatientsRepository } from '../patients/patients.repository';
import { EVENT_PROCESSOR, type EventProcessor } from './processor';
import { WorkerService } from './worker.service';

/** In-memory stand-in for the events collection: a ready queue per patient. */
class FakeEvents {
  queues = new Map<string, ClaimedEvent[]>();
  completed: Array<{ id: string; lockToken: string } & Outcome> = [];
  failed: Array<{ id: string; lockToken: string; error: string }> = [];
  completeResult = true;

  add(...events: ClaimedEvent[]): void {
    for (const event of events) {
      const queue = this.queues.get(event.patientId) ?? [];
      queue.push(event);
      this.queues.set(event.patientId, queue);
    }
  }
  claimablePatients(): Promise<string[]> {
    return Promise.resolve(
      [...this.queues].filter(([, q]) => q.length > 0).map(([p]) => p),
    );
  }
  claimHead(patientId: string): Promise<ClaimedEvent | null> {
    return Promise.resolve(this.queues.get(patientId)?.shift() ?? null);
  }
  complete(id: string, lockToken: string, outcome: Outcome): Promise<boolean> {
    this.completed.push({ id, lockToken, ...outcome });
    return Promise.resolve(this.completeResult);
  }
  fail(id: string, lockToken: string, error: string): Promise<boolean> {
    this.failed.push({ id, lockToken, error });
    return Promise.resolve(true);
  }
}

/** In-memory stand-in for the patients collection. */
class FakePatients {
  leases = new Map<string, string>();
  watermarks = new Map<string, Date>();
  released: string[] = [];
  renewResult = true;

  tryLease(patientId: string, workerId: string): Promise<boolean> {
    if (this.leases.has(patientId)) return Promise.resolve(false);
    this.leases.set(patientId, workerId);
    return Promise.resolve(true);
  }
  renewLease(patientId: string, workerId: string): Promise<boolean> {
    return Promise.resolve(
      this.renewResult && this.leases.get(patientId) === workerId,
    );
  }
  releaseLease(patientId: string, workerId: string): Promise<void> {
    if (this.leases.get(patientId) === workerId) this.leases.delete(patientId);
    this.released.push(patientId);
    return Promise.resolve();
  }
  advanceWatermark(patientId: string, ts: Date): Promise<Date | null> {
    const previous = this.watermarks.get(patientId) ?? null;
    if (!previous || ts > previous) this.watermarks.set(patientId, ts);
    return Promise.resolve(previous);
  }
}

/** Resolves each event through a per-test behaviour; records order and overlap. */
class FakeProcessor implements EventProcessor {
  calls: string[] = [];
  inFlight = 0;
  maxInFlight = 0;
  behaviour: (event: ClaimedEvent) => Promise<unknown> = () =>
    Promise.resolve('ok');

  async process(event: ClaimedEvent): Promise<unknown> {
    this.calls.push(event._id);
    this.inFlight += 1;
    this.maxInFlight = Math.max(this.maxInFlight, this.inFlight);
    try {
      return await this.behaviour(event);
    } finally {
      this.inFlight -= 1;
    }
  }
}

const claimed = (id: string, patientId = 'p1', tsSeconds = 0): ClaimedEvent =>
  ({
    _id: id,
    patientId,
    ts: new Date(tsSeconds * 1000),
    attempts: 1,
    lockToken: `token-${id}`,
  }) as ClaimedEvent;

const defaults: Partial<Env> = {
  ROLE: 'worker',
  WORKER_CONCURRENCY: 2,
  LEASE_TTL_MS: 1_000,
  POLL_IDLE_MS: 5,
  SHUTDOWN_GRACE_MS: 300,
};

async function build(overrides: Partial<Env> = {}): Promise<{
  worker: WorkerService;
  events: FakeEvents;
  patients: FakePatients;
  processor: FakeProcessor;
}> {
  const events = new FakeEvents();
  const patients = new FakePatients();
  const processor = new FakeProcessor();
  const values = { ...defaults, ...overrides };
  const moduleRef = await Test.createTestingModule({
    providers: [
      WorkerService,
      { provide: EventsRepository, useValue: events },
      { provide: PatientsRepository, useValue: patients },
      { provide: EVENT_PROCESSOR, useValue: processor },
      {
        provide: ConfigService,
        useValue: { get: (key: keyof Env) => values[key] },
      },
    ],
  }).compile();
  return { worker: moduleRef.get(WorkerService), events, patients, processor };
}

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

async function until(
  condition: () => boolean,
  timeoutMs = 2_000,
): Promise<void> {
  const started = Date.now();
  while (!condition()) {
    if (Date.now() - started > timeoutMs) throw new Error('condition not met');
    await sleep(5);
  }
}

/** A promise the test resolves by hand, to hold a lane open. */
function deferred(): { promise: Promise<unknown>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<unknown>((r) => {
    resolve = () => r('ok');
  });
  return { promise, resolve };
}

describe('WorkerService', () => {
  let warn: jest.SpyInstance;
  beforeEach(() => {
    warn = jest.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
    jest.spyOn(Logger.prototype, 'log').mockImplementation(() => {});
    jest.spyOn(Logger.prototype, 'error').mockImplementation(() => {});
  });
  afterEach(() => jest.restoreAllMocks());

  it("drains a patient's events one at a time, in order, then releases the lease", async () => {
    const { worker, events, patients, processor } = await build();
    processor.behaviour = () => sleep(10);
    events.add(
      claimed('a', 'p1', 1),
      claimed('b', 'p1', 2),
      claimed('c', 'p1', 3),
    );

    worker.onApplicationBootstrap();
    await until(() => events.completed.length === 3);
    await worker.beforeApplicationShutdown();

    expect(processor.calls).toEqual(['a', 'b', 'c']);
    expect(processor.maxInFlight).toBe(1);
    expect(events.completed[1]).toMatchObject({
      id: 'b',
      lockToken: 'token-b',
    });
    expect(patients.leases.size).toBe(0);
    expect(patients.released).toEqual(['p1']);
  });

  it('works different patients concurrently, up to WORKER_CONCURRENCY', async () => {
    const { worker, events, processor } = await build({
      WORKER_CONCURRENCY: 2,
    });
    const gate = deferred();
    processor.behaviour = () => gate.promise;
    events.add(claimed('a', 'p1'), claimed('b', 'p2'), claimed('c', 'p3'));

    worker.onApplicationBootstrap();
    await until(() => processor.calls.length === 2);
    await sleep(30);
    expect(processor.calls).toHaveLength(2);

    gate.resolve();
    await until(() => events.completed.length === 3);
    await worker.beforeApplicationShutdown();
    expect(processor.maxInFlight).toBe(2);
  });

  it('flags an event that lands behind the patient watermark', async () => {
    const { worker, events, patients, processor } = await build();
    // The repository hands out ts=10 first because ts=5 arrived late.
    events.add(claimed('newer', 'p1', 10));
    processor.behaviour = () => {
      if (processor.calls.length === 1) events.add(claimed('late', 'p1', 5));
      return Promise.resolve('ok');
    };

    worker.onApplicationBootstrap();
    await until(() => events.completed.length === 2);
    await worker.beforeApplicationShutdown();

    expect(events.completed).toEqual([
      expect.objectContaining({ id: 'newer', outOfOrder: false }),
      expect.objectContaining({ id: 'late', outOfOrder: true }),
    ]);
    expect(patients.watermarks.get('p1')).toEqual(new Date(10_000));
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('out of order'));
  });

  it("records a failed attempt and continues with the patient's next event", async () => {
    const { worker, events, processor } = await build();
    processor.behaviour = (event) =>
      event._id === 'a'
        ? Promise.reject(new Error('external system down'))
        : Promise.resolve('ok');
    events.add(claimed('a', 'p1', 1), claimed('b', 'p1', 2));

    worker.onApplicationBootstrap();
    await until(
      () => events.failed.length === 1 && events.completed.length === 1,
    );
    await worker.beforeApplicationShutdown();

    expect(events.failed[0]).toEqual({
      id: 'a',
      lockToken: 'token-a',
      error: 'external system down',
    });
    expect(events.completed[0]).toMatchObject({ id: 'b' });
  });

  it('discards the outcome and warns when the lock was lost', async () => {
    const { worker, events } = await build();
    events.completeResult = false;
    events.add(claimed('a'));

    worker.onApplicationBootstrap();
    await until(() => events.completed.length === 1);
    await worker.beforeApplicationShutdown();

    expect(warn).toHaveBeenCalledWith(expect.stringContaining('lock lost'));
  });

  it('hands a patient over when its lease can no longer be renewed', async () => {
    const { worker, events, patients, processor } = await build();
    patients.renewResult = false;
    events.add(claimed('a'));

    worker.onApplicationBootstrap();
    await until(() => patients.released.length >= 1);
    await worker.beforeApplicationShutdown();

    expect(processor.calls).toEqual([]);
    expect(events.queues.get('p1')).toHaveLength(1);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('lease lost'));
  });

  it('does not run when ROLE is api', async () => {
    const { worker, events, patients } = await build({ ROLE: 'api' });
    events.add(claimed('a'));

    worker.onApplicationBootstrap();
    await sleep(40);
    await worker.beforeApplicationShutdown();

    expect(patients.leases.size).toBe(0);
    expect(events.queues.get('p1')).toHaveLength(1);
  });

  it('on shutdown finishes the event in flight but starts no more', async () => {
    const { worker, events, patients, processor } = await build();
    const gate = deferred();
    processor.behaviour = () => gate.promise;
    events.add(claimed('a', 'p1', 1), claimed('b', 'p1', 2));

    worker.onApplicationBootstrap();
    await until(() => processor.calls.length === 1);

    let stopped = false;
    const shutdown = worker.beforeApplicationShutdown().then(() => {
      stopped = true;
    });
    await sleep(30);
    expect(stopped).toBe(false);

    gate.resolve();
    await shutdown;

    expect(events.completed.map((c) => c.id)).toEqual(['a']);
    expect(events.queues.get('p1')?.map((e) => e._id)).toEqual(['b']);
    expect(patients.leases.size).toBe(0);
  });

  it('gives up draining after the grace period and says so', async () => {
    const { worker, events, processor } = await build({
      SHUTDOWN_GRACE_MS: 50,
    });
    processor.behaviour = () => new Promise(() => {});
    events.add(claimed('a'));

    worker.onApplicationBootstrap();
    await until(() => processor.calls.length === 1);

    const started = Date.now();
    await worker.beforeApplicationShutdown();

    expect(Date.now() - started).toBeLessThan(500);
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining('still in flight'),
    );
  });
});
