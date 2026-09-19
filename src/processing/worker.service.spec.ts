import { Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test } from '@nestjs/testing';
import type { Env } from '../config/env.schema';
import type { ClaimedEvent } from '../events/event.types';
import { EventsRepository } from '../events/events.repository';
import { EVENT_PROCESSOR, type EventProcessor } from './processor';
import { WorkerService } from './worker.service';

/** In-memory stand-in for the collection: a queue of claimable events. */
class FakeEvents {
  queue: ClaimedEvent[] = [];
  claims = 0;
  completed: Array<{ id: string; lockToken: string }> = [];
  failed: Array<{ id: string; lockToken: string; error: string }> = [];
  completeResult = true;

  claimNext(): Promise<ClaimedEvent | null> {
    this.claims += 1;
    return Promise.resolve(this.queue.shift() ?? null);
  }
  complete(id: string, lockToken: string): Promise<boolean> {
    this.completed.push({ id, lockToken });
    return Promise.resolve(this.completeResult);
  }
  fail(id: string, lockToken: string, error: string): Promise<boolean> {
    this.failed.push({ id, lockToken, error });
    return Promise.resolve(true);
  }
}

/** Resolves each event through a per-test behaviour, and records the order. */
class FakeProcessor implements EventProcessor {
  calls: string[] = [];
  behaviour: (event: ClaimedEvent) => Promise<unknown> = () =>
    Promise.resolve('ok');

  process(event: ClaimedEvent): Promise<unknown> {
    this.calls.push(event._id);
    return this.behaviour(event);
  }
}

const claimed = (id: string): ClaimedEvent =>
  ({
    _id: id,
    patientId: 'p1',
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
  processor: FakeProcessor;
}> {
  const events = new FakeEvents();
  const processor = new FakeProcessor();
  const values = { ...defaults, ...overrides };
  const moduleRef = await Test.createTestingModule({
    providers: [
      WorkerService,
      { provide: EventsRepository, useValue: events },
      { provide: EVENT_PROCESSOR, useValue: processor },
      {
        provide: ConfigService,
        useValue: { get: (key: keyof Env) => values[key] },
      },
    ],
  }).compile();
  return { worker: moduleRef.get(WorkerService), events, processor };
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

  it('processes claimed events and records each outcome with its lock token', async () => {
    const { worker, events, processor } = await build();
    events.queue.push(claimed('a'), claimed('b'), claimed('c'));

    worker.onApplicationBootstrap();
    await until(() => events.completed.length === 3);
    await worker.beforeApplicationShutdown();

    expect(processor.calls.sort()).toEqual(['a', 'b', 'c']);
    expect(events.completed).toContainEqual({ id: 'b', lockToken: 'token-b' });
  });

  it('runs at most WORKER_CONCURRENCY lanes at once', async () => {
    const { worker, events, processor } = await build({
      WORKER_CONCURRENCY: 2,
    });
    const gate = deferred();
    processor.behaviour = () => gate.promise;
    events.queue.push(claimed('a'), claimed('b'), claimed('c'));

    worker.onApplicationBootstrap();
    await until(() => processor.calls.length === 2);
    await sleep(30);
    expect(processor.calls).toHaveLength(2);

    gate.resolve();
    await until(() => events.completed.length === 3);
    await worker.beforeApplicationShutdown();
  });

  it('records a failed attempt with the lock token and keeps going', async () => {
    const { worker, events, processor } = await build();
    processor.behaviour = (event) =>
      event._id === 'a'
        ? Promise.reject(new Error('external system down'))
        : Promise.resolve('ok');
    events.queue.push(claimed('a'), claimed('b'));

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
  });

  it('discards the outcome and warns when the lock was lost', async () => {
    const { worker, events } = await build();
    events.completeResult = false;
    events.queue.push(claimed('a'));

    worker.onApplicationBootstrap();
    await until(() => events.completed.length === 1);
    await worker.beforeApplicationShutdown();

    expect(warn).toHaveBeenCalledWith(expect.stringContaining('lock lost'));
  });

  it('does not run when ROLE is api', async () => {
    const { worker, events } = await build({ ROLE: 'api' });
    events.queue.push(claimed('a'));

    worker.onApplicationBootstrap();
    await sleep(40);
    await worker.beforeApplicationShutdown();

    expect(events.claims).toBe(0);
  });

  it('on shutdown stops claiming and waits for the in-flight lane', async () => {
    const { worker, events, processor } = await build({
      WORKER_CONCURRENCY: 1,
    });
    const gate = deferred();
    processor.behaviour = () => gate.promise;
    events.queue.push(claimed('a'), claimed('b'));

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
    expect(events.queue.map((e) => e._id)).toEqual(['b']);
  });

  it('gives up draining after the grace period and says so', async () => {
    const { worker, events, processor } = await build({
      SHUTDOWN_GRACE_MS: 50,
    });
    processor.behaviour = () => new Promise(() => {});
    events.queue.push(claimed('a'));

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
