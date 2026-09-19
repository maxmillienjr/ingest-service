import {
  Inject,
  Injectable,
  Logger,
  type BeforeApplicationShutdown,
  type OnApplicationBootstrap,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { randomUUID } from 'node:crypto';
import { hostname } from 'node:os';
import type { Env } from '../config/env.schema';
import type { ClaimedEvent } from '../events/event.types';
import { EventsRepository } from '../events/events.repository';
import { EVENT_PROCESSOR, type EventProcessor } from './processor';

/** Idle polling backs off from POLL_IDLE_MS up to this, then stays there. */
const MAX_IDLE_MS = 2_000;

/**
 * Pulls claimable events from the collection and runs them through the
 * processor, up to WORKER_CONCURRENCY at a time. Any number of instances
 * can run against the same database: the claim is atomic and every
 * outcome write is fenced by the lock token the claim handed out.
 */
@Injectable()
export class WorkerService
  implements OnApplicationBootstrap, BeforeApplicationShutdown
{
  readonly workerId = `${hostname()}-${process.pid}-${randomUUID().slice(0, 8)}`;

  private readonly logger = new Logger(WorkerService.name);
  private readonly lanes = new Set<Promise<void>>();
  private running = false;
  private loop: Promise<void> = Promise.resolve();
  private wake: (() => void) | undefined;

  private readonly role: Env['ROLE'];
  private readonly concurrency: number;
  private readonly lockTtlMs: number;
  private readonly pollIdleMs: number;
  private readonly shutdownGraceMs: number;

  constructor(
    private readonly events: EventsRepository,
    @Inject(EVENT_PROCESSOR) private readonly processor: EventProcessor,
    config: ConfigService<Env, true>,
  ) {
    this.role = config.get('ROLE', { infer: true });
    this.concurrency = config.get('WORKER_CONCURRENCY', { infer: true });
    this.lockTtlMs = config.get('LEASE_TTL_MS', { infer: true });
    this.pollIdleMs = config.get('POLL_IDLE_MS', { infer: true });
    this.shutdownGraceMs = config.get('SHUTDOWN_GRACE_MS', { infer: true });
  }

  onApplicationBootstrap(): void {
    if (this.role === 'api') return;
    this.running = true;
    this.loop = this.run();
    this.logger.log(
      `worker ${this.workerId} started, concurrency=${this.concurrency}`,
    );
  }

  /**
   * Runs before connections close: stop claiming, let in-flight lanes
   * finish inside the grace period. Anything still running after that keeps
   * its lock until it expires, and another worker picks it up.
   */
  async beforeApplicationShutdown(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    this.wake?.();
    await this.loop;

    const inFlight = this.lanes.size;
    const drained = await this.drain(this.shutdownGraceMs);
    if (drained) {
      this.logger.log(`worker stopped, drained ${inFlight} in-flight event(s)`);
    } else {
      this.logger.warn(
        `worker stopped with ${this.lanes.size} event(s) still in flight after ${this.shutdownGraceMs}ms; their locks will expire and be retried`,
      );
    }
  }

  private async run(): Promise<void> {
    let idleRounds = 0;
    while (this.running) {
      if (this.lanes.size >= this.concurrency) {
        await Promise.race(this.lanes);
        continue;
      }
      const event = await this.claim();
      if (event) {
        idleRounds = 0;
        this.startLane(event);
        continue;
      }
      idleRounds += 1;
      await this.idle(
        Math.min(this.pollIdleMs * 2 ** (idleRounds - 1), MAX_IDLE_MS),
      );
    }
  }

  private async claim(): Promise<ClaimedEvent | null> {
    try {
      return await this.events.claimNext(this.workerId, this.lockTtlMs);
    } catch (err) {
      this.logger.error(`claim failed: ${message(err)}`);
      return null;
    }
  }

  private startLane(event: ClaimedEvent): void {
    const lane = this.handle(event).finally(() => this.lanes.delete(lane));
    this.lanes.add(lane);
  }

  /** Never throws: a rejected lane would take the loop down with it. */
  private async handle(event: ClaimedEvent): Promise<void> {
    try {
      const result = await this.processor.process(event);
      const recorded = await this.events.complete(
        event._id,
        event.lockToken,
        result,
      );
      if (!recorded) {
        this.logger.warn(
          `event ${event._id}: lock lost before completion, outcome discarded`,
        );
      }
    } catch (err) {
      this.logger.error(
        `event ${event._id}: attempt ${event.attempts} failed: ${message(err)}`,
      );
      await this.events
        .fail(event._id, event.lockToken, message(err))
        .catch((e: unknown) =>
          this.logger.error(
            `event ${event._id}: could not record failure: ${message(e)}`,
          ),
        );
    }
  }

  /** Sleeps unless shutdown wakes it first. */
  private idle(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(done, ms);
      this.wake = done;
      function done(): void {
        clearTimeout(timer);
        resolve();
      }
    });
  }

  private async drain(graceMs: number): Promise<boolean> {
    if (this.lanes.size === 0) return true;
    let timer: NodeJS.Timeout | undefined;
    const expired = new Promise<false>((resolve) => {
      timer = setTimeout(() => resolve(false), graceMs);
    });
    try {
      return await Promise.race([
        Promise.all(this.lanes).then(() => true as const),
        expired,
      ]);
    } finally {
      clearTimeout(timer);
    }
  }
}

function message(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
