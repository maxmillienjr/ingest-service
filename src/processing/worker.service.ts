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
import { PatientsRepository } from '../patients/patients.repository';
import { EVENT_PROCESSOR, type EventProcessor } from './processor';

/** Idle polling backs off from POLL_IDLE_MS up to this, then stays there. */
const MAX_IDLE_MS = 2_000;

/**
 * Works patients, not events. A lane leases one patient and applies that
 * patient's events one at a time in `ts` order until none are ready, then
 * lets the lease go. Up to WORKER_CONCURRENCY lanes run at once, and any
 * number of instances can run against the same database: the lease keeps
 * two of them off one patient, and the lock token on each event keeps a
 * worker that lost its lock from writing the outcome.
 */
@Injectable()
export class WorkerService
  implements OnApplicationBootstrap, BeforeApplicationShutdown
{
  readonly workerId = `${hostname()}-${process.pid}-${randomUUID().slice(0, 8)}`;

  private readonly logger = new Logger(WorkerService.name);
  /** patientId -> the lane draining it. */
  private readonly lanes = new Map<string, Promise<void>>();
  private running = false;
  private loop: Promise<void> = Promise.resolve();
  private wake: (() => void) | undefined;

  private readonly role: Env['ROLE'];
  private readonly concurrency: number;
  private readonly leaseTtlMs: number;
  private readonly pollIdleMs: number;
  private readonly shutdownGraceMs: number;

  constructor(
    private readonly events: EventsRepository,
    private readonly patients: PatientsRepository,
    @Inject(EVENT_PROCESSOR) private readonly processor: EventProcessor,
    config: ConfigService<Env, true>,
  ) {
    this.role = config.get('ROLE', { infer: true });
    this.concurrency = config.get('WORKER_CONCURRENCY', { infer: true });
    this.leaseTtlMs = config.get('LEASE_TTL_MS', { infer: true });
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
   * Runs before connections close: stop claiming, let each lane finish the
   * event it is on, inside the grace period. A lane still running after
   * that keeps its locks until they expire, and another worker takes over.
   */
  async beforeApplicationShutdown(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    this.wake?.();
    await this.loop;

    const inFlight = this.lanes.size;
    const drained = await this.drain(this.shutdownGraceMs);
    if (drained) {
      this.logger.log(`worker stopped, drained ${inFlight} in-flight lane(s)`);
    } else {
      this.logger.warn(
        `worker stopped with ${this.lanes.size} lane(s) still in flight after ${this.shutdownGraceMs}ms; their locks will expire and be retried`,
      );
    }
  }

  private async run(): Promise<void> {
    let idleRounds = 0;
    while (this.running) {
      const free = this.concurrency - this.lanes.size;
      if (free <= 0) {
        await Promise.race(this.lanes.values());
        continue;
      }
      if ((await this.claimPatients(free)) > 0) {
        idleRounds = 0;
        continue;
      }
      idleRounds += 1;
      await this.idle(
        Math.min(this.pollIdleMs * 2 ** (idleRounds - 1), MAX_IDLE_MS),
      );
    }
  }

  /** Leases up to `limit` patients that have ready work and starts a lane for each. */
  private async claimPatients(limit: number): Promise<number> {
    let started = 0;
    try {
      // Over-fetch: some candidates are already ours or leased elsewhere.
      const candidates = await this.events.claimablePatients(limit * 2);
      for (const patientId of candidates) {
        if (started >= limit || !this.running) break;
        if (this.lanes.has(patientId)) continue;
        const leased = await this.patients.tryLease(
          patientId,
          this.workerId,
          this.leaseTtlMs,
        );
        if (leased) {
          this.startLane(patientId);
          started += 1;
        }
      }
    } catch (err) {
      this.logger.error(`claim failed: ${message(err)}`);
    }
    return started;
  }

  private startLane(patientId: string): void {
    const lane = this.drainPatient(patientId).finally(() =>
      this.lanes.delete(patientId),
    );
    this.lanes.set(patientId, lane);
  }

  /**
   * Applies one patient's ready events in order, one at a time. Stops when
   * none are ready, when shutdown begins, or when the lease is lost to
   * another worker. Never throws: a rejected lane would take the loop down.
   */
  private async drainPatient(patientId: string): Promise<void> {
    try {
      while (this.running) {
        const held = await this.patients.renewLease(
          patientId,
          this.workerId,
          this.leaseTtlMs,
        );
        if (!held) {
          this.logger.warn(`patient ${patientId}: lease lost, handing over`);
          break;
        }
        const event = await this.events.claimHead(
          patientId,
          this.workerId,
          this.leaseTtlMs,
        );
        if (!event) break;
        await this.handle(event);
      }
    } catch (err) {
      this.logger.error(`patient ${patientId}: lane aborted: ${message(err)}`);
    } finally {
      await this.patients
        .releaseLease(patientId, this.workerId)
        .catch((err: unknown) =>
          this.logger.error(
            `patient ${patientId}: could not release lease: ${message(err)}`,
          ),
        );
    }
  }

  private async handle(event: ClaimedEvent): Promise<void> {
    try {
      const result = await this.processor.process(event);
      const watermark = await this.patients.advanceWatermark(
        event.patientId,
        event.ts,
      );
      const outOfOrder =
        watermark !== null && event.ts.getTime() < watermark.getTime();
      const recorded = await this.events.complete(event._id, event.lockToken, {
        result,
        outOfOrder,
      });
      if (!recorded) {
        this.logger.warn(
          `event ${event._id}: lock lost before completion, outcome discarded`,
        );
      } else if (outOfOrder) {
        this.logger.warn(
          `event ${event._id}: applied out of order for patient ${event.patientId} (ts ${event.ts.toISOString()} is behind ${watermark.toISOString()})`,
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
        Promise.all(this.lanes.values()).then(() => true as const),
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
