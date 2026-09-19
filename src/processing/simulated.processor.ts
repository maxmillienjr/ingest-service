import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { setTimeout as sleep } from 'node:timers/promises';
import type { Env } from '../config/env.schema';
import type { EventDoc } from '../events/event.types';
import type { EventProcessor } from './processor';

/** Waits PROCESSING_DELAY_MS, then fails with probability FAILURE_RATE. */
@Injectable()
export class SimulatedProcessor implements EventProcessor {
  private readonly delayMs: number;
  private readonly failureRate: number;

  constructor(config: ConfigService<Env, true>) {
    this.delayMs = config.get('PROCESSING_DELAY_MS', { infer: true });
    this.failureRate = config.get('FAILURE_RATE', { infer: true });
  }

  async process(event: EventDoc): Promise<unknown> {
    await sleep(this.delayMs);
    if (Math.random() < this.failureRate) {
      throw new Error(`simulated external failure for event ${event._id}`);
    }
    // The event id is what a real external system would key its own
    // idempotency on. No payload comes back; nothing here is PHI.
    return { eventId: event._id, simulatedDelayMs: this.delayMs };
  }
}
