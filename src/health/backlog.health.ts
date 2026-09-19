import { Injectable } from '@nestjs/common';
import {
  HealthIndicatorService,
  type HealthIndicatorResult,
} from '@nestjs/terminus';
import { EventsRepository } from '../events/events.repository';

/**
 * Events by status. The number to watch is `pending`: it grows when
 * senders outrun the workers and shrinks when they catch up.
 */
@Injectable()
export class BacklogHealthIndicator {
  constructor(
    private readonly events: EventsRepository,
    private readonly indicator: HealthIndicatorService,
  ) {}

  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    const check = this.indicator.check(key);
    try {
      return check.up(await this.events.countByStatus());
    } catch (err) {
      return check.down(err instanceof Error ? err.message : String(err));
    }
  }
}
