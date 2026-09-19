import { Inject, Injectable } from '@nestjs/common';
import {
  HealthIndicatorService,
  type HealthIndicatorResult,
} from '@nestjs/terminus';
import type { Db } from 'mongodb';
import { MONGO_DB } from '../mongo/mongo.tokens';

/** Can the process reach its database right now. */
@Injectable()
export class MongoHealthIndicator {
  constructor(
    @Inject(MONGO_DB) private readonly db: Db,
    private readonly indicator: HealthIndicatorService,
  ) {}

  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    const check = this.indicator.check(key);
    try {
      await this.db.command({ ping: 1 });
      return check.up();
    } catch (err) {
      return check.down(err instanceof Error ? err.message : String(err));
    }
  }
}
