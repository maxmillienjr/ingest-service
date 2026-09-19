import { Controller, Get } from '@nestjs/common';
import {
  HealthCheck,
  HealthCheckService,
  type HealthCheckResult,
} from '@nestjs/terminus';
import { WorkerService } from '../processing/worker.service';
import { BacklogHealthIndicator } from './backlog.health';
import { MongoHealthIndicator } from './mongo.health';

@Controller('health')
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly mongo: MongoHealthIndicator,
    private readonly backlog: BacklogHealthIndicator,
    private readonly worker: WorkerService,
  ) {}

  /** 200 with the details below, or 503 when the database is unreachable. */
  @Get()
  @HealthCheck()
  check(): Promise<HealthCheckResult> {
    return this.health.check([
      () => this.mongo.isHealthy('mongo'),
      () => this.backlog.isHealthy('backlog'),
      () =>
        Promise.resolve({ worker: { status: 'up', ...this.worker.status } }),
    ]);
  }
}
