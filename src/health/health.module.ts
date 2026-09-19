import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { EventsModule } from '../events/events.module';
import { MongoModule } from '../mongo/mongo.module';
import { WorkerModule } from '../processing/worker.module';
import { BacklogHealthIndicator } from './backlog.health';
import { HealthController } from './health.controller';
import { MongoHealthIndicator } from './mongo.health';

@Module({
  imports: [
    // Terminus' own error logging says the same thing our indicator reports.
    TerminusModule.forRoot({ logger: false }),
    MongoModule,
    EventsModule,
    WorkerModule,
  ],
  controllers: [HealthController],
  providers: [MongoHealthIndicator, BacklogHealthIndicator],
})
export class HealthModule {}
