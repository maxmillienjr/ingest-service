import { ServiceUnavailableException } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { TerminusModule } from '@nestjs/terminus';
import { EventsRepository } from '../events/events.repository';
import { MONGO_DB } from '../mongo/mongo.tokens';
import { WorkerService } from '../processing/worker.service';
import { BacklogHealthIndicator } from './backlog.health';
import { HealthController } from './health.controller';
import { MongoHealthIndicator } from './mongo.health';

/** The one path the e2e suite cannot reach: the database is gone. */
describe('HealthController', () => {
  it('answers 503 with the failing indicator named when Mongo is unreachable', async () => {
    const down = Promise.reject(new Error('connection refused'));
    down.catch(() => undefined);
    const moduleRef = await Test.createTestingModule({
      imports: [TerminusModule.forRoot({ logger: false })],
      controllers: [HealthController],
      providers: [
        MongoHealthIndicator,
        BacklogHealthIndicator,
        { provide: MONGO_DB, useValue: { command: () => down } },
        { provide: EventsRepository, useValue: { countByStatus: () => down } },
        {
          provide: WorkerService,
          useValue: { status: { role: 'both', workerId: 'w1', lanes: 0 } },
        },
      ],
    }).compile();

    const err: unknown = await moduleRef
      .get(HealthController)
      .check()
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(ServiceUnavailableException);
    expect((err as ServiceUnavailableException).getResponse()).toMatchObject({
      status: 'error',
      error: {
        mongo: { status: 'down', message: 'connection refused' },
        backlog: { status: 'down' },
      },
      info: { worker: { status: 'up', role: 'both' } },
    });
  });
});
