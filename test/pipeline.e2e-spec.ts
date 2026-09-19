import type { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import type { Server } from 'node:http';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import { startMongoMemory, type MongoMemory } from './support/mongo-memory';

/** Accept over HTTP, watch the worker carry the event to `done`. */
describe('ingest to processed', () => {
  let mongo: MongoMemory;
  let app: INestApplication;
  let server: Server;

  beforeAll(async () => {
    mongo = await startMongoMemory();
    Object.assign(process.env, {
      MONGO_URI: mongo.uri,
      ROLE: 'both',
      PROCESSING_DELAY_MS: '20',
      ORDERING_GRACE_MS: '0',
      POLL_IDLE_MS: '10',
      SHUTDOWN_GRACE_MS: '2000',
    });

    const moduleRef = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleRef.createNestApplication();
    await app.init();
    server = app.getHttpServer() as Server;
  });

  afterAll(async () => {
    await app.close();
    await mongo.stop();
  });

  const sleep = (ms: number): Promise<void> =>
    new Promise((resolve) => setTimeout(resolve, ms));

  async function untilDone(id: string): Promise<Record<string, unknown>> {
    for (let i = 0; i < 100; i += 1) {
      const res = await request(server).get(`/events/${id}`);
      const body = res.body as Record<string, unknown>;
      if (body.status === 'done') return body;
      await sleep(20);
    }
    throw new Error(`event ${id} never reached done`);
  }

  it('processes accepted events exactly once and clears the lock', async () => {
    const ids: string[] = [];
    for (const patientId of ['p1', 'p2', 'p3']) {
      const res = await request(server)
        .post('/events')
        .send({
          patientId,
          type: 'vitals',
          data: { hr: 70 },
          ts: '2026-01-01T00:00:00Z',
        })
        .expect(202);
      ids.push((res.body as { id: string }).id);
    }

    for (const id of ids) {
      const done = await untilDone(id);
      expect(done).toMatchObject({ attempts: 1, result: { eventId: id } });
      expect(done).toHaveProperty('processedAt');
      expect(done).not.toHaveProperty('lockToken');
    }
  });
});
