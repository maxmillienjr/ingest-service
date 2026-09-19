import type { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import type { Server } from 'node:http';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import { startMongoMemory, type MongoMemory } from './support/mongo-memory';

/** Accept over HTTP, watch the worker carry events to `done`. */
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
      // Long enough for a burst of POSTs to land before any is claimable.
      ORDERING_GRACE_MS: '300',
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

  const post = async (
    patientId: string,
    tsSeconds: number,
  ): Promise<string> => {
    const res = await request(server)
      .post('/events')
      .send({
        patientId,
        type: 'vitals',
        data: { hr: 70 + tsSeconds },
        ts: new Date(tsSeconds * 1000).toISOString(),
      })
      .expect(202);
    return (res.body as { id: string }).id;
  };

  async function untilDone(id: string): Promise<Record<string, unknown>> {
    for (let i = 0; i < 200; i += 1) {
      const res = await request(server).get(`/events/${id}`);
      const body = res.body as Record<string, unknown>;
      if (body.status === 'done') return body;
      await sleep(20);
    }
    throw new Error(`event ${id} never reached done`);
  }

  it('processes accepted events exactly once and clears the lock', async () => {
    const ids = [await post('p1', 0), await post('p2', 0), await post('p3', 0)];

    for (const id of ids) {
      const done = await untilDone(id);
      expect(done).toMatchObject({ attempts: 1, result: { eventId: id } });
      expect(done).toHaveProperty('processedAt');
      expect(done).not.toHaveProperty('lockToken');
    }
  });

  it("applies one patient's events in ts order regardless of arrival order", async () => {
    const arrival = [3, 1, 4, 0, 2];
    const ids = new Map<number, string>();
    for (const ts of arrival) ids.set(ts, await post('p-ordered', ts));

    const processedAt = new Map<number, string>();
    for (const [ts, id] of ids) {
      const done = await untilDone(id);
      expect(done.outOfOrder).toBe(false);
      processedAt.set(ts, done.processedAt as string);
    }

    const byProcessing = [...processedAt].sort(([, a], [, b]) =>
      a.localeCompare(b),
    );
    expect(byProcessing.map(([ts]) => ts)).toEqual([0, 1, 2, 3, 4]);
  });
});
