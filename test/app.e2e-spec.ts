import type { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import type { Server } from 'node:http';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import { startMongoMemory, type MongoMemory } from './support/mongo-memory';

describe('/events', () => {
  let mongo: MongoMemory;
  let app: INestApplication;
  let server: Server;

  beforeAll(async () => {
    mongo = await startMongoMemory();
    process.env.MONGO_URI = mongo.uri;

    // Built from AppModule, so the global ValidationPipe is the real one.
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

  const valid = (): Record<string, unknown> => ({
    patientId: 'p1',
    type: 'vitals',
    data: { hr: 70 },
    ts: '2026-01-01T00:00:00Z',
  });

  describe('POST', () => {
    it('accepts a valid event with 202 and a receipt', async () => {
      const res = await request(server)
        .post('/events')
        .send(valid())
        .expect(202);
      expect(res.body).toEqual({
        id: expect.stringMatching(/^[0-9a-f]{64}$/) as string,
        status: 'pending',
      });
    });

    it('gives a retried payload the same receipt and stores it once', async () => {
      const first = await request(server)
        .post('/events')
        .send(valid())
        .expect(202);
      const second = await request(server)
        .post('/events')
        .send(valid())
        .expect(202);
      expect(second.body).toEqual(first.body);

      const stored = await request(server)
        .get(`/events/${(first.body as { id: string }).id}`)
        .expect(200);
      expect(stored.body).toMatchObject({ attempts: 0, status: 'pending' });
    });

    it.each([
      ['a missing field', { patientId: 'p1', type: 'vitals', data: {} }],
      ['a malformed timestamp', { ...valid(), ts: 'yesterday' }],
      ['an unknown top-level field', { ...valid(), extra: true }],
      ['non-object data', { ...valid(), data: [1, 2] }],
      ['an empty patientId', { ...valid(), patientId: '' }],
    ])('rejects %s with 400', async (_label, body) => {
      const res = await request(server).post('/events').send(body).expect(400);
      expect(res.body).toMatchObject({ statusCode: 400 });
    });
  });

  describe('GET /:id', () => {
    it('returns the stored event', async () => {
      const receipt = await request(server).post('/events').send(valid());
      const res = await request(server)
        .get(`/events/${(receipt.body as { id: string }).id}`)
        .expect(200);
      expect(res.body).toMatchObject({
        patientId: 'p1',
        type: 'vitals',
        data: { hr: 70 },
        ts: '2026-01-01T00:00:00.000Z',
        status: 'pending',
      });
    });

    it('404s on an unknown id', async () => {
      await request(server)
        .get(`/events/${'0'.repeat(64)}`)
        .expect(404);
    });
  });
});
