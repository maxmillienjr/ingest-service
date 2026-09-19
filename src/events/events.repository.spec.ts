import type { MongoClient } from 'mongodb';
import { createMongoClient } from '../mongo/mongo-client';
import {
  startMongoMemory,
  type MongoMemory,
} from '../../test/support/mongo-memory';
import type { EventDoc } from './event.types';
import { EventsRepository } from './events.repository';

describe('EventsRepository', () => {
  let mongo: MongoMemory;
  let client: MongoClient;
  let repo: EventsRepository;

  beforeAll(async () => {
    mongo = await startMongoMemory();
    client = createMongoClient(mongo.uri);
    await client.connect();
    repo = new EventsRepository(client.db());
    await repo.onModuleInit();
  });

  afterAll(async () => {
    await client.close();
    await mongo.stop();
  });

  beforeEach(async () => {
    await client.db().collection('events').deleteMany({});
  });

  const event = (overrides: Partial<EventDoc> = {}): EventDoc => ({
    _id: 'a'.repeat(64),
    patientId: 'p1',
    type: 'vitals',
    data: { hr: 70 },
    ts: new Date('2026-01-01T00:00:00Z'),
    receivedAt: new Date('2026-01-01T00:00:01Z'),
    status: 'pending',
    notBefore: new Date('2026-01-01T00:00:03Z'),
    attempts: 0,
    ...overrides,
  });

  it('creates the document on first insert', async () => {
    const result = await repo.insertIfAbsent(event());
    expect(result.created).toBe(true);
    expect(await repo.findById(event()._id)).toMatchObject({
      status: 'pending',
    });
  });

  it('returns the stored document, not the new one, on a duplicate', async () => {
    await repo.insertIfAbsent(event());
    await client
      .db()
      .collection<EventDoc>('events')
      .updateOne({ _id: event()._id }, { $set: { status: 'done' } });

    const retry = await repo.insertIfAbsent(event({ attempts: 99 }));

    expect(retry.created).toBe(false);
    expect(retry.event).toMatchObject({ status: 'done', attempts: 0 });
  });

  it('lets exactly one of two concurrent identical inserts create', async () => {
    const results = await Promise.all([
      repo.insertIfAbsent(event()),
      repo.insertIfAbsent(event()),
    ]);

    expect(results.filter((r) => r.created)).toHaveLength(1);
    expect(await client.db().collection('events').countDocuments()).toBe(1);
  });
});
