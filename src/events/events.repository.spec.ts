import type { MongoClient } from 'mongodb';
import {
  startMongoMemory,
  type MongoMemory,
} from '../../test/support/mongo-memory';
import { createMongoClient } from '../mongo/mongo-client';
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

  const T0 = new Date('2026-01-01T00:00:00Z');
  const at = (seconds: number): Date => new Date(T0.getTime() + seconds * 1000);

  const event = (overrides: Partial<EventDoc> = {}): EventDoc => ({
    _id: 'a'.repeat(64),
    patientId: 'p1',
    type: 'vitals',
    data: { hr: 70 },
    ts: T0,
    receivedAt: at(1),
    status: 'pending',
    notBefore: at(3),
    attempts: 0,
    ...overrides,
  });

  const stored = (id: string): Promise<EventDoc | null> =>
    client.db().collection<EventDoc>('events').findOne({ _id: id });

  describe('insertIfAbsent', () => {
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

  describe('claimablePatients', () => {
    it('lists distinct patients with ready work, oldest work first', async () => {
      await repo.insertIfAbsent(
        event({ _id: 'b1', patientId: 'pB', notBefore: at(2) }),
      );
      await repo.insertIfAbsent(
        event({ _id: 'a1', patientId: 'pA', notBefore: at(1) }),
      );
      await repo.insertIfAbsent(
        event({ _id: 'a2', patientId: 'pA', notBefore: at(3) }),
      );
      await repo.insertIfAbsent(
        event({ _id: 'c1', patientId: 'pC', notBefore: at(99) }),
      );

      expect(await repo.claimablePatients(10, at(10))).toEqual(['pA', 'pB']);
    });
  });

  describe('claimHead', () => {
    it('takes the patient event with the smallest ts, not the oldest received', async () => {
      await repo.insertIfAbsent(
        event({ _id: 'later', ts: at(20), receivedAt: at(1) }),
      );
      await repo.insertIfAbsent(
        event({ _id: 'earlier', ts: at(10), receivedAt: at(2) }),
      );

      const claimed = await repo.claimHead('p1', 'w1', 30_000, at(10));

      expect(claimed).toMatchObject({
        _id: 'earlier',
        status: 'processing',
        lockedBy: 'w1',
        lockedUntil: at(40),
        attempts: 1,
      });
      expect(claimed?.lockToken).toMatch(/[0-9a-f-]{36}/);
    });

    it('breaks a ts tie by receipt order', async () => {
      await repo.insertIfAbsent(
        event({ _id: 'second', ts: T0, receivedAt: at(2) }),
      );
      await repo.insertIfAbsent(
        event({ _id: 'first', ts: T0, receivedAt: at(1) }),
      );

      expect(await repo.claimHead('p1', 'w1', 30_000, at(10))).toMatchObject({
        _id: 'first',
      });
    });

    it('only touches the named patient', async () => {
      await repo.insertIfAbsent(event({ _id: 'other', patientId: 'p2' }));
      expect(await repo.claimHead('p1', 'w1', 30_000, at(10))).toBeNull();
    });

    it('leaves events whose notBefore has not arrived', async () => {
      await repo.insertIfAbsent(event({ notBefore: at(3) }));
      expect(await repo.claimHead('p1', 'w1', 30_000, at(2))).toBeNull();
    });

    it('hands one event to exactly one of two competing workers', async () => {
      await repo.insertIfAbsent(event());
      const [a, b] = await Promise.all([
        repo.claimHead('p1', 'w1', 30_000, at(10)),
        repo.claimHead('p1', 'w2', 30_000, at(10)),
      ]);
      expect([a, b].filter(Boolean)).toHaveLength(1);
    });
  });

  describe('complete and fail', () => {
    const outcome = { result: { n: 1 }, outOfOrder: false };

    it('records the outcome for the attempt holding the lock and clears the lock', async () => {
      await repo.insertIfAbsent(event());
      const claimed = await repo.claimHead('p1', 'w1', 30_000, at(10));

      const ok = await repo.complete(
        claimed!._id,
        claimed!.lockToken,
        outcome,
        at(15),
      );

      expect(ok).toBe(true);
      const doc = await stored(claimed!._id);
      expect(doc).toMatchObject({
        status: 'done',
        result: { n: 1 },
        outOfOrder: false,
        processedAt: at(15),
      });
      expect(doc).not.toHaveProperty('lockToken');
      expect(doc).not.toHaveProperty('lockedUntil');
    });

    it('refuses an outcome presented with a stale lock token', async () => {
      await repo.insertIfAbsent(event());
      const claimed = await repo.claimHead('p1', 'w1', 30_000, at(10));

      const ok = await repo.complete(claimed!._id, 'not-the-token', outcome);

      expect(ok).toBe(false);
      expect(await stored(claimed!._id)).toMatchObject({
        status: 'processing',
        lockToken: claimed!.lockToken,
      });
    });

    it('records a failure with its message', async () => {
      await repo.insertIfAbsent(event());
      const claimed = await repo.claimHead('p1', 'w1', 30_000, at(10));

      await repo.fail(claimed!._id, claimed!.lockToken, 'boom', at(15));

      expect(await stored(claimed!._id)).toMatchObject({
        status: 'failed',
        error: 'boom',
        processedAt: at(15),
      });
    });
  });
});
