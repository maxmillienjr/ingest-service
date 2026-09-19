import type { MongoClient } from 'mongodb';
import {
  startMongoMemory,
  type MongoMemory,
} from '../../test/support/mongo-memory';
import { createMongoClient } from '../mongo/mongo-client';
import { PatientsRepository } from './patients.repository';

describe('PatientsRepository', () => {
  let mongo: MongoMemory;
  let client: MongoClient;
  let repo: PatientsRepository;

  beforeAll(async () => {
    mongo = await startMongoMemory();
    client = createMongoClient(mongo.uri);
    await client.connect();
    repo = new PatientsRepository(client.db());
  });

  afterAll(async () => {
    await client.close();
    await mongo.stop();
  });

  beforeEach(async () => {
    await client.db().collection('patients').deleteMany({});
  });

  const T0 = new Date('2026-01-01T00:00:00Z');
  const at = (seconds: number): Date => new Date(T0.getTime() + seconds * 1000);

  describe('lease', () => {
    it('is granted to the first worker and refused to the second while live', async () => {
      expect(await repo.tryLease('p1', 'w1', 30_000, at(0))).toBe(true);
      expect(await repo.tryLease('p1', 'w2', 30_000, at(10))).toBe(false);
    });

    it('can be taken over once it has expired', async () => {
      await repo.tryLease('p1', 'w1', 30_000, at(0));
      expect(await repo.tryLease('p1', 'w2', 30_000, at(31))).toBe(true);
      expect(await repo.renewLease('p1', 'w1', 30_000, at(32))).toBe(false);
    });

    it('is granted to exactly one of two workers racing for it', async () => {
      const results = await Promise.all([
        repo.tryLease('p1', 'w1', 30_000, at(0)),
        repo.tryLease('p1', 'w2', 30_000, at(0)),
      ]);
      expect(results.filter(Boolean)).toHaveLength(1);
    });

    it('is renewed only by its owner', async () => {
      await repo.tryLease('p1', 'w1', 30_000, at(0));
      expect(await repo.renewLease('p1', 'w1', 30_000, at(5))).toBe(true);
      expect(await repo.renewLease('p1', 'w2', 30_000, at(5))).toBe(false);
    });

    it('is released only by its owner, and is then free', async () => {
      await repo.tryLease('p1', 'w1', 30_000, at(0));
      await repo.releaseLease('p1', 'w2');
      expect(await repo.tryLease('p1', 'w2', 30_000, at(1))).toBe(false);

      await repo.releaseLease('p1', 'w1');
      expect(await repo.tryLease('p1', 'w2', 30_000, at(2))).toBe(true);
    });
  });

  describe('watermark', () => {
    it('starts empty, then reports the previous value and keeps the max', async () => {
      expect(await repo.advanceWatermark('p1', at(10))).toBeNull();
      expect(await repo.advanceWatermark('p1', at(20))).toEqual(at(10));
      // A late event: watermark reported as 20, and stays at 20.
      expect(await repo.advanceWatermark('p1', at(15))).toEqual(at(20));
      expect(await repo.advanceWatermark('p1', at(21))).toEqual(at(20));
    });

    it('does not disturb a live lease on the same document', async () => {
      await repo.tryLease('p1', 'w1', 30_000, at(0));
      await repo.advanceWatermark('p1', at(10));
      expect(await repo.renewLease('p1', 'w1', 30_000, at(5))).toBe(true);
    });
  });
});
