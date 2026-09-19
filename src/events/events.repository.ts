import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { Collection, Db, MongoServerError } from 'mongodb';
import { randomUUID } from 'node:crypto';
import { MONGO_DB } from '../mongo/mongo.tokens';
import type { ClaimedEvent, EventDoc } from './event.types';

const DUPLICATE_KEY = 11000;

/** Every query against the events collection lives here. */
@Injectable()
export class EventsRepository implements OnModuleInit {
  private readonly events: Collection<EventDoc>;

  constructor(@Inject(MONGO_DB) db: Db) {
    this.events = db.collection<EventDoc>('events');
  }

  async onModuleInit(): Promise<void> {
    await this.events.createIndexes([
      // Candidate scan: what is claimable right now.
      { key: { status: 1, notBefore: 1 } },
      // Per-patient head: the next event to apply for one patient.
      { key: { patientId: 1, status: 1, ts: 1, receivedAt: 1 } },
    ]);
  }

  /**
   * The insert is the dedupe: `_id` is the content hash, so a retry of the
   * same payload hits the unique index and we hand back what is already there.
   */
  async insertIfAbsent(
    doc: EventDoc,
  ): Promise<{ event: EventDoc; created: boolean }> {
    try {
      await this.events.insertOne(doc);
      return { event: doc, created: true };
    } catch (err) {
      if (!isDuplicateKey(err)) throw err;
      const existing = await this.events.findOne({ _id: doc._id });
      if (!existing) throw err;
      return { event: existing, created: false };
    }
  }

  findById(id: string): Promise<EventDoc | null> {
    return this.events.findOne({ _id: id });
  }

  /**
   * Atomically hands the oldest claimable event to one worker. The lock
   * token it mints is the fence: every later write for this attempt must
   * present it, so a worker that lost its lock cannot overwrite the outcome.
   */
  async claimNext(
    workerId: string,
    lockTtlMs: number,
    now = new Date(),
  ): Promise<ClaimedEvent | null> {
    const claimed = await this.events.findOneAndUpdate(
      { status: 'pending', notBefore: { $lte: now } },
      {
        $set: {
          status: 'processing',
          lockToken: randomUUID(),
          lockedBy: workerId,
          lockedUntil: new Date(now.getTime() + lockTtlMs),
        },
        $inc: { attempts: 1 },
      },
      { sort: { notBefore: 1 }, returnDocument: 'after' },
    );
    return claimed as ClaimedEvent | null;
  }

  /** Records success, but only for the attempt that still holds the lock. */
  async complete(
    id: string,
    lockToken: string,
    result: unknown,
    now = new Date(),
  ): Promise<boolean> {
    const res = await this.events.updateOne(
      { _id: id, lockToken },
      {
        $set: { status: 'done', result, processedAt: now },
        $unset: { lockToken: '', lockedBy: '', lockedUntil: '' },
      },
    );
    return res.matchedCount === 1;
  }

  /** Records a failure, fenced the same way. */
  async fail(
    id: string,
    lockToken: string,
    error: string,
    now = new Date(),
  ): Promise<boolean> {
    const res = await this.events.updateOne(
      { _id: id, lockToken },
      {
        $set: { status: 'failed', error, processedAt: now },
        $unset: { lockToken: '', lockedBy: '', lockedUntil: '' },
      },
    );
    return res.matchedCount === 1;
  }
}

function isDuplicateKey(err: unknown): boolean {
  return err instanceof MongoServerError && err.code === DUPLICATE_KEY;
}
