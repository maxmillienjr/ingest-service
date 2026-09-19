import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { Collection, Db } from 'mongodb';
import { randomUUID } from 'node:crypto';
import { isDuplicateKey } from '../mongo/mongo-errors';
import { MONGO_DB } from '../mongo/mongo.tokens';
import type { ClaimedEvent, EventDoc } from './event.types';

export interface Outcome {
  result: unknown;
  outOfOrder: boolean;
}

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
   * Patients that have something claimable, oldest work first. Callers
   * lease a patient before touching its events, so this is only a hint.
   */
  async claimablePatients(limit: number, now = new Date()): Promise<string[]> {
    const docs = await this.events
      .find(
        { status: 'pending', notBefore: { $lte: now } },
        { projection: { patientId: 1 }, sort: { notBefore: 1 }, limit },
      )
      .toArray();
    return [...new Set(docs.map((doc) => doc.patientId))];
  }

  /**
   * Atomically takes one patient's next event in `ts` order (receipt order
   * breaks ties). The lock token it mints is the fence: every later write
   * for this attempt must present it, so a worker that lost its lock
   * cannot overwrite the outcome.
   */
  async claimHead(
    patientId: string,
    workerId: string,
    lockTtlMs: number,
    now = new Date(),
  ): Promise<ClaimedEvent | null> {
    const claimed = await this.events.findOneAndUpdate(
      { patientId, status: 'pending', notBefore: { $lte: now } },
      {
        $set: {
          status: 'processing',
          lockToken: randomUUID(),
          lockedBy: workerId,
          lockedUntil: new Date(now.getTime() + lockTtlMs),
        },
        $inc: { attempts: 1 },
      },
      { sort: { ts: 1, receivedAt: 1 }, returnDocument: 'after' },
    );
    return claimed as ClaimedEvent | null;
  }

  /** Records success, but only for the attempt that still holds the lock. */
  async complete(
    id: string,
    lockToken: string,
    outcome: Outcome,
    now = new Date(),
  ): Promise<boolean> {
    const res = await this.events.updateOne(
      { _id: id, lockToken },
      {
        $set: {
          status: 'done',
          result: outcome.result,
          outOfOrder: outcome.outOfOrder,
          processedAt: now,
        },
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
