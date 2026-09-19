import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { Collection, Db, MongoServerError } from 'mongodb';
import { MONGO_DB } from '../mongo/mongo.tokens';
import type { EventDoc } from './event.types';

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
}

function isDuplicateKey(err: unknown): boolean {
  return err instanceof MongoServerError && err.code === DUPLICATE_KEY;
}
