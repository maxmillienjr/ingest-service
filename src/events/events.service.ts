import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import type { Env } from '../config/env.schema';
import { canonicalEventId } from './canonical-id';
import type { CreateEventDto } from './dto/create-event.dto';
import type { EventDoc, EventReceipt } from './event.types';
import { EventsRepository } from './events.repository';

@Injectable()
export class EventsService {
  private readonly logger = new Logger(EventsService.name);

  constructor(
    private readonly events: EventsRepository,
    private readonly config: ConfigService<Env, true>,
  ) {}

  /**
   * Persist first, answer second. The sender gets the same receipt whether
   * this is the first copy of the event or a retry, so it never has to branch.
   */
  async ingest(dto: CreateEventDto, now = new Date()): Promise<EventReceipt> {
    const ts = new Date(dto.ts);
    const identity = {
      patientId: dto.patientId,
      type: dto.type,
      ts,
      data: dto.data,
    };
    const grace = this.config.get('ORDERING_GRACE_MS', { infer: true });

    const { event, created } = await this.events.insertIfAbsent({
      _id: canonicalEventId(identity),
      ...identity,
      receivedAt: now,
      status: 'pending',
      notBefore: new Date(now.getTime() + grace),
      attempts: 0,
    });

    if (!created) {
      this.logger.debug(
        `duplicate event ${event._id} (status=${event.status})`,
      );
    }
    return { id: event._id, status: event.status };
  }

  findById(id: string): Promise<EventDoc | null> {
    return this.events.findById(id);
  }
}
