import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  NotFoundException,
  Param,
  Post,
} from '@nestjs/common';
import { CreateEventDto } from './dto/create-event.dto';
import type { EventDoc, EventReceipt } from './event.types';
import { EventsService } from './events.service';

@Controller('events')
export class EventsController {
  constructor(private readonly events: EventsService) {}

  /** 202: the event is durably accepted; processing happens later. */
  @Post()
  @HttpCode(HttpStatus.ACCEPTED)
  ingest(@Body() dto: CreateEventDto): Promise<EventReceipt> {
    return this.events.ingest(dto);
  }

  @Get(':id')
  async findOne(@Param('id') id: string): Promise<EventDoc> {
    const event = await this.events.findById(id);
    if (!event) throw new NotFoundException(`No event with id ${id}`);
    return event;
  }
}
