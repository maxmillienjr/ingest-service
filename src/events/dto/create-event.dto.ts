import {
  IsISO8601,
  IsNotEmpty,
  IsObject,
  IsString,
  MaxLength,
} from 'class-validator';
import { IsNotInTheFuture } from './is-not-in-the-future';

/**
 * The wire contract from the requirements. Top-level fields are strict
 * (unknown ones are rejected by the global pipe); `data` is free-form by
 * design, it belongs to the sender.
 */
export class CreateEventDto {
  @IsString()
  @IsNotEmpty()
  @MaxLength(128)
  patientId!: string;

  @IsString()
  @IsNotEmpty()
  @MaxLength(128)
  type!: string;

  @IsObject()
  data!: Record<string, unknown>;

  @IsISO8601({ strict: true })
  @IsNotInTheFuture()
  ts!: string;
}
