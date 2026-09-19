import { Module } from '@nestjs/common';
import { EventsModule } from '../events/events.module';
import { PatientsModule } from '../patients/patients.module';
import { EVENT_PROCESSOR } from './processor';
import { SimulatedProcessor } from './simulated.processor';
import { WorkerService } from './worker.service';

@Module({
  imports: [EventsModule, PatientsModule],
  providers: [
    WorkerService,
    // Swap this binding for a real integration; the worker does not change.
    { provide: EVENT_PROCESSOR, useClass: SimulatedProcessor },
  ],
})
export class WorkerModule {}
