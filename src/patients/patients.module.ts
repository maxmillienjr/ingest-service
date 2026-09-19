import { Module } from '@nestjs/common';
import { MongoModule } from '../mongo/mongo.module';
import { PatientsRepository } from './patients.repository';

@Module({
  imports: [MongoModule],
  providers: [PatientsRepository],
  exports: [PatientsRepository],
})
export class PatientsModule {}
