import { Module, ValidationPipe } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { APP_PIPE } from '@nestjs/core';
import { validateEnv, type Env } from './config/env.schema';
import { EventsModule } from './events/events.module';
import { MongoModule } from './mongo/mongo.module';
import { WorkerModule } from './processing/worker.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      cache: true,
      // `load` runs at module init, `validate` runs when forRoot() is called
      // (at import). Init-time reading lets a test set its environment before
      // building the app; a bad value still fails the boot.
      load: [(): Env => validateEnv(process.env)],
    }),
    MongoModule,
    EventsModule,
    // Always mounted; whether its loop runs is decided by ROLE at bootstrap.
    WorkerModule,
  ],
  providers: [
    {
      // Registered as a provider rather than app.useGlobalPipes() so any
      // test app built from AppModule validates exactly like production.
      provide: APP_PIPE,
      useValue: new ValidationPipe({
        whitelist: true,
        forbidNonWhitelisted: true,
        transform: true,
      }),
    },
  ],
})
export class AppModule {}
