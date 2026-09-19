import { Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { NestFactory } from '@nestjs/core';
import type { NestExpressApplication } from '@nestjs/platform-express';
import helmet from 'helmet';
import { AppModule } from './app.module';
import type { AppConfig } from './config/env.schema';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create<NestExpressApplication>(AppModule);
  app.use(helmet());
  // Events are small; a large body is a mistake or an attack, not a patient.
  app.useBodyParser('json', { limit: '256kb' });
  // Lets SIGTERM/SIGINT reach onApplicationShutdown so in-flight work can drain.
  app.enableShutdownHooks();

  const config = app.get<AppConfig>(ConfigService);
  const port = config.get('PORT', { infer: true });
  await app.listen(port);
  Logger.log(
    `Listening on :${port} as role=${config.get('ROLE', { infer: true })}`,
    'Bootstrap',
  );
}

bootstrap().catch((err: unknown) => {
  Logger.error(err instanceof Error ? err.stack : String(err), 'Bootstrap');
  process.exit(1);
});
