/**
 * Traffic generator. Sends events at a fixed rate with exact duplicates
 * mixed in and each patient's sequence lightly shuffled, samples the
 * backlog from /health, waits for the queue to drain, and writes the
 * manifest that `verify` checks.
 *
 *   npm run load -- --rate 1000 --minutes 3 --patients 200 --dup 0.1 --shuffle 5
 */
import { writeFile } from 'node:fs/promises';
import { setTimeout as sleep } from 'node:timers/promises';
import { parseArgs } from 'node:util';
import { DEFAULT_MANIFEST, type Manifest } from './manifest';

const { values } = parseArgs({
  options: {
    url: { type: 'string', default: 'http://localhost:3000' },
    rate: { type: 'string', default: '1000' },
    minutes: { type: 'string', default: '3' },
    patients: { type: 'string', default: '200' },
    dup: { type: 'string', default: '0.1' },
    shuffle: { type: 'string', default: '5' },
    manifest: { type: 'string', default: DEFAULT_MANIFEST },
    'sample-ms': { type: 'string', default: '5000' },
    'drain-minutes': { type: 'string', default: '5' },
  },
});
const args = {
  url: values.url.replace(/\/$/, ''),
  rate: Number(values.rate),
  minutes: Number(values.minutes),
  patients: Number(values.patients),
  dup: Number(values.dup),
  shuffle: Math.max(1, Number(values.shuffle)),
};

interface Payload {
  patientId: string;
  type: string;
  data: { seq: number; hr: number };
  ts: string;
}

/** Fisher-Yates limited to a window: an element moves at most `window - 1` places. */
function windowedShuffle<T>(items: T[], window: number): T[] {
  const out = [...items];
  for (let i = 0; i < out.length; i += 1) {
    const j = i + Math.floor(Math.random() * Math.min(window, out.length - i));
    [out[i], out[j]] = [out[j]!, out[i]!];
  }
  return out;
}

/** Per-patient sequences in ts order, shuffled per patient, then interleaved. */
function plan(total: number): Payload[] {
  const perPatient = Math.ceil(total / args.patients);
  const base = Date.now();
  const lanes: Payload[][] = [];
  for (let p = 0; p < args.patients; p += 1) {
    const seq: Payload[] = [];
    for (let k = 0; k < perPatient; k += 1) {
      seq.push({
        patientId: `patient-${String(p).padStart(4, '0')}`,
        type: 'vitals',
        data: { seq: k, hr: 60 + (k % 40) },
        ts: new Date(base + k * 1000).toISOString(),
      });
    }
    lanes.push(windowedShuffle(seq, args.shuffle));
  }
  const out: Payload[] = [];
  for (let k = 0; k < perPatient; k += 1) {
    for (const lane of lanes) out.push(lane[k]!);
  }
  return out.slice(0, total);
}

function percentile(sorted: number[], p: number): number {
  return (
    sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))] ?? 0
  );
}

async function health(): Promise<{ pending: number; processing: number }> {
  const res = await fetch(`${args.url}/health`);
  const body = (await res.json()) as {
    details: { backlog: { pending: number; processing: number } };
  };
  return body.details.backlog;
}

async function main(): Promise<void> {
  const total = Math.round(args.rate * args.minutes);
  const intervalMs = 60_000 / args.rate;
  const fresh = plan(total);
  const manifest: Manifest = {
    startedAt: new Date().toISOString(),
    finishedAt: '',
    args,
    sent: [],
    duplicates: { sent: 0, mismatched: 0 },
    backlog: [],
  };
  const idOf = new Map<string, string>();
  const latencies: number[] = [];
  let rejected = 0;
  let inFlight: Promise<void>[] = [];

  const send = async (payload: Payload, duplicate: boolean): Promise<void> => {
    const started = performance.now();
    const res = await fetch(`${args.url}/events`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
    });
    latencies.push(performance.now() - started);
    if (res.status !== 202) {
      rejected += 1;
      return;
    }
    const { id } = (await res.json()) as { id: string };
    const key = `${payload.patientId}/${payload.ts}`;
    if (duplicate) {
      manifest.duplicates.sent += 1;
      if (idOf.get(key) !== id) manifest.duplicates.mismatched += 1;
    } else {
      idOf.set(key, id);
      manifest.sent.push({ id, patientId: payload.patientId, ts: payload.ts });
    }
  };

  let sampling = true;
  const sampler = (async (): Promise<void> => {
    while (sampling) {
      try {
        const { pending, processing } = await health();
        manifest.backlog.push({
          at: new Date().toISOString(),
          pending,
          processing,
        });
      } catch {
        // The API being briefly unreachable is itself visible in the sample gap.
      }
      await sleep(Number(values['sample-ms']));
    }
  })();

  console.log(
    `load: ${total} sends at ${args.rate}/min to ${args.url}, ${args.patients} patients, dup ${args.dup}, shuffle ${args.shuffle}`,
  );
  const t0 = performance.now();
  let next = 0;
  let lastReport = t0;
  for (let i = 0; i < total; i += 1) {
    const due = t0 + i * intervalMs;
    const wait = due - performance.now();
    if (wait > 0) await sleep(wait);

    const duplicate = next > 0 && Math.random() < args.dup;
    const payload = duplicate
      ? fresh[Math.max(0, next - 1 - Math.floor(Math.random() * 100))]!
      : fresh[next++]!;
    inFlight.push(send(payload, duplicate).catch(() => void (rejected += 1)));

    if (performance.now() - lastReport > 10_000) {
      lastReport = performance.now();
      const last = manifest.backlog.at(-1);
      console.log(
        `  sent ${i + 1}/${total}, pending ${last?.pending ?? '?'}, processing ${last?.processing ?? '?'}`,
      );
      inFlight = inFlight.filter(() => true);
    }
  }
  await Promise.all(inFlight);

  const sorted = [...latencies].sort((a, b) => a - b);
  console.log(
    `sent ${latencies.length} in ${((performance.now() - t0) / 1000).toFixed(1)}s: ` +
      `${manifest.sent.length} distinct, ${manifest.duplicates.sent} duplicates ` +
      `(${manifest.duplicates.mismatched} got a different id), ${rejected} rejected`,
  );
  console.log(
    `POST /events latency ms: p50 ${percentile(sorted, 0.5).toFixed(1)}, ` +
      `p99 ${percentile(sorted, 0.99).toFixed(1)}, max ${(sorted.at(-1) ?? 0).toFixed(1)}`,
  );

  console.log('waiting for the backlog to drain...');
  const deadline = Date.now() + Number(values['drain-minutes']) * 60_000;
  while (Date.now() < deadline) {
    const { pending, processing } = await health();
    if (pending + processing === 0) break;
    await sleep(1000);
  }
  sampling = false;
  await sampler;

  manifest.finishedAt = new Date().toISOString();
  await writeFile(values.manifest, JSON.stringify(manifest));
  console.log(
    `manifest written to ${values.manifest}; now run: npm run verify`,
  );
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
