import type { EventDoc } from '../events/event.types';

export const EVENT_PROCESSOR = Symbol('EVENT_PROCESSOR');

/**
 * The slow external call. The requirements say its logic is irrelevant and
 * only its timing is real, so the production binding is a simulation and
 * tests bind a recording fake. A real integration implements this and
 * nothing else changes.
 */
export interface EventProcessor {
  process(event: EventDoc): Promise<unknown>;
}
