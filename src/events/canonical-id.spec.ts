import { canonicalEventId, stableStringify } from './canonical-id';

describe('stableStringify', () => {
  it('sorts object keys at every depth', () => {
    expect(stableStringify({ b: 1, a: { d: 2, c: 3 } })).toBe(
      '{"a":{"c":3,"d":2},"b":1}',
    );
  });

  it('keeps array order', () => {
    expect(stableStringify([2, 1])).toBe('[2,1]');
  });

  it('omits undefined fields like JSON.stringify does', () => {
    expect(stableStringify({ a: undefined, b: null })).toBe('{"b":null}');
  });
});

describe('canonicalEventId', () => {
  const base = {
    patientId: 'p1',
    type: 'vitals',
    ts: new Date('2026-01-01T00:00:00Z'),
    data: { hr: 70, bp: { sys: 120, dia: 80 } },
  };

  it('is a 64-char hex sha256', () => {
    expect(canonicalEventId(base)).toMatch(/^[0-9a-f]{64}$/);
  });

  it('ignores key order in data', () => {
    const reordered = { ...base, data: { bp: { dia: 80, sys: 120 }, hr: 70 } };
    expect(canonicalEventId(reordered)).toBe(canonicalEventId(base));
  });

  it('treats different spellings of one instant as the same event', () => {
    const plusZero = { ...base, ts: new Date('2026-01-01T00:00:00.000+00:00') };
    expect(canonicalEventId(plusZero)).toBe(canonicalEventId(base));
  });

  it('changes when any identity field changes', () => {
    const id = canonicalEventId(base);
    expect(canonicalEventId({ ...base, patientId: 'p2' })).not.toBe(id);
    expect(canonicalEventId({ ...base, type: 'labs' })).not.toBe(id);
    expect(
      canonicalEventId({ ...base, ts: new Date(base.ts.getTime() + 1) }),
    ).not.toBe(id);
    expect(
      canonicalEventId({ ...base, data: { ...base.data, hr: 71 } }),
    ).not.toBe(id);
  });
});
