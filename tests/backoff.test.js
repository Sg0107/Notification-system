const { computeBackoffMs } = require('../src/utils/backoff');

describe('computeBackoffMs', () => {
  it('never exceeds maxBackoffMs', () => {
    for (let attempt = 0; attempt < 20; attempt++) {
      const delay = computeBackoffMs(attempt, 1000, 60000);
      expect(delay).toBeGreaterThanOrEqual(0);
      expect(delay).toBeLessThanOrEqual(60000);
    }
  });

  it('grows the ceiling exponentially before hitting the cap', () => {
    const samples = (attempt) =>
      Array.from({ length: 200 }, () => computeBackoffMs(attempt, 100, 100000));

    const maxAt = (attempt) => Math.max(...samples(attempt));

    expect(maxAt(0)).toBeLessThanOrEqual(100);
    expect(maxAt(3)).toBeLessThanOrEqual(800);
    expect(maxAt(3)).toBeGreaterThan(maxAt(0));
  });
});
