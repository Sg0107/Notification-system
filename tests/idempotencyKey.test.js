const { buildIdempotencyKey } = require('../src/services/idempotencyService');

describe('buildIdempotencyKey', () => {
  it('uses the caller-supplied key when present', () => {
    const key = buildIdempotencyKey({
      idempotencyKey: 'order-123-confirmation',
      channel: 'email',
      recipient: 'a@example.com',
      payload: { subject: 'hi' },
    });
    expect(key).toBe('order-123-confirmation');
  });

  it('derives a deterministic key from channel+recipient+payload when absent', () => {
    const args = {
      channel: 'sms',
      recipient: '+15551234567',
      payload: { body: 'Your code is 1234' },
    };
    const key1 = buildIdempotencyKey(args);
    const key2 = buildIdempotencyKey(args);
    expect(key1).toBe(key2);
    expect(key1.startsWith('auto:')).toBe(true);
  });

  it('produces different keys for different payloads', () => {
    const base = { channel: 'sms', recipient: '+15551234567' };
    const key1 = buildIdempotencyKey({ ...base, payload: { body: 'A' } });
    const key2 = buildIdempotencyKey({ ...base, payload: { body: 'B' } });
    expect(key1).not.toBe(key2);
  });
});
