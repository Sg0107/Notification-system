const STATES = {
    CLOSED: 'closed',
    OPEN: 'open',
    HALF_OPEN: 'half-open',
};

// Thrown instead of returned so calling code (makeHandler in baseWorker.js)
// can catch it with the exact same try/catch it already uses for a normal
// provider failure - no special-casing needed on the caller's side.
class CircuitOpenError extends Error {
    constructor(channel) {
        super(`Circuit breaker is open for channel "${channel}" - skipping call`);
        this.name = 'CircuitOpenError';
    }
}

// `failureThreshold` / `breakTimeoutMs` come from config/env.js rather than
// being hardcoded, so they can be tuned per environment without a code
// change (e.g. a lower threshold + shorter cooldown in tests).
function callWithBreaker(channel, fn, { failureThreshold, breakTimeoutMs }) {
    let state = STATES.CLOSED;
    let failures = 0;
    // True while a half-open trial call is in flight. Needed because
    // HALF_OPEN means "let exactly one call through to test the provider" -
    // without this flag, every message that arrives while half-open would
    // fire its own trial call at a still-maybe-dead provider.
    let halfOpenTrialInFlight = false;

    function transitionTo(newState) {
        if (newState === state) return;
        console.warn(`[circuit-breaker:${channel}] ${state} -> ${newState}`);
        state = newState;
    }

    function handleSuccess() {
        failures = 0;
        halfOpenTrialInFlight = false;
        transitionTo(STATES.CLOSED);
    }

    function handleFailure() {
        halfOpenTrialInFlight = false;
        failures++;
        if (state === STATES.HALF_OPEN || failures >= failureThreshold) {
            transitionTo(STATES.OPEN);
            failures = 0;
            setTimeout(() => {
                transitionTo(STATES.HALF_OPEN);
            }, breakTimeoutMs);
        }
    }

    return async (...args) => {
        if (state === STATES.OPEN) {
            throw new CircuitOpenError(channel);
        }

        if (state === STATES.HALF_OPEN) {
            if (halfOpenTrialInFlight) {
                // A trial call is already in flight - don't pile on more
                // calls to a provider we're not sure is healthy yet. Fail
                // fast, same as OPEN.
                throw new CircuitOpenError(channel);
            }
            halfOpenTrialInFlight = true;
        }

        try {
            const result = await fn(...args);
            handleSuccess();
            return result;
        } catch (error) {
            handleFailure();
            throw error;
        }
    };
}

module.exports = { callWithBreaker, CircuitOpenError, STATES };