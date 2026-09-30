const { getKafkaClient } = require('../kafka/client');
const { publish } = require('../kafka/producer');
const { topicFor, retryTopicFor, dlqTopicFor } = require('../kafka/topics');
const Notification = require('../models/Notification');
const DeadLetter = require('../models/DeadLetter');
const { tryConsume } = require('../services/rateLimiter');
const { reserveSendAttempt } = require('../services/idempotencyService');
const { withCircuitBreaker } = require('../services/circuitBreaker');
const { computeBackoffMs } = require('../utils/backoff');
const { retry: retryConfig } = require('../config/env');
const { connectMongo } = require('../config/mongo');
const logger = require('../utils/logger');

/**
 * Generic delivery worker shared by email/sms/push. Channel-specific
 * behaviour (which provider to call, provider name for the circuit
 * breaker) is injected by the caller.
 *
 * Responsibilities baked in here so every channel gets them uniformly:
 *  - priority-aware consumption (one consumer per priority topic, with
 *    higher concurrency for higher priority)
 *  - rate limiting per channel before calling the provider
 *  - idempotent send (skip if this exact attempt already ran)
 *  - provider failure isolation via circuit breaker
 *  - exponential backoff retry, routed through a dedicated retry topic
 *  - dead-lettering once max retries are exhausted
 */
function createChannelWorker({ channel, providerName, sendFn, concurrency = {} }) {
  const groupIdBase = `notification-workers-${channel}`;
  const priorities = ['high', 'normal', 'low'];
  const defaultConcurrency = { high: 4, normal: 2, low: 1 };

  async function handleMessage({ message }) {
    const job = JSON.parse(message.value.toString());
    const { notificationId, recipient, payload, attempt = 0 } = job;

    const allowed = await tryConsume(channel);
    if (!allowed) {
      // Over budget right now: park it on the retry topic with a short,
      // fixed delay rather than blocking the partition or dropping work.
      await requeueWithDelay(job, attempt, 250);
      return;
    }

    const canSend = await reserveSendAttempt(notificationId, attempt);
    if (!canSend) {
      logger.warn('Duplicate delivery attempt suppressed', { notificationId, attempt });
      return;
    }

    await Notification.updateOne(
      { notificationId },
      { $set: { status: 'PROCESSING' }, $inc: { attemptCount: 1 } }
    );

    const startedAt = new Date();
    try {
      const result = await withCircuitBreaker(providerName, () =>
        sendFn({ recipient, payload })
      );

      await Notification.updateOne(
        { notificationId },
        {
          $set: { status: 'DELIVERED', deliveredAt: new Date() },
          $push: {
            attempts: {
              attempt,
              startedAt,
              finishedAt: new Date(),
              success: true,
              provider: result.provider,
            },
          },
        }
      );
      logger.info('Notification delivered', { notificationId, channel, attempt });
    } catch (err) {
      await onDeliveryFailure(job, attempt, startedAt, err);
    }
  }

  async function onDeliveryFailure(job, attempt, startedAt, err) {
    const { notificationId } = job;
    const nextAttempt = attempt + 1;
    const isCircuitOpen = err.code === 'CIRCUIT_OPEN';

    await Notification.updateOne(
      { notificationId },
      {
        $set: { status: 'FAILED_RETRYABLE', lastError: err.message },
        $push: {
          attempts: {
            attempt,
            startedAt,
            finishedAt: new Date(),
            success: false,
            error: err.message,
          },
        },
      }
    );

    if (nextAttempt >= retryConfig.maxRetries) {
      await deadLetter(job, err);
      return;
    }

    // Circuit-open failures back off a bit longer since retrying
    // immediately against a known-broken provider wastes capacity.
    const backoffMs = isCircuitOpen
      ? Math.max(retryConfig.baseBackoffMs * 4, computeBackoffMs(nextAttempt, retryConfig.baseBackoffMs, retryConfig.maxBackoffMs))
      : computeBackoffMs(nextAttempt, retryConfig.baseBackoffMs, retryConfig.maxBackoffMs);

    logger.warn('Notification delivery failed, scheduling retry', {
      notificationId,
      channel,
      attempt,
      nextAttempt,
      backoffMs,
      error: err.message,
    });

    await requeueWithDelay(job, nextAttempt, backoffMs);
  }

  async function requeueWithDelay(job, attempt, delayMs) {
    await publish(retryTopicFor(channel), {
      ...job,
      attempt,
      deliverAt: Date.now() + delayMs,
    });
  }

  async function deadLetter(job, err) {
    const { notificationId, payload } = job;
    await Notification.updateOne(
      { notificationId },
      { $set: { status: 'DEAD_LETTERED', deadLetteredAt: new Date(), lastError: err.message } }
    );
    await DeadLetter.create({
      notificationId,
      channel,
      payload,
      reason: 'max_retries_exceeded',
      attemptCount: retryConfig.maxRetries,
      lastError: err.message,
      rawMessage: job,
    });
    await publish(dlqTopicFor(channel), { ...job, finalError: err.message });
    logger.error('Notification dead-lettered', { notificationId, channel, error: err.message });
  }

  async function start() {
    await connectMongo();
    const kafka = getKafkaClient();

    const consumers = await Promise.all(
      priorities.map(async (priority) => {
        const consumer = kafka.consumer({ groupId: `${groupIdBase}-${priority}` });
        await consumer.connect();
        await consumer.subscribe({ topic: topicFor(channel, priority), fromBeginning: false });
        await consumer.run({
          partitionsConsumedConcurrently:
            concurrency[priority] || defaultConcurrency[priority] || 1,
          eachMessage: async (payload) => {
            try {
              await handleMessage(payload);
            } catch (err) {
              logger.error('Unhandled error processing message', {
                channel,
                priority,
                error: err.message,
                stack: err.stack,
              });
            }
          },
        });
        return consumer;
      })
    );

    logger.info(`${channel} worker started`, { priorities });

    const shutdown = async () => {
      logger.info(`Shutting down ${channel} worker`);
      await Promise.all(consumers.map((c) => c.disconnect()));
      process.exit(0);
    };
    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);
  }

  return { start, handleMessage };
}

module.exports = { createChannelWorker };
