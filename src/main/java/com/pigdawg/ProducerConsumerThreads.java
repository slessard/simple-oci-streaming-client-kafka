package com.pigdawg;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ProducerConsumerThreads {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerConsumerThreads.class);

    private static final Duration CONSUMER_POLL_TIMEOUT = Duration.ofSeconds(1);
    private static final long PRODUCER_SEND_PAUSE_MS = 200;

    private final String bootstrapServers;
    private final String topic;
    private final String consumerGroupId;
    private final String username;
    private final String password;
    private final String truststoreLocation;
    private final String truststorePassword;
    private final int messageCount;
    private final long consumerDeadlineMs;
    private final AtomicInteger producedCount = new AtomicInteger(0);
    private final AtomicInteger consumedCount = new AtomicInteger(0);
    private final AtomicBoolean producerDone = new AtomicBoolean(false);
    private final AtomicReference<Exception> threadFailure = new AtomicReference<>();
    private final Set<String> consumedMessages = ConcurrentHashMap.newKeySet();

    ProducerConsumerThreads(
            String bootstrapServers,
            String topic,
            String consumerGroupId,
            String username,
            String password,
            String truststoreLocation,
            String truststorePassword,
            int messageCount,
            long consumerDeadlineMs) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.consumerGroupId = consumerGroupId;
        this.username = username;
        this.password = password;
        this.truststoreLocation = truststoreLocation;
        this.truststorePassword = truststorePassword;
        this.messageCount = messageCount;
        this.consumerDeadlineMs = consumerDeadlineMs;
    }

    int getProducedCount() {
        return producedCount.get();
    }

    int getConsumedCount() {
        return consumedCount.get();
    }

    void throwIfFailed() {
        Exception failure = threadFailure.get();
        if (failure == null) {
            throwIfMessagesWereNotDrained();
            return;
        }

        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }

        throw new IllegalStateException("Kafka producer/consumer thread failed", failure);
    }

    Thread createProducerThread() {
        return new Thread(() -> {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(
                    KafkaStreamingHelper.createProducerProperties(
                            bootstrapServers,
                            username,
                            password,
                            truststoreLocation,
                            truststorePassword))) {
                LOG.info("Kafka producer thread started for topic={}", topic);
                for (int sequence = 1; sequence <= messageCount; sequence++) {
                    String payload = "message-" + sequence;
                    producer.send(new ProducerRecord<>(topic, "demo-key", payload)).get();
                    producedCount.incrementAndGet();
                    LOG.info("Produced: {}", payload);
                    Thread.sleep(PRODUCER_SEND_PAUSE_MS);
                }
            } catch (Exception ex) {
                rememberThreadFailure(ex);
                LOG.error("Kafka producer thread error for topic={}", topic, ex);
            } finally {
                producerDone.set(true);
                LOG.info("Kafka producer thread exiting. totalProduced={}", producedCount.get());
            }
        }, "kafka-producer");
    }

    Thread createConsumerThread() {
        return new Thread(() -> {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                    KafkaStreamingHelper.createConsumerProperties(
                            bootstrapServers,
                            username,
                            password,
                            truststoreLocation,
                            truststorePassword,
                            consumerGroupId))) {
                LOG.info("Kafka consumer thread started for topic={} groupId={}", topic, consumerGroupId);
                consumer.subscribe(List.of(topic));

                while (System.currentTimeMillis() < consumerDeadlineMs) {
                    ConsumerRecords<String, String> records = consumer.poll(CONSUMER_POLL_TIMEOUT);
                    if (!records.isEmpty()) {
                        LOG.debug("Kafka consumer received {} messages in batch", records.count());
                        for (ConsumerRecord<String, String> record : records) {
                            consumedMessages.add(record.value());
                            int totalConsumed = consumedCount.incrementAndGet();
                            LOG.info("Consumed ({}) : {}", totalConsumed, record.value());
                        }
                    } else {
                        LOG.debug("Kafka consumer poll returned no messages");
                    }

                    if (producerDone.get() && consumedCount.get() >= producedCount.get()) {
                        break;
                    }
                }

                consumer.commitSync();

                if (producerDone.get() && consumedCount.get() < producedCount.get()) {
                    LOG.warn(
                            "Kafka consumer reached timeout before full drain. produced={} consumed={}",
                            producedCount.get(),
                            consumedCount.get());
                }
                LOG.info("Kafka consumer thread exiting. produced={} consumed={}", producedCount.get(), consumedCount.get());
            } catch (Exception ex) {
                rememberThreadFailure(ex);
                LOG.error("Kafka consumer thread error for topic={}", topic, ex);
            }
        }, "kafka-consumer");
    }

    private void rememberThreadFailure(Exception failure) {
        Exception existingFailure = threadFailure.get();
        if (existingFailure != null) {
            existingFailure.addSuppressed(failure);
            return;
        }

        if (!threadFailure.compareAndSet(null, failure)) {
            threadFailure.get().addSuppressed(failure);
        }
    }

    private void throwIfMessagesWereNotDrained() {
        int produced = producedCount.get();
        int consumed = consumedCount.get();
        Set<String> expectedMessages = expectedMessages();
        if (consumed < produced || !consumedMessages.equals(expectedMessages)) {
            throw new IllegalStateException(
                    "Kafka consumer did not drain produced messages. produced=" + produced
                            + " consumed=" + consumed
                            + " expectedMessages=" + expectedMessages
                            + " consumedMessages=" + consumedMessages);
        }
    }

    private Set<String> expectedMessages() {
        return IntStream.rangeClosed(1, messageCount)
                .mapToObj(sequence -> "message-" + sequence)
                .collect(Collectors.toUnmodifiableSet());
    }
}
