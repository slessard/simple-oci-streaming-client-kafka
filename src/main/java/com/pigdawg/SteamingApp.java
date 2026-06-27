package com.pigdawg;

import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.model.StreamPool;
import com.oracle.bmc.streaming.responses.CreateStreamPoolResponse;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SteamingApp {
    private static final Logger LOG = LoggerFactory.getLogger(SteamingApp.class);

    private static final Properties APP_PROPERTIES = ConfigLoader.loadApplicationProperties(SteamingApp.class);

    private static final String ENDPOINT_OCI_STREAMING = ConfigLoader.getRequiredProperty(APP_PROPERTIES, "oci.streaming.endpoint");
    private static final String ENDPOINT_OCI_IDENTITY = ConfigLoader.getRequiredProperty(APP_PROPERTIES, "oci.identity.endpoint");
    private static final String TENANT_STREAMING = ConfigLoader.getRequiredProperty(APP_PROPERTIES, "oci.streaming.tenancy");
    private static final String COMPARTMENT = ConfigLoader.getRequiredProperty(APP_PROPERTIES, "oci.streaming.compartment");
    private static final String AUTH_PROFILE = ConfigLoader.getRequiredProperty(APP_PROPERTIES, "oci.auth.profile");
    private static final String KAFKA_USERNAME_BASE = ConfigLoader.getRequiredProperty(APP_PROPERTIES, "oci.kafka.username.base");
    private static final String KAFKA_PASSWORD = ConfigLoader.getRequiredProperty(APP_PROPERTIES, "oci.kafka.password");
    private static final String KAFKA_TRUSTSTORE_LOCATION = APP_PROPERTIES.getProperty("oci.kafka.truststore.location");
    private static final String KAFKA_TRUSTSTORE_PASSWORD = APP_PROPERTIES.getProperty("oci.kafka.truststore.password");

    private static final long RESOURCE_CREATE_TIMEOUT_MS = Duration.ofMinutes(5).toMillis();
    private static final long RESOURCE_DELETE_TIMEOUT_MS = Duration.ofMinutes(5).toMillis();

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        LOG.info("SteamingApp starting");
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
        final String nowString = now.format(formatter);

        final String poolName = String.format("kafka-pool-%s", nowString);
        final String streamName = String.format("kafka-stream-%s", nowString);
        final String consumerGroupName = String.format("kafka-consumergroup-%s", nowString);
        LOG.info("Generated names pool={} stream={} consumerGroup={}", poolName, streamName, consumerGroupName);

        OciStreamingHelper.validateAuthenticationToken(ENDPOINT_OCI_IDENTITY, AUTH_PROFILE, TENANT_STREAMING);

        StreamAdminClient adminClient = OciStreamingHelper.createStreamAdminClient(ENDPOINT_OCI_STREAMING, AUTH_PROFILE);
        LOG.info("Initialized StreamAdminClient for endpoint={}", ENDPOINT_OCI_STREAMING);

        CreateStreamPoolResponse createStreamPoolResponse = null;
        String streamId = null;
        Exception primaryFailure = null;
        try {
            LOG.info("Creating stream pool {} in compartment {}", poolName, COMPARTMENT);
            createStreamPoolResponse = OciStreamingHelper.createStreamPool(adminClient, COMPARTMENT, poolName);
            String streamPoolId = createStreamPoolResponse.getStreamPool().getId();
            LOG.info("Created stream pool id={}", streamPoolId);
            String kafkaUsername = KafkaStreamingHelper.createStreamPoolUsername(KAFKA_USERNAME_BASE, streamPoolId);

            OciStreamingHelper.waitForStreamPoolToBecomeActive(adminClient, streamPoolId, RESOURCE_CREATE_TIMEOUT_MS);

            StreamPool streamPool = OciStreamingHelper.getStreamPool(adminClient, streamPoolId).getStreamPool();
            String bootstrapServers = streamPool.getKafkaSettings() == null
                    ? null
                    : streamPool.getKafkaSettings().getBootstrapServers();
            if (bootstrapServers == null || bootstrapServers.isBlank()) {
                throw new IllegalStateException("Kafka bootstrap servers were not available for streamPoolId=" + streamPoolId);
            }
            LOG.info("Kafka bootstrap servers resolved for streamPoolId={}", streamPoolId);

            streamId = createKafkaTopicAndResolveStreamId(
                    adminClient,
                    streamPoolId,
                    bootstrapServers,
                    streamName,
                    kafkaUsername);
            LOG.info("Created Kafka topic as stream id={} in pool={}", streamId, streamPoolId);

            OciStreamingHelper.waitForStreamToBecomeActive(adminClient, streamId, RESOURCE_CREATE_TIMEOUT_MS);

            final long startMs = System.currentTimeMillis();
            final long producerDeadlineMs = startMs + 10_000;
            final long consumerDeadlineMs = startMs + 15_000;

            ProducerConsumerThreads producerConsumerThreads =
                    new ProducerConsumerThreads(
                            bootstrapServers,
                            streamName,
                            consumerGroupName,
                            kafkaUsername,
                            KAFKA_PASSWORD,
                            KAFKA_TRUSTSTORE_LOCATION,
                            KAFKA_TRUSTSTORE_PASSWORD,
                            producerDeadlineMs,
                            consumerDeadlineMs);
            Thread producerThread = producerConsumerThreads.createProducerThread();
            Thread consumerThread = producerConsumerThreads.createConsumerThread();

            LOG.info("Starting Kafka producer and consumer threads for topic={}", streamName);
            consumerThread.start();
            producerThread.start();
            producerThread.join();
            consumerThread.join();
            producerConsumerThreads.throwIfFailed();
            LOG.info(
                    "Threads completed. produced={} consumed={}",
                    producerConsumerThreads.getProducedCount(),
                    producerConsumerThreads.getConsumedCount());
        } catch (RuntimeException | InterruptedException | ExecutionException ex) {
            primaryFailure = ex;
            throw ex;
        } finally {
            cleanupResources(adminClient, createStreamPoolResponse, streamId, streamName, primaryFailure);
        }

        LOG.info("SteamingApp done");
    }

    private static String createKafkaTopicAndResolveStreamId(
            StreamAdminClient adminClient,
            String streamPoolId,
            String bootstrapServers,
            String streamName,
            String kafkaUsername)
            throws ExecutionException, InterruptedException {
        LOG.info("Creating Kafka topic={} in streamPoolId={}", streamName, streamPoolId);
        try {
            KafkaStreamingHelper.createTopic(
                    bootstrapServers,
                    streamName,
                    kafkaUsername,
                    KAFKA_PASSWORD,
                    KAFKA_TRUSTSTORE_LOCATION,
                    KAFKA_TRUSTSTORE_PASSWORD,
                    1);
        } catch (ExecutionException ex) {
            Optional<String> recoveredStreamId = OciStreamingHelper.findStreamInPoolByName(
                            adminClient,
                            COMPARTMENT,
                            streamPoolId,
                            streamName)
                    .map(streamSummary -> streamSummary.getId());
            if (recoveredStreamId.isEmpty()) {
                throw ex;
            }

            LOG.warn(
                    "Kafka topic creation reported an error, but the stream exists. streamPoolId={} streamName={} streamId={}",
                    streamPoolId,
                    streamName,
                    recoveredStreamId.get(),
                    ex);
        }

        return OciStreamingHelper.waitForStreamInPoolByName(
                    adminClient,
                    COMPARTMENT,
                    streamPoolId,
                    streamName,
                    RESOURCE_CREATE_TIMEOUT_MS);
    }

    private static void cleanupResources(
            StreamAdminClient adminClient,
            CreateStreamPoolResponse createStreamPoolResponse,
            String streamId,
            String streamName,
            Exception primaryFailure)
            throws InterruptedException {
        Exception cleanupFailure = null;
        String streamPoolId = createStreamPoolResponse == null ? null : createStreamPoolResponse.getStreamPool().getId();
        String streamIdToDelete = resolveStreamIdForCleanup(adminClient, streamPoolId, streamId, streamName);
        boolean streamDeleted = streamIdToDelete == null;

        if (streamIdToDelete != null) {
            try {
                OciStreamingHelper.waitForStreamToBecomeActive(adminClient, streamIdToDelete, RESOURCE_DELETE_TIMEOUT_MS);
                LOG.info("Deleting stream id={}", streamIdToDelete);
                OciStreamingHelper.deleteStream(adminClient, streamIdToDelete);
                OciStreamingHelper.waitForStreamToBecomeDeleted(adminClient, streamIdToDelete, RESOURCE_DELETE_TIMEOUT_MS);
                streamDeleted = true;
            } catch (RuntimeException | InterruptedException ex) {
                cleanupFailure = rememberCleanupFailure(primaryFailure, cleanupFailure, ex);
                LOG.warn("Stream deletion was not confirmed. streamId={}", streamIdToDelete, ex);
            }
        }

        if (createStreamPoolResponse != null) {
            if (streamDeleted) {
                try {
                    LOG.info("Deleting stream pool id={}", streamPoolId);
                    OciStreamingHelper.deleteStreamPoolWhenEmpty(adminClient, streamPoolId, RESOURCE_DELETE_TIMEOUT_MS);
                    OciStreamingHelper.waitForStreamPoolToBecomeDeleted(adminClient, streamPoolId, RESOURCE_DELETE_TIMEOUT_MS);
                } catch (RuntimeException | InterruptedException ex) {
                    cleanupFailure = rememberCleanupFailure(primaryFailure, cleanupFailure, ex);
                    LOG.warn("Stream pool deletion failed. streamPoolId={}", streamPoolId, ex);
                }
            } else {
                LOG.warn("Skipping stream pool deletion. streamPoolId={} reason=stream deletion was not confirmed", streamPoolId);
            }
        }

        throwCleanupFailureIfNoPrimaryFailure(primaryFailure, cleanupFailure);
    }

    private static String resolveStreamIdForCleanup(
            StreamAdminClient adminClient,
            String streamPoolId,
            String streamId,
            String streamName) {
        if (streamId != null || streamPoolId == null) {
            return streamId;
        }

        return OciStreamingHelper.findStreamInPoolByName(adminClient, COMPARTMENT, streamPoolId, streamName)
                .map(streamSummary -> {
                    LOG.info("Resolved Kafka-created stream for cleanup. streamPoolId={} streamName={} streamId={}",
                            streamPoolId, streamName, streamSummary.getId());
                    return streamSummary.getId();
                })
                .orElse(null);
    }

    private static Exception rememberCleanupFailure(
            Exception primaryFailure,
            Exception existingCleanupFailure,
            Exception cleanupFailure) {
        if (cleanupFailure instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }

        if (primaryFailure != null) {
            primaryFailure.addSuppressed(cleanupFailure);
            return existingCleanupFailure;
        }

        if (existingCleanupFailure != null) {
            existingCleanupFailure.addSuppressed(cleanupFailure);
            return existingCleanupFailure;
        }

        return cleanupFailure;
    }

    private static void throwCleanupFailureIfNoPrimaryFailure(
            Exception primaryFailure,
            Exception cleanupFailure)
            throws InterruptedException {
        if (primaryFailure != null || cleanupFailure == null) {
            return;
        }

        if (cleanupFailure instanceof InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw interruptedException;
        }

        if (cleanupFailure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }

        throw new IllegalStateException("Cleanup failed", cleanupFailure);
    }
}
