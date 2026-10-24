package com.pigdawg;

import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.model.StreamPool;
import com.oracle.bmc.streaming.responses.CreateStreamPoolResponse;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SteamingApp {
    private static final Logger LOG = LoggerFactory.getLogger(SteamingApp.class);

    private static final Properties APP_PROPERTIES = ConfigLoader.loadApplicationProperties(SteamingApp.class);

    private static final String ENDPOINT_OCI_STREAMING = ConfigLoader.getRequiredProperty(APP_PROPERTIES, "oci.streaming.endpoint");
    private static final String ENDPOINT_OCI_STREAMING_ADMIN = ConfigLoader.getAdminEndpoint(APP_PROPERTIES);
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
    private static final int MESSAGE_COUNT = 10;
    private static final long CONSUMER_TIMEOUT_MS = Duration.ofMinutes(2).toMillis();

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        try {
            LOG.info("SteamingApp starting");
            LocalDateTime now = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
            final String runSuffix = now.format(formatter) + "-" + UUID.randomUUID().toString().substring(0, 8);

            final String poolName = String.format("kafka-pool-%s", runSuffix);
            final String streamName = String.format("kafka-stream-%s", runSuffix);
            final String consumerGroupName = String.format("kafka-consumergroup-%s", runSuffix);
            LOG.info("Generated names pool={} stream={} consumerGroup={}", poolName, streamName, consumerGroupName);

            OciStreamingHelper.validateAuthenticationToken(ENDPOINT_OCI_IDENTITY, AUTH_PROFILE, TENANT_STREAMING);

            StreamAdminClient adminClient = OciStreamingHelper.createStreamAdminClient(ENDPOINT_OCI_STREAMING_ADMIN, AUTH_PROFILE);
            LOG.info("Initialized StreamAdminClient for endpoint={}", ENDPOINT_OCI_STREAMING_ADMIN);

            String streamPoolId = null;
            String streamId = null;
            String streamNameForCleanup = null;
            Exception primaryFailure = null;
            try {
                LOG.info("Creating stream pool {} in compartment {}", poolName, COMPARTMENT);
                CreateStreamPoolResponse createStreamPoolResponse =
                        OciStreamingHelper.createStreamPool(adminClient, COMPARTMENT, poolName);
                streamPoolId = createStreamPoolResponse.getStreamPool().getId();
                LOG.info("Created stream pool id={}", streamPoolId);

                OciStreamingHelper.waitForStreamPoolToBecomeActive(adminClient, streamPoolId, RESOURCE_CREATE_TIMEOUT_MS);

                String kafkaUsername = KafkaStreamingHelper.createStreamPoolUsername(KAFKA_USERNAME_BASE, streamPoolId);

                StreamPool streamPool = OciStreamingHelper.getStreamPool(adminClient, streamPoolId).getStreamPool();
                String bootstrapServers = streamPool.getKafkaSettings() == null
                        ? null
                        : streamPool.getKafkaSettings().getBootstrapServers();
                if (bootstrapServers == null || bootstrapServers.isBlank()) {
                    throw new IllegalStateException("Kafka bootstrap servers were not available for streamPoolId=" + streamPoolId);
                }
                LOG.info("Kafka bootstrap servers resolved for streamPoolId={}", streamPoolId);

                streamNameForCleanup = streamName;
                streamId = createKafkaTopicAndResolveStreamId(
                        adminClient,
                        streamPoolId,
                        bootstrapServers,
                        streamName,
                        kafkaUsername);
                LOG.info("Created Kafka topic as stream id={} in pool={}", streamId, streamPoolId);

                OciStreamingHelper.waitForStreamToBecomeActive(adminClient, streamId, RESOURCE_CREATE_TIMEOUT_MS);

                final long startMs = System.currentTimeMillis();
                final long consumerDeadlineMs = startMs + CONSUMER_TIMEOUT_MS;

                ProducerConsumerThreads producerConsumerThreads =
                        new ProducerConsumerThreads(
                                bootstrapServers,
                                streamName,
                                consumerGroupName,
                                kafkaUsername,
                                KAFKA_PASSWORD,
                                KAFKA_TRUSTSTORE_LOCATION,
                                KAFKA_TRUSTSTORE_PASSWORD,
                                MESSAGE_COUNT,
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
                cleanupResources(adminClient, streamPoolId, streamId, streamNameForCleanup, primaryFailure);
            }
        } finally {
            LOG.info("SteamingApp done");
        }
    }

    private static void cleanupResources(
            StreamAdminClient adminClient,
            String streamPoolId,
            String streamId,
            String streamName,
            Exception primaryFailure)
            throws InterruptedException {
        Exception cleanupFailure = null;
        boolean streamDeleted = streamId == null;
        try {
            streamId = resolveStreamIdForCleanup(adminClient, streamPoolId, streamId, streamName);
            streamDeleted = streamId == null;
        } catch (RuntimeException | InterruptedException ex) {
            streamDeleted = false;
            cleanupFailure = rememberCleanupFailure(primaryFailure, cleanupFailure, ex);
            LOG.warn("Stream resolution for cleanup failed. streamPoolId={} streamName={}", streamPoolId, streamName, ex);
        }

        if (streamId != null) {
            try {
                OciStreamingHelper.waitForStreamToBecomeActive(adminClient, streamId, RESOURCE_DELETE_TIMEOUT_MS);
                LOG.info("Deleting stream id={}", streamId);
                OciStreamingHelper.deleteStream(adminClient, streamId);
                OciStreamingHelper.waitForStreamToBecomeDeleted(adminClient, streamId, RESOURCE_DELETE_TIMEOUT_MS);
                streamDeleted = true;
            } catch (RuntimeException | InterruptedException ex) {
                cleanupFailure = rememberCleanupFailure(primaryFailure, cleanupFailure, ex);
                LOG.warn("Stream deletion was not confirmed. streamId={}", streamId, ex);
            }
        }

        if (streamPoolId != null) {
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
            return recoverStreamIdAfterTopicCreationError(adminClient, streamPoolId, streamName, ex);
        }

        return OciStreamingHelper.waitForStreamInPoolByName(
                adminClient,
                COMPARTMENT,
                streamPoolId,
                streamName,
                RESOURCE_CREATE_TIMEOUT_MS);
    }

    private static String recoverStreamIdAfterTopicCreationError(
            StreamAdminClient adminClient,
            String streamPoolId,
            String streamName,
            ExecutionException topicCreationFailure)
            throws ExecutionException {
        try {
            String recoveredStreamId = OciStreamingHelper.waitForStreamInPoolByName(
                    adminClient,
                    COMPARTMENT,
                    streamPoolId,
                    streamName,
                    RESOURCE_CREATE_TIMEOUT_MS);
            LOG.warn(
                    "Kafka topic creation reported an error, but the stream exists. streamPoolId={} streamName={} streamId={}",
                    streamPoolId,
                    streamName,
                    recoveredStreamId,
                    topicCreationFailure);
            return recoveredStreamId;
        } catch (RuntimeException recoveryFailure) {
            topicCreationFailure.addSuppressed(recoveryFailure);
            throw topicCreationFailure;
        } catch (InterruptedException recoveryFailure) {
            Thread.currentThread().interrupt();
            topicCreationFailure.addSuppressed(recoveryFailure);
            throw topicCreationFailure;
        }
    }

    private static String resolveStreamIdForCleanup(
            StreamAdminClient adminClient,
            String streamPoolId,
            String streamId,
            String streamName)
            throws InterruptedException {
        if (streamId != null || streamPoolId == null) {
            return streamId;
        }

        if (streamName == null) {
            return null;
        }

        String resolvedStreamId = OciStreamingHelper.waitForStreamInPoolByName(
                adminClient,
                COMPARTMENT,
                streamPoolId,
                streamName,
                RESOURCE_DELETE_TIMEOUT_MS);
        LOG.info("Resolved Kafka-created stream for cleanup. streamPoolId={} streamName={} streamId={}",
                streamPoolId, streamName, resolvedStreamId);
        return resolvedStreamId;
    }
}
