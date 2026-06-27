package com.pigdawg;

import com.oracle.bmc.auth.SessionTokenAuthenticationDetailsProvider;
import com.oracle.bmc.identity.IdentityClient;
import com.oracle.bmc.identity.requests.ListCompartmentsRequest;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.model.CreateStreamPoolDetails;
import com.oracle.bmc.streaming.model.Stream;
import com.oracle.bmc.streaming.model.StreamPool;
import com.oracle.bmc.streaming.model.StreamSummary;
import com.oracle.bmc.streaming.requests.CreateStreamPoolRequest;
import com.oracle.bmc.streaming.requests.DeleteStreamPoolRequest;
import com.oracle.bmc.streaming.requests.DeleteStreamRequest;
import com.oracle.bmc.streaming.requests.GetStreamPoolRequest;
import com.oracle.bmc.streaming.requests.GetStreamRequest;
import com.oracle.bmc.streaming.requests.ListStreamsRequest;
import com.oracle.bmc.streaming.responses.CreateStreamPoolResponse;
import com.oracle.bmc.streaming.responses.DeleteStreamPoolResponse;
import com.oracle.bmc.streaming.responses.DeleteStreamResponse;
import com.oracle.bmc.streaming.responses.GetStreamPoolResponse;
import com.oracle.bmc.streaming.responses.GetStreamResponse;
import com.oracle.bmc.streaming.responses.ListStreamsResponse;
import java.io.IOException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OciStreamingHelper {
    private static final Logger LOG = LoggerFactory.getLogger(OciStreamingHelper.class);

    private static final long RESOURCE_WAIT_POLL_INTERVAL_MS = 2_000;

    private OciStreamingHelper() {
    }

    public static StreamAdminClient createStreamAdminClient(String streamingEndpoint, String authProfile) throws IOException {
        return StreamAdminClient.builder()
                .endpoint(streamingEndpoint)
                .build(new SessionTokenAuthenticationDetailsProvider(authProfile));
    }

    public static IdentityClient createIdentityClient(String identityEndpoint, String authProfile) throws IOException {
        return IdentityClient.builder()
                .endpoint(identityEndpoint)
                .build(new SessionTokenAuthenticationDetailsProvider(authProfile));
    }

    public static void waitForStreamPoolToBecomeActive(StreamAdminClient adminClient, String streamPoolId, long resourceWaitTimeoutMs)
            throws InterruptedException {
        final long deadline = System.currentTimeMillis() + resourceWaitTimeoutMs;

        while (System.currentTimeMillis() < deadline) {
            GetStreamPoolResponse response = getStreamPool(adminClient, streamPoolId);

            StreamPool.LifecycleState state = response.getStreamPool().getLifecycleState();
            LOG.debug("Polling stream pool state. streamPoolId={} state={}", streamPoolId, state);
            if (StreamPool.LifecycleState.Active.equals(state)) {
                LOG.info("Stream pool is ACTIVE. streamPoolId={}", streamPoolId);
                return;
            }

            if (StreamPool.LifecycleState.Failed.equals(state) || StreamPool.LifecycleState.Deleted.equals(state)) {
                throw new IllegalStateException(
                        "Stream pool " + streamPoolId + " entered terminal state: " + state);
            }

            Thread.sleep(RESOURCE_WAIT_POLL_INTERVAL_MS);
        }

        throw new IllegalStateException(
                "Timed out waiting for stream pool " + streamPoolId + " to become ACTIVE");
    }

    public static void waitForStreamToBecomeActive(StreamAdminClient adminClient, String streamId, long resourceWaitTimeoutMs)
            throws InterruptedException {
        final long deadline = System.currentTimeMillis() + resourceWaitTimeoutMs;

        while (System.currentTimeMillis() < deadline) {
            GetStreamResponse response = getStream(adminClient, streamId);

            Stream.LifecycleState state = response.getStream().getLifecycleState();
            LOG.debug("Polling stream state. streamId={} state={}", streamId, state);
            if (Stream.LifecycleState.Active.equals(state)) {
                LOG.info("Stream is ACTIVE. streamId={}", streamId);
                return;
            }

            if (Stream.LifecycleState.Failed.equals(state) || Stream.LifecycleState.Deleted.equals(state)) {
                throw new IllegalStateException(
                        "Stream " + streamId + " entered terminal state: " + state);
            }

            Thread.sleep(RESOURCE_WAIT_POLL_INTERVAL_MS);
        }

        throw new IllegalStateException(
                "Timed out waiting for stream " + streamId + " to become ACTIVE");
    }

    public static void waitForStreamToBecomeDeleted(StreamAdminClient adminClient, String streamId, long resourceWaitTimeoutMs)
            throws InterruptedException {
        final long deadline = System.currentTimeMillis() + resourceWaitTimeoutMs;

        while (System.currentTimeMillis() < deadline) {
            GetStreamResponse response;
            try {
                response = getStream(adminClient, streamId);
            } catch (BmcException ex) {
                if (ex.getStatusCode() == 404) {
                    LOG.info("Stream is no longer returned by OCI; treating it as DELETED. streamId={}", streamId);
                    return;
                }

                throw ex;
            }

            Stream.LifecycleState state = response.getStream().getLifecycleState();
            LOG.debug("Polling stream deletion state. streamId={} state={}", streamId, state);
            if (Stream.LifecycleState.Deleted.equals(state)) {
                LOG.info("Stream is DELETED. streamId={}", streamId);
                return;
            }

            if (Stream.LifecycleState.Failed.equals(state)) {
                throw new IllegalStateException(
                        "Stream " + streamId + " entered terminal state: " + state);
            }

            Thread.sleep(RESOURCE_WAIT_POLL_INTERVAL_MS);
        }

        throw new IllegalStateException(
                "Timed out waiting for stream " + streamId + " to become DELETED");
    }

    public static GetStreamPoolResponse getStreamPool(StreamAdminClient adminClient, String streamPoolId) {
        return adminClient.getStreamPool(
                GetStreamPoolRequest.builder()
                        .streamPoolId(streamPoolId)
                        .build());
    }

    public static void waitForStreamPoolToBecomeDeleted(StreamAdminClient adminClient, String streamPoolId, long resourceWaitTimeoutMs)
            throws InterruptedException {
        final long deadline = System.currentTimeMillis() + resourceWaitTimeoutMs;

        while (System.currentTimeMillis() < deadline) {
            GetStreamPoolResponse response;
            try {
                response = getStreamPool(adminClient, streamPoolId);
            } catch (BmcException ex) {
                if (ex.getStatusCode() == 404) {
                    LOG.info("Stream pool is no longer returned by OCI; treating it as DELETED. streamPoolId={}", streamPoolId);
                    return;
                }

                throw ex;
            }

            StreamPool.LifecycleState state = response.getStreamPool().getLifecycleState();
            LOG.debug("Polling stream pool deletion state. streamPoolId={} state={}", streamPoolId, state);
            if (StreamPool.LifecycleState.Deleted.equals(state)) {
                LOG.info("Stream pool is DELETED. streamPoolId={}", streamPoolId);
                return;
            }

            if (StreamPool.LifecycleState.Failed.equals(state)) {
                throw new IllegalStateException(
                        "Stream pool " + streamPoolId + " entered terminal state: " + state);
            }

            Thread.sleep(RESOURCE_WAIT_POLL_INTERVAL_MS);
        }

        throw new IllegalStateException(
                "Timed out waiting for stream pool " + streamPoolId + " to become DELETED");
    }

    public static CreateStreamPoolResponse createStreamPool(
            StreamAdminClient adminClient,
            String compartmentId,
            String poolName) {
        LOG.info("Creating stream pool via API. compartmentId={} poolName={}", compartmentId, poolName);
        return adminClient.createStreamPool(CreateStreamPoolRequest.builder()
                .createStreamPoolDetails(CreateStreamPoolDetails.builder()
                        .compartmentId(compartmentId)
                        .name(poolName)
                        .build())
                .build());
    }

    public static DeleteStreamPoolResponse deleteStreamPoolWhenEmpty(
            StreamAdminClient adminClient,
            String streamPoolId,
            long resourceWaitTimeoutMs)
            throws InterruptedException {
        final long deadline = System.currentTimeMillis() + resourceWaitTimeoutMs;

        LOG.info("Deleting stream pool via API streamPoolId={}", streamPoolId);
        while (true) {
            try {
                return adminClient.deleteStreamPool(DeleteStreamPoolRequest.builder().streamPoolId(streamPoolId).build());
            } catch (BmcException ex) {
                if (!isStreamPoolNotEmpty(ex) || System.currentTimeMillis() >= deadline) {
                    throw ex;
                }

                LOG.debug("Stream pool is not empty yet; retrying deletion. streamPoolId={}", streamPoolId);
                Thread.sleep(RESOURCE_WAIT_POLL_INTERVAL_MS);
            }
        }
    }

    public static GetStreamResponse getStream(StreamAdminClient adminClient, String streamId) {
        return adminClient.getStream(GetStreamRequest.builder()
                .streamId(streamId)
                .build());
    }

    public static DeleteStreamResponse deleteStream(StreamAdminClient adminClient, String streamId) {
        LOG.info("Deleting stream via API streamId={}", streamId);
        return adminClient.deleteStream(DeleteStreamRequest.builder().streamId(streamId).build());
    }

    private static boolean isStreamPoolNotEmpty(BmcException ex) {
        return ex.getStatusCode() == 400
                && "InvalidParameter".equals(ex.getServiceCode())
                && ex.getMessage() != null
                && ex.getMessage().contains("is not empty");
    }

    public static Optional<StreamSummary> findStreamInPoolByName(
            StreamAdminClient adminClient,
            String compartmentId,
            String streamPoolId,
            String streamName) {
        String page = null;

        do {
            ListStreamsResponse listStreamsResponse = adminClient.listStreams(ListStreamsRequest.builder()
                    .compartmentId(compartmentId)
                    .streamPoolId(streamPoolId)
                    .page(page)
                    .build());

            Optional<StreamSummary> matchingStream = listStreamsResponse.getItems().stream()
                    .filter(streamSummary -> streamName.equals(streamSummary.getName()))
                    .findFirst();
            if (matchingStream.isPresent()) {
                return matchingStream;
            }

            page = listStreamsResponse.getOpcNextPage();
        } while (page != null);

        return Optional.empty();
    }

    public static String waitForStreamInPoolByName(
            StreamAdminClient adminClient,
            String compartmentId,
            String streamPoolId,
            String streamName,
            long resourceWaitTimeoutMs)
            throws InterruptedException {
        final long deadline = System.currentTimeMillis() + resourceWaitTimeoutMs;

        while (System.currentTimeMillis() < deadline) {
            Optional<StreamSummary> streamSummary = findStreamInPoolByName(adminClient, compartmentId, streamPoolId, streamName);
            if (streamSummary.isPresent()) {
                String streamId = streamSummary.get().getId();
                LOG.info("Stream was found in stream pool. streamPoolId={} streamName={} streamId={}",
                        streamPoolId, streamName, streamId);
                return streamId;
            }

            LOG.debug("Polling for stream in stream pool. streamPoolId={} streamName={}", streamPoolId, streamName);
            Thread.sleep(RESOURCE_WAIT_POLL_INTERVAL_MS);
        }

        throw new IllegalStateException(
                "Timed out waiting for stream " + streamName + " to appear in stream pool " + streamPoolId);
    }

    public static void validateAuthenticationToken(
            String identityEndpoint,
            String authProfile,
            String tenancyOcid) throws IOException {
        LOG.info("Validating authentication token against OCI Identity");

        try (IdentityClient identityClient = createIdentityClient(identityEndpoint, authProfile)) {
            identityClient.listCompartments(ListCompartmentsRequest.builder()
                    .compartmentId(tenancyOcid)
                    .compartmentIdInSubtree(Boolean.FALSE)
                    .accessLevel(ListCompartmentsRequest.AccessLevel.Accessible)
                    .limit(1)
                    .build());
            LOG.info("Authentication token validation succeeded");
        }
    }
}
