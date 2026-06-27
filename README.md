# simple-oci-streaming-client-kafka

This project is a small Java 17 demo for using OCI Streaming through its Kafka-compatible API.

It is meant to show the basic lifecycle of using the streaming service from a Kafka client: authenticate, create the needed resources, create a topic, send messages, read them back, and clean up when finished. The app is intentionally simple and is useful as a working reference for OCI Streaming Kafka setup and request flow.

## Prerequisites

- Java 17
- Maven 3.9.x
- An OCI session token profile configured locally for the profile named in `application.properties`
- OCI permissions to create and delete stream pools in the target compartment
- Kafka-compatible credentials for OCI Streaming

## Configure the app

The program reads runtime settings from `src/main/resources/application.properties`.

That file is intentionally not committed. Start from the example file in the same directory:

- [src/main/resources/example_application.properties](src/main/resources/example_application.properties)

Copy that file to `src/main/resources/application.properties`, then replace the placeholder values with real values for your environment.

Required properties:

- `oci.streaming.endpoint`
- `oci.identity.endpoint`
- `oci.streaming.tenancy`
- `oci.streaming.compartment`
- `oci.auth.profile`
- `oci.kafka.username.base`
- `oci.kafka.password`

`oci.kafka.username.base` is the tenancy and user part of the Kafka username, such as `my-tenancy/my-user` or `my-tenancy/my-identity-domain/my-user`. The app appends the created stream pool OCID at runtime. `oci.kafka.password` is an auth token for that user.

Optional Kafka properties:

- `oci.kafka.truststore.location`
- `oci.kafka.truststore.password`

## Run it

From the repository root:

```bash
mvn compile
mvn exec:java
```

`exec-maven-plugin` is already configured to run `com.pigdawg.SteamingApp`.

## What the test covers

When you run it, the app checks the full Streaming Kafka-compatible workflow:

1. It confirms the OCI session token and Identity endpoint work.
2. It creates a stream pool in the configured compartment.
3. It waits for that pool to become active.
4. It resolves the pool's Kafka bootstrap servers.
5. It creates the stream with Kafka `CREATE_TOPICS`.
6. It resolves the Kafka-created topic back to an OCI stream ID.
7. It waits for the stream to become active.
8. It starts one Kafka producer thread and one Kafka consumer thread.
9. The producer publishes messages to the topic.
10. The consumer reads messages back from the topic.
11. It deletes the stream, waits for deletion, deletes the pool, and waits for the pool to disappear.

OCI APIs are still used for authentication validation, stream pool lifecycle, resolving the Kafka-created topic back to an OCI stream ID, and cleanup.

Because the app manages live OCI resources, make sure the compartment and profile in `application.properties` point at the environment you expect.
