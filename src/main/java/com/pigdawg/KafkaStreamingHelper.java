package com.pigdawg;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

final class KafkaStreamingHelper {
    private static final String KAFKA_USERNAME_SEPARATOR = "/";
    private static final String SECURITY_PROTOCOL_SASL_SSL = "SASL_SSL";
    private static final String SASL_MECHANISM_PLAIN = "PLAIN";
    private static final String ACKS_ALL = "all";
    private static final String DISABLE_AUTO_COMMIT = "false";
    private static final String DISABLE_IDEMPOTENCE = "false";
    private static final String AUTO_OFFSET_RESET_EARLIEST = "earliest";
    private static final int PRODUCER_RETRIES = 3;
    private static final int MAX_POLL_RECORDS = 10;
    private static final short TOPIC_REPLICATION_FACTOR = 1;

    private KafkaStreamingHelper() {
    }

    static String createStreamPoolUsername(String usernameBase, String streamPoolId) {
        requireNonBlank(usernameBase, "usernameBase");
        requireNonBlank(streamPoolId, "streamPoolId");

        String normalizedUsernameBase = trimTrailingSeparators(usernameBase);
        return normalizedUsernameBase + KAFKA_USERNAME_SEPARATOR + streamPoolId;
    }

    static Properties createProducerProperties(
            String bootstrapServers,
            String username,
            String password,
            String truststoreLocation,
            String truststorePassword) {
        Properties properties = createBaseKafkaProperties(
                bootstrapServers,
                username,
                password,
                truststoreLocation,
                truststorePassword);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, ACKS_ALL);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, DISABLE_IDEMPOTENCE);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        properties.put(ProducerConfig.RETRIES_CONFIG, PRODUCER_RETRIES);
        return properties;
    }

    static Properties createAdminProperties(
            String bootstrapServers,
            String username,
            String password,
            String truststoreLocation,
            String truststorePassword) {
        return createBaseKafkaProperties(
                bootstrapServers,
                username,
                password,
                truststoreLocation,
                truststorePassword);
    }

    static Properties createConsumerProperties(
            String bootstrapServers,
            String username,
            String password,
            String truststoreLocation,
            String truststorePassword,
            String groupId) {
        Properties properties = createBaseKafkaProperties(
                bootstrapServers,
                username,
                password,
                truststoreLocation,
                truststorePassword);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, DISABLE_AUTO_COMMIT);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_EARLIEST);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        return properties;
    }

    static void createTopic(
            String bootstrapServers,
            String topic,
            String username,
            String password,
            String truststoreLocation,
            String truststorePassword,
            int partitions)
            throws ExecutionException, InterruptedException {
        Properties properties = createAdminProperties(
                bootstrapServers,
                username,
                password,
                truststoreLocation,
                truststorePassword);
        try (AdminClient adminClient = AdminClient.create(properties)) {
            NewTopic newTopic = new NewTopic(topic, partitions, TOPIC_REPLICATION_FACTOR);
            adminClient.createTopics(java.util.List.of(newTopic)).all().get();
        }
    }

    private static Properties createBaseKafkaProperties(
            String bootstrapServers,
            String username,
            String password,
            String truststoreLocation,
            String truststorePassword) {
        requireNonBlank(bootstrapServers, "bootstrapServers");
        requireNonBlank(username, "username");
        requireNonBlank(password, "password");

        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SECURITY_PROTOCOL_SASL_SSL);
        properties.put(SaslConfigs.SASL_MECHANISM, SASL_MECHANISM_PLAIN);
        properties.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"" + escapeJaasValue(username) + "\" "
                        + "password=\"" + escapeJaasValue(password) + "\";");

        if (truststoreLocation != null && !truststoreLocation.isBlank()) {
            properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
        }

        if (truststorePassword != null && !truststorePassword.isBlank()) {
            properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
        }

        return properties;
    }

    private static String escapeJaasValue(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private static String trimTrailingSeparators(String value) {
        String trimmedValue = value.trim();
        while (trimmedValue.endsWith(KAFKA_USERNAME_SEPARATOR)) {
            trimmedValue = trimmedValue.substring(0, trimmedValue.length() - KAFKA_USERNAME_SEPARATOR.length());
        }

        return trimmedValue;
    }
}
