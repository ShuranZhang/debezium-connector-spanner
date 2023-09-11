/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerState;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class KafkaBrokerApi<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaBrokerApi.class);

    protected static final String SCHEMA_REGISTRY_PORT = "8081";

    protected static final String SCHEMA_REGISTRY_HOST = "http://localhost";

    protected static final int POLL_DURATION_MILLIS = 100;

    protected static final int WAIT_TOPIC_HAS_NO_MORE_RECORDS_SECONDS = 60;

    public static final int POLL_FIRST_RECORDS_TIMEOUT_MAX_MINUTES = 10;

    private final ContainerState containerState;

    private final int kafkaPort;

    private final Properties properties;

    public KafkaBrokerApi(ContainerState containerState, int kafkaPort, Properties properties) {
        this.containerState = containerState;
        this.kafkaPort = kafkaPort;
        this.properties = SerializationUtils.clone(properties);
    }

    public static String getSchemaRegistryAddress() {
        return SCHEMA_REGISTRY_HOST + ":" + SCHEMA_REGISTRY_PORT;
    }

    public static KafkaBrokerApi<GenericRecord, GenericRecord> createKafkaBrokerApiGenericRecord(
                                                                                                 ContainerState containerState, int kafkaPort) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, containerState.getHost() + ":" + kafkaPort);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryAddress());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        return new KafkaBrokerApi<>(containerState, kafkaPort, props);
    }

    public static KafkaBrokerApi<ObjectNode, ObjectNode> createKafkaBrokerApiObjectNode(ContainerState containerState,
                                                                                        int kafkaPort) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, containerState.getHost() + ":" + kafkaPort);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryAddress());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaBrokerApi<>(containerState, kafkaPort, props);
    }

    public String getAddress() {
        return containerState.getHost() + ":" + kafkaPort; // containerState.getMappedPort(kafkaHttpPort);
    }

    public Consumer<K, V> createConsumer(String consumerGroup) {
        final Properties props = SerializationUtils.clone(properties);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        return new KafkaConsumer<>(props);
    }

    public CreateTopicsResult createTopic(String topicName, int numPartitions) {
        try (AdminClient adminClient = createAdminClient()) {
            NewTopic newTopic = new NewTopic(topicName, Optional.of(numPartitions), Optional.empty());
            return adminClient.createTopics(List.of(newTopic));
        }
    }

    public AdminClient createAdminClient() {
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getAddress());
        return AdminClient.create(props);
    }

    public Consumer<K, V> createSubscribeConsumer(String consumerGroup, String topic) {
        final Consumer<K, V> consumer = createConsumer(consumerGroup);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    public ConsumerRecord<K, V> receiveRecord(String topic, Duration timeout) {
        return receiveRecords(topic, 1, timeout).stream().findFirst().orElseThrow();
    }

    public List<ConsumerRecord<K, V>> receiveRecords(String topic, int expectedRecords, Duration timeout) {
        final String testConsumerGroup = "group-" + UUID.randomUUID();
        final List<ConsumerRecord<K, V>> recordsList = new ArrayList<>();
        try (Consumer<K, V> testConsumer = createConsumer(testConsumerGroup)) {
            await().atMost(timeout).until(() -> {
                if (testConsumer.listTopics().containsKey(topic)) {
                    testConsumer.subscribe(Collections.singletonList(topic));
                    LOG.info("{} found, polling...", topic);
                    final ConsumerRecords<K, V> consumerRecords = testConsumer
                            .poll(Duration.ofMillis(POLL_DURATION_MILLIS));
                    LOG.info("Records count: {}", consumerRecords.count());
                    consumerRecords.forEach(record -> {
                        recordsList.add(record);
                        LOG.info("Consumer Record:(key={}\n value={}\n partition={} offset={})",
                                record.key(), record.value(), record.partition(), record.offset());
                    });
                }
                else {
                    LOG.info("KeySet: {}", testConsumer.listTopics().keySet());
                }
                return recordsList.size() >= expectedRecords;
            });
        }
        catch (ConditionTimeoutException ignored) {
        }
        return recordsList;
    }

    public List<ConsumerRecord<K, V>> receiveAllRecords(String topic) {
        return receiveAllRecords(topic, WAIT_TOPIC_HAS_NO_MORE_RECORDS_SECONDS);
    }

    public List<ConsumerRecord<K, V>> receiveAllRecords(String topic, int timeoutSeconds) {
        return receiveAllRecords(topic, timeoutSeconds, "group-" + UUID.randomUUID());
    }

    public List<ConsumerRecord<K, V>> receiveAllRecords(String topic, int timeoutSeconds, String testConsumerGroup) {
        final List<ConsumerRecord<K, V>> recordsList = new ArrayList<>();
        try (Consumer<K, V> testConsumer = createConsumer(testConsumerGroup)) {
            final var duration = Duration.ofSeconds(timeoutSeconds);
            var absentTime = Instant.now().plus(duration);
            boolean subscribed = false;
            while (absentTime.isAfter(Instant.now())) {
                // LOG.info("Cusomer topics {}, topic ", testConsumer.listTopics(), topic);
                if (testConsumer.listTopics().containsKey(topic)) {
                    if (!subscribed) {
                        testConsumer.subscribe(Collections.singletonList(topic));
                        subscribed = true;
                    }
                    LOG.info("{} found, polling...", topic);

                    final ConsumerRecords<K, V> consumerRecords = testConsumer
                            .poll(Duration.ofMillis(POLL_DURATION_MILLIS));
                    if (consumerRecords.count() > 0) {
                        LOG.info("Records count: {}", consumerRecords.count());
                    }
                    if (consumerRecords.count() > 0) {
                        absentTime = Instant.now().plus(duration);
                        consumerRecords.forEach(record -> {
                            recordsList.add(record);
                            LOG.info("Consumer Record:(key={}\n value={}\n partition={} offset={})",
                                    record.key(), record.value(), record.partition(), record.offset());
                        });
                    }
                }

                else {
                    LOG.info("KeySet: {}", testConsumer.listTopics().keySet());
                }
            }
        }
        catch (ConditionTimeoutException ignored) {
        }
        return recordsList;
    }

    public List<ConsumerRecord<K, V>> pollFirstRecords(String topic, int expectedRecords) {
        return receiveRecords(topic, expectedRecords, Duration.ofMinutes(POLL_FIRST_RECORDS_TIMEOUT_MAX_MINUTES));
    }

    public Consumer<ObjectNode, ObjectNode> createSubscribeConsumer(String consumerGroup, String topic,
                                                                    String keyDeserializer,
                                                                    String valueDeserializer) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                this.getAddress());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                consumerGroup);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                valueDeserializer);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("schema.registry.url", "http://schema-registry:8081");

        Consumer<ObjectNode, ObjectNode> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    public Consumer<ObjectNode, ObjectNode> createSubscribeConsumer(String consumerGroup, String topic,
                                                                    String keyDeserializer,
                                                                    String valueDeserializer,
                                                                    String schemaRegistryUrl) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                this.getAddress());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                consumerGroup);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                valueDeserializer);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("schema.registry.url", schemaRegistryUrl);

        Consumer<ObjectNode, ObjectNode> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }
}
