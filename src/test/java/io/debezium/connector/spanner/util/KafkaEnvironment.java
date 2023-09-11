/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

import static org.awaitility.Awaitility.await;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import com.fasterxml.jackson.databind.node.ObjectNode;

// import utils.dto.SchemasDto;

public class KafkaEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaEnvironment.class);

    private static final String KAFKA_CONNECT_SERVICE_NAME = "kafka-connect_1";
    private static final int KAFKA_CONNECT_SERVICE_API_PORT = 8083;

    private static final String KAFKA_BROKER_SERVICE_NAME = "broker_1";
    private static final int KAFKA_BROKER_SERVICE_API_PORT = 9092;

    private static final String GENERATOR_SERVICE_NAME = "generator_1";
    private static final int GENERATOR_SERVICE_API_PORT = 8666;

    private static final String SCHEMA_REGISTRY_NAME = "schema-registry_1";
    private static final int SCHEMA_REGISTRY_API_PORT = 8081;

    public static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(200L);
    public static final Duration STARTUP_CONNECTOR_TIMEOUT = Duration.ofSeconds(600L);
    public static final Duration CONFIGURE_CONNECTOR_TIMEOUT = Duration.ofSeconds(200L);

    public static final String DOCKER_COMPOSE_FILE = "src/test/java/io/debezium/connector/spanner/util/docker-compose.yml";
    public static final KafkaEnvironment TEST_KAFKA_ENVIRONMENT = new KafkaEnvironment(DOCKER_COMPOSE_FILE);

    private boolean isStarted = false;

    private DockerComposeContainer composeContainer;
    private KafkaConnectApi connectApi;

    private KafkaBrokerApi<ObjectNode, ObjectNode> brokerApiOn;

    private KafkaBrokerApi<GenericRecord, GenericRecord> brokerApiGr;

    // private GeneratorApi generatorApi;

    private SchemaRegistryApi schemaRegistryApi;

    // public KafkaEnvironment() {

    // }

    public KafkaEnvironment(String dockerComposeFilePath) {
        System.out.println("Initializing kafka environment");
        this.composeContainer = new DockerComposeContainer(new File(dockerComposeFilePath))
                // .withExposedService(KAFKA_CONNECT_SERVICE_NAME,
                // KAFKA_CONNECT_SERVICE_API_PORT,
                // Wait.forListeningPort().withStartupTimeout(STARTUP_TIMEOUT))
                .withExposedService(KAFKA_BROKER_SERVICE_NAME, KAFKA_BROKER_SERVICE_API_PORT,
                        Wait.forListeningPort().withStartupTimeout(STARTUP_TIMEOUT))
                .withExposedService(SCHEMA_REGISTRY_NAME, SCHEMA_REGISTRY_API_PORT,
                        Wait.forListeningPort().withStartupTimeout(STARTUP_TIMEOUT));
        // .withExposedService(GENERATOR_SERVICE_NAME, GENERATOR_SERVICE_API_PORT,
        // Wait.forListeningPort().withStartupTimeout(STARTUP_TIMEOUT))
        // .waitingFor(KAFKA_CONNECT_SERVICE_NAME, Wait.forHttp("/connectors")
        // .withStartupTimeout(STARTUP_TIMEOUT));
        System.out.println("Finished initializing kafka environment");
    }

    public DockerComposeContainer getComposeContainer() {
        return composeContainer;
    }

    public void start() {

        System.out.println("Starting Kafka environment");
        this.composeContainer.start();
        System.out.println("Starting compose container");

        // ContainerState connectState = getConnectorState();

        System.out.println("Start broker .......");
        ContainerState brokerState = (ContainerState) composeContainer
                .getContainerByServiceName(KAFKA_BROKER_SERVICE_NAME)
                .orElseThrow();

        // System.out.println("Start generator .......");
        // ContainerState generatorState = (ContainerState) composeContainer
        // .getContainerByServiceName(GENERATOR_SERVICE_NAME)
        // .orElseThrow();

        System.out.println("Start schema registry .......");
        ContainerState schemaRegistryState = (ContainerState) composeContainer
                .getContainerByServiceName(SCHEMA_REGISTRY_NAME)
                .orElseThrow();

        // this.connectApi = new KafkaConnectApi(connectState,
        // KAFKA_CONNECT_SERVICE_API_PORT);
        this.brokerApiOn = KafkaBrokerApi.createKafkaBrokerApiObjectNode(brokerState, KAFKA_BROKER_SERVICE_API_PORT);
        this.brokerApiGr = KafkaBrokerApi.createKafkaBrokerApiGenericRecord(brokerState, KAFKA_BROKER_SERVICE_API_PORT);
        this.schemaRegistryApi = new SchemaRegistryApi(schemaRegistryState, SCHEMA_REGISTRY_API_PORT);

    }

    public ContainerState getConnectorState() {
        return (ContainerState) composeContainer.getContainerByServiceName(KAFKA_CONNECT_SERVICE_NAME)
                .orElseThrow();
    }

    public KafkaBrokerApi<ObjectNode, ObjectNode> kafkaBrokerApiOn() {
        return brokerApiOn;
    }

    public KafkaBrokerApi<GenericRecord, GenericRecord> kafkaBrokerApiGr() {
        return brokerApiGr;
    }

    // public GeneratorApi getGeneratorApi() {
    // return generatorApi;
    // }

    public SchemaRegistryApi getSchemaRegistryApi() {
        return schemaRegistryApi;
    }

    public void registerConnector(Connector connector) {
        await().atMost(CONFIGURE_CONNECTOR_TIMEOUT).until(() -> this.connectApi.isKafkaConnectReady());
        this.connectApi.registerConnector(connector);

        await().atMost(CONFIGURE_CONNECTOR_TIMEOUT)
                .until(() -> this.connectApi.isConnectorConfigured(connector.getName()));

        await().atMost(CONFIGURE_CONNECTOR_TIMEOUT)
                .until(() -> this.connectApi.isConnectorHasRunningStatus(connector.getName()));

        await().atMost(STARTUP_CONNECTOR_TIMEOUT).until(() -> this.connectApi.isTaskStarted(connector.getName(), 0));
    }

    public void registerConnectorIgnoreLogCheck(Connector connector) {
        await().atMost(CONFIGURE_CONNECTOR_TIMEOUT).until(() -> this.connectApi.isKafkaConnectReady());
        this.connectApi.registerConnector(connector);

        await().atMost(CONFIGURE_CONNECTOR_TIMEOUT)
                .until(() -> this.connectApi.isConnectorConfigured(connector.getName()));

        await().atMost(CONFIGURE_CONNECTOR_TIMEOUT)
                .until(() -> this.connectApi.isConnectorHasRunningStatus(connector.getName()));
    }

    public String getLogs() {
        return connectApi.getLogs();
    }

    public void deleteConnector(String connectorName) {
        this.connectApi.deleteConnector(connectorName);
    }

    public boolean isStarted() {
        return isStarted;
    }

    public void setStarted() {
        isStarted = true;
    }

    public void clearTopics() {
        try (AdminClient adminClient = kafkaBrokerApiOn().createAdminClient()) {
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            Set<String> topics = listTopicsResult.names().get();
            Arrays.asList("_kafka-connect-configs",
                    "_kafka-connect-offsets",
                    "_kafka-connect-status",
                    "_kafka-connect-status",
                    "_schemas",
                    "_confluent-command",
                    "_confluent_balancer_api_state",
                    "_confluent-metrics",
                    "__consumer_offsets",
                    "_confluent-telemetry-metrics").forEach(
                            topics::remove);
            adminClient.deleteTopics(topics);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void stopConnector(Connector connector) {
        this.connectApi.stopConnector(connector);
    }

    public void startConnector(Connector connector) {
        this.connectApi.startConnector(connector);
    }

    // public Map<String, String> getSchemaFormat(String topic) {
    // Map<String, String> resp = new LinkedHashMap<>();
    // final OkHttpClient httpClient = new OkHttpClient.Builder()
    // .callTimeout(300, TimeUnit.SECONDS)
    // .build();
    // String url = "http://schema-registry:8081/schemas/";
    // LOG.info("GET {}", url);
    // final Request request = new Request.Builder().url(url).get().build();
    // await().atMost(Duration.ofSeconds(30)).until(() -> {
    // Response response = httpClient.newCall(request).execute();
    // return response.body() != null;
    // });
    // try (Response response = httpClient.newCall(request).execute()) {
    // if (!response.isSuccessful()) {
    // LOG.warn("Response body: {}", response.body().string());
    // throw new RuntimeException();
    // }
    // String body = response.body().string();
    // System.out.println(body);
    // ObjectMapper objectMapper = new ObjectMapper();
    // SchemasDto[] schemasArray = objectMapper.readValue(body, SchemasDto[].class);
    // for (SchemasDto schemasDto : schemasArray) {
    // if (schemasDto.getSubject().contains(topic)) {
    // resp.put(
    // schemasDto.getSubject().replaceAll(topic, "").replaceAll("-",
    // "").replaceAll(" ", ""),
    // schemasDto.getSchemaType());
    // }
    // }
    // if (resp.size() == 0) {
    // LOG.warn("Schemas for topic {} not found: {}", topic, schemasArray);
    // throw new RuntimeException();
    // }
    // return resp;
    // } catch (IOException ex) {
    // throw new RuntimeException("Error connecting to kafka connect container",
    // ex);
    // }
    // }

    public KafkaConnectApi getConnectApi() {
        return connectApi;
    }
}
