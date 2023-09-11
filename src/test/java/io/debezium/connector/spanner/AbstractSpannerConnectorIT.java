/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.Database;
import io.debezium.connector.spanner.util.KafkaEnvironment;
import io.debezium.embedded.AbstractConnectorTest;

public class AbstractSpannerConnectorIT extends AbstractConnectorTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaEnvironment.class);

    private static final KafkaEnvironment KAFKA_ENVIRONMENT = new KafkaEnvironment(
            KafkaEnvironment.DOCKER_COMPOSE_FILE);
    protected static final Database database = Database.TEST_DATABASE;
    protected static final Connection databaseConnection = database.getConnection();
    protected static final int WAIT_FOR_CDC = 3 * 3000;
    // protected static final Configuration baseConfig = Configuration.create()
    // .with("gcp.spanner.instance.id", database.getInstanceId())
    // .with("gcp.spanner.project.id", database.getProjectId())
    // .with("gcp.spanner.database.id", database.getDatabaseId())
    // .with("gcp.spanner.emulator.host", Connection.emulatorHost)
    // .build();
    static {
        if (!KAFKA_ENVIRONMENT.isStarted()) {
            KAFKA_ENVIRONMENT.start();
            KAFKA_ENVIRONMENT.setStarted();
        }
    }
    protected static final Configuration baseConfig = Configuration.create()
            .with("gcp.spanner.instance.id", "change-stream-load-test-3")
            .with("gcp.spanner.project.id", "cloud-spanner-backups-loadtest")
            .with("gcp.spanner.database.id", "load-test-change-stream-enable")

            .with("gcp.spanner.change.stream", "changeStreamAll")
            .with("gcp.spanner.host", "https://staging-wrenchworks.sandbox.googleapis.com")
            .with("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            .with("connector.spanner.sync.kafka.bootstrap.servers", KAFKA_ENVIRONMENT.kafkaBrokerApiOn().getAddress())
            .with("database.history.kafka.bootstrap.servers", KAFKA_ENVIRONMENT.kafkaBrokerApiOn().getAddress())
            .with("bootstrap.servers", KAFKA_ENVIRONMENT.kafkaBrokerApiOn().getAddress())
            .build();

    // @AfterAll
    // public static void after() throws InterruptedException {
    // System.out.println("after all tests");
    // }

    // @BeforeAll
    // public static void before() throws InterruptedException {
    // Testing.Print.enable();
    // System.out.println("before all");

    // databaseConnection = database.getConnection();
    // }

    @AfterAll
    public static void after() throws InterruptedException {
        System.out.println("Cleaning up kafka...");
        KAFKA_ENVIRONMENT.clearTopics();
        System.out.println("Cleaning complete!");
    }

    protected static void waitForCDC() {
        try {
            Thread.sleep(WAIT_FOR_CDC);
        } catch (Exception e) {

        }
    }

    protected static void waitForCDC(long minutes) {
        try {
            Thread.sleep(Duration.ofMinutes(minutes).toMillis());
        } catch (Exception e) {
        }
    }

    protected String getTopicName(Configuration config, String tableName) {
        String debeziumConnectorName = "testing-connector";
        return debeziumConnectorName + "." + tableName;
    }
}
