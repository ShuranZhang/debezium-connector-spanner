/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.time.Duration;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.Database;
import io.debezium.connector.spanner.util.EmulatorEnvironment;
import io.debezium.embedded.AbstractConnectorTest;

public class AbstractSpannerConnectorIT extends AbstractConnectorTest {
    private static final EmulatorEnvironment EMULATOR_ENVIRONMENT = new EmulatorEnvironment();
    protected static final Database database = Database.TEST_DATABASE;
    protected static final Connection databaseConnection = database.getConnection();
    protected static final int WAIT_FOR_CDC = 3 * 1000;
    protected static final Configuration baseConfig = Configuration.create()
            .with("gcp.spanner.instance.id", database.getInstanceId())
            .with("gcp.spanner.project.id", database.getProjectId())
            .with("gcp.spanner.database.id", database.getDatabaseId())
            .with("gcp.spanner.emulator.host", Connection.emulatorHost)
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

    protected static void waitForCDC() {
        try {
            Thread.sleep(WAIT_FOR_CDC);
        }
        catch (Exception e) {

        }
    }

    protected static void waitForCDC(long minutes) {
        try {
            Thread.sleep(Duration.ofMinutes(minutes).toMillis());
        }
        catch (Exception e) {
        }
    }

    protected String getTopicName(Configuration config, String tableName) {
        String debeziumConnectorName = "testing-connector";
        return debeziumConnectorName + "." + tableName;
    }
}
