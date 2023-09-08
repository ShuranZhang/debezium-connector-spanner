/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

// import io.debezium.embedded.EmbeddedEngine;

public class BasicSanityCheckIT extends AbstractSpannerConnectorIT {

    private static final String tableName = "embedded_sanity_tests_table";
    private static final String changeStreamName = "embeddedSanityTestChangeStream";
    private static final String heartbeatTopic = "__debezium-heartbeat.testing-connector";

    @BeforeAll
    static void setup() throws InterruptedException, ExecutionException {
        databaseConnection.createTable(tableName + " (\n" +
                " id INT64,\n" +
                " float FLOAT64,\n" +
                " int INT64,\n" +
                " num NUMERIC,\n" +
                " str STRING(1000),\n" +
                " time TIMESTAMP,\n" +
                " date DATE,\n" +
                " byt BYTES(2000),\n" +
                " bool BOOL,\n" +
                ") PRIMARY KEY(id)");
        databaseConnection.createChangeStream(changeStreamName, tableName);

        System.out.println("EmbeddedBasicSanityCheckIT is ready...");
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropChangeStream(changeStreamName);
        databaseConnection.dropTable(tableName);
    }

    @Test
    public void testDocker() {
        System.out.println("test hahahaha");
        assertEquals(10, 9 + 1);
        // try {
        // Thread.sleep(10000);
        // }
        // catch (InterruptedException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }

    }
}
