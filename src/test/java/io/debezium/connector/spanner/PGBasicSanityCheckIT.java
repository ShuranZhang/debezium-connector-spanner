/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

public class PGBasicSanityCheckIT extends AbstractSpannerConnectorIT {

    private static final String tableName = "embedded_sanity_tests_table_pg";
    private static final String changeStreamName = "embeddedsanitytestchangestream_pg";

    @BeforeAll
    static void setup() throws InterruptedException, ExecutionException {
        pgDatabaseConnection.createTable(tableName + "(id bigint, name varchar, primary key(id))");
        pgDatabaseConnection.createChangeStream(changeStreamName, tableName);

        System.out.println("PgBasicSanityCheckIT is ready...");
    }

    @AfterAll
    static void clear() throws InterruptedException {
        pgDatabaseConnection.dropChangeStream(changeStreamName);
        pgDatabaseConnection.dropTable(tableName);
    }

    @Test
    public void shouldStreamUpdatesToKafka() throws InterruptedException {
        final Configuration config = Configuration.copy(basePgConfig)
                .with("gcp.spanner.change.stream", changeStreamName)
                .with("name", tableName + "_test")
                .with("gcp.spanner.start.time",
                        DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();
        initializeConnectorTestFramework();
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();
        pgDatabaseConnection.executeUpdate("insert into " + tableName + "(id, name) values (1, 'some name')");
        pgDatabaseConnection.executeUpdate("update " + tableName + " set name = 'test' where id = 1");
        pgDatabaseConnection.executeUpdate("delete from " + tableName + " where id = 1");
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);

        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableName));
        assertThat(records).hasSize(4);
        // Verify that mod types are create + update + delete + TOMBSTONE in order.
        assertThat((String) ((Struct) (records.get(0).value())).get("op")).isEqualTo("c");
        assertThat((String) ((Struct) (records.get(1).value())).get("op")).isEqualTo("u");
        assertThat((String) ((Struct) (records.get(2).value())).get("op")).isEqualTo("d");
        assertThat(records.get(3).value()).isEqualTo(null);
        stopConnector();
        assertConnectorNotRunning();
    }
}
