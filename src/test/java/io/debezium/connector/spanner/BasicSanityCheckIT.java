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
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.KafkaEnvironment;

public class BasicSanityCheckIT extends AbstractSpannerConnectorIT {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaEnvironment.class);

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
    public void checkUpdatesStreamedToKafka() throws InterruptedException {
        System.out.println("test hahahaha");
        final Configuration config = Configuration.copy(baseConfig)
                // .with("gcp.spanner.change.stream", changeStreamName)
                .with("name", tableName + "_test")
                .with("gcp.spanner.start.time",
                        DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .with("heartbeat.interval.ms", WAIT_FOR_CDC)
                .with("gcp.spanner.low-watermark.enabled", true)
                .with("heartbeat.interval.ms", "300000")
                .build();
        initializeConnectorTestFramework();
        // start(SpannerConnector.class, config);
        start(
                SpannerConnector.class,
                config,
                (success, msg, error) -> {
                    // assertThat(success).isFalse();
                    // assertThat(error).isNull();
                    // System.out.println("Has any errors? :" + error.isNull());
                    System.out.println("message is: " + msg);
                    // System.out.println("error is: " + error.getMessage());
                });
        System.out.println("about to check if connectoring is running");
        assertConnectorIsRunning();
        databaseConnection.executeUpdate("insert into " + tableName +
                "(id, float, int, num, str, time, date, byt, bool) " +
                "VALUES (" +
                "-2147476731, " +
                "0.2912236, " +
                "834314319, " +
                "0.340000000000000, " +
                "'rncwA7ZBAQLh5J&NezDuXkC#j#n99gRI$002CMzwY91Z$012V25Qj4O#C0Iuk$K745MPFPONQVY7$F9hyiZtFvaA!CJTAdK4Zia', "
                +
                "'1950-03-09 22:38:47', " +
                "'1986-07-15', " +
                "FROM_BASE64('hello world'), " +
                "true);");
        databaseConnection.executeUpdate("update " + tableName + " set str = 'test' where id = -2147476731");
        databaseConnection.executeUpdate("delete from " + tableName + " where id = -2147476731");
        waitForCDC();

        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        // System.out.println("Records: " + sourceRecords.allRecordsInOrder().size());
        // System.out.println("**************ALL RECORDS***************");
        // for (SourceRecord record : sourceRecords.allRecordsInOrder()) {
        // System.out.println("Record: " + record.toString());
        // System.out.println("");
        // }
        // System.out.println("");
        // System.out.println("");
        String outputTopicName = getTopicName(config, tableName);
        // System.out.println("**************RECORDS FOR TRACKING TABLE
        // TOPIC***************");
        // System.out.println("Topic name: " + outputTopicName);
        // for (String topicName : sourceRecords.topics()) {
        // System.out.println(
        // "Is " + outputTopicName + "equivalent to " + topicName + ": " +
        // topicName.equals(outputTopicName));
        // List<SourceRecord> topicRecords = sourceRecords.recordsForTopic(topicName);
        // System.out.println("**************INDIVIDUAL TOPIC RECORD***************");
        // for (SourceRecord topicRecord : topicRecords) {
        // System.out.println("Topic record: " + topicRecord.toString() + "for topic: "
        // + topicName);
        // System.out.println("");
        // }
        // System.out.println("");
        // }
        // List<SourceRecord> heartbeatRecords =
        // sourceRecords.recordsForTopic(heartbeatTopic);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableName));
        System.out.println("Total source records: " + sourceRecords.allRecordsInOrder().size());
        List<Long> lowWatermarks = records.stream()
                .map(rec -> rec.value() != null
                        ? (Long) ((Struct) ((Struct) rec.value()).get("source")).get("low_watermark")
                        : null)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        System.out.println("Num of watermark record: " + lowWatermarks.size());
        // assertThat(heartbeatRecords).isNotEmpty();
        // assertThat(lowWatermarks).hasSizeGreaterThanOrEqualTo(3); // insert + update
        // + delete + possible M records
        // validateLowWatermarks(records, lowWatermarks);
        System.out.println("**************FINAL***************");
        for (SourceRecord record : records) {
            System.out.println(
                    "Data change record: " + record.toString() + "for topic: " + getTopicName(config, tableName));
            System.out.println("");
        }
        assertThat(records).hasSizeGreaterThanOrEqualTo(4); // insert + update + delete + TOMBSTONE + possible M records
        stopConnector();
    }

    private void validateLowWatermarks(List<SourceRecord> records, List<Long> lowWatermarks) {
        for (SourceRecord record : records) {
            if (record.timestamp() != null) {
                Assert.assertTrue(record.timestamp().longValue() > lowWatermarks.get(0).longValue());
            }
        }
    }
}
