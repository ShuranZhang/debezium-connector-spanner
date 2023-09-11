/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerState;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;

public class SchemaRegistryApi {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryApi.class);

    public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    private final ContainerState containerState;
    private final int kafkaHttpPort;

    private final OkHttpClient httpClient = new OkHttpClient.Builder()
            .callTimeout(300, TimeUnit.SECONDS)
            .build();

    public SchemaRegistryApi(ContainerState containerState, int schemaRegistryApiPort) {
        this.containerState = containerState;
        this.kafkaHttpPort = schemaRegistryApiPort;
    }

    private String getAddress() {
        return containerState.getHost() + ":" + containerState.getMappedPort(kafkaHttpPort);
    }

    public String getSchemaRegistryEndpoint() {
        return "http://" + getAddress();
    }
}