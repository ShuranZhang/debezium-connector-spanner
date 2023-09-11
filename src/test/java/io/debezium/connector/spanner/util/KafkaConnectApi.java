/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class KafkaConnectApi {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectApi.class);

    public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    private final ContainerState containerState;
    private final int kafkaHttpPort;

    private final OkHttpClient httpClient = new OkHttpClient.Builder()
            .callTimeout(300, TimeUnit.SECONDS)
            .build();

    public KafkaConnectApi(ContainerState containerState, int kafkaConnectApiPort) {
        this.containerState = containerState;
        this.kafkaHttpPort = kafkaConnectApiPort;
    }

    private String getAddress() {
        return containerState.getHost() + ":" + containerState.getMappedPort(kafkaHttpPort);
    }

    private String getConnectorEndpoint(String connectorName) {
        return "http://" + getAddress() + "/connectors/" + connectorName;
    }

    private String getConnectorsEndpoint() {
        return "http://" + getAddress() + "/connectors/";
    }

    private String getConnectorsEndpointStatus() {
        return "http://" + getAddress() + "/connectors/?expand=status";
    }

    public boolean isConnectorConfigured(String connectorName) {
        String connectorEndpoint = getConnectorEndpoint(connectorName);

        final Request request = new Request.Builder().url(connectorEndpoint).build();
        return get(request);
    }

    public String getLogs() {
        return containerState.getLogs();
    }

    public boolean isTaskStarted(String connectorName, int index) {

        Pattern pattern = Pattern.compile(".*WorkerSourceTask\\{id=" + connectorName + "-" + index
                + "\\}\\sSource\\stask\\sfinished\\sinitialization\\sand\\sstart.*");

        return this.containerState.getLogs().lines()
                .filter(line -> line.contains("WorkerSourceTask"))
                .anyMatch(line -> pattern.matcher(line).find());
    }

    public void registerConnector(Connector connector) {
        String json;
        try {
            json = new ObjectMapper().writeValueAsString(connector.getConfiguration());
            LOG.info(json);
        }
        catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }

        post(json, getConnectorsEndpoint());
    }

    public void deleteConnector(String connectorName) {
        delete(connectorName);
    }

    public boolean isKafkaConnectReady() {
        final Request request = new Request.Builder().url(getConnectorsEndpointStatus()).build();
        return get(request);
    }

    public boolean isConnectorHasRunningStatus(String connectorName) {
        final Request request = new Request.Builder().url(getConnectorsEndpointStatus()).build();
        try (Response response = getWithResponse(request)) {
            if (!response.isSuccessful()) {
                return false;
            }
            String body = response.body().string();
            LOG.info("Response body: {}", body);

            JsonNode connectorNameNode = new ObjectMapper().readTree(body).get(connectorName);
            if (connectorNameNode == null) {
                Thread.sleep(3_000);
                return false;
            }
            return connectorNameNode.get("status").get("connector").get("state").asText().equals("RUNNING");
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Error parse response body", e);
        }
        catch (IOException e) {
            throw new RuntimeException("Error connecting to kafka connect container", e);
        }
        catch (InterruptedException e) {
            return false;
        }
    }

    public JsonNode connectorStatus(String connectorName) {
        final Request request = new Request.Builder().url(getConnectorsEndpointStatus()).build();
        try (Response response = getWithResponse(request)) {
            if (!response.isSuccessful()) {
                throw new RuntimeException(response.message());
            }
            String body = response.body().string();
            LOG.info("Response body: {}", body);

            JsonNode connectorNameNode = new ObjectMapper().readTree(body).get(connectorName);
            return connectorNameNode;
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Error parse response body", e);
        }
        catch (IOException e) {
            throw new RuntimeException("Error connecting to kafka connect container", e);
        }
    }

    private boolean get(Request request) {
        try {
            LOG.info("GET {}", request.url());
            try (Response response = httpClient.newCall(request).execute()) {
                LOG.info("Response body: {}", response.body().string());
                return response.isSuccessful();
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Error connecting to kafka connect container", e);
        }
    }

    private Response getWithResponse(Request request) {
        try {
            LOG.info("GET {}", request.url());
            return httpClient.newCall(request).execute();
        }
        catch (IOException e) {
            throw new RuntimeException("Error connecting to kafka connect container", e);
        }
    }

    private void post(String payload, String url) {
        LOG.info("POST {} \n {}", url, payload);
        final RequestBody body = RequestBody.create(payload, JSON);
        final Request request = new Request.Builder().url(url).post(body).build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String exMessage = response.body() != null ? response.body().string() : null;
                LOG.warn("Response body: {}", exMessage);
                throw new RuntimeException(exMessage);
            }
        }
        catch (IOException ex) {
            throw new RuntimeException("Error connecting to kafka connect container", ex);
        }
    }

    private void delete(final String name) {
        final Request request = new Request.Builder().url(getConnectorEndpoint(name)).delete().build();
        try {
            Response execute = httpClient.newCall(request).execute();
            System.out.println("delete " + name + " is " + (execute.isSuccessful() ? "success" : "unsuccessful"));
        }
        catch (IOException ex) {
            throw new RuntimeException("Error connecting to kafka connect container", ex);
        }
    }

    private void put(final String payload, final String url) {
        LOG.info("PUT {} \n {}", url, payload);
        final RequestBody body = RequestBody.create(payload, JSON);
        final Request request = new Request.Builder().url(url).put(body).build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                LOG.warn("Response body: {}", response.body().string());
                throw new RuntimeException();
            }
        }
        catch (IOException ex) {
            throw new RuntimeException("Error connecting to kafka connect container", ex);
        }
    }

    public void startConnector(Connector connector) {
        put("", getResumeUrl(connector));
    }

    private String getStopUrl(Connector connector) {
        return "http://" + getAddress() + "/connectors/" + connector.getName() + "/pause";
    }

    private String getResumeUrl(Connector connector) {
        return "http://" + getAddress() + "/connectors/" + connector.getName() + "/resume";
    }

    public void stopConnector(Connector connector) {
        put("", getStopUrl(connector));
    }

}
