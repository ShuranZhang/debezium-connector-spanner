/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.spanner.util;

// import java.io.IOException;
// import java.util.Map;
// import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import org.testcontainers.containers.ContainerState;
// import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;
// import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

// import okhttp3.MediaType;
// import okhttp3.OkHttpClient;
// import okhttp3.Request;
// import okhttp3.RequestBody;
// import okhttp3.Response;
// import test.dto.GeneratorStatusResponseDto;

public class GeneratorApi {

    private static final Logger LOG = LoggerFactory.getLogger(GeneratorApi.class);

    // public static final MediaType JSON = MediaType.get("application/json;
    // charset=utf-8");

    // private final ContainerState containerState;
    // private final int generatorHttpPort;

    // private final OkHttpClient httpClient = new OkHttpClient.Builder()
    // .callTimeout(300, TimeUnit.SECONDS)
    // .build();

    // public GeneratorApi(ContainerState containerState, int kafkaConnectApiPort) {
    // this.containerState = containerState;
    // this.generatorHttpPort = kafkaConnectApiPort;
    // }

    // private String getAddress() {
    // return containerState.getHost() + ":" +
    // containerState.getMappedPort(generatorHttpPort);
    // }

    // private String getRegisterEndpoint() {
    // return "http://" + getAddress() + "/init";
    // }

    // private String getStatusEndpoint() {
    // return "http://" + getAddress() + "/test";
    // }

    // private String getStateEndpoint() {
    // return "http://" + getAddress() + "/process";
    // }

    // private String getStartEndpoint() {
    // return "http://" + getAddress() + "/start";
    // }

    // public boolean isConnectorConfigured() {
    // String connectorEndpoint = getStatusEndpoint();
    // final Request request = new Request.Builder().url(connectorEndpoint).build();
    // return get(request);
    // }

    // private String getWithBody(Request request) throws IOException {
    // String result = "";
    // LOG.info("GET {}", request.url());
    // try (Response response = httpClient.newCall(request).execute()) {
    // result = response.body().string();
    // LOG.info("Response body: {}", result);
    // }
    // return result;
    // }

    // private boolean get(Request request) {
    // try {
    // LOG.info("GET {}", request.url());
    // try (Response response = httpClient.newCall(request).execute()) {
    // LOG.info("Response body: {}", response.body().string());
    // return response.isSuccessful();
    // }
    // } catch (IOException e) {
    // throw new RuntimeException("Error connecting to kafka connect container", e);
    // }
    // }

    // private String post(final String payload, final String url) {
    // LOG.info("POST {} \n {}", url, payload);
    // final RequestBody body = RequestBody.create(payload, JSON);
    // final Request request = new Request.Builder().url(url).post(body).build();
    // int i = 10;
    // while (true) {
    // try (Response response = httpClient.newCall(request).execute()) {
    // if (!response.isSuccessful()) {
    // LOG.warn("Response body: {}", response.body().string());
    // if (i == 0) {
    // throw new RuntimeException();
    // }
    // }
    // return response.body().string();
    // } catch (IOException ex) {
    // if (i == 0) {
    // throw new RuntimeException("Error connecting to kafka connect container",
    // ex);
    // }
    // }
    // i--;
    // }
    // }

    // public void config(String json) {
    // post(json, getRegisterEndpoint());
    // }

    // public String startGenerate(Map<String, Object> body) {
    // String json;
    // try {
    // json = new ObjectMapper().writeValueAsString(body);
    // } catch (JsonProcessingException ex) {
    // ex.printStackTrace();
    // throw new RuntimeException(ex);
    // }

    // return post(json, getStartEndpoint());
    // }

    // public Boolean isFinished(GeneratorStatusResponseDto dto) {
    // try {
    // String withBody = getWithBody(new
    // Request.Builder().url(getStateEndpoint()).build());
    // return !withBody.contains(dto.getId());
    // } catch (IOException e) {
    // e.printStackTrace();
    // return false;
    // }
    // }
}
