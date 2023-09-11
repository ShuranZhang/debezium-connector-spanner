/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Connector {
    private static final Logger LOG = LoggerFactory.getLogger(Connection.class);

    private final String name;
    private final String test;
    private final Map<String, Object> configMap;

    private Connector(String name, String test, Map<String, Object> config) {
        this.name = name;
        this.test = test;
        this.configMap = new HashMap<>(config);
    }

    public Map<String, Object> getConfiguration() {
        if (this.name == null || this.test == null) {
            throw new IllegalStateException();
        }
        return Map.of("name", getName(), "config", configMap);
    }

    public String getName() {
        return test == null ? name : name + "_" + test;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder edit() {
        return new Builder(name, configMap);
    }

    public static class Builder {
        private String name;
        private String test;
        private Map<String, Object> configMap = new HashMap<>();

        public Builder() {
        }

        public Builder(String name, Map<String, Object> configMap) {
            this.name = name;
            this.configMap = new HashMap<>(configMap);
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder forTest(String test) {
            this.test = test;
            return this;
        }

        public Builder connectorClass(String clazz) {
            this.configMap.put("connector.class", clazz);
            return this;
        }

        public Builder taskMax(int tasksMax) {
            this.configMap.put("tasks.max", tasksMax);
            return this;
        }

        public Builder with(String property, String value) {
            this.configMap.put(property, value);
            return this;
        }

        public Builder withDatabase(Database database) {
            this.with("gcp.spanner.project.id", database.getProjectId())
                    .with("gcp.spanner.instance.id", database.getInstanceId())
                    .with("gcp.spanner.database.id", database.getDatabaseId())
                    .with("gcp.spanner.emulator.host", "http://localhost:9010");

            return this;
        }

        public Connector build() {
            return new Connector(name, test, configMap);
        }
    }

}
