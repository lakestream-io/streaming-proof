/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package io.lakestream.streaming.proof.common.records;

import io.lakestream.streaming.proof.common.ProofDriver;
import java.util.Map;

/**
 * Configuration record that holds the settings for workers and drivers in the streaming proof system.
 * This immutable record provides a structured way to manage distributed system configurations
 * across multiple workers and messaging system drivers.
 *
 * <p>The configuration consists of two main components:
 * <ul>
 *   <li>Worker configurations: Defines the network endpoints and settings for distributed workers</li>
 *   <li>Driver configurations: Specifies the messaging system drivers and their configurations</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * Map<String, String> workers = Map.of(
 *     "worker1", "http://worker1:8080",
 *     "worker2", "http://worker2:8080"
 * );
 * Map<String, Driver> drivers = Map.of(
 *     "kafka", new Driver("kafka", kafkaConfigs)
 * );
 * Map<String, Object> report = Map.of(
 *     "maxTimeSeriesPoints", 481,
 *     "latencyUnit", "ms"
 * );
 * Configs configs = new Configs(workers, drivers, report);
 * }</pre>
 *
 * @param workers A map of worker configurations where the key is the worker identifier
 *               (e.g., "worker1") and the value contains the worker's HTTP endpoint URL.
 *               These workers are responsible for executing the streaming proof tasks.
 * @param drivers A map of driver configurations where the key is the driver identifier
 *               (e.g., "kafka") and the value is a {@link Driver} instance containing
 *               the driver type and its specific configurations. The driver provides the
 *               interface to interact with the underlying messaging system.
 * @param report Optional report-related settings such as time-series sampling limits
 *               and display preferences.
 *
 * @see Driver
 * @see ProofDriver
 */
public record Configs(Map<String, String> workers, Map<String, Driver> drivers, Map<String, Object> report) {

    public Configs(Map<String, String> workers, Map<String, Driver> drivers) {
        this(workers, drivers, null);
    }

    public int reportIntSetting(String key, int defaultValue) {
        if (report == null) {
            return defaultValue;
        }

        Object value = report.get(key);
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String text) {
            try {
                return Integer.parseInt(text);
            } catch (NumberFormatException ignored) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    public String reportStringSetting(String key, String defaultValue) {
        if (report == null) {
            return defaultValue;
        }

        Object value = report.get(key);
        return value == null ? defaultValue : String.valueOf(value);
    }

    public boolean reportBooleanSetting(String key, boolean defaultValue) {
        if (report == null) {
            return defaultValue;
        }

        Object value = report.get(key);
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof String text) {
            return Boolean.parseBoolean(text);
        }
        return defaultValue;
    }

    public int reportSetting(String key, int defaultValue) {
        return reportIntSetting(key, defaultValue);
    }
}
