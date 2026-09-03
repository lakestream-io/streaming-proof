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
package io.lakestream.streaming.proof.worker;

import io.lakestream.streaming.proof.common.ProofDriver;
import io.lakestream.streaming.proof.common.records.Driver;
import io.lakestream.streaming.proof.driver.kafka.KafkaProofDriver;
import io.lakestream.streaming.proof.driver.mqtt.MqttProofDriver;
import io.lakestream.streaming.proof.driver.pulsar.PulsarProofDriver;
import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DriverCache implements Closeable {

    private final Map<String, ProofDriver> cache = new ConcurrentHashMap<>();

    public ProofDriver getDriver(String driverName, Driver driver) {
        return cache.compute(driverName, (k, v) -> {
            if (v == null) {
                return createDriver(driver);
            }
            return v;
        });
    }

    private ProofDriver createDriver(Driver d) {
        ProofDriver driver;
        if ("kafka".equals(d.driverType())) {
            driver = new KafkaProofDriver();
        } else if ("pulsar".equals(d.driverType())) {
            driver = new PulsarProofDriver();
        } else if ("mqtt".equals(d.driverType())) {
            driver = new MqttProofDriver();
        } else {
            throw new IllegalArgumentException("Unsupported driver: " + d.driverType());
        }
        driver.init(d.driverConfigs());
        return driver;
    }

    @Override
    public void close() {
        for (ProofDriver driver : cache.values()) {
            try {
                driver.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close driver", e);
            }
        }
        cache.clear();
    }
}
