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

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import io.lakestream.streaming.proof.common.records.ConsumerCheckPoint;
import io.lakestream.streaming.proof.common.records.ProducerCheckpoint;
import java.util.Map;
import org.testng.annotations.Test;

public class WorkerTest {

    @Test
    public void testStopProducersIgnoresMissingProducer() {
        Worker worker = new Worker();
        worker.stopProducers("missing-proof");
    }

    @Test
    public void testProducerCheckpointReturnsEmptyCheckpointWhenProducerIsMissing() {
        Worker worker = new Worker();

        ProducerCheckpoint checkpoint = worker.producerCheckPoint("missing-proof");

        assertNotNull(checkpoint);
        assertTrue(checkpoint.getPublished().isEmpty());
        assertTrue(checkpoint.getErrorDetails().isEmpty());
    }

    @Test
    public void testConsumerCheckpointReturnsEmptyCheckpointWhenConsumerIsMissing() {
        Worker worker = new Worker();

        ConsumerCheckPoint checkpoint = worker.consumerCheckPoint("missing-proof");

        assertNotNull(checkpoint);
        assertTrue(checkpoint.getConsumed().isEmpty());
    }

    @Test
    public void testConsumerCheckpointDetailsReturnsEmptyMapWhenConsumerIsMissing() {
        Worker worker = new Worker();

        Map<String, ConsumerCheckPoint> checkpoints = worker.consumerCheckPointDetails("missing-proof");

        assertNotNull(checkpoints);
        assertTrue(checkpoints.isEmpty());
    }
}
