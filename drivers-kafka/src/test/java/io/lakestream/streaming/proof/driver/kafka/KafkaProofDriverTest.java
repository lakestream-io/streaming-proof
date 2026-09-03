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
package io.lakestream.streaming.proof.driver.kafka;

import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class KafkaProofDriverTest {

    @Test
    public void testCreateTopicPassesTopicConfigToKafka() throws Exception {
        AdminClient admin = mock(AdminClient.class);
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        when(admin.createTopics(anyCollection())).thenReturn(result);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));

        KafkaProofDriver driver = new KafkaProofDriver();
        setAdmin(driver, admin);
        Map<String, String> topicConfig = Map.of("unclean.leader.election.enable", "true");

        driver.createTopic("latency-topic", 8, topicConfig);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<NewTopic>> topicsCaptor =
                ArgumentCaptor.forClass(Collection.class);
        verify(admin).createTopics(topicsCaptor.capture());
        NewTopic topic = topicsCaptor.getValue().iterator().next();
        assertEquals(topic.name(), "latency-topic");
        assertEquals(topic.numPartitions(), 8);
        assertEquals(topic.configs(), topicConfig);
    }

    private static void setAdmin(KafkaProofDriver driver, AdminClient admin) throws Exception {
        Field adminField = KafkaProofDriver.class.getDeclaredField("admin");
        adminField.setAccessible(true);
        adminField.set(driver, admin);
    }
}
