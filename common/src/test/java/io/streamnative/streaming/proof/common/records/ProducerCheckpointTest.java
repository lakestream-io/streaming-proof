/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.streamnative.streaming.proof.common.records;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class ProducerCheckpointTest {

    @Test
    public void testMergeRetainsErrorTimingDetails() {
        ProducerCheckpoint left = new ProducerCheckpoint();
        left.addErrors("boom", 2);
        left.addErrorDetails("boom", new ErrorOccurrence(2, 100L, 200L));

        ProducerCheckpoint right = new ProducerCheckpoint();
        right.addErrors("boom", 3);
        right.addErrorDetails("boom", new ErrorOccurrence(3, 150L, 400L));

        left.merge(right);

        assertEquals(left.getErrors().get("boom").intValue(), 5);
        assertEquals(left.getErrorDetails().get("boom").getCount(), 5);
        assertEquals(left.getErrorDetails().get("boom").getFirstSeenAtMillis(), 100L);
        assertEquals(left.getErrorDetails().get("boom").getLastSeenAtMillis(), 400L);
    }
}
