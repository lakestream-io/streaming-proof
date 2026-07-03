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
package io.streamnative.streaming.proof.common;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import io.streamnative.streaming.proof.common.records.Proof;
import io.streamnative.streaming.proof.common.records.ProofSummary;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for WebhookNotificationService.
 */
class WebhookNotificationServiceTest {

    @Mock
    private AsyncHttpClient mockHttpClient;
    
    @Mock
    private BoundRequestBuilder mockRequestBuilder;
    
    @Mock
    private ListenableFuture<Response> mockFuture;
    
    @Mock
    private Response mockResponse;

    private WebhookNotificationService webhookService;

    @BeforeMethod
    void setUp() {
        MockitoAnnotations.openMocks(this);
        webhookService = new WebhookNotificationService(mockHttpClient);
    }

    @Test
    void testSendProofCompletionNotification_WithValidSlackWebhook() throws Exception {
        // Arrange
        WebhookConfig webhookConfig = WebhookConfig.builder()
                .url("https://hooks.slack.com/test")
                .type("slack")
                .enabled(true)
                .build();
        
        Proof proof = Proof.builder()
                .id("test-proof-1")
                .name("Test Proof")
                .driver("kafka")
                .topic("test-topic")
                .duration(3600)
                .startTime(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                .webhookConfig(webhookConfig)
                .build();
        
        ProofSummary summary = new ProofSummary(
                1000L,  // verified
                0,      // errors
                0,      // outOfOrders
                0,      // missed
                0L,     // duplicates
                0L,     // writeDuplicates
                0,      // timeouts
                0L,     // verifiedStallSeconds
                0L      // maxVerifiedStallSeconds
        );
        
        when(mockHttpClient.executeRequest(any(Request.class))).thenReturn(mockFuture);
        when(mockFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(mockResponse));
        when(mockResponse.getStatusCode()).thenReturn(200);

        // Act
        CompletableFuture<Void> result = webhookService.sendProofCompletionNotification(proof, summary);

        // Assert
        assertNotNull(result);
        verify(mockHttpClient).executeRequest(any(Request.class));
    }

    @Test
    void testSendProofTimeoutNotification_WithValidDiscordWebhook() throws Exception {
        // Arrange
        WebhookConfig webhookConfig = WebhookConfig.builder()
                .url("https://discord.com/api/webhooks/test")
                .type("discord")
                .enabled(true)
                .build();
        
        Proof proof = Proof.builder()
                .id("test-proof-2")
                .name("Test Proof 2")
                .driver("pulsar")
                .topic("test-topic-2")
                .duration(1800)
                .startTime(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                .webhookConfig(webhookConfig)
                .build();
        
        when(mockHttpClient.executeRequest(any(Request.class))).thenReturn(mockFuture);
        when(mockFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(mockResponse));
        when(mockResponse.getStatusCode()).thenReturn(200);

        // Act
        CompletableFuture<Void> result = webhookService.sendProofTimeoutNotification(proof);

        // Assert
        assertNotNull(result);
        verify(mockHttpClient).executeRequest(any(Request.class));
    }

    @Test
    void testSendNotification_WithDisabledWebhook() {
        // Arrange
        WebhookConfig webhookConfig = WebhookConfig.builder()
                .url("https://hooks.slack.com/test")
                .type("slack")
                .enabled(false)
                .build();
        
        Proof proof = Proof.builder()
                .id("test-proof-3")
                .webhookConfig(webhookConfig)
                .build();

        // Act
        CompletableFuture<Void> result = webhookService.sendProofTimeoutNotification(proof);

        // Assert
        assertNotNull(result);
        assertTrue(result.isDone());
        verifyNoInteractions(mockHttpClient);
    }

    @Test
    void testSendNotification_WithNullWebhookConfig() {
        // Arrange
        Proof proof = Proof.builder()
                .id("test-proof-4")
                .webhookConfig(null)
                .build();

        // Act
        CompletableFuture<Void> result = webhookService.sendProofTimeoutNotification(proof);

        // Assert
        assertNotNull(result);
        assertTrue(result.isDone());
        verifyNoInteractions(mockHttpClient);
    }

    @Test
    void testWebhookConfigCreation() {
        // Test static factory methods
        WebhookConfig config1 = WebhookConfig.of("https://test.com");
        assertEquals("https://test.com", config1.getUrl());
        assertEquals("slack", config1.getType());
        assertTrue(config1.isEnabled());

        WebhookConfig config2 = WebhookConfig.of("https://test.com", "discord");
        assertEquals("https://test.com", config2.getUrl());
        assertEquals("discord", config2.getType());
        assertTrue(config2.isEnabled());
    }

    @Test
    void testClose() {
        // Act & Assert
        try {
            webhookService.close();
            // Verify
            verify(mockHttpClient).close();
        } catch (Exception e) {
            // Should not throw exception
            throw new AssertionError("close() should not throw exception", e);
        }
    }
}