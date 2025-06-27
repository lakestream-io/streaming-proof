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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Configuration for webhook notifications.
 * Supports different webhook types like Slack, Discord, etc.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WebhookConfig {
    
    /** The webhook URL to send notifications to */
    private String url;
    
    /** The type of webhook (slack, discord, etc.) */
    @Builder.Default
    private String type = "slack";
    
    /** Whether webhook notifications are enabled */
    @Builder.Default
    private boolean enabled = true;
    
    /**
     * Creates a new WebhookConfig with the specified URL and default type (slack).
     * 
     * @param url The webhook URL
     * @return A new WebhookConfig instance
     */
    public static WebhookConfig of(String url) {
        return WebhookConfig.builder()
                .url(url)
                .type("slack")
                .enabled(true)
                .build();
    }
    
    /**
     * Creates a new WebhookConfig with the specified URL and type.
     * 
     * @param url The webhook URL
     * @param type The webhook type
     * @return A new WebhookConfig instance
     */
    public static WebhookConfig of(String url, String type) {
        return WebhookConfig.builder()
                .url(url)
                .type(type)
                .enabled(true)
                .build();
    }
}