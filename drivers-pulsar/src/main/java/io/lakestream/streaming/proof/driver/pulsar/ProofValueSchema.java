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
package io.lakestream.streaming.proof.driver.pulsar;

import io.lakestream.streaming.proof.common.ProofValue;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Pulsar schema for {@link ProofValue}: a sequence number followed by optional padding.
 *
 * <p>Declared as {@link SchemaType#BYTES} rather than a structured type because the
 * padding length varies with configuration and carries no meaning. At the default
 * message size the encoding is byte-identical to {@code Schema.INT64}.
 */
public class ProofValueSchema implements Schema<ProofValue> {

    private static final SchemaInfo SCHEMA_INFO = SchemaInfo.builder()
            .name("ProofValue")
            .type(SchemaType.BYTES)
            .schema(new byte[0])
            .build();

    @Override
    public byte[] encode(ProofValue message) {
        return message == null ? null : message.encode();
    }

    @Override
    public ProofValue decode(byte[] bytes) {
        return bytes == null ? null : ProofValue.decode(bytes);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return SCHEMA_INFO;
    }

    @Override
    public Schema<ProofValue> clone() {
        return new ProofValueSchema();
    }

    @Override
    public void validate(byte[] message) {
        decode(message);
    }

    @Override
    public boolean supportSchemaVersioning() {
        return false;
    }

    @Override
    public void configureSchemaInfo(String topic, String componentName, SchemaInfo schemaInfo) {
        // Nothing to configure: the schema carries no per-topic state.
    }

    @Override
    public boolean requireFetchingSchemaInfo() {
        return false;
    }
}
