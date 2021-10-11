/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.sink.writer.serializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.connector.pulsar.sink.writer.selector.MessageMetadata;

import org.apache.pulsar.client.api.Schema;

/**
 * A {@link PulsarSerializationSchema} implementation which based on the given flink's {@link
 * PulsarSerializationSchema}. We would consume the message as byte array from pulsar and serialize
 * it by using flink serialization logic.
 *
 * @param <IN> The output type of the message.
 */
@Internal
class PulsarSerializationSchemaWrapper<IN> extends PulsarSerializationSchemaBase<IN, byte[]> {
    private static final long serialVersionUID = -630646912412751300L;

    private final SerializationSchema<IN> serializationSchema;

    public PulsarSerializationSchemaWrapper(
            SerializationSchema<IN> deserializationSchema, MessageMetadata<IN> messageMetadata) {
        super(messageMetadata);
        this.serializationSchema = deserializationSchema;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        // Initialize it for some custom logic.
        serializationSchema.open(context);
    }

    @Override
    public Schema<byte[]> getSchema() {
        return Schema.BYTES;
    }

    @Override
    public byte[] serialize(IN element) {
        return serializationSchema.serialize(element);
    }
}
