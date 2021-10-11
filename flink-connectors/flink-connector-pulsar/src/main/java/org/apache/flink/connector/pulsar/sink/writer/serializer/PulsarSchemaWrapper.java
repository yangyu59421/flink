/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.sink.writer.serializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchema;
import org.apache.flink.connector.pulsar.sink.writer.selector.MessageMetadata;

import org.apache.pulsar.client.api.Schema;

/**
 * The serialization schema wrapper for pulsar original {@link Schema}. Pulsar would serialize the
 * message and pass it to flink with a auto generate or given {@link TypeInformation}.
 *
 * @param <T> The output type of the message.
 */
@Internal
class PulsarSchemaWrapper<IN> extends PulsarSerializationSchemaBase<IN, IN> {
    private static final long serialVersionUID = -4864701207257059158L;

    /** The serializable pulsar schema, it wrap the schema with type class. */
    private final PulsarSchema<IN> pulsarSchema;

    @SuppressWarnings("java:S2065")
    private transient Schema<IN> schema;

    public PulsarSchemaWrapper(PulsarSchema<IN> pulsarSchema, MessageMetadata<IN> messageMetadata) {
        super(messageMetadata);
        this.pulsarSchema = pulsarSchema;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        if (schema == null) {
            this.schema = pulsarSchema.getPulsarSchema();
        }
    }

    @Override
    public Schema<IN> getSchema() {
        return schema;
    }

    @Override
    public IN serialize(IN element) {
        return element;
    }
}
