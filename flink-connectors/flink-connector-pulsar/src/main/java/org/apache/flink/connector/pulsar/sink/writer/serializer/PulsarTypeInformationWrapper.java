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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils;
import org.apache.flink.connector.pulsar.sink.writer.selector.MessageMetadata;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.apache.pulsar.client.api.Schema;

import java.io.IOException;

/**
 * Wrap the flink TypeInformation into a {@code PulsarDeserializationSchema}. We would create a
 * flink {@code TypeSerializer} by using given ExecutionConfig. This execution config could be
 * {@link ExecutionEnvironment#getConfig()}.
 */
public class PulsarTypeInformationWrapper<IN> extends PulsarSerializationSchemaBase<IN, byte[]> {
    private static final long serialVersionUID = 6647084180084963022L;

    private final TypeInformation<IN> information;
    private final TypeSerializer<IN> serializer;

    /**
     * PulsarSerializationSchema would be shared for multiple SplitReaders in different fetcher
     * thread. Use a thread-local DataInputDeserializer would be better.
     */
    @SuppressWarnings("java:S5164")
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(1024 * 1024));

    public PulsarTypeInformationWrapper(
            TypeInformation<IN> information,
            ExecutionConfig config,
            MessageMetadata<IN> messageMetadata) {
        super(messageMetadata);
        this.information = information;
        this.serializer = information.createSerializer(config);
    }

    @Override
    public Schema<byte[]> getSchema() {
        return Schema.BYTES;
    }

    @Override
    public byte[] serialize(IN element) {
        final DataOutputSerializer dataOutputSerializer = SERIALIZER.get();
        try {
            serializer.serialize(element, dataOutputSerializer);
            return dataOutputSerializer.getCopyOfBuffer();
        } catch (IOException e) {
            PulsarExceptionUtils.sneakyThrow(e);
            return null; // Never run to here.
        }
    }
}
