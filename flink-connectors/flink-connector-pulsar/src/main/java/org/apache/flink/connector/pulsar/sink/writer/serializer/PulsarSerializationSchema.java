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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchema;
import org.apache.flink.connector.pulsar.sink.writer.selector.MessageMetadata;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.schema.KeyValue;

import java.io.Serializable;

/**
 * A schema bridge for serializing the pulsar's {@code Message<T>} into a flink managed instance. We
 * support both the pulsar's self managed schema and flink managed schema.
 *
 * @param <T> The output message type for sinking to downstream flink operator.
 */
@PublicEvolving
public interface PulsarSerializationSchema<IN, T> extends Serializable {

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #serialize} and thus suitable for one time setup work.
     *
     * <p>The provided {@link InitializationContext} can be used to access additional features such
     * as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     */
    default void open(InitializationContext context) throws Exception {
        // Nothing to do here for the default implementation.
    }

    Schema<T> getSchema();

    /**
     * Serializes the incoming element to a specified type.
     *
     * @param element The incoming element to be serialized
     * @param out pulsar message to be sent
     */
    void serialize(IN element, TypedMessageBuilder<T> out);

    /**
     * Create a PulsarDeserializationSchema by using the flink's {@link SerializationSchema}. It
     * would consume the pulsar message as byte array and decode the message by using flink's logic.
     */
    static <IN> PulsarSerializationSchema<IN, byte[]> flinkSchema(
            SerializationSchema<IN> serializationSchema) {
        return new PulsarSerializationSchemaWrapper<>(serializationSchema, null);
    }

    /**
     * Create a PulsarDeserializationSchema by using the flink's {@link SerializationSchema}. It
     * would consume the pulsar message as byte array and decode the message by using flink's logic.
     */
    static <IN> PulsarSerializationSchema<IN, byte[]> flinkSchema(
            SerializationSchema<IN> serializationSchema, MessageMetadata<IN> messageMetadata) {
        return new PulsarSerializationSchemaWrapper<>(serializationSchema, messageMetadata);
    }

    /**
     * Create a PulsarDeserializationSchema by using the Pulsar {@link Schema} instance. The message
     * bytes must be encoded by pulsar Schema.
     *
     * <p>We only support <a
     * href="https://pulsar.apache.org/docs/en/schema-understand/#primitive-type">primitive
     * types</a> here.
     */
    static <IN> PulsarSerializationSchema<IN, IN> pulsarSchema(Schema<IN> schema) {
        PulsarSchema<IN> pulsarSchema = new PulsarSchema<>(schema);
        return new PulsarSchemaWrapper<>(pulsarSchema, null);
    }

    /**
     * Create a PulsarDeserializationSchema by using the Pulsar {@link Schema} instance. The message
     * bytes must be encoded by pulsar Schema.
     *
     * <p>We only support <a
     * href="https://pulsar.apache.org/docs/en/schema-understand/#primitive-type">primitive
     * types</a> here.
     */
    static <IN> PulsarSerializationSchema<IN, IN> pulsarSchema(
            Schema<IN> schema, MessageMetadata<IN> messageMetadata) {
        PulsarSchema<IN> pulsarSchema = new PulsarSchema<>(schema);
        return new PulsarSchemaWrapper<>(pulsarSchema, messageMetadata);
    }

    /**
     * Create a PulsarDeserializationSchema by using the Pulsar {@link Schema} instance. The message
     * bytes must be encoded by pulsar Schema.
     *
     * <p>We only support <a
     * href="https://pulsar.apache.org/docs/en/schema-understand/#struct">struct types</a> here.
     */
    static <IN> PulsarSerializationSchema<IN, IN> pulsarSchema(
            Schema<IN> schema, Class<IN> typeClass) {
        PulsarSchema<IN> pulsarSchema = new PulsarSchema<>(schema, typeClass);
        return new PulsarSchemaWrapper<>(pulsarSchema, null);
    }

    /**
     * Create a PulsarDeserializationSchema by using the Pulsar {@link Schema} instance. The message
     * bytes must be encoded by pulsar Schema.
     *
     * <p>We only support <a
     * href="https://pulsar.apache.org/docs/en/schema-understand/#struct">struct types</a> here.
     */
    static <IN> PulsarSerializationSchema<IN, IN> pulsarSchema(
            Schema<IN> schema, Class<IN> typeClass, MessageMetadata<IN> messageMetadata) {
        PulsarSchema<IN> pulsarSchema = new PulsarSchema<>(schema, typeClass);
        return new PulsarSchemaWrapper<>(pulsarSchema, messageMetadata);
    }

    /**
     * Create a PulsarDeserializationSchema by using the Pulsar {@link Schema} instance. The message
     * bytes must be encoded by pulsar Schema.
     *
     * <p>We only support <a
     * href="https://pulsar.apache.org/docs/en/schema-understand/#keyvalue">keyvalue types</a> here.
     */
    static <K, V> PulsarSerializationSchema<KeyValue<K, V>, KeyValue<K, V>> pulsarSchema(
            Schema<KeyValue<K, V>> schema, Class<K> keyClass, Class<V> valueClass) {
        PulsarSchema<KeyValue<K, V>> pulsarSchema =
                new PulsarSchema<>(schema, keyClass, valueClass);
        return new PulsarSchemaWrapper<>(pulsarSchema, null);
    }

    /**
     * Create a PulsarDeserializationSchema by using the Pulsar {@link Schema} instance. The message
     * bytes must be encoded by pulsar Schema.
     *
     * <p>We only support <a
     * href="https://pulsar.apache.org/docs/en/schema-understand/#keyvalue">keyvalue types</a> here.
     */
    static <K, V> PulsarSerializationSchema<KeyValue<K, V>, KeyValue<K, V>> pulsarSchema(
            Schema<KeyValue<K, V>> schema,
            Class<K> keyClass,
            Class<V> valueClass,
            MessageMetadata<KeyValue<K, V>> messageMetadata) {
        PulsarSchema<KeyValue<K, V>> pulsarSchema =
                new PulsarSchema<>(schema, keyClass, valueClass);
        return new PulsarSchemaWrapper<>(pulsarSchema, messageMetadata);
    }

    /**
     * Create a PulsarDeserializationSchema by using the given {@link TypeInformation}. This method
     * is only used for treating message that was written into pulsar by {@link TypeInformation}.
     */
    static <IN> PulsarSerializationSchema<IN, byte[]> flinkTypeInfo(
            TypeInformation<IN> information, ExecutionConfig config) {
        return new PulsarTypeInformationWrapper<>(information, config, null);
    }

    /**
     * Create a PulsarDeserializationSchema by using the given {@link TypeInformation}. This method
     * is only used for treating message that was written into pulsar by {@link TypeInformation}.
     */
    static <IN> PulsarSerializationSchema<IN, byte[]> flinkTypeInfo(
            TypeInformation<IN> information,
            ExecutionConfig config,
            MessageMetadata<IN> messageMetadata) {
        return new PulsarTypeInformationWrapper<>(information, config, messageMetadata);
    }
}
