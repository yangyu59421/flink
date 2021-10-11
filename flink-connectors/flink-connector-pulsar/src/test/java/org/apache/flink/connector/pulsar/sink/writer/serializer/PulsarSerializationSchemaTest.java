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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.pulsar.SampleMessage;
import org.apache.flink.connector.pulsar.sink.writer.selector.MessageMetadata;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema.flinkSchema;
import static org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema.flinkTypeInfo;
import static org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema.pulsarSchema;
import static org.apache.pulsar.client.api.Schema.PROTOBUF_NATIVE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** test for PulsarSerializationSchema. */
class PulsarSerializationSchemaTest {

    private MessageMetadata<String> metadata;
    private long currentTimeMillis = 1632796702000L;

    @BeforeEach
    void setUp() {
        Long currentTimeMillis = 1632796702000L;
        this.metadata =
                MessageMetadata.<String>newBuilder()
                        .setKey(s -> s)
                        .setOrderingKey(s -> s.getBytes(StandardCharsets.UTF_8))
                        .setProperties(
                                s -> {
                                    Map<String, String> properties = new HashMap<>();
                                    properties.put("key", s);
                                    return properties;
                                })
                        .setEventTime(
                                s -> {
                                    final Long time = currentTimeMillis;
                                    return time;
                                })
                        .setSequenceId(s -> currentTimeMillis)
                        .setReplicationClusters(s -> Collections.emptyList())
                        .setDisableReplication(s -> false)
                        .setDeliverAt(s -> currentTimeMillis)
                        .build();
    }

    @Test
    void createFromFlinkSerializationSchema() throws Exception {
        PulsarSerializationSchema<String, byte[]> schema =
                flinkSchema(new SimpleStringSchema(), metadata);
        schema.open(new DummySchemaContext());
        assertDoesNotThrow(() -> InstantiationUtil.clone(schema));

        final MockTypedMessageBuilder<byte[]> messageBuilder = new MockTypedMessageBuilder<>();

        schema.serialize("some-sample-message", messageBuilder);

        assertNotNull(messageBuilder.value);
        assertEquals(
                new String(messageBuilder.value, StandardCharsets.UTF_8), "some-sample-message");
        assertMessageMetadata(messageBuilder, "some-sample-message");
    }

    @Test
    void createFromPulsarSchema() throws Exception {
        Schema<SampleMessage.TestMessage> schema1 =
                PROTOBUF_NATIVE(SampleMessage.TestMessage.class);
        PulsarSerializationSchema<SampleMessage.TestMessage, SampleMessage.TestMessage> schema2 =
                pulsarSchema(schema1, SampleMessage.TestMessage.class);
        schema2.open(new DummySchemaContext());
        assertDoesNotThrow(() -> InstantiationUtil.clone(schema2));

        SampleMessage.TestMessage message1 =
                SampleMessage.TestMessage.newBuilder()
                        .setStringField(randomAlphabetic(10))
                        .setDoubleField(ThreadLocalRandom.current().nextDouble())
                        .setIntField(ThreadLocalRandom.current().nextInt())
                        .build();
        final MockTypedMessageBuilder<SampleMessage.TestMessage> messageBuilder =
                new MockTypedMessageBuilder<>();
        schema2.serialize(message1, messageBuilder);

        assertNotNull(messageBuilder.value);
        assertEquals(messageBuilder.value, message1);
    }

    @Test
    void createFromFlinkTypeInformation() throws Exception {
        PulsarSerializationSchema<String, byte[]> schema =
                flinkTypeInfo(Types.STRING, null, metadata);
        schema.open(new DummySchemaContext());
        assertDoesNotThrow(() -> InstantiationUtil.clone(schema));

        final MockTypedMessageBuilder<byte[]> messageBuilder = new MockTypedMessageBuilder<>();
        schema.serialize("some-sample-message", messageBuilder);

        assertNotNull(messageBuilder.value);
        DataInputDeserializer serializer = new DataInputDeserializer(messageBuilder.value);
        assertEquals(StringValue.readString(serializer), "some-sample-message");
        assertMessageMetadata(messageBuilder, "some-sample-message");
    }

    private <T> void assertMessageMetadata(
            MockTypedMessageBuilder<T> messageBuilder, String value) {
        assertEquals(value, messageBuilder.key);
        assertArrayEquals(value.getBytes(StandardCharsets.UTF_8), messageBuilder.orderingKey);
        final Map<String, String> properties = messageBuilder.properties;
        assertEquals(1, properties.size());
        assertEquals(value, properties.get("key"));
        assertEquals(currentTimeMillis, messageBuilder.eventTime);
        assertEquals(currentTimeMillis, messageBuilder.sequenceId);
        assertEquals(Collections.emptyList(), messageBuilder.replicationClusters);
        assertFalse(messageBuilder.disableReplication);
        assertEquals(currentTimeMillis, messageBuilder.deliverAt);
    }

    private static class MockTypedMessageBuilder<T> implements TypedMessageBuilder<T> {

        protected T value;
        protected String key;
        protected byte[] orderingKey;
        protected Map<String, String> properties = new HashMap<>();
        protected long eventTime;
        protected long sequenceId;
        protected List<String> replicationClusters;
        protected boolean disableReplication;
        protected long deliverAt;

        @Override
        public MessageId send() throws PulsarClientException {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public CompletableFuture<MessageId> sendAsync() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TypedMessageBuilder<T> key(String key) {
            this.key = key;
            return this;
        }

        @Override
        public TypedMessageBuilder<T> keyBytes(byte[] key) {
            this.key = Base64.getEncoder().encodeToString(key);
            return this;
        }

        @Override
        public TypedMessageBuilder<T> orderingKey(byte[] orderingKey) {
            this.orderingKey = orderingKey;
            return this;
        }

        @Override
        public TypedMessageBuilder<T> value(T value) {
            this.value = value;
            return this;
        }

        @Override
        public TypedMessageBuilder<T> property(String name, String value) {
            this.properties.put(name, value);
            return this;
        }

        @Override
        public TypedMessageBuilder<T> properties(Map<String, String> properties) {
            this.properties.putAll(properties);
            return this;
        }

        @Override
        public TypedMessageBuilder<T> eventTime(long eventTime) {
            this.eventTime = eventTime;
            return this;
        }

        @Override
        public TypedMessageBuilder<T> sequenceId(long sequenceId) {
            this.sequenceId = sequenceId;
            return this;
        }

        @Override
        public TypedMessageBuilder<T> replicationClusters(List<String> clusters) {
            this.replicationClusters = clusters;
            return this;
        }

        @Override
        public TypedMessageBuilder<T> disableReplication() {
            this.disableReplication = true;
            return this;
        }

        @Override
        public TypedMessageBuilder<T> deliverAt(long timestamp) {
            this.deliverAt = timestamp;
            return this;
        }

        @Override
        public TypedMessageBuilder<T> deliverAfter(long delay, TimeUnit unit) {
            this.deliverAt(System.currentTimeMillis() + unit.toMillis(delay));
            return this;
        }

        @Override
        public TypedMessageBuilder<T> loadConf(Map<String, Object> config) {
            throw new UnsupportedOperationException("Not implemented.");
        }
    }

    private static class DummySchemaContext implements SerializationSchema.InitializationContext {

        @Override
        public MetricGroup getMetricGroup() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            throw new UnsupportedOperationException("Not implemented.");
        }
    }
}
