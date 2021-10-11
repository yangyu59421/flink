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

package org.apache.flink.connector.pulsar.sink.writer.selector;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** unit test for {@link MessageMetadataBuilder}. */
class MessageMetadataBuilderTest {

    @Test
    public void testBuilder() {
        final long currentTimeMillis = System.currentTimeMillis();

        final MessageMetadata<String> metadata =
                MessageMetadata.<String>newBuilder()
                        .setKey(s -> s)
                        .setOrderingKey(s -> s.getBytes(StandardCharsets.UTF_8))
                        .setProperties(
                                s -> {
                                    Map<String, String> properties = new HashMap<>();
                                    properties.put("key", s);
                                    return properties;
                                })
                        .setEventTime(s -> currentTimeMillis)
                        .setSequenceId(s -> currentTimeMillis)
                        .setReplicationClusters(s -> Collections.emptyList())
                        .setDisableReplication(s -> false)
                        .setDeliverAfterSeconds(s -> 100L)
                        .build();

        final String value = "test";
        assertEquals("test", metadata.getKey().apply(value));
        assertArrayEquals(
                "test".getBytes(StandardCharsets.UTF_8), metadata.getOrderingKey().apply(value));
        final Map<String, String> properties = metadata.getProperties().apply(value);
        assertEquals(1, properties.size());
        assertEquals(value, properties.get("key"));
        assertEquals(currentTimeMillis, metadata.getEventTime().apply(value));
        assertEquals(currentTimeMillis, metadata.getSequenceId().apply(value));
        assertEquals(Collections.emptyList(), metadata.getReplicationClusters().apply(value));
        assertEquals(false, metadata.getDisableReplication().apply(value));
        assertEquals(100L, metadata.getDeliverAfterSeconds().apply(value));
    }

    @Test
    void keyAndKeyBytesCouldChooseOnlyOne() {
        final MessageMetadataBuilder<String> metadataBuilder =
                MessageMetadata.<String>newBuilder()
                        .setKey(s -> s)
                        .setKeyBytes(s -> s.getBytes(StandardCharsets.UTF_8));

        assertThrows(IllegalStateException.class, metadataBuilder::build);
    }

    @Test
    void deliverAfterSecondsAndKeyBytesCouldChooseOnlyOne() {
        final MessageMetadataBuilder<String> metadataBuilder =
                MessageMetadata.<String>newBuilder()
                        .setDeliverAfterSeconds(s -> 100L)
                        .setDeliverAt(s -> 100L);

        assertThrows(IllegalStateException.class, metadataBuilder::build);
    }
}
