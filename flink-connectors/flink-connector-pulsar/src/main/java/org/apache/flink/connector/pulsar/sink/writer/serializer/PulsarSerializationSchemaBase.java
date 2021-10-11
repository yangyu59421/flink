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

import org.apache.flink.connector.pulsar.sink.writer.selector.MessageMetadata;

import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * abstract the process of generating pulsar Message to simplify metadata processing.
 *
 * @param <IN> record
 * @param <T> pulsar Message Type
 */
public abstract class PulsarSerializationSchemaBase<IN, T>
        implements PulsarSerializationSchema<IN, T> {

    protected final MessageMetadata<IN> messageMetadata;

    public PulsarSerializationSchemaBase(MessageMetadata<IN> messageMetadata) {
        this.messageMetadata = messageMetadata;
    }

    @Override
    public void serialize(IN element, TypedMessageBuilder<T> out) {
        out.value(serialize(element));
        handleMessageMetadata(element, out);
    }

    public abstract T serialize(IN element);

    protected void handleMessageMetadata(IN record, TypedMessageBuilder<T> out) {
        if (messageMetadata == null) {
            return;
        }
        setMetadata(record, messageMetadata.getOrderingKey(), out::orderingKey);
        setMetadata(record, messageMetadata.getKeyBytes(), out::keyBytes);
        setMetadata(record, messageMetadata.getKey(), out::key);
        setMetadata(record, messageMetadata.getEventTime(), out::eventTime);
        setMetadata(record, messageMetadata.getProperties(), out::properties);
        setMetadata(record, messageMetadata.getSequenceId(), out::sequenceId);
        setMetadata(record, messageMetadata.getOrderingKey(), out::orderingKey);
        setMetadata(record, messageMetadata.getReplicationClusters(), out::replicationClusters);
        setMetadata(
                record,
                messageMetadata.getDisableReplication(),
                v -> {
                    if (v) {
                        out.disableReplication();
                    }
                });
        setMetadata(
                record,
                messageMetadata.getDeliverAfterSeconds(),
                v -> out.deliverAfter(v, TimeUnit.SECONDS));
        setMetadata(record, messageMetadata.getDeliverAt(), out::deliverAt);
    }

    private <V> void setMetadata(
            IN element, Function<IN, V> selectValue, Consumer<V> consumeValue) {
        if (selectValue == null) {
            return;
        }
        final V value = selectValue.apply(element);
        if (value == null) {
            return;
        }
        consumeValue.accept(value);
    }
}
