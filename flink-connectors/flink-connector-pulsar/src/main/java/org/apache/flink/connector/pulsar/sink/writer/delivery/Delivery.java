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

package org.apache.flink.connector.pulsar.sink.writer.delivery;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.PulsarSinkCommittable;
import org.apache.flink.connector.pulsar.sink.writer.PulsarWriterState;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/** Delivery Guarantee for pulsar sink. */
public interface Delivery extends Serializable, Closeable {

    DeliveryGuarantee supportGuarantee();

    <T> TypedMessageBuilder<T> newMessage(Producer<T> producer) throws PulsarClientException;

    List<PulsarSinkCommittable> prepareCommit(boolean flush) throws IOException;

    default List<PulsarWriterState> snapshotState(long checkpointId) throws IOException {
        return Collections.emptyList();
    }

    default void initializeState(List<PulsarWriterState> states) throws IOException {}

    @Override
    default void close() throws IOException {}
}
