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

package org.apache.flink.connector.pulsar.sink.writer.selector;

import org.apache.flink.annotation.PublicEvolving;

import org.apache.pulsar.client.api.Message;

import java.io.Serializable;

/**
 * Select a partition where the message is sent.
 *
 * @param <IN>
 */
@PublicEvolving
@FunctionalInterface
public interface PartitionSelector<IN> extends Serializable {

    /**
     * Select a partition based on the current message and topic metadata.
     *
     * @param message current message
     * @param topicMetadata current topic metadata
     * @return index for pulsar topic partition
     */
    int select(Message<IN> message, TopicMetadata topicMetadata);
}
