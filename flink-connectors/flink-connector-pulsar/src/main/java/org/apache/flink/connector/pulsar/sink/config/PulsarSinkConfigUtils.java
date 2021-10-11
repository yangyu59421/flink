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

package org.apache.flink.connector.pulsar.sink.config;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import java.util.List;
import java.util.Set;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.connector.pulsar.common.config.PulsarConfigUtils.setOptionValue;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAMS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAM_MAP;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_BATCHING_ENABLED;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_BATCHING_MAX_MESSAGES;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_BATCHING_MAX_PUBLISH_DELAY_MICROS;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_BLOCK_IF_QUEUE_FULL;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_COMPRESSION_TYPE;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_CRYPTO_FAILURE_ACTION;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_ENABLE_CHUNKING;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_HASHING_SCHEME;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_MAX_PENDING_MESSAGES;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_MESSAGE_ROUTING_MODE;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_PRODUCER_NAME;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_SEND_TIMEOUT_MS;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_TOPIC_NAME;

/** Create source related {@link Consumer} and validate config. */
@Internal
public final class PulsarSinkConfigUtils {

    private PulsarSinkConfigUtils() {
        // No need to create instance.
    }

    private static final List<Set<ConfigOption<?>>> CONFLICT_SINK_OPTIONS =
            ImmutableList.<Set<ConfigOption<?>>>builder()
                    .add(ImmutableSet.of(PULSAR_AUTH_PARAMS, PULSAR_AUTH_PARAM_MAP))
                    .build();

    private static final Set<ConfigOption<?>> REQUIRED_SINK_OPTIONS =
            ImmutableSet.<ConfigOption<?>>builder()
                    .add(PULSAR_SERVICE_URL)
                    .add(PULSAR_ADMIN_URL)
                    .build();

    /**
     * Helper method for checking client related config options. We would validate:
     *
     * <ul>
     *   <li>If user have provided the required client config options.
     *   <li>If user have provided some conflict options.
     * </ul>
     */
    public static void checkConfigurations(Configuration configuration) {
        REQUIRED_SINK_OPTIONS.forEach(
                option ->
                        Preconditions.checkArgument(
                                configuration.contains(option),
                                "Config option %s is not provided for pulsar source.",
                                option));

        CONFLICT_SINK_OPTIONS.forEach(
                options -> {
                    long nums = options.stream().filter(configuration::contains).count();
                    Preconditions.checkArgument(
                            nums <= 1,
                            "Conflict config options %s were provided, we only support one of them for creating pulsar source.",
                            options);
                });
    }

    /** Create a pulsar consumer builder by using the given Configuration. */
    public static <T> ProducerBuilder<T> createProducerBuilder(
            PulsarClient client, Schema<T> schema, Configuration configuration) {
        ProducerBuilder<T> builder = client.newProducer(schema);

        setOptionValue(configuration, PULSAR_TOPIC_NAME, builder::topic);
        setOptionValue(configuration, PULSAR_PRODUCER_NAME, builder::producerName);
        setOptionValue(
                configuration,
                PULSAR_SEND_TIMEOUT_MS,
                v -> builder.sendTimeout(v.intValue(), MILLISECONDS));
        setOptionValue(configuration, PULSAR_BLOCK_IF_QUEUE_FULL, builder::blockIfQueueFull);
        setOptionValue(configuration, PULSAR_MAX_PENDING_MESSAGES, builder::maxPendingMessages);
        setOptionValue(
                configuration,
                PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS,
                builder::maxPendingMessagesAcrossPartitions);
        setOptionValue(configuration, PULSAR_MESSAGE_ROUTING_MODE, builder::messageRoutingMode);
        setOptionValue(configuration, PULSAR_HASHING_SCHEME, builder::hashingScheme);
        setOptionValue(configuration, PULSAR_CRYPTO_FAILURE_ACTION, builder::cryptoFailureAction);
        setOptionValue(
                configuration,
                PULSAR_BATCHING_MAX_PUBLISH_DELAY_MICROS,
                v -> builder.batchingMaxPublishDelay(v, MICROSECONDS));
        setOptionValue(configuration, PULSAR_BATCHING_MAX_MESSAGES, builder::batchingMaxMessages);
        setOptionValue(configuration, PULSAR_BATCHING_ENABLED, builder::enableBatching);
        setOptionValue(configuration, PULSAR_COMPRESSION_TYPE, builder::compressionType);
        setOptionValue(configuration, PULSAR_ENABLE_CHUNKING, builder::enableChunking);
        return builder;
    }
}
