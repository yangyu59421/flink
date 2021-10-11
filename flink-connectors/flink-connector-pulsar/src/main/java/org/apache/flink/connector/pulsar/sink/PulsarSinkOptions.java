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

package org.apache.flink.connector.pulsar.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PRODUCER_CONFIG_PREFIX;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.SINK_CONFIG_PREFIX;

/** the options for Pulsar Sink. */
@PublicEvolving
@ConfigGroups(
        groups = {
            @ConfigGroup(name = "PulsarSink", keyPrefix = SINK_CONFIG_PREFIX),
            @ConfigGroup(name = "PulsarProducer", keyPrefix = PRODUCER_CONFIG_PREFIX)
        })
public final class PulsarSinkOptions {

    // Pulsar sink connector config prefix.
    public static final String SINK_CONFIG_PREFIX = "pulsar.sink.";
    // Pulsar producer API config prefix.
    public static final String PRODUCER_CONFIG_PREFIX = "pulsar.producer.";

    private PulsarSinkOptions() {
        // This is a constant class
    }

    ///////////////////////////////////////////////////////////////////////////////
    //
    // The configuration for pulsar sink part.
    // All the configuration listed below should have the pulsar.sink prefix.
    //
    ///////////////////////////////////////////////////////////////////////////////

    public static final ConfigOption<Boolean> PULSAR_FAIL_ON_WRITE =
            ConfigOptions.key(SINK_CONFIG_PREFIX + "failOnWrite")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "This option is used to set whether to throw an exception to trigger a termination or retry of the Pulsar sink operator when writing a message to Pulsar Topic fails. "
                                    + "The behavior of the termination or retry is determined by the Flink checkpiont configuration.");

    public static final ConfigOption<Long> PULSAR_TRANSACTION_TIMEOUT_MILLIS =
            ConfigOptions.key(SINK_CONFIG_PREFIX + "transactionTimeoutMillis")
                    .longType()
                    .defaultValue(Duration.ofHours(3).toMillis())
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "This option is used for when using Shared or Key_Shared subscription."
                                                    + " You should set this option when you didn't enable the %s option.",
                                            code("pulsar.source.enableAutoAcknowledgeMessage"))
                                    .linebreak()
                                    .text(
                                            "This value should be greater than the checkpoint interval.")
                                    .text("It uses milliseconds as the unit of time.")
                                    .build());

    ///////////////////////////////////////////////////////////////////////////////
    //
    // The configuration for pulsar producer part.
    // All the configuration listed below should have the pulsar.sink prefix.
    //
    ///////////////////////////////////////////////////////////////////////////////

    public static final ConfigOption<String> PULSAR_TOPIC_NAME =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "topicName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The interval in milliseconds for the Pulsar source to discover "
                                    + "the new partitions. A non-positive value disables the partition discovery.");
    public static final ConfigOption<String> PULSAR_PRODUCER_NAME =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "producerName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Producer name.");
    public static final ConfigOption<Long> PULSAR_SEND_TIMEOUT_MS =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "sendTimeoutMs")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription(
                            Description.builder()
                                    .text("Message send timeout in ms.")
                                    .linebreak()
                                    .text(
                                            "If a message is not acknowledged by a server before the sendTimeout expires, an error occurs.")
                                    .build());
    public static final ConfigOption<Boolean> PULSAR_BLOCK_IF_QUEUE_FULL =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "blockIfQueueFull")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If it is set to true, when the outgoing message queue is full, the Send and SendAsync methods of producer block, rather than failing and throwing errors.")
                                    .linebreak()
                                    .text(
                                            "If it is set to false, when the outgoing message queue is full, the Send and SendAsync methods of producer fail and ProducerQueueIsFullError exceptions occur.")
                                    .linebreak()
                                    .text(
                                            "The MaxPendingMessages parameter determines the size of the outgoing message queue.")
                                    .build());
    public static final ConfigOption<Integer> PULSAR_MAX_PENDING_MESSAGES =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "maxPendingMessages")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            Description.builder()
                                    .text("The maximum size of a queue holding pending messages.")
                                    .linebreak()
                                    .text(
                                            "For example, a message waiting to receive an acknowledgment from a broker.")
                                    .linebreak()
                                    .text(
                                            "By default, when the queue is full, all calls to the Send and SendAsync "
                                                    + "methods fail unless you set BlockIfQueueFull to true.")
                                    .build());

    public static final ConfigOption<Integer> PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "maxPendingMessagesAcrossPartitions")
                    .intType()
                    .defaultValue(50000)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The maximum number of pending messages across partitions.")
                                    .linebreak()
                                    .text(
                                            "Use the setting to lower the max pending messages for each partition ({@link "
                                                    + "#setMaxPendingMessages(int)}) if the total number exceeds the configured value.")
                                    .build());

    public static final ConfigOption<MessageRoutingMode> PULSAR_MESSAGE_ROUTING_MODE =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "messageRoutingMode")
                    .enumType(MessageRoutingMode.class)
                    .defaultValue(MessageRoutingMode.RoundRobinPartition)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Message routing logic for producers on partitioned topics.")
                                    .linebreak()
                                    .text("Apply the logic only when setting no key on messages.")
                                    .linebreak()
                                    .text("Available options are as follows:")
                                    .linebreak()
                                    .list(
                                            text("pulsar.RoundRobinDistribution: round robin"),
                                            text(
                                                    "pulsar.UseSinglePartition: publish all messages to a single partition"),
                                            text(
                                                    "pulsar.CustomPartition: a custom partitioning scheme"))
                                    .build());

    public static final ConfigOption<HashingScheme> PULSAR_HASHING_SCHEME =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "hashingScheme")
                    .enumType(HashingScheme.class)
                    .defaultValue(HashingScheme.JavaStringHash)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Hashing function determining the partition where you publish a particular "
                                                    + "message (partitioned topics only).")
                                    .linebreak()
                                    .text("Available options are as follows:")
                                    .linebreak()
                                    .list(
                                            text(
                                                    "pulsar.JavaStringHash: the equivalent of String.hashCode() in Java"),
                                            text(
                                                    "pulsar.Murmur3_32Hash: applies the Murmur3 hashing function"))
                                    .build());
    public static final ConfigOption<ProducerCryptoFailureAction> PULSAR_CRYPTO_FAILURE_ACTION =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "cryptoFailureAction")
                    .enumType(ProducerCryptoFailureAction.class)
                    .defaultValue(ProducerCryptoFailureAction.FAIL)
                    .withDescription(
                            Description.builder()
                                    .text("Producer should take action when encryption fails.")
                                    .linebreak()
                                    .list(
                                            text(
                                                    "FAIL: if encryption fails, unencrypted messages fail to send."),
                                            text(
                                                    "SEND: if encryption fails, unencrypted messages are sent."))
                                    .build());
    public static final ConfigOption<Long> PULSAR_BATCHING_MAX_PUBLISH_DELAY_MICROS =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "batchingMaxPublishDelayMicros")
                    .longType()
                    .defaultValue(TimeUnit.MILLISECONDS.toMicros(1))
                    .withDescription("Batching time period of sending messages.");
    public static final ConfigOption<Integer> PULSAR_BATCHING_MAX_MESSAGES =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "batchingMaxMessages")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("The maximum number of messages permitted in a batch.");

    public static final ConfigOption<Boolean> PULSAR_BATCHING_ENABLED =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "batchingEnabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable batching of messages.");

    public static final ConfigOption<CompressionType> PULSAR_COMPRESSION_TYPE =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "enableChunking")
                    .enumType(CompressionType.class)
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("Message data compression type used by a producer.")
                                    .linebreak()
                                    .text("Available options:")
                                    .list(text("LZ4"), text("ZLIB"), text("ZSTD"), text("SNAPPY"))
                                    .build());
    public static final ConfigOption<Boolean> PULSAR_ENABLE_CHUNKING =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "enableChunking")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If message size is higher than allowed max publish-payload size by broker"
                                                    + " then enableChunking helps producer to split message into "
                                                    + "multiple chunks and publish them to broker separately and in order.")
                                    .linebreak()
                                    .text(
                                            "So, it allows client to successfully publish large size of messages in "
                                                    + "pulsar.")
                                    .build());
}
