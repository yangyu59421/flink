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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.sink.writer.selector.PartitionSelector;
import org.apache.flink.connector.pulsar.sink.writer.selector.TopicSelector;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;

import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_MESSAGE_ROUTING_MODE;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_TRANSACTION_TIMEOUT_MILLIS;
import static org.apache.flink.connector.pulsar.sink.config.PulsarSinkConfigUtils.checkConfigurations;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * PulsarSink's builder is used to simplify the creation of Sink.
 *
 * @param <IN>
 */
public class PulsarSinkBuilder<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSinkBuilder.class);

    private DeliveryGuarantee deliveryGuarantee;
    private TopicSelector<IN> topicSelector;
    private PulsarSerializationSchema<IN, ?> serializationSchema;
    private PartitionSelector<IN> partitionSelector;
    private final Configuration configuration;

    // private builder constructor.
    PulsarSinkBuilder() {
        this.configuration = new Configuration();
        this.deliveryGuarantee = DeliveryGuarantee.EXACTLY_ONCE;
    }

    /**
     * Sets the admin endpoint for the PulsarAdmin of the PulsarSink.
     *
     * @param adminUrl the url for the PulsarAdmin.
     * @return this PulsarSinkBuilder.
     */
    public PulsarSinkBuilder<IN> setAdminUrl(String adminUrl) {
        return setConfig(PULSAR_ADMIN_URL, adminUrl);
    }

    /**
     * Sets the server's link for the PulsarProducer of the PulsarSink.
     *
     * @param serviceUrl the server url of the Pulsar cluster.
     * @return this PulsarSinkBuilder.
     */
    public PulsarSinkBuilder<IN> setServiceUrl(String serviceUrl) {
        return setConfig(PULSAR_SERVICE_URL, serviceUrl);
    }

    /**
     * Set the topic for the PulsarSink.
     *
     * @param topic pulsar topic
     * @return this PulsarSinkBuilder.
     */
    public PulsarSinkBuilder<IN> setTopic(String topic) {
        this.topicSelector = e -> topic;
        return this;
    }

    /**
     * set a topic selector for the PulsarSink.
     *
     * @param topicSelector select a topic by record.
     * @return this PulsarSinkBuilder.
     */
    public PulsarSinkBuilder<IN> setTopic(TopicSelector<IN> topicSelector) {
        this.topicSelector = topicSelector;
        return this;
    }

    /**
     * Set a DeliverGuarantees for the PulsarSink.
     *
     * @param deliveryGuarantee deliver guarantees.
     * @return this PulsarSinkBuilder.
     */
    public PulsarSinkBuilder<IN> setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = deliveryGuarantee;
        return this;
    }

    /**
     * SerializationSchema is required for getting the {@link Schema} for serialize message from
     * pulsar and getting the {@link TypeInformation} for message serialization in flink.
     *
     * <p>We have defined a set of implementations, using {@code
     * PulsarSerializationSchema#pulsarSchema} or {@code PulsarSerializationSchema#flinkSchema} for
     * creating the desired schema.
     */
    public <T extends IN> PulsarSinkBuilder<T> setSerializationSchema(
            PulsarSerializationSchema<T, ?> serializationSchema) {
        PulsarSinkBuilder<T> self = specialized();
        self.serializationSchema = serializationSchema;
        return self;
    }

    /**
     * Write the message to a partition of the Pulsar topic using a random method.
     *
     * @return this PulsarSinkBuilder.
     */
    public PulsarSinkBuilder<IN> singlePartition() {
        return setConfig(PULSAR_MESSAGE_ROUTING_MODE, MessageRoutingMode.SinglePartition);
    }

    /**
     * Write the message to a partition of the Pulsar topic using a round Robin method.
     *
     * @return this PulsarSinkBuilder.
     */
    public PulsarSinkBuilder<IN> roundRobinPartition() {
        return setConfig(PULSAR_MESSAGE_ROUTING_MODE, MessageRoutingMode.RoundRobinPartition);
    }

    /**
     * Write the message to a partition of the Pulsar topic using user defined method.
     *
     * @return this PulsarSinkBuilder.
     */
    public <T extends IN> PulsarSinkBuilder<T> customPartition(
            PartitionSelector<T> partitionSelector) {
        setConfig(PULSAR_MESSAGE_ROUTING_MODE, MessageRoutingMode.CustomPartition);
        PulsarSinkBuilder<T> self = specialized();
        self.partitionSelector = partitionSelector;
        return self;
    }

    /**
     * Build the {@link PulsarSink}.
     *
     * @return a PulsarSink with the settings made for this builder.
     */
    @SuppressWarnings("java:S3776")
    public PulsarSink<IN> build() {
        // Check builder configuration.
        checkConfigurations(configuration);

        // Ensure the topic  for pulsar.
        checkNotNull(topicSelector, "No topic names or topic pattern are provided.");

        if (DeliveryGuarantee.EXACTLY_ONCE == deliveryGuarantee) {
            configuration.set(PulsarOptions.PULSAR_ENABLE_TRANSACTION, true);
            configuration.set(PulsarSinkOptions.PULSAR_SEND_TIMEOUT_MS, 0L);
            if (!configuration.contains(PULSAR_TRANSACTION_TIMEOUT_MILLIS)) {
                LOG.warn(
                        "The default pulsar transaction timeout is 3 hours, "
                                + "make sure it was greater than your checkpoint interval.");
            } else {
                Long timeout = configuration.get(PULSAR_TRANSACTION_TIMEOUT_MILLIS);
                LOG.warn(
                        "The configured transaction timeout is {} mille seconds, "
                                + "make sure it was greater than your checkpoint interval.",
                        timeout);
            }
        }
        // Make the configuration unmodifiable.
        UnmodifiableConfiguration config = new UnmodifiableConfiguration(configuration);
        return new PulsarSink<>(
                deliveryGuarantee, topicSelector, serializationSchema, partitionSelector, config);
    }

    /**
     * Set an arbitrary property for the PulsarSink and PulsarProducer. The valid keys can be found
     * in {@link PulsarSinkOptions} and {@link PulsarOptions}.
     *
     * <p>Make sure the option could be set only once or with same value.
     *
     * @param key the key of the property.
     * @param value the value of the property.
     * @return this PulsarSinkBuilder.
     */
    public <T> PulsarSinkBuilder<IN> setConfig(ConfigOption<T> key, T value) {
        checkNotNull(key);
        checkNotNull(value);
        if (configuration.contains(key)) {
            T oldValue = configuration.get(key);
            checkArgument(
                    Objects.equals(oldValue, value),
                    "This option %s has been already set to value %s.",
                    key.key(),
                    oldValue);
        } else {
            configuration.set(key, value);
        }
        return this;
    }

    /**
     * Set arbitrary properties for the PulsarSink and PulsarProducer. The valid keys can be found
     * in {@link PulsarSinkOptions} and {@link PulsarOptions}.
     *
     * @param config the config to set for the PulsarSink.
     * @return this PulsarSinkBuilder.
     */
    public PulsarSinkBuilder<IN> setConfig(Configuration config) {
        Map<String, String> existedConfigs = configuration.toMap();
        List<String> duplicatedKeys = new ArrayList<>();
        for (Map.Entry<String, String> entry : config.toMap().entrySet()) {
            String key = entry.getKey();
            String value2 = existedConfigs.get(key);
            if (value2 != null && !value2.equals(entry.getValue())) {
                duplicatedKeys.add(key);
            }
        }
        checkArgument(
                duplicatedKeys.isEmpty(),
                "Invalid configuration, these keys %s are already exist with different config value.",
                duplicatedKeys);
        configuration.addAll(config);
        return this;
    }

    // ------------- private helpers  --------------

    /** Helper method for java compiler recognize the generic type. */
    @SuppressWarnings("unchecked")
    private <T extends IN> PulsarSinkBuilder<T> specialized() {
        return (PulsarSinkBuilder<T>) this;
    }
}
