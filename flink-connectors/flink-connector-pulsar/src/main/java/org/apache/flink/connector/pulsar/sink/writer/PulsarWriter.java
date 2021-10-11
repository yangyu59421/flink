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

package org.apache.flink.connector.pulsar.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.common.config.PulsarConfigUtils;
import org.apache.flink.connector.pulsar.common.schema.IncompatibleSchemaException;
import org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils;
import org.apache.flink.connector.pulsar.sink.PulsarSinkCommittable;
import org.apache.flink.connector.pulsar.sink.config.PulsarSinkConfigUtils;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.delivery.AtLeastOnceDelivery;
import org.apache.flink.connector.pulsar.sink.writer.delivery.Delivery;
import org.apache.flink.connector.pulsar.sink.writer.delivery.ExactlyOnceDelivery;
import org.apache.flink.connector.pulsar.sink.writer.selector.PartitionSelector;
import org.apache.flink.connector.pulsar.sink.writer.selector.TopicSelector;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.flink.shaded.guava30.com.google.common.io.Closer;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static org.apache.flink.connector.base.DeliveryGuarantee.EXACTLY_ONCE;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * a pulsar SinkWriter implement.
 *
 * @param <IN> record data type.
 */
@Internal
public class PulsarWriter<IN> implements SinkWriter<IN, PulsarSinkCommittable, PulsarWriterState> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarWriter.class);

    private transient Map<String, Producer<?>> topic2Producer;

    private transient PulsarAdmin admin;

    private transient BiConsumer<MessageId, Throwable> sendCallback;
    private final AtomicLong pendingRecords = new AtomicLong();
    //    private ConcurrentHashMap<TxnID, List<MessageId>> tid2MessagesMap;
    //
    //    private ConcurrentHashMap<TxnID, List<CompletableFuture<MessageId>>> tid2FuturesMap;

    //    private Transaction currentTransaction;

    private List<CompletableFuture<MessageId>> futures;
    //    private ConcurrentHashMap<PulsarWriterState, List<CompletableFuture<MessageId>>>
    // tid2FuturesMap;
    // ---------------- //
    private final Configuration configuration;
    private final SinkConfiguration sinkConfiguration;
    private final TopicSelector<IN> topicSelector;
    private final PulsarSerializationSchema<IN, ?> serializationSchema;

    private final PartitionSelector<IN> partitionSelector;
    private final UserCodeClassLoader userCodeClassLoader;

    private final Delivery delivery;

    private transient PulsarClientImpl pulsarClient;

    private final Closer closer = Closer.create();

    public PulsarWriter(
            DeliveryGuarantee deliveryGuarantee,
            TopicSelector<IN> topicSelector,
            PulsarSerializationSchema<IN, ?> serializationSchema,
            PartitionSelector<IN> partitionSelector,
            Configuration configuration,
            Sink.InitContext sinkInitContext) {
        this.topicSelector = topicSelector;
        this.serializationSchema = serializationSchema;
        this.sinkConfiguration = new SinkConfiguration(configuration);
        this.partitionSelector = partitionSelector;
        this.configuration = configuration;
        this.userCodeClassLoader = sinkInitContext.getUserCodeClassLoader();
        this.topic2Producer = new HashMap<>();

        this.futures = Collections.synchronizedList(new ArrayList<>());
        try {
            admin = PulsarConfigUtils.createAdmin(configuration);
            closer.register(admin);
            serializationSchema.open(
                    new SerializationSchema.InitializationContext() {
                        @Override
                        public MetricGroup getMetricGroup() {
                            return null;
                        }

                        @Override
                        public UserCodeClassLoader getUserCodeClassLoader() {
                            return userCodeClassLoader;
                        }
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (deliveryGuarantee == EXACTLY_ONCE) {
            this.delivery = new ExactlyOnceDelivery(sinkConfiguration, this::getPulsarClient);
        } else {
            this.delivery = new AtLeastOnceDelivery();
        }
        closer.register(delivery);
        this.sendCallback =
                initializeSendCallback(sinkConfiguration, sinkInitContext.getMailboxExecutor());
    }

    @Override
    public void write(IN value, Context context) throws IOException {
        String topic = topicSelector.selector(value);
        TypedMessageBuilder messageBuilder = delivery.newMessage(getProducer(topic));
        serializationSchema.serialize(value, messageBuilder);

        CompletableFuture<MessageId> messageIdFuture = messageBuilder.sendAsync();
        pendingRecords.incrementAndGet();
        futures.add(messageIdFuture);
        messageIdFuture.whenComplete(sendCallback);
    }

    public void initializeState(List<PulsarWriterState> states) throws IOException {
        checkNotNull(states, "The retrieved state was null.");
        for (PulsarWriterState state : states) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Restoring: {}", state);
            }
        }
    }

    @Override
    public List<PulsarSinkCommittable> prepareCommit(boolean flush) throws IOException {
        if (!flush) {
            return Collections.emptyList();
        }
        producerFlush();
        List<PulsarSinkCommittable> committables = delivery.prepareCommit(flush);
        LOG.debug("Committing {} committables, final commit={}.", committables, flush);
        return committables;
    }

    @Override
    public List<PulsarWriterState> snapshotState(long checkpointId) throws IOException {
        return delivery.snapshotState(checkpointId);
    }

    @Override
    public void close() throws Exception {
        producerFlush();
        closer.close();
    }

    public void producerFlush() throws IOException {
        for (Producer<?> p : topic2Producer.values()) {
            p.flush();
        }

        for (CompletableFuture<MessageId> completableFuture : futures) {
            try {
                completableFuture.get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        futures.clear();

        if (pendingRecords.get() > 0) {
            throw new RuntimeException("The message could not be sent");
        }
    }

    // ------------------------------internal method------------------------------

    private synchronized PulsarClientImpl getPulsarClient() {
        if (pulsarClient != null) {
            return pulsarClient;
        }
        pulsarClient = (PulsarClientImpl) PulsarConfigUtils.createClient(configuration);
        closer.register(pulsarClient);
        return pulsarClient;
    }

    protected Producer<?> getProducer(String topic) throws PulsarClientException {
        Producer<?> producer = topic2Producer.get(topic);
        if (producer != null && producer.isConnected()) {
            return producer;
        }

        final ProducerBuilder<?> producerBuilder =
                PulsarSinkConfigUtils.createProducerBuilder(
                        getPulsarClient(), serializationSchema.getSchema(), configuration);
        producerBuilder.topic(topic);

        if (partitionSelector != null) {
            producerBuilder.messageRouter(generateMessageRouter());
        }
        producer = producerBuilder.create();
        closer.register(producer);
        uploadSchema(topic);
        topic2Producer.put(topic, producer);
        return producer;
    }

    @SuppressWarnings("unchecked")
    private MessageRouter generateMessageRouter() {
        return new MessageRouter() {
            @Override
            public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                return partitionSelector.select(
                        (Message<IN>) msg,
                        new org.apache.flink.connector.pulsar.sink.writer.selector.TopicMetadata(
                                metadata.numPartitions()));
            }
        };
    }

    protected BiConsumer<MessageId, Throwable> initializeSendCallback(
            SinkConfiguration sinkConfiguration, MailboxExecutor mailboxExecutor) {
        if (sinkConfiguration.isFailOnWrite()) {
            return (t, u) -> {
                if (u == null) {
                    acknowledgeMessage();
                    return;
                }
                mailboxExecutor.execute(
                        () -> {
                            throw new FlinkRuntimeException(u);
                        },
                        "Failed to send data to Pulsar");
            };
        } else {
            return (t, u) -> {
                if (u != null) {
                    LOG.error(
                            "Error while sending message to Pulsar: {}",
                            ExceptionUtils.stringifyException(u));
                }
                acknowledgeMessage();
            };
        }
    }

    private void acknowledgeMessage() {
        synchronized (pendingRecords) {
            if (pendingRecords.decrementAndGet() == 0L) {
                pendingRecords.notifyAll();
            }
        }
    }

    private void uploadSchema(String topic) {
        try {
            admin.schemas().createSchema(topic, serializationSchema.getSchema().getSchemaInfo());
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 409) {
                throw new IncompatibleSchemaException("Incompatible schema used", e);
            }
            PulsarExceptionUtils.sneakyThrow(e);
        }
    }
}
