package org.apache.flink.connector.pulsar.sink.writer.delivery;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils;
import org.apache.flink.connector.pulsar.sink.PulsarSinkCommittable;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.PulsarWriterState;
import org.apache.flink.util.function.SerializableSupplier;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.flink.connector.base.DeliveryGuarantee.EXACTLY_ONCE;
import static org.apache.flink.connector.pulsar.common.utils.PulsarTransactionUtils.getId;

/** exactly once delivery. */
public class ExactlyOnceDelivery implements Delivery {

    private static final Logger LOG = LoggerFactory.getLogger(ExactlyOnceDelivery.class);

    private final SinkConfiguration sinkConfiguration;

    private transient Supplier<PulsarClientImpl> pulsarClient;

    private Transaction currentTransaction;

    public ExactlyOnceDelivery(
            SinkConfiguration sinkConfiguration,
            SerializableSupplier<PulsarClientImpl> pulsarClient) {
        this.sinkConfiguration = sinkConfiguration;
        this.pulsarClient = pulsarClient;
    }

    @Override
    public DeliveryGuarantee supportGuarantee() {
        return EXACTLY_ONCE;
    }

    @Override
    public <T> TypedMessageBuilder<T> newMessage(Producer<T> producer)
            throws PulsarClientException {
        return producer.newMessage(getCurrentTransaction());
    }

    @Override
    public List<PulsarSinkCommittable> prepareCommit(boolean flush) throws IOException {
        if (!flush) {
            return Collections.emptyList();
        }
        if (currentTransaction == null) {
            LOG.debug("not init currentTransaction");
            return Collections.emptyList();
        }
        final TxnID txnID = getId(currentTransaction);
        return ImmutableList.of(new PulsarSinkCommittable(Collections.emptyList(), txnID));
    }

    @Override
    public List<PulsarWriterState> snapshotState(long checkpointId) throws IOException {
        LOG.debug("transaction is beginning in EXACTLY_ONCE mode");
        TxnID txnID = getId(currentTransaction);
        try {
            currentTransaction = createTransaction();
        } catch (Exception e) {
            PulsarExceptionUtils.sneakyThrow(e);
        }
        return Collections.singletonList(new PulsarWriterState(txnID));
    }

    @Override
    public void initializeState(List<PulsarWriterState> states) throws IOException {
        if (states.isEmpty()) {
            return;
        }
    }

    // ------------------------------internal method------------------------------

    /**
     * For each checkpoint we create new {@link Transaction} so that new transactions will not clash
     * with transactions created during previous checkpoints.
     */
    private Transaction createTransaction() throws Exception {
        return pulsarClient
                .get()
                .newTransaction()
                .withTransactionTimeout(
                        sinkConfiguration.getTransactionTimeoutMillis(), TimeUnit.MILLISECONDS)
                .build()
                .get();
    }

    private Transaction getCurrentTransaction() {
        try {
            if (currentTransaction == null) {
                currentTransaction = createTransaction();
            }

        } catch (Exception e) {
            PulsarExceptionUtils.sneakyThrow(e);
        }
        return currentTransaction;
    }

    @Override
    public void close() throws IOException {}
}
