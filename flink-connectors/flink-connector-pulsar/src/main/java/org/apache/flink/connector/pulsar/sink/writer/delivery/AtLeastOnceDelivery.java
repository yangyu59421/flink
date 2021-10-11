package org.apache.flink.connector.pulsar.sink.writer.delivery;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.PulsarSinkCommittable;
import org.apache.flink.connector.pulsar.sink.writer.PulsarWriterState;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** at least once delivery. */
public class AtLeastOnceDelivery implements Delivery {

    @Override
    public DeliveryGuarantee supportGuarantee() {
        return DeliveryGuarantee.AT_LEAST_ONCE;
    }

    @Override
    public <T> TypedMessageBuilder<T> newMessage(Producer<T> producer)
            throws PulsarClientException {
        return producer.newMessage();
    }

    @Override
    public List<PulsarSinkCommittable> prepareCommit(boolean flush) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public List<PulsarWriterState> snapshotState(long checkpointId) throws IOException {
        return Collections.emptyList();
    }
}
