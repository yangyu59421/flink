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
