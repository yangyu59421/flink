package org.apache.flink.connector.pulsar.sink.writer.selector;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Pulsar Message Metadata support tool. This allows users to customize the metadata for each
 * Message in Pulsar Topic based on the current data.
 *
 * @param <IN> record
 */
public class MessageMetadata<IN> implements Serializable {

    private Function<IN, String> key;
    private Function<IN, byte[]> keyBytes;
    private Function<IN, byte[]> orderingKey;
    private Function<IN, Map<String, String>> properties;
    private Function<IN, Long> eventTime;
    private Function<IN, Long> sequenceId;
    private Function<IN, List<String>> replicationClusters;
    private Function<IN, Boolean> disableReplication;
    private Function<IN, Long> deliverAfterSeconds;
    private Function<IN, Long> deliverAt;

    public Function<IN, String> getKey() {
        return key;
    }

    public void setKey(Function<IN, String> key) {
        this.key = key;
    }

    public Function<IN, byte[]> getKeyBytes() {
        return keyBytes;
    }

    public void setKeyBytes(Function<IN, byte[]> keyBytes) {
        this.keyBytes = keyBytes;
    }

    public Function<IN, byte[]> getOrderingKey() {
        return orderingKey;
    }

    public void setOrderingKey(Function<IN, byte[]> orderingKey) {
        this.orderingKey = orderingKey;
    }

    public Function<IN, Map<String, String>> getProperties() {
        return properties;
    }

    public void setProperties(Function<IN, Map<String, String>> properties) {
        this.properties = properties;
    }

    public Function<IN, Long> getEventTime() {
        return eventTime;
    }

    public void setEventTime(Function<IN, Long> eventTime) {
        this.eventTime = eventTime;
    }

    public Function<IN, Long> getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(Function<IN, Long> sequenceId) {
        this.sequenceId = sequenceId;
    }

    public Function<IN, List<String>> getReplicationClusters() {
        return replicationClusters;
    }

    public void setReplicationClusters(Function<IN, List<String>> replicationClusters) {
        this.replicationClusters = replicationClusters;
    }

    public Function<IN, Boolean> getDisableReplication() {
        return disableReplication;
    }

    public void setDisableReplication(Function<IN, Boolean> disableReplication) {
        this.disableReplication = disableReplication;
    }

    public Function<IN, Long> getDeliverAfterSeconds() {
        return deliverAfterSeconds;
    }

    public void setDeliverAfterSeconds(Function<IN, Long> deliverAfterSeconds) {
        this.deliverAfterSeconds = deliverAfterSeconds;
    }

    public Function<IN, Long> getDeliverAt() {
        return deliverAt;
    }

    public void setDeliverAt(Function<IN, Long> deliverAt) {
        this.deliverAt = deliverAt;
    }

    public static <IN> MessageMetadataBuilder<IN> newBuilder() {
        return new MessageMetadataBuilder<>();
    }
}
