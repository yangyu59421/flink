package org.apache.flink.connector.pulsar.sink.writer.selector;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.util.InstantiationUtil.isSerializable;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * build for {@link MessageMetadata}.
 *
 * @param <IN>
 */
public class MessageMetadataBuilder<IN> {

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

    public MessageMetadataBuilder<IN> setKey(SerializableFunction<IN, String> key) {
        this.key = key;
        return this;
    }

    public MessageMetadataBuilder<IN> setKeyBytes(SerializableFunction<IN, byte[]> keyBytes) {
        this.keyBytes = keyBytes;
        return this;
    }

    public MessageMetadataBuilder<IN> setOrderingKey(SerializableFunction<IN, byte[]> orderingKey) {
        this.orderingKey = orderingKey;
        return this;
    }

    public MessageMetadataBuilder<IN> setProperties(
            SerializableFunction<IN, Map<String, String>> properties) {
        this.properties = properties;
        return this;
    }

    public MessageMetadataBuilder<IN> setEventTime(SerializableFunction<IN, Long> eventTime) {
        this.eventTime = eventTime;
        return this;
    }

    public MessageMetadataBuilder<IN> setSequenceId(SerializableFunction<IN, Long> sequenceId) {
        this.sequenceId = sequenceId;
        return this;
    }

    public MessageMetadataBuilder<IN> setReplicationClusters(
            SerializableFunction<IN, List<String>> replicationClusters) {
        this.replicationClusters = replicationClusters;
        return this;
    }

    public MessageMetadataBuilder<IN> setDisableReplication(
            SerializableFunction<IN, Boolean> disableReplication) {
        this.disableReplication = disableReplication;
        return this;
    }

    public MessageMetadataBuilder<IN> setDeliverAfterSeconds(
            SerializableFunction<IN, Long> deliverAfterSeconds) {
        this.deliverAfterSeconds = deliverAfterSeconds;
        return this;
    }

    public MessageMetadataBuilder<IN> setDeliverAt(SerializableFunction<IN, Long> deliverAt) {
        this.deliverAt = deliverAt;
        return this;
    }

    public MessageMetadata<IN> build() {
        if (key != null && keyBytes != null) {
            throw new IllegalStateException("Only one of key and keyBytes can be selected");
        }
        if (deliverAfterSeconds != null && deliverAt != null) {
            throw new IllegalStateException(
                    "Only one of deliverAfterSeconds and deliverAt can be selected");
        }
        // Since these implementation could be a lambda, make sure they are serializable.
        checkState(isSerializable(key), "key isn't serializable");
        checkState(isSerializable(keyBytes), "keyBytes isn't serializable");
        checkState(isSerializable(orderingKey), "orderingKey isn't serializable");
        checkState(isSerializable(properties), "properties isn't serializable");
        checkState(isSerializable(eventTime), "eventTime isn't serializable");
        checkState(isSerializable(sequenceId), "sequenceId isn't serializable");
        checkState(isSerializable(replicationClusters), "replicationClusters isn't serializable");
        checkState(isSerializable(disableReplication), "disableReplication isn't serializable");
        checkState(isSerializable(deliverAfterSeconds), "deliverAfterSeconds isn't serializable");
        checkState(isSerializable(deliverAt), "deliverAt isn't serializable");

        MessageMetadata<IN> metadata = new MessageMetadata<>();
        metadata.setKey(key);
        metadata.setKeyBytes(keyBytes);
        metadata.setOrderingKey(orderingKey);
        metadata.setProperties(properties);
        metadata.setEventTime(eventTime);
        metadata.setSequenceId(sequenceId);
        metadata.setReplicationClusters(replicationClusters);
        metadata.setDisableReplication(disableReplication);
        metadata.setDeliverAfterSeconds(deliverAfterSeconds);
        metadata.setDeliverAt(deliverAt);
        return metadata;
    }

    /**
     * Support Serializable Function.
     *
     * @param <T>
     * @param <R>
     */
    @FunctionalInterface
    public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {}
}
