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
