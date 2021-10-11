package org.apache.flink.connector.pulsar.sink.writer.selector;

import java.io.Serializable;

/** Metadata of a topic that can be used for partition selector. */
public class TopicMetadata implements Serializable {

    private final int numPartitions;

    public TopicMetadata(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public int getNumPartitions() {
        return numPartitions;
    }
}
