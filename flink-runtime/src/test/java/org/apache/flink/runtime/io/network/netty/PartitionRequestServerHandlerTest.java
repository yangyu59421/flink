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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ResumeConsumption;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link PartitionRequestServerHandler}. */
public class PartitionRequestServerHandlerTest extends TestLogger {

    /**
     * Tests that {@link PartitionRequestServerHandler} responds {@link ErrorResponse} with wrapped
     * {@link PartitionNotFoundException} after receiving invalid {@link PartitionRequest}.
     */
    @Test
    public void testResponsePartitionNotFoundException() {
        final PartitionRequestServerHandler serverHandler =
                new PartitionRequestServerHandler(
                        new ResultPartitionManager(),
                        new TaskEventDispatcher(),
                        new PartitionRequestQueue());
        final EmbeddedChannel channel = new EmbeddedChannel(serverHandler);
        final ResultPartitionID partitionId = new ResultPartitionID();

        // Write the message of partition request to server
        channel.writeInbound(new PartitionRequest(partitionId, 0, new InputChannelID(), 2));
        channel.runPendingTasks();

        // Read the response message after handling partition request
        final Object msg = channel.readOutbound();
        assertThat(msg, instanceOf(ErrorResponse.class));

        final ErrorResponse err = (ErrorResponse) msg;
        assertThat(err.cause, instanceOf(PartitionNotFoundException.class));

        final ResultPartitionID actualPartitionId =
                ((PartitionNotFoundException) err.cause).getPartitionId();
        assertThat(partitionId, is(actualPartitionId));
    }

    @Test
    public void testResumeConsumption() {
        final InputChannelID inputChannelID = new InputChannelID();
        final PartitionRequestQueue partitionRequestQueue = new PartitionRequestQueue();
        final TestViewReader testViewReader =
                new TestViewReader(inputChannelID, 2, partitionRequestQueue);
        final PartitionRequestServerHandler serverHandler =
                new PartitionRequestServerHandler(
                        new ResultPartitionManager(),
                        new TaskEventDispatcher(),
                        partitionRequestQueue);
        final EmbeddedChannel channel = new EmbeddedChannel(serverHandler);
        partitionRequestQueue.notifyReaderCreated(testViewReader);

        // Write the message of resume consumption to server
        channel.writeInbound(new ResumeConsumption(inputChannelID));
        channel.runPendingTasks();

        assertTrue(testViewReader.consumptionResumed);
    }

    private static class TestViewReader extends CreditBasedSequenceNumberingViewReader {
        private boolean consumptionResumed = false;

        TestViewReader(
                InputChannelID receiverId, int initialCredit, PartitionRequestQueue requestQueue) {
            super(receiverId, initialCredit, requestQueue);
        }

        @Override
        public void resumeConsumption() {
            consumptionResumed = true;
        }
    }
}
