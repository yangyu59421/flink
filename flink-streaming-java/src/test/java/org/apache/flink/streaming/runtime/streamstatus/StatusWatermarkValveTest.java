/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamstatus;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link StatusWatermarkValve}. While tests in {@link
 * org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTest} and {@link
 * org.apache.flink.streaming.runtime.tasks.TwoInputStreamTaskTest} may also implicitly test {@link
 * StatusWatermarkValve} and that valves are correctly used in the tasks' input processors, the unit
 * tests here additionally makes sure that the watermarks and stream statuses to forward are
 * generated from the valve at the exact correct times and in a deterministic behaviour. The unit
 * tests here also test more complex stream status / watermark input cases.
 *
 * <p>The tests are performed by a series of watermark and stream status inputs to the valve. On
 * every input method call, the output is checked to contain only the expected watermark or stream
 * status, and nothing else. This ensures that no redundant outputs are generated by the output
 * logic of {@link StatusWatermarkValve}. The behaviours that a series of input calls to the valve
 * is trying to test is explained as inline comments within the tests.
 */
public class StatusWatermarkValveTest {

    /**
     * Tests that watermarks correctly advance with increasing watermarks for a single input valve.
     */
    @Test
    public void testSingleInputIncreasingWatermarks() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(1);

        valve.inputWatermark(new Watermark(0), 0, valveOutput);
        assertEquals(new Watermark(0), valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());

        valve.inputWatermark(new Watermark(25), 0, valveOutput);
        assertEquals(new Watermark(25), valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());
    }

    /**
     * Tests that watermarks do not advance with decreasing watermark inputs for a single input
     * valve.
     */
    @Test
    public void testSingleInputDecreasingWatermarksYieldsNoOutput() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(1);

        valve.inputWatermark(new Watermark(25), 0, valveOutput);
        assertEquals(new Watermark(25), valveOutput.popLastSeenOutput());

        valve.inputWatermark(new Watermark(18), 0, valveOutput);
        assertEquals(null, valveOutput.popLastSeenOutput());

        valve.inputWatermark(new Watermark(42), 0, valveOutput);
        assertEquals(new Watermark(42), valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());
    }

    /**
     * Tests that stream status toggling works correctly, as well as that non-toggling status inputs
     * do not yield output for a single input valve.
     */
    @Test
    public void testSingleInputStreamStatusToggling() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(1);

        valve.inputStreamStatus(StreamStatus.ACTIVE, 0, valveOutput);
        // this also implicitly verifies that input channels start as ACTIVE
        assertEquals(null, valveOutput.popLastSeenOutput());

        valve.inputStreamStatus(StreamStatus.IDLE, 0, valveOutput);
        assertEquals(StreamStatus.IDLE, valveOutput.popLastSeenOutput());

        valve.inputStreamStatus(StreamStatus.IDLE, 0, valveOutput);
        assertEquals(null, valveOutput.popLastSeenOutput());

        valve.inputStreamStatus(StreamStatus.ACTIVE, 0, valveOutput);
        assertEquals(StreamStatus.ACTIVE, valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());
    }

    /** Tests that the watermark of an input channel remains intact while in the IDLE status. */
    @Test
    public void testSingleInputWatermarksIntactDuringIdleness() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(1);

        valve.inputWatermark(new Watermark(25), 0, valveOutput);
        assertEquals(new Watermark(25), valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());

        valve.inputStreamStatus(StreamStatus.IDLE, 0, valveOutput);
        assertEquals(StreamStatus.IDLE, valveOutput.popLastSeenOutput());

        valve.inputWatermark(new Watermark(50), 0, valveOutput);
        assertEquals(null, valveOutput.popLastSeenOutput());
        assertEquals(25, valve.getInputChannelStatus(0).watermark);

        valve.inputStreamStatus(StreamStatus.ACTIVE, 0, valveOutput);
        assertEquals(StreamStatus.ACTIVE, valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());

        valve.inputWatermark(new Watermark(50), 0, valveOutput);
        assertEquals(new Watermark(50), valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());
    }

    /** Tests that the valve yields a watermark only when all inputs have received a watermark. */
    @Test
    public void testMultipleInputYieldsWatermarkOnlyWhenAllChannelsReceivesWatermarks()
            throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(3);

        valve.inputWatermark(new Watermark(0), 0, valveOutput);
        valve.inputWatermark(new Watermark(0), 1, valveOutput);
        assertEquals(null, valveOutput.popLastSeenOutput());

        // now, all channels have watermarks
        valve.inputWatermark(new Watermark(0), 2, valveOutput);
        assertEquals(new Watermark(0), valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());
    }

    /**
     * Tests that new min watermark is emitted from the valve as soon as the overall new min
     * watermark across inputs advances.
     */
    @Test
    public void testMultipleInputIncreasingWatermarks() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(3);

        valve.inputWatermark(new Watermark(0), 0, valveOutput);
        valve.inputWatermark(new Watermark(0), 1, valveOutput);
        valve.inputWatermark(new Watermark(0), 2, valveOutput);
        assertEquals(new Watermark(0), valveOutput.popLastSeenOutput());

        valve.inputWatermark(new Watermark(12), 0, valveOutput);
        valve.inputWatermark(new Watermark(8), 2, valveOutput);
        valve.inputWatermark(new Watermark(10), 2, valveOutput);
        assertEquals(null, valveOutput.popLastSeenOutput());

        valve.inputWatermark(new Watermark(15), 1, valveOutput);
        // lowest watermark across all channels is now channel 2, with watermark @ 10
        assertEquals(new Watermark(10), valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());

        valve.inputWatermark(new Watermark(17), 2, valveOutput);
        // lowest watermark across all channels is now channel 0, with watermark @ 12
        assertEquals(new Watermark(12), valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());

        valve.inputWatermark(new Watermark(20), 0, valveOutput);
        // lowest watermark across all channels is now channel 1, with watermark @ 15
        assertEquals(new Watermark(15), valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());
    }

    /** Tests that for a multiple input valve, decreasing watermarks will yield no output. */
    @Test
    public void testMultipleInputDecreasingWatermarksYieldsNoOutput() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(3);

        valve.inputWatermark(new Watermark(25), 0, valveOutput);
        valve.inputWatermark(new Watermark(10), 1, valveOutput);
        valve.inputWatermark(new Watermark(17), 2, valveOutput);
        assertEquals(new Watermark(10), valveOutput.popLastSeenOutput());

        valve.inputWatermark(new Watermark(12), 0, valveOutput);
        valve.inputWatermark(new Watermark(8), 1, valveOutput);
        valve.inputWatermark(new Watermark(15), 2, valveOutput);
        assertEquals(null, valveOutput.popLastSeenOutput());
    }

    /**
     * Tests that stream status toggling works correctly, as well as that non-toggling status inputs
     * do not yield output for a multiple input valve.
     */
    @Test
    public void testMultipleInputStreamStatusToggling() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(2);

        // this also implicitly verifies that all input channels start as active
        valve.inputStreamStatus(StreamStatus.ACTIVE, 0, valveOutput);
        valve.inputStreamStatus(StreamStatus.ACTIVE, 1, valveOutput);
        assertEquals(null, valveOutput.popLastSeenOutput());

        valve.inputStreamStatus(StreamStatus.IDLE, 1, valveOutput);
        assertEquals(null, valveOutput.popLastSeenOutput());

        // now, all channels are IDLE
        valve.inputStreamStatus(StreamStatus.IDLE, 0, valveOutput);
        assertEquals(StreamStatus.IDLE, valveOutput.popLastSeenOutput());

        valve.inputStreamStatus(StreamStatus.IDLE, 0, valveOutput);
        valve.inputStreamStatus(StreamStatus.IDLE, 1, valveOutput);
        assertEquals(null, valveOutput.popLastSeenOutput());

        // as soon as at least one input becomes active again, the ACTIVE marker should be forwarded
        valve.inputStreamStatus(StreamStatus.ACTIVE, 1, valveOutput);
        assertEquals(StreamStatus.ACTIVE, valveOutput.popLastSeenOutput());

        valve.inputStreamStatus(StreamStatus.ACTIVE, 0, valveOutput);
        // already back to ACTIVE, should yield no output
        assertEquals(null, valveOutput.popLastSeenOutput());
    }

    /**
     * Tests that for multiple inputs, when some inputs are idle, the min watermark is correctly
     * computed and advanced from the remaining active inputs.
     */
    @Test
    public void testMultipleInputWatermarkAdvancingWithPartiallyIdleChannels() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(3);

        valve.inputWatermark(new Watermark(15), 0, valveOutput);
        valve.inputWatermark(new Watermark(10), 1, valveOutput);
        assertEquals(null, valveOutput.popLastSeenOutput());

        valve.inputStreamStatus(StreamStatus.IDLE, 2, valveOutput);
        // min watermark should be computed from remaining ACTIVE channels
        assertEquals(new Watermark(10), valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());

        valve.inputWatermark(new Watermark(18), 1, valveOutput);
        // now, min watermark should be 15 from channel #0
        assertEquals(new Watermark(15), valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());

        valve.inputWatermark(new Watermark(20), 0, valveOutput);
        // now, min watermark should be 18 from channel #1
        assertEquals(new Watermark(18), valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());
    }

    /**
     * Tests that as input channels individually and gradually become idle, watermarks are output as
     * soon remaining active channels can yield a new min watermark.
     */
    @Test
    public void testMultipleInputWatermarkAdvancingAsChannelsIndividuallyBecomeIdle()
            throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(3);

        valve.inputWatermark(new Watermark(25), 0, valveOutput);
        valve.inputWatermark(new Watermark(10), 1, valveOutput);
        valve.inputWatermark(new Watermark(17), 2, valveOutput);
        assertEquals(new Watermark(10), valveOutput.popLastSeenOutput());

        valve.inputStreamStatus(StreamStatus.IDLE, 1, valveOutput);
        // only channel 0 & 2 is ACTIVE; 17 is the overall min watermark now
        assertEquals(new Watermark(17), valveOutput.popLastSeenOutput());

        valve.inputStreamStatus(StreamStatus.IDLE, 2, valveOutput);
        // only channel 0 is ACTIVE; 25 is the overall min watermark now
        assertEquals(new Watermark(25), valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());
    }

    /**
     * Tests that when all inputs become idle, the max watermark across all channels is correctly
     * "flushed" from the valve, as well as the stream status IDLE marker.
     *
     * <p>This test along with {@link
     * #testMultipleInputWatermarkAdvancingAsChannelsIndividuallyBecomeIdle} should completely
     * verify that the eventual watermark advancement result when all inputs become idle is
     * independent of the order that the inputs become idle.
     */
    @Test
    public void testMultipleInputFlushMaxWatermarkAndStreamStatusOnceAllInputsBecomeIdle()
            throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(3);

        // -------------------------------------------------------------------------------------------
        // Setup valve for test case:
        //  channel #1: Watermark 10, ACTIVE
        //  channel #2: Watermark 5, ACTIVE
        //  channel #3: Watermark 3, ACTIVE
        //  Min Watermark across channels = 3 (from channel #3)
        // -------------------------------------------------------------------------------------------

        valve.inputWatermark(new Watermark(10), 0, valveOutput);
        valve.inputWatermark(new Watermark(5), 1, valveOutput);
        valve.inputWatermark(new Watermark(3), 2, valveOutput);
        assertEquals(new Watermark(3), valveOutput.popLastSeenOutput());

        // -------------------------------------------------------------------------------------------
        // Order of becoming IDLE:
        //  channel #1 ----------------> channel #2 ----------------> channel #3
        //   |-> (nothing emitted)        |-> (nothing emitted)        |-> Emit Watermark(10) & IDLE
        // -------------------------------------------------------------------------------------------

        valve.inputStreamStatus(StreamStatus.IDLE, 0, valveOutput);
        valve.inputStreamStatus(StreamStatus.IDLE, 1, valveOutput);
        assertEquals(null, valveOutput.popLastSeenOutput());

        valve.inputStreamStatus(StreamStatus.IDLE, 2, valveOutput);
        assertEquals(new Watermark(10), valveOutput.popLastSeenOutput());
        assertEquals(StreamStatus.IDLE, valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());
    }

    /**
     * Tests that when idle channels become active again, they need to "catch up" with the latest
     * watermark before they are considered for min watermark computation again.
     */
    @Test
    public void testMultipleInputWatermarkRealignmentAfterResumeActive() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(3);

        valve.inputWatermark(new Watermark(10), 0, valveOutput);
        valve.inputWatermark(new Watermark(7), 1, valveOutput);
        valve.inputWatermark(new Watermark(3), 2, valveOutput);
        assertEquals(new Watermark(3), valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());

        valve.inputStreamStatus(StreamStatus.IDLE, 2, valveOutput);
        assertEquals(new Watermark(7), valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());

        // let channel 2 become active again; since the min watermark has now advanced to 7,
        // channel 2 should have been marked as non-aligned.
        valve.inputStreamStatus(StreamStatus.ACTIVE, 2, valveOutput);
        assertFalse(valve.getInputChannelStatus(2).isWatermarkAligned);

        // during the realignment process, watermarks should still be accepted by channel 2 (but
        // shouldn't yield new watermarks)
        valve.inputWatermark(new Watermark(5), 2, valveOutput);
        assertEquals(5, valve.getInputChannelStatus(2).watermark);
        assertEquals(null, valveOutput.popLastSeenOutput());

        // let channel 2 catch up with the min watermark; now should be realigned
        valve.inputWatermark(new Watermark(9), 2, valveOutput);
        assertTrue(valve.getInputChannelStatus(2).isWatermarkAligned);
        assertEquals(null, valveOutput.popLastSeenOutput());

        // check that realigned inputs is now taken into account for watermark advancement
        valve.inputWatermark(new Watermark(12), 1, valveOutput);
        assertEquals(new Watermark(9), valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());
    }

    /**
     * Verify that we don't see any state changes/watermarks when all ACTIVE channels are unaligned.
     * Earlier versions of the valve had a bug that would cause it to emit a {@code Long.MAX_VALUE}
     * watermark in that case.
     */
    @Test
    public void testNoOutputWhenAllActiveChannelsAreUnaligned() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(3);

        valve.inputWatermark(new Watermark(10), 0, valveOutput);
        valve.inputWatermark(new Watermark(7), 1, valveOutput);

        // make channel 2 ACTIVE, it is now in "catch up" mode (unaligned watermark)
        valve.inputStreamStatus(StreamStatus.IDLE, 2, valveOutput);
        assertEquals(new Watermark(7), valveOutput.popLastSeenOutput());
        assertEquals(null, valveOutput.popLastSeenOutput());

        // make channel 2 ACTIVE again, it is still unaligned
        valve.inputStreamStatus(StreamStatus.ACTIVE, 2, valveOutput);
        assertEquals(null, valveOutput.popLastSeenOutput());

        // make channel 0 and 1 IDLE, now channel 2 is the only ACTIVE channel but it's unaligned
        valve.inputStreamStatus(StreamStatus.IDLE, 0, valveOutput);
        valve.inputStreamStatus(StreamStatus.IDLE, 1, valveOutput);

        // we should not see any output
        assertEquals(null, valveOutput.popLastSeenOutput());
    }

    private static class StatusWatermarkOutput implements PushingAsyncDataInput.DataOutput {

        private BlockingQueue<StreamElement> allOutputs = new LinkedBlockingQueue<>();

        @Override
        public void emitWatermark(Watermark watermark) {
            allOutputs.add(watermark);
        }

        @Override
        public void emitStreamStatus(StreamStatus streamStatus) {
            allOutputs.add(streamStatus);
        }

        @Override
        public void emitRecord(StreamRecord record) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {
            throw new UnsupportedOperationException();
        }

        public StreamElement popLastSeenOutput() {
            return allOutputs.poll();
        }
    }
}
