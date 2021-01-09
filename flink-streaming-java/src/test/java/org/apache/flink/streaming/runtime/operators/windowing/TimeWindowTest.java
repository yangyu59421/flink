/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

/** Tests for {@link TimeWindow}. */
public class TimeWindowTest {
    @Test
    public void testGetWindowStartWithOffset() {
        // [0, 7), [7, 14), [14, 21)...
        long offset = 0;
        Assertions.assertEquals(TimeWindow.getWindowStartWithOffset(1, offset, 7), 0);
        Assertions.assertEquals(TimeWindow.getWindowStartWithOffset(6, offset, 7), 0);
        Assertions.assertEquals(TimeWindow.getWindowStartWithOffset(7, offset, 7), 7);
        Assertions.assertEquals(TimeWindow.getWindowStartWithOffset(8, offset, 7), 7);

        // [-4, 3), [3, 10), [10, 17)...
        offset = 3;
        Assertions.assertEquals(TimeWindow.getWindowStartWithOffset(1, offset, 7), -4);
        Assertions.assertEquals(TimeWindow.getWindowStartWithOffset(2, offset, 7), -4);
        Assertions.assertEquals(TimeWindow.getWindowStartWithOffset(3, offset, 7), 3);
        Assertions.assertEquals(TimeWindow.getWindowStartWithOffset(9, offset, 7), 3);
        Assertions.assertEquals(TimeWindow.getWindowStartWithOffset(10, offset, 7), 10);

        // [-2, 5), [5, 12), [12, 19)...
        offset = -2;
        Assertions.assertEquals(TimeWindow.getWindowStartWithOffset(1, offset, 7), -2);
        Assertions.assertEquals(TimeWindow.getWindowStartWithOffset(-2, offset, 7), -2);
        Assertions.assertEquals(TimeWindow.getWindowStartWithOffset(3, offset, 7), -2);
        Assertions.assertEquals(TimeWindow.getWindowStartWithOffset(4, offset, 7), -2);
        Assertions.assertEquals(TimeWindow.getWindowStartWithOffset(7, offset, 7), 5);
        Assertions.assertEquals(TimeWindow.getWindowStartWithOffset(12, offset, 7), 12);

        // for GMT+8:00
        offset = -TimeUnit.HOURS.toMillis(8);
        long size = TimeUnit.DAYS.toMillis(1);
        Assertions.assertEquals(
                TimeWindow.getWindowStartWithOffset(1470902048450L, offset, size), 1470844800000L);
    }

    private boolean intersects(TimeWindow w0, TimeWindow w1) {
        Assertions.assertEquals(w0.intersects(w1), w1.intersects(w0));
        return w0.intersects(w1);
    }

    @Test
    public void testIntersect() {
        // must intersect with itself
        TimeWindow window = new TimeWindow(10, 20);
        Assertions.assertTrue(window.intersects(window));

        // windows are next to each other
        Assertions.assertTrue(intersects(window, new TimeWindow(20, 30)));

        // there is distance between the windows
        Assertions.assertFalse(intersects(window, new TimeWindow(21, 30)));

        // overlaps by one
        Assertions.assertTrue(intersects(window, new TimeWindow(19, 22)));
    }
}
