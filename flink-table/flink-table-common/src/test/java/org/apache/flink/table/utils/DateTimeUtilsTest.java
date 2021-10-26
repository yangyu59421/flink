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

package org.apache.flink.table.utils;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.TimeZone;

/** Tests for {@link org.apache.flink.table.utils.DateTimeUtils}. */
public class DateTimeUtilsTest {

    @Test
    public void testCeilWithDSTTransition() {
        // DST in EU finishes October 31
        TimeZone timeZone = TimeZone.getTimeZone("Europe/Rome");
        ZonedDateTime checkedDate = ZonedDateTime.of(2021, 10, 25, 0, 0, 0, 0, timeZone.toZoneId());
        ZonedDateTime expectedYearQuarter =
                ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, timeZone.toZoneId());
        ZonedDateTime expectedMonth =
                ZonedDateTime.of(2021, 11, 1, 0, 0, 0, 0, timeZone.toZoneId());
        Assert.assertEquals(
                toMillis(expectedYearQuarter),
                DateTimeUtils.timestampCeil(
                        DateTimeUtils.TimeUnitRange.YEAR, toMillis(checkedDate), timeZone));
        Assert.assertEquals(
                toMillis(expectedYearQuarter),
                DateTimeUtils.timestampCeil(
                        DateTimeUtils.TimeUnitRange.QUARTER, toMillis(checkedDate), timeZone));
        Assert.assertEquals(
                toMillis(expectedMonth),
                DateTimeUtils.timestampCeil(
                        DateTimeUtils.TimeUnitRange.MONTH, toMillis(checkedDate), timeZone));
    }

    @Test
    public void testFloorWithDSTTransition() {
        // DST in USA started 14.03.2021
        TimeZone timeZone = TimeZone.getTimeZone("America/Los_Angeles");
        ZonedDateTime checkedDate = ZonedDateTime.of(2021, 3, 31, 0, 0, 0, 0, timeZone.toZoneId());
        ZonedDateTime expectedYearQuarter =
                ZonedDateTime.of(2021, 1, 1, 0, 0, 0, 0, timeZone.toZoneId());
        ZonedDateTime expectedMonth = ZonedDateTime.of(2021, 3, 1, 0, 0, 0, 0, timeZone.toZoneId());
        Assert.assertEquals(
                toMillis(expectedYearQuarter),
                DateTimeUtils.timestampFloor(
                        DateTimeUtils.TimeUnitRange.YEAR, toMillis(checkedDate), timeZone));
        Assert.assertEquals(
                toMillis(expectedYearQuarter),
                DateTimeUtils.timestampFloor(
                        DateTimeUtils.TimeUnitRange.QUARTER, toMillis(checkedDate), timeZone));
        Assert.assertEquals(
                toMillis(expectedMonth),
                DateTimeUtils.timestampFloor(
                        DateTimeUtils.TimeUnitRange.MONTH, toMillis(checkedDate), timeZone));
    }

    // ----------------------------------------------------------------------------------------------
    private long toMillis(ZonedDateTime zonedDateTime) {
        return zonedDateTime.toEpochSecond() * 1000;
    }
}
