/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.runtime.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/** Test for {@link BinPacking}. */
public class BinPackingTest {

    @Test
    public void testBinPacking() {
        Assertions.assertEquals(                asList(asList(1, 2), singletonList(3), singletonList(4), singletonList(5)),                pack(asList(1, 2, 3, 4, 5), 3),                 "Should pack the first 2 values");

        Assertions.assertEquals(                asList(asList(1, 2), singletonList(3), singletonList(4), singletonList(5)),                pack(asList(1, 2, 3, 4, 5), 5),                 "Should pack the first 2 values");

        Assertions.assertEquals(                asList(asList(1, 2, 3), singletonList(4), singletonList(5)),                pack(asList(1, 2, 3, 4, 5), 6),                 "Should pack the first 3 values");

        Assertions.assertEquals(                asList(asList(1, 2, 3), singletonList(4), singletonList(5)),                pack(asList(1, 2, 3, 4, 5), 8),                 "Should pack the first 3 values");

        Assertions.assertEquals(                asList(asList(1, 2, 3), asList(4, 5)),                pack(asList(1, 2, 3, 4, 5), 9),                 "Should pack the first 3 values, last 2 values");

        Assertions.assertEquals(                asList(asList(1, 2, 3, 4), singletonList(5)),                pack(asList(1, 2, 3, 4, 5), 10),                 "Should pack the first 4 values");

        Assertions.assertEquals(                asList(asList(1, 2, 3, 4), singletonList(5)),                pack(asList(1, 2, 3, 4, 5), 14),                 "Should pack the first 4 values");

        Assertions.assertEquals(                singletonList(asList(1, 2, 3, 4, 5)),                pack(asList(1, 2, 3, 4, 5), 15),                 "Should pack the first 5 values");
    }

    private List<List<Integer>> pack(List<Integer> items, long targetWeight) {
        return BinPacking.pack(items, Integer::longValue, targetWeight);
    }
}
