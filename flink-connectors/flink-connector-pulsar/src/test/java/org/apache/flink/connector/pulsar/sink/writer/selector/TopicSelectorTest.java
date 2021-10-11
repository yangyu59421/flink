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

package org.apache.flink.connector.pulsar.sink.writer.selector;

import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** unit test for TopicSelector. */
class TopicSelectorTest extends TestLogger {

    @Test
    void selector() {
        TopicSelector<List<String>> selector =
                record -> {
                    if (record.contains("key1")) {
                        return "topic1";
                    } else if (record.contains("key2")) {
                        return "topic2";
                    } else if (record.contains("key3")) {
                        return "topic3";
                    }
                    return "topic-default";
                };
        assertEquals("topic1", selector.selector(Arrays.asList("key1")));
        assertEquals("topic2", selector.selector(Arrays.asList("key2")));
        assertEquals("topic3", selector.selector(Arrays.asList("key3")));
        assertEquals("topic-default", selector.selector(Arrays.asList("key-xxx")));
    }
}
