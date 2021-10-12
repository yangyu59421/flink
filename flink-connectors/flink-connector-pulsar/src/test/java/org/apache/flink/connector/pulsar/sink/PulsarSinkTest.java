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

package org.apache.flink.connector.pulsar.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;
import org.apache.flink.connector.pulsar.testutils.SampleData;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** unit test for PulsarSink. */
public class PulsarSinkTest extends PulsarTestSuiteBase {

    @Override
    protected PulsarRuntime runtime() {
        return PulsarRuntime.container();
    }

    @ParameterizedTest
    @EnumSource(DeliveryGuarantee.class)
    void testPulsarSink(DeliveryGuarantee deliveryGuarantee) throws Exception {
        final String topic = "test-" + deliveryGuarantee.name().toLowerCase(Locale.ROOT);
        operator().createTopic(topic, 5);
        operator().admin().topics().createSubscription(topic, "test", MessageId.earliest);

        final PulsarSink<String> sink =
                PulsarSink.builder()
                        .setServiceUrl(operator().serviceUrl())
                        .setAdminUrl(operator().adminUrl())
                        .setTopic(topic)
                        .setSerializationSchema(
                                PulsarSerializationSchema.pulsarSchema(Schema.STRING))
                        .setDeliveryGuarantee(deliveryGuarantee)
                        .build();

        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        final StreamExecutionEnvironment env = new LocalStreamEnvironment(config);
        env.enableCheckpointing(100L);
        List<String> expected = SampleData.STRING_LIST;
        env.fromCollection(expected).sinkTo(sink);
        env.execute();

        List<String> messages =
                operator().consumeMessage(topic, Schema.STRING, expected.size(), 30 * 1000);
        assertEquals(new HashSet<>(expected), new HashSet<>(messages));
    }
}
