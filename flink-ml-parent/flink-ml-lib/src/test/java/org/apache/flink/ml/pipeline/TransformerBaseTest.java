/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.ml.pipeline;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.common.MLEnvironment;
import org.apache.flink.ml.common.MLEnvironmentFactory;
import org.apache.flink.ml.operator.batch.BatchOperator;
import org.apache.flink.ml.operator.stream.StreamOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link TransformerBase}. */
public class TransformerBaseTest extends PipelineStageTestBase {

    /** This fake transformer simply record which transform method is invoked. */
    private static class FakeTransFormer extends TransformerBase {

        boolean batchTransformed = false;
        boolean streamTransformed = false;

        @Override
        protected BatchOperator transform(BatchOperator input) {
            batchTransformed = true;
            return input;
        }

        @Override
        protected StreamOperator transform(StreamOperator input) {
            streamTransformed = true;
            return input;
        }
    }

    @Override
    protected PipelineStageBase createPipelineStage() {
        return new FakeTransFormer();
    }

    @Test
    public void testFitBatchTable() {
        Long id = MLEnvironmentFactory.getNewMLEnvironmentId();
        MLEnvironment env = MLEnvironmentFactory.get(id);
        DataSet<Integer> input = env.getExecutionEnvironment().fromElements(1, 2, 3);
        Table table = env.getBatchTableEnvironment().fromDataSet(input);

        FakeTransFormer transFormer = new FakeTransFormer();
        transFormer.setMLEnvironmentId(id);
        transFormer.transform(env.getBatchTableEnvironment(), table);

        Assertions.assertTrue(transFormer.batchTransformed);
        Assertions.assertFalse(transFormer.streamTransformed);
    }

    @Test
    public void testFitStreamTable() {
        Long id = MLEnvironmentFactory.getNewMLEnvironmentId();
        MLEnvironment env = MLEnvironmentFactory.get(id);
        DataStream<Integer> input = env.getStreamExecutionEnvironment().fromElements(1, 2, 3);
        Table table = env.getStreamTableEnvironment().fromDataStream(input);

        FakeTransFormer transFormer = new FakeTransFormer();
        transFormer.setMLEnvironmentId(id);
        transFormer.transform(env.getStreamTableEnvironment(), table);

        Assertions.assertFalse(transFormer.batchTransformed);
        Assertions.assertTrue(transFormer.streamTransformed);
    }
}
