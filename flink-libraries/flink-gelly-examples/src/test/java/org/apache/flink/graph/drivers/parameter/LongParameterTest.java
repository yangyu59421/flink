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

package org.apache.flink.graph.drivers.parameter;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.rules.ExpectedException;

/** Tests for {@link LongParameter}. */
public class LongParameterTest extends ParameterTestBase {

    @Rule public ExpectedException expectedException = ExpectedException.none();

    private LongParameter parameter;

    @Before
    public void setup() {
        super.setup();

        parameter = new LongParameter(owner, "test");
    }

    // Test configuration

    @Test
    public void testMinimumValueAboveMaximum() {
        parameter.setMaximumValue(0);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage(
                "Minimum value (1) must be less than or equal to maximum (0)");

        parameter.setMinimumValue(1);
    }

    @Test
    public void testMaximumValueBelowMinimum() {
        parameter.setMinimumValue(0);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage(
                "Maximum value (-1) must be greater than or equal to minimum (0)");

        parameter.setMaximumValue(-1);
    }

    // With default

    @Test
    public void testWithDefaultWithParameter() {
        parameter.setDefaultValue(42);
        Assertions.assertEquals("[--test TEST]", parameter.getUsage());

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "54"}));
        Assertions.assertEquals(new Long(54), parameter.getValue());
    }

    @Test
    public void testWithDefaultWithoutParameter() {
        parameter.setDefaultValue(13);
        Assertions.assertEquals("[--test TEST]", parameter.getUsage());

        parameter.configure(ParameterTool.fromArgs(new String[] {}));
        Assertions.assertEquals(new Long(13), parameter.getValue());
    }

    // Without default

    @Test
    public void testWithoutDefaultWithParameter() {
        Assertions.assertEquals("--test TEST", parameter.getUsage());

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "42"}));
        Assertions.assertEquals(new Long(42), parameter.getValue());
    }

    @Test
    public void testWithoutDefaultWithoutParameter() {
        Assertions.assertEquals("--test TEST", parameter.getUsage());

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("No data for required key 'test'");

        parameter.configure(ParameterTool.fromArgs(new String[] {}));
    }

    // Min

    @Test
    public void testMinInRange() {
        parameter.setMinimumValue(0);
        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "1"}));
        Assertions.assertEquals(new Long(1), parameter.getValue());
    }

    @Test
    public void testMinOutOfRange() {
        parameter.setMinimumValue(0);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("test must be greater than or equal to 0");

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "-1"}));
    }

    // Max

    @Test
    public void testMaxInRange() {
        parameter.setMaximumValue(0);
        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "-1"}));
        Assertions.assertEquals(new Long(-1), parameter.getValue());
    }

    @Test
    public void testMaxOutOfRange() {
        parameter.setMaximumValue(0);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("test must be less than or equal to 0");

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "1"}));
    }

    // Min and max

    @Test
    public void testMinAndMaxBelowRange() {
        parameter.setMinimumValue(-1);
        parameter.setMaximumValue(1);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("test must be greater than or equal to -1");

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "-2"}));
    }

    @Test
    public void testMinAndMaxInRange() {
        parameter.setMinimumValue(-1);
        parameter.setMaximumValue(1);
        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "0"}));
        Assertions.assertEquals(new Long(0), parameter.getValue());
    }

    @Test
    public void testMinAndMaxAboveRange() {
        parameter.setMinimumValue(-1);
        parameter.setMaximumValue(1);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("test must be less than or equal to 1");

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "2"}));
    }
}
