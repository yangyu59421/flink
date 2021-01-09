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

package org.apache.flink.client.python;

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

/** Tests for the {@link PythonShellParser}. */
public class PythonShellParserTest {
    @Test
    public void testParseLocalWithoutOptions() {
        String[] args = {"local"};
        List<String> commandOptions = PythonShellParser.parseLocal(args);
        String[] expectedCommandOptions = {"local"};
        Assertions.assertArrayEquals(expectedCommandOptions, commandOptions.toArray());
    }

    @Test
    public void testParseRemoteWithoutOptions() {
        String[] args = {"remote", "localhost", "8081"};
        List<String> commandOptions = PythonShellParser.parseRemote(args);
        String[] expectedCommandOptions = {"remote", "-m", "localhost:8081"};
        Assertions.assertArrayEquals(expectedCommandOptions, commandOptions.toArray());
    }

    @Test
    public void testParseYarnWithoutOptions() {
        String[] args = {"yarn"};
        List<String> commandOptions = PythonShellParser.parseYarn(args);
        String[] expectedCommandOptions = {"yarn", "-m", "yarn-cluster"};
        Assertions.assertArrayEquals(expectedCommandOptions, commandOptions.toArray());
    }

    @Test
    public void testParseYarnWithOptions() {
        String[] args = {"yarn", "-jm", "1024m", "-tm", "4096m"};
        List<String> commandOptions = PythonShellParser.parseYarn(args);
        String[] expectedCommandOptions = {
            "yarn", "-m", "yarn-cluster", "-yjm", "1024m", "-ytm", "4096m"
        };
        Assertions.assertArrayEquals(expectedCommandOptions, commandOptions.toArray());
    }
}
