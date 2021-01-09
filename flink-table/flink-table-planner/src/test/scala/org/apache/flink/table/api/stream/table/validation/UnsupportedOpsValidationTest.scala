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

package org.apache.flink.table.api.stream.table.validation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{EnvironmentSettings, ValidationException, _}
import org.apache.flink.table.runtime.utils.StreamTestData
import org.apache.flink.test.util.AbstractTestBase

import org.junit.jupiter.api.Test

class UnsupportedOpsValidationTest extends AbstractTestBase {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
  val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().build()
  val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

  @Test
  def testJoin(): Unit = {
        assertThrows[ValidationException] {
                val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv)
    t1.join(t2)
        }
    }

  @Test
  def testUnion(): Unit = {
        assertThrows[ValidationException] {
                val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv)
    t1.union(t2)
        }
    }

  @Test
  def testIntersect(): Unit = {
        assertThrows[ValidationException] {
                val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv)
    t1.intersect(t2)
        }
    }

  @Test
  def testIntersectAll(): Unit = {
        assertThrows[ValidationException] {
                val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv)
    t1.intersectAll(t2)
        }
    }

  @Test
  def testMinus(): Unit = {
        assertThrows[ValidationException] {
                val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv)
    t1.minus(t2)
        }
    }

  @Test
  def testMinusAll(): Unit = {
        assertThrows[ValidationException] {
                val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv)
    t1.minusAll(t2)
        }
    }
}
