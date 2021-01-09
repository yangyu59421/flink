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

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvg
import org.apache.flink.table.utils.{TableFunc0, TableTestBase}

import org.junit.jupiter.api.Test

import java.math.BigDecimal

class CalcValidationTest extends TableTestBase {

  @Test
  def testInvalidUseOfRowtime(): Unit = {
        assertThrows[ValidationException] {
                val util = streamTestUtil()
    util.addTable[(Long, Int, Double, Float, BigDecimal, String)](
      "MyTable",
      'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
    .select('rowtime.rowtime)
        }
    }

  @Test
  def testInvalidUseOfRowtime2(): Unit = {
        assertThrows[ValidationException] {
                val util = streamTestUtil()
    util.addTable[(Long, Int, Double, Float, BigDecimal, String)](
      "MyTable",
      'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
    .window(Tumble over 2.millis on 'rowtime as 'w)
    .groupBy('w)
    .select('w.end.rowtime, 'int.count as 'int) // no rowtime on non-window reference
        }
    }

  @Test
  def testAddColumnsWithAgg(): Unit = {
        assertThrows[ValidationException] {
                val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    tab.addColumns('a.sum)
        }
    }

  @Test
  def testAddOrReplaceColumnsWithAgg(): Unit = {
        assertThrows[ValidationException] {
                val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    tab.addOrReplaceColumns('a.sum)
        }
    }

  @Test
  def testRenameColumnsWithAgg(): Unit = {
        assertThrows[ValidationException] {
                  val util = streamTestUtil()
      val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
      tab.renameColumns('a.sum)
        }
    }

  @Test
  def testRenameColumnsWithoutAlias(): Unit = {
        assertThrows[ValidationException] {
                val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    tab.renameColumns('a)
        }
    }

  @Test
  def testRenameColumnsWithFunctallCall(): Unit = {
        assertThrows[ValidationException] {
                val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    tab.renameColumns('a + 1  as 'a2)
        }
    }

  @Test
  def testRenameColumnsNotExist(): Unit = {
        assertThrows[ValidationException] {
                val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    tab.renameColumns('e as 'e2)
        }
    }

  @Test
  def testDropColumnsWithAgg(): Unit = {
        assertThrows[ValidationException] {
                val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    tab.dropColumns('a.sum)
        }
    }

  @Test
  def testDropColumnsNotExist(): Unit = {
        assertThrows[ValidationException] {
                val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    tab.dropColumns('e)
        }
    }

  @Test
  def testDropColumnsWithValueLiteral(): Unit = {
        assertThrows[ValidationException] {
                val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    tab.dropColumns("'a'")
        }
    }

  @Test
  def testInvalidMapFunctionTypeAggregation(): Unit = {
        assertThrows[ValidationException] {
                val util = streamTestUtil()
    util.addTable[(Int)](
      "MyTable", 'int)
      .map('int.sum) // do not support AggregateFunction as input
        }
    }

  @Test
  def testInvalidMapFunctionTypeUDAGG(): Unit = {
        assertThrows[ValidationException] {
                val util = streamTestUtil()

    val weightedAvg = new WeightedAvg
    util.addTable[(Int)](
      "MyTable", 'int)
      .map(weightedAvg('int, 'int)) // do not support AggregateFunction as input
        }
    }

  @Test
  def testInvalidMapFunctionTypeUDAGG2(): Unit = {
        assertThrows[ValidationException] {
                val util = streamTestUtil()

    util.tableEnv.registerFunction("weightedAvg", new WeightedAvg)
    util.addTable[(Int)](
      "MyTable", 'int)
      .map(call("weightedAvg", $"int", $"int")) // do not support AggregateFunction as input
        }
    }

  @Test
  def testInvalidMapFunctionTypeTableFunction(): Unit = {
        assertThrows[ValidationException] {
                val util = streamTestUtil()

    util.tableEnv.registerFunction("func", new TableFunc0)
    util.addTable[(String)](
      "MyTable", 'string)
      .map(call("func", $"string") as "a") // do not support TableFunction as input
        }
    }
}
