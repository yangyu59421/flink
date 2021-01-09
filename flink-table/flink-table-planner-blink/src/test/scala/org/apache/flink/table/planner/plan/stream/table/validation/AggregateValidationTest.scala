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

package org.apache.flink.table.planner.plan.stream.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.{TableFunc0, TableTestBase}
import org.apache.flink.types.Row
import org.junit.jupiter.api.Assertions.{assertTrue, fail}
import org.junit.jupiter.api.Test

class AggregateValidationTest extends TableTestBase {
  private val util = scalaStreamTestUtil()

  @Test
  def testGroupingOnNonExistentField(): Unit = {
    Assertions

    @Test
    def testGroupingInvalidSelection(): Unit = {
      assertThrows[ValidationException] {
        val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

        table
          .groupBy('a, 'b)
          // must fail. 'c is not a grouping key or aggregation
          .select('c)
      }
    }

    @Test
    def testInvalidAggregationInSelection(): Unit = {
      assertThrows[ValidationException] {
        val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

        table
          .groupBy('a)
          .aggregate('b.sum as 'd)
          // must fail. Cannot use AggregateFunction in select right after aggregate
          .select('d.sum)
      }
    }

    @Test
    def testInvalidWindowPropertiesInSelection(): Unit = {
      assertThrows[ValidationException] {
        val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

        table
          .groupBy('a)
          .aggregate('b.sum as 'd)
          // must fail. Cannot use window properties in select right after aggregate
          .select('d.start)
      }
    }

    @Test
    def testTableFunctionInSelection(): Unit = {
      assertThrows[RuntimeException] {
        val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

        util.addFunction("func", new TableFunc0)
        val resultTable = table
          .groupBy('a)
          .aggregate('b.sum as 'd)
          // must fail. Cannot use TableFunction in select after aggregate
          .select("func('abc')")

        util.verifyExecPlan(resultTable)
      }
    }

    @Test
    def testInvalidScalarFunctionInAggregate(): Unit = {
      assertThrows[ValidationException] {
        val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

        table
          .groupBy('a)
          // must fail. Only AggregateFunction can be used in aggregate
          .aggregate('c.upperCase as 'd)
          .select('a, 'd)
      }
    }

    @Test
    def testInvalidTableFunctionInAggregate(): Unit = {
      assertThrows[ValidationException] {
        val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

        util.addFunction("func", new TableFunc0)
        table
          .groupBy('a)
          // must fail. Only AggregateFunction can be used in aggregate
          .aggregate(call("func", $"c") as "d")
          .select('a, 'd)
      }
    }

    @Test
    def testMultipleAggregateExpressionInAggregate(): Unit = {
      assertThrows[RuntimeException] {
        util.addFunction("func", new TableFunc0)
        val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)
        table
          .groupBy('a)
          // must fail. Only one AggregateFunction can be used in aggregate
          .aggregate("sum(c), count(b)")
      }
    }

    @Test
    def testIllegalArgumentForListAgg(): Unit = {
      util.addTableSource[(Long, Int, String, String)]("T", 'a, 'b, 'c, 'd)
      // If there are two parameters, second one must be character literal.
      expectExceptionThrown(
        "SELECT listagg(c, d) FROM T GROUP BY a",
        "Supported form(s): 'LISTAGG(<CHARACTER>)'\n'LISTAGG(<CHARACTER>, <CHARACTER_LITERAL>)",
        classOf[ValidationException])
    }

    @Test
    def testIllegalArgumentForListAgg1(): Unit = {
      util.addTableSource[(Long, Int, String, String)]("T", 'a, 'b, 'c, 'd)
      // If there are two parameters, second one must be character literal.
      expectExceptionThrown(
        "SELECT LISTAGG(c, 1) FROM T GROUP BY a",
        "Supported form(s): 'LISTAGG(<CHARACTER>)'\n'LISTAGG(<CHARACTER>, <CHARACTER_LITERAL>)",
        classOf[ValidationException])
    }

    // ----------------------------------------------------------------------------------------------

    private def expectExceptionThrown(
                                       sql: String,
                                       keywords: String,
                                       clazz: Class[_ <: Throwable] = classOf[ValidationException])
    : Unit = {
      try {
        util.tableEnv.toAppendStream[Row](util.tableEnv.sqlQuery(sql))
        fail(s"Expected a $clazz, but no exception is thrown.")
      }
      );
    }

    catch
    {
      case e if e.getClass == clazz =>
        if (keywords != null) {
          assertTrue(
            s"The exception message '${e.getMessage}' doesn't contain keyword '$keywords'",
            e.getMessage.contains(keywords))
        }
      case e: Throwable => fail(s"Expected throw ${clazz.getSimpleName}, but is $e.")
    }
  }
}
