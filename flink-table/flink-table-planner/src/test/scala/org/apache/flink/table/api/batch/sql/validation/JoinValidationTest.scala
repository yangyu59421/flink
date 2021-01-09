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

package org.apache.flink.table.api.batch.sql.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.types.Row

import org.junit.jupiter.api.Test

class JoinValidationTest extends TableTestBase {

  @Test
  def testJoinNonExistingKey(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE foo = e"

    util.tableEnv.sqlQuery(sqlQuery)
        }
    }

  @Test
  def testJoinNonMatchingKeyTypes(): Unit = {
        assertThrows[TableException] {
                val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE a = g"

    util.tableEnv.sqlQuery(sqlQuery).toDataSet[Row]
        }
    }

  @Test
  def testJoinWithAmbiguousFields(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'c)

    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE a = d"

    util.tableEnv.sqlQuery(sqlQuery).toDataSet[Row]
        }
    }

  @Test
  def testJoinNoEqualityPredicate(): Unit = {
        assertThrows[TableException] {
                val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE d = f"

    util.tableEnv.sqlQuery(sqlQuery).toDataSet[Row]
        }
    }

  @Test
  def testCrossJoin(): Unit = {
        assertThrows[TableException] {
                val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("Table4", 'a1, 'b1, 'c1)

    val sqlQuery = "SELECT a, a1 FROM Table3 CROSS JOIN Table4"

    util.tableEnv.sqlQuery(sqlQuery).toDataSet[Row]
        }
    }

  @Test
  def testRightOuterJoinNoEquiJoinPredicate(): Unit = {
        assertThrows[TableException] {
                val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table3 RIGHT OUTER JOIN Table5 ON b < e"

    util.tableEnv.sqlQuery(sqlQuery).toDataSet[Row]
        }
    }

  @Test
  def testLeftOuterJoinNoEquiJoinPredicate(): Unit = {
        assertThrows[TableException] {
                val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table3 LEFT OUTER JOIN Table5 ON b > e"

    util.tableEnv.sqlQuery(sqlQuery).toDataSet[Row]
        }
    }

  @Test
  def testFullOuterJoinNoEquiJoinPredicate(): Unit = {
        assertThrows[TableException] {
                val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table3 FULL OUTER JOIN Table5 ON b <> e"

    util.tableEnv.sqlQuery(sqlQuery).toDataSet[Row]
        }
    }
}
