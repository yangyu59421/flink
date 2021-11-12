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

package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.testutils.TestFileSystem

import org.junit.After
import org.junit.Assert.assertTrue

/**
  * Test for file system table factory with testcsv format.
  */
class FileSystemTestCsvITCase extends BatchFileSystemITCaseBase {

  override def formatProperties(): Array[String] = {
    super.formatProperties() ++ Seq("'format' = 'testcsv'")
  }

  override def getScheme: String = "test"

  @After
  def close(): Unit = {
    try {
      val unclosedOutputStream = TestFileSystem.getCurrentUnclosedOutputStream
      assertTrue(
        "There are unclosed file output stream which are " + unclosedOutputStream,
        unclosedOutputStream.isEmpty)
    } finally {
      TestFileSystem.resetStreamCounter()
    }
  }
}
