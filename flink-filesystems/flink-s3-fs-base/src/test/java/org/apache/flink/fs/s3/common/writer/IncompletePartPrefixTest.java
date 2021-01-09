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

package org.apache.flink.fs.s3.common.writer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the {@link
 * RecoverableMultiPartUploadImpl#createIncompletePartObjectNamePrefix(String)}.
 */
public class IncompletePartPrefixTest {

    @Test
    public void nullObjectNameShouldThroughException() {
        assertThrows(
                NullPointerException.class,
                () -> {
                    RecoverableMultiPartUploadImpl.createIncompletePartObjectNamePrefix(null);
                });
    }

    @Test
    public void emptyInitialNameShouldSucceed() {
        String objectNamePrefix =
                RecoverableMultiPartUploadImpl.createIncompletePartObjectNamePrefix("");
        Assertions.assertEquals("_tmp_", objectNamePrefix);
    }

    @Test
    public void nameWithoutSlashShouldSucceed() {
        String objectNamePrefix =
                RecoverableMultiPartUploadImpl.createIncompletePartObjectNamePrefix(
                        "no_slash_path");
        Assertions.assertEquals("_no_slash_path_tmp_", objectNamePrefix);
    }

    @Test
    public void nameWithOnlySlashShouldSucceed() {
        String objectNamePrefix =
                RecoverableMultiPartUploadImpl.createIncompletePartObjectNamePrefix("/");
        Assertions.assertEquals("/_tmp_", objectNamePrefix);
    }

    @Test
    public void normalPathShouldSucceed() {
        String objectNamePrefix =
                RecoverableMultiPartUploadImpl.createIncompletePartObjectNamePrefix(
                        "/root/home/test-file");
        Assertions.assertEquals("/root/home/_test-file_tmp_", objectNamePrefix);
    }
}
