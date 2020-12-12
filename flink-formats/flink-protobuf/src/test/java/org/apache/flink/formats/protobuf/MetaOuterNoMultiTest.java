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

package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.deserialize.PbRowDataDeserializationSchema;
import org.apache.flink.formats.protobuf.serialize.PbRowDataSerializationSchema;
import org.apache.flink.formats.protobuf.testproto.SimpleTestOuterNomultiProto;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

/**
 * Test class for below case
 *
 * <PRE>
 * syntax = "proto2";
 * package org.apache.flink.formats.protobuf.testproto;
 * option java_package = "org.apache.flink.formats.protobuf.testproto";
 * option java_outer_classname = "SimpleTestOuterNomultiProto";
 * message SimpleTestOuterNomulti {
 * </PRE>
 *
 * <p>It is valid proto definition.
 */
public class MetaOuterNoMultiTest {
    @Test
    public void testSimple() {
        RowType rowType =
                PbRowTypeInformationUtil.generateRowType(
                        SimpleTestOuterNomultiProto.SimpleTestOuterNomulti.getDescriptor());
        new PbRowDataDeserializationSchema(
                rowType,
                InternalTypeInfo.of(rowType),
                SimpleTestOuterNomultiProto.SimpleTestOuterNomulti.class.getName(),
                false,
                false);

        new PbRowDataSerializationSchema(
                rowType, SimpleTestOuterNomultiProto.SimpleTestOuterNomulti.class.getName());
        // validation success
    }
}
