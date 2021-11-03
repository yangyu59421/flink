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

package org.apache.flink.formats.parquet.vector;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.ColumnarRowData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.DateTimeUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

import static org.apache.flink.formats.parquet.utils.ParquetWriterUtil.createTempParquetFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for {@link ParquetColumnarRowSplitReader}. */
@RunWith(Parameterized.class)
public class ParquetColumnarRowSplitReaderTest {

    private static final int FIELD_NUMBER = 33;
    private static final LocalDateTime BASE_TIME = LocalDateTime.now();

    private static final MessageType PARQUET_SCHEMA =
            new MessageType(
                    "TOP",
                    Types.primitive(
                                    PrimitiveType.PrimitiveTypeName.BINARY,
                                    Type.Repetition.OPTIONAL)
                            .named("f0"),
                    Types.primitive(
                                    PrimitiveType.PrimitiveTypeName.BOOLEAN,
                                    Type.Repetition.OPTIONAL)
                            .named("f1"),
                    Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                            .named("f2"),
                    Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                            .named("f3"),
                    Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                            .named("f4"),
                    Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
                            .named("f5"),
                    Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.OPTIONAL)
                            .named("f6"),
                    Types.primitive(
                                    PrimitiveType.PrimitiveTypeName.DOUBLE,
                                    Type.Repetition.OPTIONAL)
                            .named("f7"),
                    Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, Type.Repetition.OPTIONAL)
                            .named("f8"),
                    Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                            .precision(5)
                            .as(OriginalType.DECIMAL)
                            .named("f9"),
                    Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
                            .precision(15)
                            .as(OriginalType.DECIMAL)
                            .named("f10"),
                    Types.primitive(
                                    PrimitiveType.PrimitiveTypeName.BINARY,
                                    Type.Repetition.OPTIONAL)
                            .precision(20)
                            .as(OriginalType.DECIMAL)
                            .named("f11"),
                    Types.primitive(
                                    PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                                    Type.Repetition.OPTIONAL)
                            .length(16)
                            .precision(5)
                            .as(OriginalType.DECIMAL)
                            .named("f12"),
                    Types.primitive(
                                    PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                                    Type.Repetition.OPTIONAL)
                            .length(16)
                            .precision(15)
                            .as(OriginalType.DECIMAL)
                            .named("f13"),
                    Types.primitive(
                                    PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                                    Type.Repetition.OPTIONAL)
                            .length(16)
                            .precision(20)
                            .as(OriginalType.DECIMAL)
                            .named("f14"),
                    Types.list(Type.Repetition.OPTIONAL)
                            .element(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.BINARY,
                                                    Type.Repetition.OPTIONAL)
                                            .named("element"))
                            .named("f15"),
                    Types.list(Type.Repetition.OPTIONAL)
                            .element(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.BOOLEAN,
                                                    Type.Repetition.OPTIONAL)
                                            .named("element"))
                            .named("f16"),
                    Types.list(Type.Repetition.OPTIONAL)
                            .element(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.INT32,
                                                    Type.Repetition.OPTIONAL)
                                            .named("element"))
                            .named("f17"),
                    Types.list(Type.Repetition.OPTIONAL)
                            .element(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.INT32,
                                                    Type.Repetition.OPTIONAL)
                                            .named("element"))
                            .named("f18"),
                    Types.list(Type.Repetition.OPTIONAL)
                            .element(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.INT32,
                                                    Type.Repetition.OPTIONAL)
                                            .named("element"))
                            .named("f19"),
                    Types.list(Type.Repetition.OPTIONAL)
                            .element(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.INT64,
                                                    Type.Repetition.OPTIONAL)
                                            .named("element"))
                            .named("f20"),
                    Types.list(Type.Repetition.OPTIONAL)
                            .element(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.FLOAT,
                                                    Type.Repetition.OPTIONAL)
                                            .named("element"))
                            .named("f21"),
                    Types.list(Type.Repetition.OPTIONAL)
                            .element(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.DOUBLE,
                                                    Type.Repetition.OPTIONAL)
                                            .named("element"))
                            .named("f22"),
                    Types.list(Type.Repetition.OPTIONAL)
                            .element(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.INT96,
                                                    Type.Repetition.OPTIONAL)
                                            .named("element"))
                            .named("f23"),
                    Types.list(Type.Repetition.OPTIONAL)
                            .element(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.INT32,
                                                    Type.Repetition.OPTIONAL)
                                            .precision(5)
                                            .as(OriginalType.DECIMAL)
                                            .named("element"))
                            .named("f24"),
                    Types.list(Type.Repetition.OPTIONAL)
                            .element(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.INT64,
                                                    Type.Repetition.OPTIONAL)
                                            .precision(15)
                                            .as(OriginalType.DECIMAL)
                                            .named("element"))
                            .named("f25"),
                    Types.list(Type.Repetition.OPTIONAL)
                            .element(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.BINARY,
                                                    Type.Repetition.OPTIONAL)
                                            .precision(20)
                                            .as(OriginalType.DECIMAL)
                                            .named("element"))
                            .named("f26"),
                    Types.list(Type.Repetition.OPTIONAL)
                            .element(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName
                                                            .FIXED_LEN_BYTE_ARRAY,
                                                    Type.Repetition.OPTIONAL)
                                            .length(16)
                                            .precision(5)
                                            .as(OriginalType.DECIMAL)
                                            .named("element"))
                            .named("f27"),
                    Types.list(Type.Repetition.OPTIONAL)
                            .element(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName
                                                            .FIXED_LEN_BYTE_ARRAY,
                                                    Type.Repetition.OPTIONAL)
                                            .length(16)
                                            .precision(15)
                                            .as(OriginalType.DECIMAL)
                                            .named("element"))
                            .named("f28"),
                    Types.list(Type.Repetition.OPTIONAL)
                            .element(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName
                                                            .FIXED_LEN_BYTE_ARRAY,
                                                    Type.Repetition.OPTIONAL)
                                            .length(16)
                                            .precision(20)
                                            .as(OriginalType.DECIMAL)
                                            .named("element"))
                            .named("f29"),
                    Types.map(Type.Repetition.OPTIONAL)
                            .key(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.BINARY,
                                                    Type.Repetition.OPTIONAL)
                                            .named("key"))
                            .value(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.BINARY,
                                                    Type.Repetition.OPTIONAL)
                                            .named("value"))
                            .named("f30"),
                    Types.map(Type.Repetition.OPTIONAL)
                            .key(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.INT32,
                                                    Type.Repetition.OPTIONAL)
                                            .named("key"))
                            .value(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.BOOLEAN,
                                                    Type.Repetition.OPTIONAL)
                                            .named("value"))
                            .named("f31"),
                    Types.buildGroup(Type.Repetition.OPTIONAL)
                            .addField(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.BINARY,
                                                    Type.Repetition.OPTIONAL)
                                            .named("f32_1"))
                            .addField(
                                    Types.primitive(
                                                    PrimitiveType.PrimitiveTypeName.INT32,
                                                    Type.Repetition.OPTIONAL)
                                            .named("f32_2"))
                            .named("f32"));

    private static final RowType ROW_TYPE =
            RowType.of(
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new BooleanType(),
                    new TinyIntType(),
                    new SmallIntType(),
                    new IntType(),
                    new BigIntType(),
                    new FloatType(),
                    new DoubleType(),
                    new TimestampType(9),
                    new DecimalType(5, 0),
                    new DecimalType(15, 0),
                    new DecimalType(20, 0),
                    new DecimalType(5, 0),
                    new DecimalType(15, 0),
                    new DecimalType(20, 0),
                    new ArrayType(new VarCharType(VarCharType.MAX_LENGTH)),
                    new ArrayType(new BooleanType()),
                    new ArrayType(new TinyIntType()),
                    new ArrayType(new SmallIntType()),
                    new ArrayType(new IntType()),
                    new ArrayType(new BigIntType()),
                    new ArrayType(new FloatType()),
                    new ArrayType(new DoubleType()),
                    new ArrayType(new TimestampType(9)),
                    new ArrayType(new DecimalType(5, 0)),
                    new ArrayType(new DecimalType(15, 0)),
                    new ArrayType(new DecimalType(20, 0)),
                    new ArrayType(new DecimalType(5, 0)),
                    new ArrayType(new DecimalType(15, 0)),
                    new ArrayType(new DecimalType(20, 0)),
                    new MapType(
                            new VarCharType(VarCharType.MAX_LENGTH),
                            new VarCharType(VarCharType.MAX_LENGTH)),
                    new MapType(new IntType(), new BooleanType()),
                    RowType.of(new VarCharType(VarCharType.MAX_LENGTH), new IntType()));

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private final int rowGroupSize;

    @Parameterized.Parameters(name = "rowGroupSize-{0}")
    public static Collection<Integer> parameters() {
        return Arrays.asList(10, 1000);
    }

    public ParquetColumnarRowSplitReaderTest(int rowGroupSize) {
        this.rowGroupSize = rowGroupSize;
    }

    @Test
    public void testNormalTypesReadWithSplits() throws IOException {
        // prepare parquet file
        int number = 10000;
        List<RowData> records = new ArrayList<>(number);
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        for (int i = 0; i < number; i++) {
            Integer v = random.nextInt(number / 2);
            if (v % 10 == 0) {
                values.add(null);
                records.add(new GenericRowData(FIELD_NUMBER));
            } else {
                values.add(v);
                records.add(newRow(v));
            }
        }

        testNormalTypes(number, records, values);
    }

    @Test
    public void testReachEnd() throws Exception {
        // prepare parquet file
        int number = 5;
        List<RowData> records = new ArrayList<>(number);
        Random random = new Random();
        for (int i = 0; i < number; i++) {
            Integer v = random.nextInt(number / 2);
            if (v % 10 == 0) {
                records.add(new GenericRowData(FIELD_NUMBER));
            } else {
                records.add(newRow(v));
            }
        }

        Path testPath =
                createTempParquetFile(
                        TEMPORARY_FOLDER.newFolder(),
                        PARQUET_SCHEMA,
                        ROW_TYPE,
                        records,
                        rowGroupSize);

        ParquetColumnarRowSplitReader reader =
                createReader(
                        testPath, 0, testPath.getFileSystem().getFileStatus(testPath).getLen());
        while (!reader.reachedEnd()) {
            reader.nextRecord();
        }
        assertTrue(reader.reachedEnd());
    }

    private void testNormalTypes(int number, List<RowData> records, List<Integer> values)
            throws IOException {
        Path testPath =
                createTempParquetFile(
                        TEMPORARY_FOLDER.newFolder(),
                        PARQUET_SCHEMA,
                        ROW_TYPE,
                        records,
                        rowGroupSize);

        // test reading and splitting
        long fileLen = testPath.getFileSystem().getFileStatus(testPath).getLen();
        int len1 = readSplitAndCheck(0, 0, testPath, 0, fileLen / 3, values);
        int len2 = readSplitAndCheck(len1, 0, testPath, fileLen / 3, fileLen * 2 / 3, values);
        int len3 =
                readSplitAndCheck(
                        len1 + len2, 0, testPath, fileLen * 2 / 3, Long.MAX_VALUE, values);
        assertEquals(number, len1 + len2 + len3);

        // test seek
        assertEquals(
                number - number / 2,
                readSplitAndCheck(number / 2, number / 2, testPath, 0, fileLen, values));
    }

    private ParquetColumnarRowSplitReader createReader(
            Path testPath, long splitStart, long splitLength) throws IOException {
        return new ParquetColumnarRowSplitReader(
                false,
                true,
                new Configuration(),
                ROW_TYPE.getChildren().toArray(new LogicalType[] {}),
                new String[] {
                    "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12",
                    "f13", "f14", "f15", "f16", "f17", "f18", "f19", "f20", "f21", "f22", "f23",
                    "f24", "f25", "f26", "f27", "f28", "f29", "f30", "f31", "f32"
                },
                VectorizedColumnBatch::new,
                500,
                new org.apache.hadoop.fs.Path(testPath.getPath()),
                splitStart,
                splitLength);
    }

    private int readSplitAndCheck(
            int start,
            long seekToRow,
            Path testPath,
            long splitStart,
            long splitLength,
            List<Integer> values)
            throws IOException {
        ParquetColumnarRowSplitReader reader = createReader(testPath, splitStart, splitLength);
        reader.seekToRow(seekToRow);

        int i = start;
        while (!reader.reachedEnd()) {
            ColumnarRowData row = reader.nextRecord();
            Integer v = values.get(i);
            if (v == null) {
                assertTrue(row.isNullAt(0));
                assertTrue(row.isNullAt(1));
                assertTrue(row.isNullAt(2));
                assertTrue(row.isNullAt(3));
                assertTrue(row.isNullAt(4));
                assertTrue(row.isNullAt(5));
                assertTrue(row.isNullAt(6));
                assertTrue(row.isNullAt(7));
                assertTrue(row.isNullAt(8));
                assertTrue(row.isNullAt(9));
                assertTrue(row.isNullAt(10));
                assertTrue(row.isNullAt(11));
                assertTrue(row.isNullAt(12));
                assertTrue(row.isNullAt(13));
                assertTrue(row.isNullAt(14));
                assertTrue(row.isNullAt(15));
                assertTrue(row.isNullAt(16));
                assertTrue(row.isNullAt(17));
                assertTrue(row.isNullAt(18));
                assertTrue(row.isNullAt(19));
                assertTrue(row.isNullAt(20));
                assertTrue(row.isNullAt(21));
                assertTrue(row.isNullAt(22));
                assertTrue(row.isNullAt(23));
                assertTrue(row.isNullAt(24));
                assertTrue(row.isNullAt(25));
                assertTrue(row.isNullAt(26));
                assertTrue(row.isNullAt(27));
                assertTrue(row.isNullAt(28));
                assertTrue(row.isNullAt(29));
                assertTrue(row.isNullAt(30));
                assertTrue(row.isNullAt(31));
                assertTrue(row.isNullAt(32));
            } else {
                assertEquals("" + v, row.getString(0).toString());
                assertEquals(v % 2 == 0, row.getBoolean(1));
                assertEquals(v.byteValue(), row.getByte(2));
                assertEquals(v.shortValue(), row.getShort(3));
                assertEquals(v.intValue(), row.getInt(4));
                assertEquals(v.longValue(), row.getLong(5));
                assertEquals(v.floatValue(), row.getFloat(6), 0);
                assertEquals(v.doubleValue(), row.getDouble(7), 0);
                assertEquals(toDateTime(v), row.getTimestamp(8, 9).toLocalDateTime());
                if (DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0) == null) {
                    assertTrue(row.isNullAt(9));
                    assertTrue(row.isNullAt(12));
                    assertTrue(row.isNullAt(24));
                    assertTrue(row.isNullAt(27));
                } else {
                    assertEquals(
                            DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0),
                            row.getDecimal(9, 5, 0));
                    assertEquals(
                            DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0),
                            row.getDecimal(12, 5, 0));
                    assertEquals(
                            DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0),
                            row.getArray(24).getDecimal(0, 5, 0));
                    assertEquals(
                            DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0),
                            row.getArray(27).getDecimal(0, 5, 0));
                }
                assertEquals(
                        DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0),
                        row.getDecimal(10, 15, 0));
                assertEquals(
                        DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0),
                        row.getDecimal(11, 20, 0));
                assertEquals(
                        DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0),
                        row.getDecimal(13, 15, 0));
                assertEquals(
                        DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0),
                        row.getDecimal(14, 20, 0));
                assertEquals("" + v, row.getArray(15).getString(0).toString());
                assertEquals(v % 2 == 0, row.getArray(16).getBoolean(0));
                assertEquals(v.byteValue(), row.getArray(17).getByte(0));
                assertEquals(v.shortValue(), row.getArray(18).getShort(0));
                assertEquals(v.intValue(), row.getArray(19).getInt(0));
                assertEquals(v.longValue(), row.getArray(20).getLong(0));
                assertEquals(v.floatValue(), row.getArray(21).getFloat(0), 0);
                assertEquals(v.doubleValue(), row.getArray(22).getDouble(0), 0);
                assertEquals(toDateTime(v), row.getArray(23).getTimestamp(0, 9).toLocalDateTime());

                assertEquals(
                        DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0),
                        row.getArray(25).getDecimal(0, 15, 0));
                assertEquals(
                        DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0),
                        row.getArray(26).getDecimal(0, 20, 0));
                assertEquals(
                        DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0),
                        row.getArray(28).getDecimal(0, 15, 0));
                assertEquals(
                        DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0),
                        row.getArray(29).getDecimal(0, 20, 0));
                assertEquals("" + v, row.getMap(30).valueArray().getString(0).toString());
                assertEquals(v % 2 == 0, row.getMap(31).valueArray().getBoolean(0));
                assertEquals("" + v, row.getRow(32, 2).getString(0).toString());
                assertEquals(v.intValue(), row.getRow(32, 2).getInt(1));
            }
            i++;
        }
        reader.close();
        return i - start;
    }

    private RowData newRow(Integer v) {
        Map<StringData, StringData> f30 = new HashMap<>();
        f30.put(StringData.fromString("" + v), StringData.fromString("" + v));

        Map<Integer, Boolean> f31 = new HashMap<>();
        f31.put(v, v % 2 == 0);

        return GenericRowData.of(
                StringData.fromString("" + v),
                v % 2 == 0,
                v.byteValue(),
                v.shortValue(),
                v,
                v.longValue(),
                v.floatValue(),
                v.doubleValue(),
                TimestampData.fromLocalDateTime(toDateTime(v)),
                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0),
                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0),
                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0),
                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0),
                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0),
                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0),
                new GenericArrayData(new Object[] {StringData.fromString("" + v), null}),
                new GenericArrayData(new Object[] {v % 2 == 0, null}),
                new GenericArrayData(new Object[] {v.byteValue(), null}),
                new GenericArrayData(new Object[] {v.shortValue(), null}),
                new GenericArrayData(new Object[] {v, null}),
                new GenericArrayData(new Object[] {v.longValue(), null}),
                new GenericArrayData(new Object[] {v.floatValue(), null}),
                new GenericArrayData(new Object[] {v.doubleValue(), null}),
                new GenericArrayData(
                        new Object[] {TimestampData.fromLocalDateTime(toDateTime(v)), null}),
                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0) == null
                        ? null
                        : new GenericArrayData(
                                new Object[] {
                                    DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0), null
                                }),
                new GenericArrayData(
                        new Object[] {
                            DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0), null
                        }),
                new GenericArrayData(
                        new Object[] {
                            DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0), null
                        }),
                DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0) == null
                        ? null
                        : new GenericArrayData(
                                new Object[] {
                                    DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 5, 0), null
                                }),
                new GenericArrayData(
                        new Object[] {
                            DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 15, 0), null
                        }),
                new GenericArrayData(
                        new Object[] {
                            DecimalData.fromBigDecimal(BigDecimal.valueOf(v), 20, 0), null
                        }),
                new GenericMapData(f30),
                new GenericMapData(f31),
                GenericRowData.of(StringData.fromString("" + v), v));
    }

    private LocalDateTime toDateTime(Integer v) {
        v = (v > 0 ? v : -v) % 10000;
        return BASE_TIME.plusNanos(v).plusSeconds(v);
    }

    @Test
    public void testDictionary() throws IOException {
        // prepare parquet file
        int number = 10000;
        List<RowData> records = new ArrayList<>(number);
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        int[] intValues = new int[10];
        // test large values in dictionary
        for (int i = 0; i < intValues.length; i++) {
            intValues[i] = random.nextInt();
        }
        for (int i = 0; i < number; i++) {
            Integer v = intValues[random.nextInt(10)];
            if (v == 0) {
                values.add(null);
                records.add(new GenericRowData(FIELD_NUMBER));
            } else {
                values.add(v);
                records.add(newRow(v));
            }
        }

        testNormalTypes(number, records, values);
    }

    @Test
    public void testPartialDictionary() throws IOException {
        // prepare parquet file
        int number = 10000;
        List<RowData> records = new ArrayList<>(number);
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        int[] intValues = new int[10];
        // test large values in dictionary
        for (int i = 0; i < intValues.length; i++) {
            intValues[i] = random.nextInt();
        }
        for (int i = 0; i < number; i++) {
            Integer v = i < 5000 ? intValues[random.nextInt(10)] : i;
            if (v == 0) {
                values.add(null);
                records.add(new GenericRowData(FIELD_NUMBER));
            } else {
                values.add(v);
                records.add(newRow(v));
            }
        }

        testNormalTypes(number, records, values);
    }

    @Test
    public void testContinuousRepetition() throws IOException {
        // prepare parquet file
        int number = 10000;
        List<RowData> records = new ArrayList<>(number);
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            Integer v = random.nextInt(10);
            for (int j = 0; j < 100; j++) {
                if (v == 0) {
                    values.add(null);
                    records.add(new GenericRowData(FIELD_NUMBER));
                } else {
                    values.add(v);
                    records.add(newRow(v));
                }
            }
        }

        testNormalTypes(number, records, values);
    }

    @Test
    public void testLargeValue() throws IOException {
        // prepare parquet file
        int number = 10000;
        List<RowData> records = new ArrayList<>(number);
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        for (int i = 0; i < number; i++) {
            Integer v = random.nextInt();
            if (v % 10 == 0) {
                values.add(null);
                records.add(new GenericRowData(FIELD_NUMBER));
            } else {
                values.add(v);
                records.add(newRow(v));
            }
        }

        testNormalTypes(number, records, values);
    }

    @Test
    public void testProject() throws IOException {
        // prepare parquet file
        int number = 1000;
        List<RowData> records = new ArrayList<>(number);
        for (int i = 0; i < number; i++) {
            Integer v = i;
            records.add(newRow(v));
        }
        Path testPath =
                createTempParquetFile(
                        TEMPORARY_FOLDER.newFolder(),
                        PARQUET_SCHEMA,
                        ROW_TYPE,
                        records,
                        rowGroupSize);
        RowType rowType = RowType.of(new DoubleType(), new TinyIntType(), new IntType());
        // test reader
        ParquetColumnarRowSplitReader reader =
                new ParquetColumnarRowSplitReader(
                        false,
                        true,
                        new Configuration(),
                        rowType.getChildren().toArray(new LogicalType[] {}),
                        new String[] {"f7", "f2", "f4"},
                        VectorizedColumnBatch::new,
                        500,
                        new org.apache.hadoop.fs.Path(testPath.getPath()),
                        0,
                        Long.MAX_VALUE);
        int i = 0;
        while (!reader.reachedEnd()) {
            ColumnarRowData row = reader.nextRecord();
            assertEquals(i, row.getDouble(0), 0);
            assertEquals((byte) i, row.getByte(1));
            assertEquals(i, row.getInt(2));
            i++;
        }
        reader.close();
    }

    @Test
    public void testPartitionValues() throws IOException {
        // prepare parquet file
        int number = 1000;
        List<RowData> records = new ArrayList<>(number);
        for (int i = 0; i < number; i++) {
            Integer v = i;
            records.add(newRow(v));
        }
        Path testPath =
                createTempParquetFile(
                        TEMPORARY_FOLDER.newFolder(),
                        PARQUET_SCHEMA,
                        ROW_TYPE,
                        records,
                        rowGroupSize);

        // test reader
        Map<String, Object> partSpec = new HashMap<>();
        partSpec.put("f33", true);
        partSpec.put("f34", Date.valueOf("2020-11-23"));
        partSpec.put("f35", LocalDateTime.of(1999, 1, 1, 1, 1));
        partSpec.put("f36", 6.6);
        partSpec.put("f37", (byte) 9);
        partSpec.put("f38", (short) 10);
        partSpec.put("f39", 11);
        partSpec.put("f40", 12L);
        partSpec.put("f41", 13f);
        partSpec.put("f42", new BigDecimal(42));
        partSpec.put("f43", new BigDecimal(43));
        partSpec.put("f44", new BigDecimal(44));
        partSpec.put("f45", "f45");

        innerTestPartitionValues(testPath, partSpec, false);

        for (String k : new ArrayList<>(partSpec.keySet())) {
            partSpec.put(k, null);
        }

        innerTestPartitionValues(testPath, partSpec, true);
    }

    private void innerTestPartitionValues(
            Path testPath, Map<String, Object> partSpec, boolean nullPartValue) throws IOException {
        LogicalType[] fieldTypes =
                new LogicalType[] {
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new BooleanType(),
                    new TinyIntType(),
                    new SmallIntType(),
                    new IntType(),
                    new BigIntType(),
                    new FloatType(),
                    new DoubleType(),
                    new TimestampType(9),
                    new DecimalType(5, 0),
                    new DecimalType(15, 0),
                    new DecimalType(20, 0),
                    new DecimalType(5, 0),
                    new DecimalType(15, 0),
                    new DecimalType(20, 0),
                    new ArrayType(new VarCharType(VarCharType.MAX_LENGTH)),
                    new ArrayType(new BooleanType()),
                    new ArrayType(new TinyIntType()),
                    new ArrayType(new SmallIntType()),
                    new ArrayType(new IntType()),
                    new ArrayType(new BigIntType()),
                    new ArrayType(new FloatType()),
                    new ArrayType(new DoubleType()),
                    new ArrayType(new TimestampType(9)),
                    new ArrayType(new DecimalType(5, 0)),
                    new ArrayType(new DecimalType(15, 0)),
                    new ArrayType(new DecimalType(20, 0)),
                    new ArrayType(new DecimalType(5, 0)),
                    new ArrayType(new DecimalType(15, 0)),
                    new ArrayType(new DecimalType(20, 0)),
                    new MapType(
                            new VarCharType(VarCharType.MAX_LENGTH),
                            new VarCharType(VarCharType.MAX_LENGTH)),
                    new MapType(new IntType(), new BooleanType()),
                    RowType.of(new VarCharType(VarCharType.MAX_LENGTH), new IntType()),
                    new BooleanType(),
                    new DateType(),
                    new TimestampType(9),
                    new DoubleType(),
                    new TinyIntType(),
                    new SmallIntType(),
                    new IntType(),
                    new BigIntType(),
                    new FloatType(),
                    new DecimalType(5, 0),
                    new DecimalType(15, 0),
                    new DecimalType(20, 0),
                    new VarCharType(VarCharType.MAX_LENGTH)
                };
        ParquetColumnarRowSplitReader reader =
                ParquetSplitReaderUtil.genPartColumnarRowReader(
                        false,
                        true,
                        new Configuration(),
                        IntStream.range(0, 46).mapToObj(i -> "f" + i).toArray(String[]::new),
                        Arrays.stream(fieldTypes)
                                .map(TypeConversions::fromLogicalToDataType)
                                .toArray(DataType[]::new),
                        partSpec,
                        new int[] {7, 2, 4, 33, 37, 38, 39, 40, 41, 36, 34, 35, 42, 43, 44, 45},
                        rowGroupSize,
                        new Path(testPath.getPath()),
                        0,
                        Long.MAX_VALUE);
        int i = 0;
        while (!reader.reachedEnd()) {
            ColumnarRowData row = reader.nextRecord();

            // common values
            assertEquals(i, row.getDouble(0), 0);
            assertEquals((byte) i, row.getByte(1));
            assertEquals(i, row.getInt(2));

            // partition values
            if (nullPartValue) {
                for (int j = 3; j < 16; j++) {
                    assertTrue(row.isNullAt(j));
                }
            } else {
                assertTrue(row.getBoolean(3));
                assertEquals(9, row.getByte(4));
                assertEquals(10, row.getShort(5));
                assertEquals(11, row.getInt(6));
                assertEquals(12, row.getLong(7));
                assertEquals(13, row.getFloat(8), 0);
                assertEquals(6.6, row.getDouble(9), 0);
                assertEquals(
                        DateTimeUtils.dateToInternal(Date.valueOf("2020-11-23")), row.getInt(10));
                assertEquals(
                        LocalDateTime.of(1999, 1, 1, 1, 1),
                        row.getTimestamp(11, 9).toLocalDateTime());
                assertEquals(
                        DecimalData.fromBigDecimal(new BigDecimal(42), 5, 0),
                        row.getDecimal(12, 5, 0));
                assertEquals(
                        DecimalData.fromBigDecimal(new BigDecimal(43), 15, 0),
                        row.getDecimal(13, 15, 0));
                assertEquals(
                        DecimalData.fromBigDecimal(new BigDecimal(44), 20, 0),
                        row.getDecimal(14, 20, 0));
                assertEquals("f45", row.getString(15).toString());
            }

            i++;
        }
        reader.close();
    }
}
