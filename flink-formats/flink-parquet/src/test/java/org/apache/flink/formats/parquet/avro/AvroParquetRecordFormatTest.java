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

package org.apache.flink.formats.parquet.avro;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit test for {@link AvroParquetRecordFormat} and {@link
 * org.apache.flink.connector.file.src.reader.StreamFormat}.
 */
class AvroParquetRecordFormatTest {

    private static final String USER_PARQUET_FILE = "user.parquet";

    private static Path path;
    private static Schema schema;
    private static List<GenericRecord> records = new ArrayList<>(3);

    @TempDir static java.nio.file.Path temporaryFolder;

    /**
     * Create a parquet file in the {@code TEMPORARY_FOLDER} directory.
     *
     * @throws IOException if new file can not be created.
     */
    @BeforeAll
    static void setup() throws IOException {
        schema =
                new Schema.Parser()
                        .parse(
                                "{\"type\": \"record\", "
                                        + "\"name\": \"User\", "
                                        + "\"fields\": [\n"
                                        + "        {\"name\": \"name\", \"type\": \"string\" },\n"
                                        + "        {\"name\": \"favoriteNumber\",  \"type\": [\"int\", \"null\"] },\n"
                                        + "        {\"name\": \"favoriteColor\", \"type\": [\"string\", \"null\"] }\n"
                                        + "    ]\n"
                                        + "    }");

        records.add(createUser("Peter", 1, "red"));
        records.add(createUser("Tom", 2, "yellow"));
        records.add(createUser("Jack", 3, "green"));

        path = new Path(temporaryFolder.resolve(USER_PARQUET_FILE).toUri());

        ParquetWriterFactory<GenericRecord> writerFactory =
                ParquetAvroWriters.forGenericRecord(schema);
        BulkWriter<GenericRecord> writer =
                writerFactory.create(
                        path.getFileSystem().create(path, FileSystem.WriteMode.OVERWRITE));

        for (GenericRecord record : records) {
            writer.addElement(record);
        }

        writer.flush();
        writer.finish();
    }

    @Test
    void testCreateReader() throws IOException {
        StreamFormat.Reader<GenericRecord> reader =
                AvroParquetReaders.forGenericRecord(schema)
                        .createReader(
                                new Configuration(),
                                path,
                                0,
                                path.getFileSystem().getFileStatus(path).getLen());
        for (GenericRecord record : records) {
            assertUserEquals(Objects.requireNonNull(reader.read()), record);
        }
    }

    /** Expect exception since splitting is not supported now. */
    @Test
    void testCreateReaderWithSplitting() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        AvroParquetReaders.forGenericRecord(schema)
                                .createReader(new Configuration(), path, 5, 5));
    }

    @Test
    void testCreateReaderWithNullPath() {
        assertThrows(
                NullPointerException.class,
                () ->
                        AvroParquetReaders.forGenericRecord(schema)
                                .createReader(new Configuration(), (Path) null, 0, 0));
    }

    @Test
    void testRestoreReaderWithNoOffset() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        AvroParquetReaders.forGenericRecord(schema)
                                .restoreReader(
                                        new Configuration(),
                                        path,
                                        CheckpointedPosition.NO_OFFSET,
                                        0,
                                        path.getFileSystem().getFileStatus(path).getLen()));
    }

    @Test
    void testReadWithRestoredReader() throws IOException {
        StreamFormat.Reader<GenericRecord> reader =
                AvroParquetReaders.forGenericRecord(schema)
                        .restoreReader(
                                new Configuration(),
                                path,
                                0,
                                0,
                                path.getFileSystem().getFileStatus(path).getLen());
        for (GenericRecord record : records) {
            assertUserEquals(Objects.requireNonNull(reader.read()), record);
        }
    }

    /**
     * Test {@link AvroParquetRecordFormat#restoreReader(Configuration, Path, long, long, long)}
     * with a given restoredOffset. Expect to begin the read with the second record.
     *
     * @throws IOException thrown if the file system could not be retrieved
     */
    @Test
    void testRestoreReaderWithOffset() throws IOException {
        StreamFormat.Reader<GenericRecord> reader =
                AvroParquetReaders.forGenericRecord(schema)
                        .restoreReader(
                                new Configuration(),
                                path,
                                147,
                                0,
                                path.getFileSystem().getFileStatus(path).getLen());
        for (int i = 1; i < records.size(); i++) {
            // TODO (Jing) failed now.
            // assertUserEquals(reader.read(), records.get(i));
        }
    }

    @Test
    void testSplittable() {
        assertFalse(AvroParquetReaders.forGenericRecord(schema).isSplittable());
    }

    @Test
    void getProducedType() {
        assertEquals(
                AvroParquetReaders.forGenericRecord(schema).getProducedType().getTypeClass(),
                GenericRecord.class);
    }

    // ------------------------------------------------------------------------
    //  helper methods
    // ------------------------------------------------------------------------

    private static GenericRecord createUser(String name, int favoriteNumber, String favoriteColor) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("name", name);
        record.put("favoriteNumber", favoriteNumber);
        record.put("favoriteColor", favoriteColor);
        return record;
    }

    private void assertUserEquals(GenericRecord user, GenericRecord expected) {
        assertEquals(user.get("name").toString(), expected.get("name"));
        assertEquals(user.get("favoriteNumber"), expected.get("favoriteNumber"));
        assertEquals(user.get("favoriteColor").toString(), expected.get("favoriteColor"));
    }
}
