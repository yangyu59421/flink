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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import javax.annotation.Nullable;

import java.io.IOException;

/** */
public class AvroParquetRecordFormat implements StreamFormat<GenericRecord> {

    private final transient Schema schema;

    public AvroParquetRecordFormat(Schema schema) {
        this.schema = schema;
    }

    /**
     * Creates a new reader to read avro {@link GenericRecord} from Parquet input stream.
     *
     * <p>Several wrapper classes haven be created to Flink abstraction become compatible with the
     * parquet abstraction. Please refer to the inner classes {@link AvroParquetRecordReader},
     * {@link ParquetInputFile}, {@link FSDataInputStreamAdapter} for details.
     */
    @Override
    public Reader<GenericRecord> createReader(
            Configuration config, FSDataInputStream stream, long fileLen, long splitEnd)
            throws IOException {

        // current version does not support splitting.
        checkNotSplit(fileLen, splitEnd);

        return new AvroParquetRecordReader(
                AvroParquetReader.<GenericRecord>builder(new ParquetInputFile(stream, fileLen))
                        .withDataModel(GenericData.get())
                        .build());
    }

    /**
     * Restores the reader from a checkpointed position. Since current version does not support
     * splitting, {@code restoredOffset} will used for seeking.
     */
    @Override
    public Reader<GenericRecord> restoreReader(
            Configuration config,
            FSDataInputStream stream,
            long restoredOffset,
            long fileLen,
            long splitEnd)
            throws IOException {

        // current version does not support splitting.
        checkNotSplit(fileLen, splitEnd);

        // current version just ignore the splitOffset and use restoredOffset
        stream.seek(restoredOffset);

        return createReader(config, stream, fileLen, splitEnd);
    }

    /** Current version does not support splitting. */
    @Override
    public boolean isSplittable() {
        return false;
    }

    /**
     * Gets the type produced by this format. This type will be the type produced by the file source
     * as a whole.
     */
    @Override
    public TypeInformation<GenericRecord> getProducedType() {
        return new GenericRecordAvroTypeInfo(schema);
    }

    private static void checkNotSplit(long fileLen, long splitEnd) {
        if (splitEnd != fileLen) {
            throw new IllegalArgumentException(
                    String.format(
                            "Current version of AvroParquetRecordFormat is not splittable, "
                                    + "but found split end (%d) different from file length (%d)",
                            splitEnd, fileLen));
        }
    }

    /**
     * {@link StreamFormat.Reader} implementation. Using {@link ParquetReader} internally to read
     * avro {@link GenericRecord} from parquet {@link InputFile}.
     */
    private static class AvroParquetRecordReader implements StreamFormat.Reader<GenericRecord> {

        private final ParquetReader<GenericRecord> parquetReader;

        private AvroParquetRecordReader(ParquetReader<GenericRecord> parquetReader) {
            this.parquetReader = parquetReader;
        }

        @Nullable
        @Override
        public GenericRecord read() throws IOException {
            return parquetReader.read();
        }

        @Override
        public void close() throws IOException {
            parquetReader.close();
        }
    }

    /**
     * Parquet {@link InputFile} implementation, {@link #newStream()} call will delegate to Flink
     * {@link FSDataInputStream}.
     */
    private static class ParquetInputFile implements InputFile {

        private final FSDataInputStream inputStream;
        private final long length;

        private ParquetInputFile(FSDataInputStream inputStream, long length) {
            this.inputStream = inputStream;
            this.length = length;
        }

        @Override
        public long getLength() {
            return length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new FSDataInputStreamAdapter(inputStream);
        }
    }

    /**
     * Adapter which makes {@link FSDataInputStream} become compatible with parquet {@link
     * SeekableInputStream}.
     */
    private static class FSDataInputStreamAdapter extends DelegatingSeekableInputStream {

        private final FSDataInputStream inputStream;

        private FSDataInputStreamAdapter(FSDataInputStream inputStream) {
            super(inputStream);
            this.inputStream = inputStream;
        }

        @Override
        public long getPos() throws IOException {
            return inputStream.getPos();
        }

        @Override
        public void seek(long newPos) throws IOException {
            inputStream.seek(newPos);
        }
    }
}
