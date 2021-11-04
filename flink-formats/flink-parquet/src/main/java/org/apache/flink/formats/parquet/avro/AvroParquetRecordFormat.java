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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/** */
public class AvroParquetRecordFormat<E> implements StreamFormat<E> {

    private static final long serialVersionUID = 1L;

    static final Logger LOG = LoggerFactory.getLogger(AvroParquetRecordFormat.class);

    private final TypeInformation<E> type;

    AvroParquetRecordFormat(TypeInformation<E> type) {
        this.type = type;
    }

    /**
     * Creates a new reader to read avro {@link GenericRecord} from Parquet input stream.
     *
     * <p>Several wrapper classes haven be created to Flink abstraction become compatible with the
     * parquet abstraction. Please refer to the inner classes {@link AvroParquetRecordReader},
     * {@link ParquetInputFile}, {@link FSDataInputStreamAdapter} for details.
     */
    @Override
    public Reader<E> createReader(
            Configuration config, FSDataInputStream stream, long fileLen, long splitEnd)
            throws IOException {

        // current version does not support splitting.
        checkNotSplit(fileLen, splitEnd);

        return new AvroParquetRecordReader<E>(
                AvroParquetReader.<E>builder(new ParquetInputFile(stream, fileLen))
                        .withDataModel(getDataModel())
                        .build());
    }

    @VisibleForTesting
    GenericData getDataModel() {
        Class<E> typeClass = getProducedType().getTypeClass();
        if (org.apache.avro.specific.SpecificRecordBase.class.isAssignableFrom(typeClass)) {
            return SpecificData.get();
        } else if (org.apache.avro.generic.GenericRecord.class.isAssignableFrom(typeClass)) {
            return GenericData.get();
        } else {
            return ReflectData.get();
        }
    }

    /**
     * Restores the reader from a checkpointed position. Since current version does not support
     * splitting, {@code restoredOffset} will be used for seeking.
     */
    @Override
    public Reader<E> restoreReader(
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
    public TypeInformation<E> getProducedType() {
        return type;
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
    private static class AvroParquetRecordReader<E> implements StreamFormat.Reader<E> {

        private final ParquetReader<E> parquetReader;

        private AvroParquetRecordReader(ParquetReader<E> parquetReader) {
            this.parquetReader = parquetReader;
        }

        @Nullable
        @Override
        public E read() throws IOException {
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
