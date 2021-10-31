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

package org.apache.flink.formats.csv;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Scanner;

import static org.apache.flink.util.Preconditions.checkNotNull;

@PublicEvolving
public class CsvStreamFormat<T> extends SimpleStreamFormat<T> {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_CHARSET_NAME = "UTF-8";
    public static final String DEFAULT_LINE_DELIMITER = "\n";

    private final String charset;
    private final String delimiter;
    private final ObjectReader objectReader;
    private final Class<T> outType;

    private CsvStreamFormat(
            ObjectReader objectReader,
            Class<T> outType,
            String delimiter,
            String charset
        ) {
        checkNotNull(objectReader);
        checkNotNull(outType);
        checkNotNull(delimiter);
        checkNotNull(charset);
        this.objectReader = objectReader;
        this.outType = outType;
        this.charset = charset;
        this.delimiter = delimiter;
    }

    public static <T> CsvStreamFormat<T> from(ObjectReader objectReader, Class<T> outType) {
        return new CsvStreamFormat<>(objectReader, outType, DEFAULT_LINE_DELIMITER, DEFAULT_CHARSET_NAME);
    }

    public static <T> CsvStreamFormat<T> from(ObjectReader objectReader, Class<T> outType, String delimiter) {
        return new CsvStreamFormat<>(objectReader, outType, delimiter, DEFAULT_CHARSET_NAME);
    }

    public static <T> CsvStreamFormat<T> from(ObjectReader objectReader, Class<T> outType, String delimiter, String charset) {
        return new CsvStreamFormat<>(objectReader, outType, delimiter, charset);
    }

    @Override
    public Reader createReader(Configuration config, FSDataInputStream stream) throws IOException {
        final Scanner scanner = new Scanner(stream, charset);
        scanner.useDelimiter(delimiter);
        return new Reader(scanner);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(outType);
    }

    // ------------------------------------------------------------------------

    /** The actual reader for the {@code DelimitedFormat}. */
    public final class Reader implements StreamFormat.Reader<T> {

        private final Scanner scanner;

        Reader(final Scanner scanner) {
            this.scanner = scanner;
        }

        @Nullable
        @Override
        public T read() throws IOException {
            String token = scanner.hasNext() ? scanner.next() : null;
            return token != null ? objectReader.readValue(token, outType): null;
        }

        @Override
        public void close() throws IOException {
            scanner.close();
        }
    }
}
