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
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.formats.csv.RowDataToCsvConverters.RowDataToCsvFormatConverter
        .RowDataToCsvFormatConverterContext;
import org.apache.flink.table.data.RowData;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;

@PublicEvolving
public class CsvBulkWriter implements BulkWriter<RowData> {

    private final FSDataOutputStream stream;
    private final CsvMapper mapper;
    private final Converter<RowData, JsonNode, RowDataToCsvFormatConverterContext> converter;
    private final ObjectWriter csvWriter;
    /** Reusable within the converter **/
    private final transient ObjectNode container;

    public CsvBulkWriter(
            CsvMapper mapper,
            CsvSchema schema,
            Converter<RowData, JsonNode, RowDataToCsvFormatConverterContext> converter,
            FSDataOutputStream stream) {
        this.mapper = mapper;
        this.converter = converter;
        this.stream = stream;

        this.container = mapper.createObjectNode();
        this.csvWriter = mapper.writerFor(JsonNode.class).with(schema);
    }

    @Override
    public void addElement(RowData element) throws IOException {
        System.out.println(element);
        RowDataToCsvFormatConverterContext context =
                new RowDataToCsvFormatConverterContext(mapper, container);
        JsonNode jsonNode = converter.convert(element, context);
        System.out.println(jsonNode);
        csvWriter.writeValue(stream, jsonNode);
    }

    @Override
    public void flush() throws IOException {
        stream.flush();
    }

    @Override
    public void finish() throws IOException {
        stream.sync();
    }
}
