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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.BulkWriter.Factory;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.formats.csv.RowDataToCsvConverters.RowDataToCsvFormatConverter;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.BulkDecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.BulkReaderFormatFactory;
import org.apache.flink.table.factories.BulkWriterFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.apache.commons.lang3.StringEscapeUtils;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.formats.csv.CsvFormatOptions.ALLOW_COMMENTS;
import static org.apache.flink.formats.csv.CsvFormatOptions.ARRAY_ELEMENT_DELIMITER;
import static org.apache.flink.formats.csv.CsvFormatOptions.DISABLE_QUOTE_CHARACTER;
import static org.apache.flink.formats.csv.CsvFormatOptions.ESCAPE_CHARACTER;
import static org.apache.flink.formats.csv.CsvFormatOptions.FIELD_DELIMITER;
import static org.apache.flink.formats.csv.CsvFormatOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.csv.CsvFormatOptions.NULL_LITERAL;
import static org.apache.flink.formats.csv.CsvFormatOptions.QUOTE_CHARACTER;

/** CSV format factory for file system. */
@Internal
//public class CsvFormatFactory implements BulkReaderFormatFactory {
public class CsvFormatFactory implements BulkReaderFormatFactory, BulkWriterFormatFactory {

    public static final String IDENTIFIER = "csv";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FIELD_DELIMITER);
        options.add(DISABLE_QUOTE_CHARACTER);
        options.add(QUOTE_CHARACTER);
        options.add(ALLOW_COMMENTS);
        options.add(IGNORE_PARSE_ERRORS);
        options.add(ARRAY_ELEMENT_DELIMITER);
        options.add(ESCAPE_CHARACTER);
        options.add(NULL_LITERAL);
        return options;
    }

    // TODO: is it possible to avoid the cast with a reasonable effort?
    @Override
    @SuppressWarnings({
        "unchecked",
        "rawtypes"
    })
    public BulkDecodingFormat<RowData> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return new BulkDecodingFormat<RowData>() {
            @Override
            public BulkFormat<RowData, FileSourceSplit> createRuntimeDecoder(
                    DynamicTableSource.Context context, DataType physicalDataType) {
                RowType rowType = (RowType) physicalDataType.getLogicalType();
                CsvSchema schema = buildCsvSchema(rowType, formatOptions);

                Converter<JsonNode, RowData, Void> converter =
                        (Converter)
                                new CsvToRowDataConverters(false).createRowConverter(rowType, true);
                return new StreamFormatAdapter<>(
                        new CsvFormat<>(
                                new CsvMapper(),
                                schema,
                                JsonNode.class,
                                converter,
                                context.createTypeInformation(physicalDataType)));
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<Factory<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return new EncodingFormat<BulkWriter.Factory<RowData>>() {
            @Override
            public BulkWriter.Factory<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType physicalDataType) {

                RowType rowType = (RowType) physicalDataType.getLogicalType();
                CsvSchema schema = buildCsvSchema(rowType, formatOptions);

                RowDataToCsvFormatConverter converter =
                        RowDataToCsvConverters.createRowFormatConverter(rowType);
                return out -> new CsvBulkWriter(new CsvMapper(), schema, converter, out);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }


    private CsvSchema buildCsvSchema(RowType rowType, ReadableConfig options) {
        CsvSchema csvSchema = CsvRowSchemaConverter.convert(rowType);
        CsvSchema.Builder csvBuilder = csvSchema.rebuild();
        // format properties
        options.getOptional(FIELD_DELIMITER)
                .map(s -> StringEscapeUtils.unescapeJava(s).charAt(0))
                .ifPresent(csvBuilder::setColumnSeparator);

        options.getOptional(QUOTE_CHARACTER)
                .map(s -> s.charAt(0))
                .ifPresent(csvBuilder::setQuoteChar);

        options.getOptional(ALLOW_COMMENTS).ifPresent(csvBuilder::setAllowComments);

        options.getOptional(ARRAY_ELEMENT_DELIMITER)
                .ifPresent(csvBuilder::setArrayElementSeparator);

        options.getOptional(ARRAY_ELEMENT_DELIMITER)
                .ifPresent(csvBuilder::setArrayElementSeparator);

        options.getOptional(ESCAPE_CHARACTER)
                .map(s -> s.charAt(0))
                .ifPresent(csvBuilder::setEscapeChar);

        options.getOptional(NULL_LITERAL).ifPresent(csvBuilder::setNullValue);

        return csvBuilder.build();
    }
}
