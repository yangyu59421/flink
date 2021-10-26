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

package org.apache.flink.formats.avro;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.BulkDecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.BulkReaderFormatFactory;
import org.apache.flink.table.factories.BulkWriterFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.filesystem.FileSystemConnectorOptions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.PartitionPathUtils;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.formats.avro.AvroFormatOptions.AVRO_OUTPUT_CODEC;

/** Avro format factory for file system. */
@Internal
public class AvroFileFormatFactory implements BulkReaderFormatFactory, BulkWriterFormatFactory {

    public static final String IDENTIFIER = "avro";

    @Override
    public BulkDecodingFormat<RowData> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return new BulkDecodingFormat<RowData>() {
            @Override
            public BulkFormat<RowData, FileSourceSplit> createRuntimeDecoder(
                    DynamicTableSource.Context sourceContext, DataType producedDataType) {
                DataType physicalDataType =
                        context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
                List<String> partitionKeys = context.getCatalogTable().getPartitionKeys();
                String defaultPartitionName =
                        context.getCatalogTable()
                                .getOptions()
                                .getOrDefault(
                                        FileSystemConnectorOptions.PARTITION_DEFAULT_NAME.key(),
                                        FileSystemConnectorOptions.PARTITION_DEFAULT_NAME
                                                .defaultValue());
                return new AvroGenericRecordBulkFormat(
                        sourceContext,
                        physicalDataType,
                        producedDataType,
                        partitionKeys,
                        defaultPartitionName);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<BulkWriter.Factory<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return new EncodingFormat<BulkWriter.Factory<RowData>>() {

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }

            @Override
            public BulkWriter.Factory<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                return new RowDataAvroWriterFactory(
                        (RowType) consumedDataType.getLogicalType(),
                        formatOptions.get(AVRO_OUTPUT_CODEC));
            }
        };
    }

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
        options.add(AVRO_OUTPUT_CODEC);
        return options;
    }

    private static class AvroGenericRecordBulkFormat
            extends AbstractAvroBulkFormat<GenericRecord, RowData, FileSourceSplit> {

        private static final long serialVersionUID = 1L;

        // all physical fields in the source schema
        private final DataType physicalDataType;
        // projected physical fields
        private final DataType producedDataType;
        TypeInformation<RowData> producedTypeInfo;
        private final List<String> partitionKeys;
        private final String defaultPartitionValue;

        private transient AvroToRowDataConverters.AvroToRowDataConverter converter;
        private transient GenericRecord reusedAvroRecord;
        private transient GenericRowData reusedRowData;
        // we should fill i-th field of reusedRowData with
        // readerRowTypeIndex[i]-th field of reusedAvroRecord
        private transient int[] readerRowTypeIndex;

        public AvroGenericRecordBulkFormat(
                DynamicTableSource.Context context,
                DataType physicalDataType,
                DataType producedDataType,
                List<String> partitionKeys,
                String defaultPartitionValue) {
            // partition keys are stored in file paths, not in avro file contents
            super(getNotNullRowTypeWithExclusion(producedDataType, partitionKeys));
            this.physicalDataType = physicalDataType;
            this.producedDataType = producedDataType;
            this.producedTypeInfo = context.createTypeInformation(producedDataType);
            this.partitionKeys = partitionKeys;
            this.defaultPartitionValue = defaultPartitionValue;
        }

        @Override
        protected void open(FileSourceSplit split) {
            converter = AvroToRowDataConverters.createRowConverter(readerRowType);

            Schema schema = AvroSchemaConverter.convertToSchema(readerRowType);
            reusedAvroRecord = new GenericData.Record(schema);

            List<String> physicalFieldNames = DataType.getFieldNames(physicalDataType);
            int[] selectFieldIndices =
                    DataType.getFieldNames(producedDataType).stream()
                            .mapToInt(physicalFieldNames::indexOf)
                            .toArray();
            reusedRowData =
                    PartitionPathUtils.fillPartitionValueForRecord(
                            physicalFieldNames.toArray(new String[0]),
                            physicalDataType.getChildren().toArray(new DataType[0]),
                            selectFieldIndices,
                            partitionKeys,
                            split.path(),
                            defaultPartitionValue);

            readerRowTypeIndex =
                    DataType.getFieldNames(producedDataType).stream()
                            .mapToInt(name -> readerRowType.getFieldNames().indexOf(name))
                            .toArray();
        }

        @Override
        protected RowData convert(GenericRecord record) {
            if (record == null) {
                return null;
            }
            GenericRowData row = (GenericRowData) converter.convert(record);

            for (int i = 0; i < readerRowTypeIndex.length; i++) {
                if (readerRowTypeIndex[i] >= 0) {
                    reusedRowData.setField(i, row.getField(readerRowTypeIndex[i]));
                }
            }
            return reusedRowData;
        }

        @Override
        protected GenericRecord createReusedAvroRecord() {
            return reusedAvroRecord;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return producedTypeInfo;
        }

        private static RowType getNotNullRowTypeWithExclusion(
                DataType rowDataType, List<String> excludedFieldNames) {
            RowType rowType = (RowType) rowDataType.getLogicalType();
            List<LogicalType> types = new ArrayList<>();
            List<String> names = new ArrayList<>();
            for (RowType.RowField field : rowType.getFields()) {
                if (excludedFieldNames.contains(field.getName())) {
                    continue;
                }
                types.add(field.getType());
                names.add(field.getName());
            }
            return RowType.of(
                    false, types.toArray(new LogicalType[0]), names.toArray(new String[0]));
        }
    }

    /**
     * A {@link BulkWriter.Factory} to convert {@link RowData} to {@link GenericRecord} and wrap
     * {@link AvroWriterFactory}.
     */
    private static class RowDataAvroWriterFactory implements BulkWriter.Factory<RowData> {

        private static final long serialVersionUID = 1L;

        private final AvroWriterFactory<GenericRecord> factory;
        private final RowType rowType;

        private RowDataAvroWriterFactory(RowType rowType, String codec) {
            this.rowType = rowType;
            this.factory =
                    new AvroWriterFactory<>(
                            new AvroBuilder<GenericRecord>() {
                                @Override
                                public DataFileWriter<GenericRecord> createWriter(OutputStream out)
                                        throws IOException {
                                    Schema schema = AvroSchemaConverter.convertToSchema(rowType);
                                    DatumWriter<GenericRecord> datumWriter =
                                            new GenericDatumWriter<>(schema);
                                    DataFileWriter<GenericRecord> dataFileWriter =
                                            new DataFileWriter<>(datumWriter);

                                    if (codec != null) {
                                        dataFileWriter.setCodec(CodecFactory.fromString(codec));
                                    }
                                    dataFileWriter.create(schema, out);
                                    return dataFileWriter;
                                }
                            });
        }

        @Override
        public BulkWriter<RowData> create(FSDataOutputStream out) throws IOException {
            BulkWriter<GenericRecord> writer = factory.create(out);
            RowDataToAvroConverters.RowDataToAvroConverter converter =
                    RowDataToAvroConverters.createConverter(rowType);
            Schema schema = AvroSchemaConverter.convertToSchema(rowType);
            return new BulkWriter<RowData>() {

                @Override
                public void addElement(RowData element) throws IOException {
                    GenericRecord record = (GenericRecord) converter.convert(schema, element);
                    writer.addElement(record);
                }

                @Override
                public void flush() throws IOException {
                    writer.flush();
                }

                @Override
                public void finish() throws IOException {
                    writer.finish();
                }
            };
        }
    }
}
