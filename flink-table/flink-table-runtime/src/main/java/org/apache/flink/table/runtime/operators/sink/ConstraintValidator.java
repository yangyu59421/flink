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

package org.apache.flink.table.runtime.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.UpdatableRowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;

import java.util.List;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.CharPrecisionEnforcer;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Processes {@link RowData} to enforce constraints such as <code>NOT NULL</code> and string
 * trimming to comply with the {@code precision} defined in their corresponding {@code
 * CHAR<precision>} or {@code VARCHAR<precision>} types.
 */
@Internal
public class ConstraintValidator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private final NotNullEnforcer notNullEnforcer;
    private final int[] notNullFieldIndices;
    private final String[] allFieldNames;
    private final CharPrecisionEnforcer charPrecisionEnforcer;
    private final int[] charFieldIndices;
    private final int[] charFieldPrecisions;

    private transient UpdatableRowData reusableRowData;
    private transient StreamRecord<RowData> reusableStreamRecord;

    private ConstraintValidator(
            NotNullEnforcer notNullEnforcer,
            int[] notNullFieldIndices,
            CharPrecisionEnforcer charPrecisionEnforcer,
            int[] charFieldIndices,
            int[] charFieldPrecisions,
            String[] allFieldNames) {
        this.notNullEnforcer = notNullEnforcer;
        this.notNullFieldIndices = notNullFieldIndices;
        this.charPrecisionEnforcer = charPrecisionEnforcer;
        this.charFieldIndices = charFieldIndices;
        this.charFieldPrecisions = charFieldPrecisions;
        this.allFieldNames = allFieldNames;
    }

    @Override
    public void open() throws Exception {
        super.open();
        reusableRowData = new UpdatableRowData(null, allFieldNames.length);
        reusableStreamRecord = new StreamRecord<>(null);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Helper builder, so that the {@link ConstraintValidator} can be instantiated with only the NOT
     * NULL constraint validation, only the CHAR/VARCHAR precision validation, or both.
     */
    public static class Builder {

        private NotNullEnforcer notNullEnforcer;
        private int[] notNullFieldIndices;

        private CharPrecisionEnforcer charPrecisionEnforcer;
        private List<Tuple2<Integer, Integer>> charFields;
        private String[] allFieldNames;

        private boolean mustApply = false;

        public boolean mustApply() {
            return mustApply;
        }

        public void addNotNullConstraint(
                NotNullEnforcer notNullEnforcer,
                int[] notNullFieldIndices,
                String[] allFieldNames) {
            checkArgument(
                    notNullFieldIndices.length > 0,
                    "ConstraintValidator requires that there are not-null fields.");
            this.notNullFieldIndices = notNullFieldIndices;
            this.notNullEnforcer = notNullEnforcer;
            this.allFieldNames = allFieldNames;
            if (notNullEnforcer != null) {
                this.mustApply = true;
            }
        }

        public void addCharPrecisionConstraint(
                CharPrecisionEnforcer charPrecisionEnforcer,
                List<Tuple2<Integer, Integer>> charFields,
                String[] allFieldNames) {
            this.charPrecisionEnforcer = charPrecisionEnforcer;
            if (this.charPrecisionEnforcer == CharPrecisionEnforcer.TRIM) {
                checkArgument(
                        charFields.size() > 0,
                        "ConstraintValidator requires that there are CHAR/VARCHAR fields.");
                this.charFields = charFields;
                this.allFieldNames = allFieldNames;
                this.mustApply = true;
            }
        }

        public ConstraintValidator build() {
            return new ConstraintValidator(
                    notNullEnforcer,
                    notNullFieldIndices,
                    charPrecisionEnforcer,
                    charFields != null ? charFields.stream().mapToInt(t -> t.f0).toArray() : null,
                    charFields != null ? charFields.stream().mapToInt(t -> t.f1).toArray() : null,
                    allFieldNames);
        }
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        StreamRecord<RowData> processedElement = processNotNullConstraint(element);
        if (processedElement != null) {
            output.collect(processCharConstraint(processedElement));
        }
    }

    private StreamRecord<RowData> processNotNullConstraint(StreamRecord<RowData> element) {
        if (notNullEnforcer == null) {
            return element;
        }

        final RowData rowData = element.getValue();

        for (int index : notNullFieldIndices) {
            if (rowData.isNullAt(index)) {
                switch (notNullEnforcer) {
                    case ERROR:
                        throw new TableException(
                                String.format(
                                        "Column '%s' is NOT NULL, however, a null value is being written into it. "
                                                + "You can set job configuration '%s'='drop' "
                                                + "to suppress this exception and drop such records silently.",
                                        allFieldNames[index],
                                        TABLE_EXEC_SINK_NOT_NULL_ENFORCER.key()));
                    case DROP:
                        return null;
                }
            }
        }
        return element;
    }

    private StreamRecord<RowData> processCharConstraint(StreamRecord<RowData> element) {
        if (charPrecisionEnforcer == null
                || charPrecisionEnforcer == CharPrecisionEnforcer.IGNORE) {
            return element;
        }

        final RowData rowData = element.getValue();

        boolean trimmed = false;
        for (int i = 0; i < charFieldIndices.length; i++) {
            final int fieldIdx = charFieldIndices[i];
            final int precision = charFieldPrecisions[i];
            final String stringValue = rowData.getString(fieldIdx).toString();

            if (stringValue.length() > precision) {
                reusableRowData.setRow(rowData);
                reusableRowData.setField(
                        fieldIdx, StringData.fromString(stringValue.substring(0, precision)));
                trimmed = true;
            }
        }

        if (trimmed) {
            reusableStreamRecord.replace(reusableRowData);
            return reusableStreamRecord;
        }
        return element;
    }
}
