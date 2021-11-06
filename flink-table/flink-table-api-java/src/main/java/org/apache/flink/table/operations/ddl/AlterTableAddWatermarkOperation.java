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

package org.apache.flink.table.operations.ddl;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

/** Operation of "ALTER TABLE ADD WATERMARK AS ..." clause. */
public class AlterTableAddWatermarkOperation extends AlterTableOperation {

    private final String rowtimeAttribute;
    private final String expressions;
    private final DataType exprDataType;

    public AlterTableAddWatermarkOperation(
            ObjectIdentifier tableName,
            String rowtimeAttribute,
            String expressions,
            DataType exprDataType) {
        super(tableName);
        Preconditions.checkState(!StringUtils.isNullOrWhitespaceOnly(rowtimeAttribute));
        Preconditions.checkState(!StringUtils.isNullOrWhitespaceOnly(expressions));
        this.exprDataType = Preconditions.checkNotNull(exprDataType);
        this.rowtimeAttribute = rowtimeAttribute;
        this.expressions = expressions;
    }

    public String getRowtimeAttribute() {
        return rowtimeAttribute;
    }

    public String getExpressions() {
        return expressions;
    }

    public DataType getExprDataType() {
        return exprDataType;
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "ALTER TABLE ADD WATERMARK FOR %s AS %s", rowtimeAttribute, expressions);
    }
}
