/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.util.Preconditions;

/**
 * Builder of {@link OneInputStreamOperatorTestHarness}.
 *
 * @param <IN> The input type of the operator.
 * @param <OUT> The output type of the operator.
 */
public class OneInputStreamOperatorTestHarnessBuilder<IN, OUT> extends AbstractBasicStreamOperatorTestHarnessBuilder<OneInputStreamOperatorTestHarnessBuilder<IN, OUT>, OUT> {
	protected TypeSerializer<IN> typeSerializerIn;

	public OneInputStreamOperatorTestHarnessBuilder<IN, OUT> setTypeSerializerIn(TypeSerializer<IN> typeSerializerIn) {
		this.typeSerializerIn = Preconditions.checkNotNull(typeSerializerIn);
		return this;
	}

	@Override
	public OneInputStreamOperatorTestHarness<IN, OUT> build() throws Exception {
		return new OneInputStreamOperatorTestHarness<>(
			(OneInputStreamOperator<IN, OUT>) super.operator,
			super.computeFactoryIfAbsent(),
			typeSerializerIn,
			super.computeEnvironmentIfAbsent(),
			super.isInternalEnvironment,
			super.operatorID);
	}
}
