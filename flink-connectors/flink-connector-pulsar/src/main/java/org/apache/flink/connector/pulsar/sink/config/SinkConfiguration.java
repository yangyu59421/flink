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

package org.apache.flink.connector.pulsar.sink.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_FAIL_ON_WRITE;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_TRANSACTION_TIMEOUT_MILLIS;

/** The configure class for pulsar sink. */
@PublicEvolving
public class SinkConfiguration implements Serializable {
    private static final long serialVersionUID = 8488507275800787580L;

    /**
     * This option is used to set whether to throw an exception to trigger a termination or retry of
     * the Pulsar sink operator when writing a message to Pulsar Topic fails. The behavior of the
     * termination or retry is determined by the Flink checkpiont configuration.
     */
    private final boolean failOnWrite;

    /**
     * Pulsar's transaction have a timeout mechanism for uncommitted transaction. When we use
     * exactly-once, we will use this parameter to open the transaction.
     */
    private final long transactionTimeoutMillis;

    public SinkConfiguration(Configuration configuration) {
        this.failOnWrite = configuration.get(PULSAR_FAIL_ON_WRITE);
        this.transactionTimeoutMillis = configuration.get(PULSAR_TRANSACTION_TIMEOUT_MILLIS);
    }

    public boolean isFailOnWrite() {
        return failOnWrite;
    }

    public long getTransactionTimeoutMillis() {
        return transactionTimeoutMillis;
    }
}
