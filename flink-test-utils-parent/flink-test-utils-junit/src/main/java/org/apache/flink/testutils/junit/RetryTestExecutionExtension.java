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

package org.apache.flink.testutils.junit;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.flink.testutils.junit.RetryExtension.RETRY_KEY;
import static org.apache.flink.testutils.junit.RetryExtension.RETRY_NAMESPACE;
import static org.apache.flink.testutils.junit.RetryExtension.getTestMethodKey;

/** Extension to decide whether a retry test should run. */
public class RetryTestExecutionExtension
        implements ExecutionCondition, TestExecutionExceptionHandler, AfterEachCallback {
    public static final Logger LOG = LoggerFactory.getLogger(RetryTestExecutionExtension.class);
    private final int retryIndex;
    private final int totalTimes;
    private Class<? extends Throwable> repeatableException;

    public RetryTestExecutionExtension(
            int retryIndex, int totalTimes, Class<? extends Throwable> repeatableException) {
        this.retryIndex = retryIndex;
        this.totalTimes = totalTimes;
        this.repeatableException = repeatableException;
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        Map<String, Boolean> shouldRetry =
                (Map<String, Boolean>) context.getStore(RETRY_NAMESPACE).get(RETRY_KEY);
        String method = getTestMethodKey(context);
        if (!shouldRetry.get(method)) {
            return ConditionEvaluationResult.disabled(method + "have already passed or failed.");
        }
        return ConditionEvaluationResult.enabled(
                String.format("Test %s: [%d/%d]", method, retryIndex, totalTimes));
    }

    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable)
            throws Throwable {
        String method = getTestMethodKey(context);
        if (repeatableException != null) {
            // RetryOnException
            if (repeatableException.isAssignableFrom(throwable.getClass())) {
                // 1. continue retrying when get some repeatable exceptions
                String retryMsg =
                        String.format(
                                "Retry test %s[%d/%d] failed with repeatable exception, continue retrying.",
                                method, retryIndex, totalTimes);
                LOG.error(retryMsg, throwable);
                throw new TestAbortedException(retryMsg);
            } else {
                // 2. stop retrying when get an unrepeatable exception
                Map<String, Boolean> shouldRetry =
                        (Map<String, Boolean>) context.getStore(RETRY_NAMESPACE).get(RETRY_KEY);
                shouldRetry.put(method, false);
                LOG.error(
                        String.format(
                                "Retry test %s[%d/%d] failed with unrepeatable exception, stop retrying.",
                                method, retryIndex, totalTimes),
                        throwable);
                throw throwable;
            }
        } else {
            // RetryOnFailure
            // 1. Failed when reach the total retry times
            if (retryIndex == totalTimes) {
                LOG.error("Test Failed at the last retry.", throwable);
                throw throwable;
            }

            // 2. continue retrying
            String retryMsg =
                    String.format(
                            "Retry test %s[%d/%d] failed, continue retrying.",
                            method, retryIndex, totalTimes);
            LOG.error(retryMsg, throwable);
            throw new TestAbortedException(retryMsg);
        }
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        Throwable exception = context.getExecutionException().orElse(null);
        if (exception == null) {
            Map<String, Boolean> shouldRetry =
                    (Map<String, Boolean>) context.getStore(RETRY_NAMESPACE).get(RETRY_KEY);
            String method = getTestMethodKey(context);
            shouldRetry.put(method, false);
            LOG.info(
                    String.format(
                            "Retry test %s[%d/%d] passed, stop retrying.",
                            method, retryIndex, totalTimes));
        }
    }
}
