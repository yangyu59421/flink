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

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** An extension to let failed test retry. */
public class RetryExtension implements TestTemplateInvocationContextProvider, AfterAllCallback {
    public static final Logger LOG = LoggerFactory.getLogger(RetryExtension.class);
    public static final ExtensionContext.Namespace RETRY_NAMESPACE =
            ExtensionContext.Namespace.create("retryLog");
    public static final String RETRY_KEY = "testRetry";

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        Method testMethod = context.getRequiredTestMethod();
        RetryOnFailure retryOnFailure = testMethod.getAnnotation(RetryOnFailure.class);
        RetryOnException retryOnException = testMethod.getAnnotation(RetryOnException.class);

        if (retryOnFailure == null && retryOnException == null) {
            // if nothing is specified on the test method, fall back to annotations on the class
            retryOnFailure = context.getTestClass().get().getAnnotation(RetryOnFailure.class);
            retryOnException = context.getTestClass().get().getAnnotation(RetryOnException.class);
        }
        return retryOnException != null || retryOnFailure != null;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
            ExtensionContext context) {
        Method testMethod = context.getRequiredTestMethod();
        RetryOnFailure retryOnFailure = testMethod.getAnnotation(RetryOnFailure.class);
        RetryOnException retryOnException = testMethod.getAnnotation(RetryOnException.class);

        if (retryOnFailure == null && retryOnException == null) {
            // if nothing is specified on the test method, fall back to annotations on the class
            retryOnFailure = context.getTestClass().get().getAnnotation(RetryOnFailure.class);
            retryOnException = context.getTestClass().get().getAnnotation(RetryOnException.class);
        }
        // sanity check that we don't use both annotations
        if (retryOnFailure != null && retryOnException != null) {
            throw new IllegalArgumentException(
                    "You cannot combine the RetryOnFailure and RetryOnException annotations.");
        }

        Map<String, Boolean> testLog =
                (Map<String, Boolean>)
                        context.getStore(RETRY_NAMESPACE)
                                .getOrComputeIfAbsent(RETRY_KEY, key -> new HashMap<>());
        testLog.put(getTestMethodKey(context), true);
        if (retryOnFailure == null) {
            int totalTimes = retryOnException.times() + 1;
            RetryOnException finalRetryOnException = retryOnException;
            return IntStream.rangeClosed(1, totalTimes)
                    .mapToObj(
                            i ->
                                    new RetryContext(
                                            i, totalTimes, finalRetryOnException.exception()));
        } else {
            int totalTimes = retryOnFailure.times() + 1;
            return IntStream.rangeClosed(1, totalTimes)
                    .mapToObj(i -> new RetryContext(i, totalTimes, null));
        }
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        context.getStore(RETRY_NAMESPACE).remove(RETRY_KEY);
    }

    public static String getTestMethodKey(ExtensionContext context) {
        return context.getRequiredTestClass().getCanonicalName()
                + "."
                + context.getRequiredTestMethod().getName();
    }

    class RetryContext implements TestTemplateInvocationContext {
        int retryIndex;
        int totalTimes;
        Class<? extends Throwable> repeatableException;

        RetryContext(
                int retryIndex, int totalTimes, Class<? extends Throwable> repeatableException) {
            this.retryIndex = retryIndex;
            this.totalTimes = totalTimes;
            this.repeatableException = repeatableException;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return String.format(
                    "RetryIndex: [%d], Test:[%d/%d]", invocationIndex, retryIndex, totalTimes);
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return Arrays.asList(
                    new RetryTestExecutionExtension(retryIndex, totalTimes, repeatableException));
        }
    }
}
