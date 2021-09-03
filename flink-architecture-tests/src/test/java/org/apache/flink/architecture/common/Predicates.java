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

package org.apache.flink.architecture.common;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.domain.JavaModifier;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.stream.Collectors;

/** Common predicates for architecture tests. */
public class Predicates {

    /**
     * Tests that a class has at least one of the given annotations.
     *
     * <p>This is tested on the top-level enclosing class, i.e. if the tested class is a nested
     * class, the top-level class is identified and tested instead.
     */
    @SafeVarargs
    public static DescribedPredicate<JavaClass> areAnnotatedWithAtLeastOneOf(
            Class<? extends Annotation>... annotations) {
        return new DescribedPredicate<JavaClass>(
                "be annotated with at least one of "
                        + Arrays.stream(annotations)
                                .map(Class::getSimpleName)
                                .collect(Collectors.joining(", "))) {
            @Override
            public boolean apply(JavaClass clazz) {
                final JavaClass enclosingClazz = getEnclosingClass(clazz).getBaseComponentType();
                for (Class<? extends Annotation> annotation : annotations) {
                    if (enclosingClazz.isAnnotatedWith(annotation)) {
                        return true;
                    }
                }

                return false;
            }

            private JavaClass getEnclosingClass(JavaClass clazz) {
                return clazz.getEnclosingClass().map(this::getEnclosingClass).orElse(clazz);
            }
        };
    }

    /** Tests that the given field is {@code public static} and of the given type. */
    public static DescribedPredicate<JavaField> arePublicStaticOfType(Class<?> clazz) {
        return new DescribedPredicate<JavaField>(
                "are public, static, and of type " + clazz.getSimpleName()) {
            @Override
            public boolean apply(JavaField input) {
                return input.getModifiers().contains(JavaModifier.PUBLIC)
                        && input.getModifiers().contains(JavaModifier.STATIC)
                        && input.getRawType().isEquivalentTo(clazz);
            }
        };
    }

    private Predicates() {}
}
