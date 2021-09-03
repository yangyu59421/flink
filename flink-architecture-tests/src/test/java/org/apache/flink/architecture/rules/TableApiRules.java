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

package org.apache.flink.architecture.rules;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.factories.DynamicTableFactory;

import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.fields;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noFields;
import static org.apache.flink.architecture.common.Predicates.arePublicStaticOfType;
import static org.apache.flink.architecture.common.SourcePredicates.areJavaClasses;

/** Rules for Table API. */
public class TableApiRules {
    @ArchTest
    public static final ArchRule CONFIG_OPTIONS_IN_OPTIONS_CLASSES =
            fields().that(arePublicStaticOfType(ConfigOption.class))
                    .and()
                    .areDeclaredInClassesThat(
                            areJavaClasses().and(resideInAPackage("org.apache.flink.table")))
                    .should()
                    .beDeclaredInClassesThat()
                    .haveSimpleNameEndingWith("Options");

    @ArchTest
    public static final ArchRule TABLE_FACTORIES_CONTAIN_NO_CONFIG_OPTIONS =
            noFields()
                    .that(arePublicStaticOfType(ConfigOption.class))
                    .and()
                    .areDeclaredInClassesThat(
                            areJavaClasses().and(resideInAPackage("org.apache.flink.table")))
                    .should()
                    .beDeclaredInClassesThat()
                    .implement(DynamicTableFactory.class);
}
