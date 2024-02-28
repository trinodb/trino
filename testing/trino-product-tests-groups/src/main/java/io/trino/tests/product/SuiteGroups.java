/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.product;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static io.trino.tests.product.TestGroups.AZURE;
import static io.trino.tests.product.TestGroups.CLI;
import static io.trino.tests.product.TestGroups.FUNCTIONS;
import static io.trino.tests.product.TestGroups.HIVE_COMPRESSION;
import static io.trino.tests.product.TestGroups.JDBC;
import static io.trino.tests.product.TestGroups.JDBC_KERBEROS_CONSTRAINED_DELEGATION;
import static io.trino.tests.product.TestGroups.LARGE_QUERY;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS_DETAILED;
import static io.trino.tests.product.TestGroups.TPCDS;
import static io.trino.tests.product.TestGroups.TPCH;
import static io.trino.tests.product.TestGroups.TRINO_JDBC;

public abstract class SuiteGroups
{
    private SuiteGroups() {}

    public static final Set<String> SUITE1_EXCLUSIONS = ImmutableSet.of(
            AZURE,
            CLI,
            JDBC,
            TRINO_JDBC,
            JDBC_KERBEROS_CONSTRAINED_DELEGATION,
            FUNCTIONS,
            HIVE_COMPRESSION,
            LARGE_QUERY,
            PROFILE_SPECIFIC_TESTS,
            STORAGE_FORMATS,
            STORAGE_FORMATS_DETAILED,
            TPCH,
            TPCDS);
}
