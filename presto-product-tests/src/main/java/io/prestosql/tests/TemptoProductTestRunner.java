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
package io.prestosql.tests;

import com.google.common.collect.ImmutableList;
import io.prestosql.tempto.fulfillment.table.kafka.KafkaTableManager;
import io.prestosql.tempto.internal.fulfillment.ldap.LdapObjectFulfiller;
import io.prestosql.tempto.internal.fulfillment.ldap.LdapObjectModuleProvider;
import io.prestosql.tempto.runner.TemptoRunner;
import io.prestosql.tempto.runner.TemptoRunnerCommandLineParser;
import io.prestosql.tests.querystats.QueryStatsClientModuleProvider;

import static io.prestosql.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_AVRO;
import static io.prestosql.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_ORC;
import static io.prestosql.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_PARQUET;
import static io.prestosql.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_RCFILE;
import static io.prestosql.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_TEXTFILE;
import static io.prestosql.tests.hive.TestHiveBucketedTables.BUCKETED_NATION;
import static io.prestosql.tests.hive.TestHiveBucketedTables.BUCKETED_NATION_PREPARED;
import static io.prestosql.tests.hive.TestHiveBucketedTables.BUCKETED_PARTITIONED_NATION;
import static io.prestosql.tests.hive.TestHiveBucketedTables.BUCKETED_SORTED_NATION;

public final class TemptoProductTestRunner
{
    public static void main(String[] args)
    {
        TemptoRunnerCommandLineParser parser = TemptoRunnerCommandLineParser.builder("Presto product tests")
                .setTestsPackage("io.prestosql.tests.*", false)
                .setExcludedGroups("quarantine", true)
                .build();
        TemptoRunner.runTempto(
                parser,
                args,
                () -> ImmutableList.of(),
                () -> ImmutableList.of(
                        LdapObjectModuleProvider.class,
                        QueryStatsClientModuleProvider.class),
                () -> ImmutableList.of(LdapObjectFulfiller.class),
                () -> ImmutableList.of(KafkaTableManager.class),
                () -> ImmutableList.of(
                        ALL_HIVE_SIMPLE_TYPES_TEXTFILE,
                        ALL_HIVE_SIMPLE_TYPES_RCFILE,
                        ALL_HIVE_SIMPLE_TYPES_AVRO,
                        ALL_HIVE_SIMPLE_TYPES_ORC,
                        ALL_HIVE_SIMPLE_TYPES_PARQUET,
                        BUCKETED_NATION,
                        BUCKETED_PARTITIONED_NATION,
                        BUCKETED_NATION_PREPARED,
                        BUCKETED_SORTED_NATION));
    }

    private TemptoProductTestRunner() {}
}
