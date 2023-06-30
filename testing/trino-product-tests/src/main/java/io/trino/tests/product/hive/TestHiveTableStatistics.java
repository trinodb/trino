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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableList;
import io.trino.tempto.Requirement;
import io.trino.tempto.Requirements;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.Requires;
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.table.MutableTableRequirement;
import io.trino.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.trino.tempto.fulfillment.table.hive.InlineDataSource;
import io.trino.testng.services.Flaky;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Objects;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.anyOf;
import static io.trino.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static io.trino.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.trino.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_TEXTFILE;
import static io.trino.tests.product.hive.HiveTableDefinitions.NATION_PARTITIONED_BY_BIGINT_REGIONKEY;
import static io.trino.tests.product.hive.HiveTableDefinitions.NATION_PARTITIONED_BY_VARCHAR_REGIONKEY;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveTableStatistics
        extends HiveProductTest
{
    private static class UnpartitionedNationTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return mutableTable(
                    HiveTableDefinition.from(NATION)
                            .injectStats(false)
                            .build());
        }
    }

    private static class NationPartitionedByBigintTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return mutableTable(
                    HiveTableDefinition.from(NATION_PARTITIONED_BY_BIGINT_REGIONKEY)
                            .injectStats(false)
                            .build());
        }
    }

    private static class NationPartitionedByVarcharTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return mutableTable(
                    HiveTableDefinition.from(NATION_PARTITIONED_BY_VARCHAR_REGIONKEY)
                            .injectStats(false)
                            .build());
        }
    }

    private static final String ALL_TYPES_TABLE_NAME = "all_types";
    private static final String EMPTY_ALL_TYPES_TABLE_NAME = "empty_all_types";
    private static final String ALL_TYPES_ALL_NULL_TABLE_NAME = "all_types_all_null";

    private static final HiveTableDefinition ALL_TYPES_TABLE = HiveTableDefinition.like(ALL_HIVE_SIMPLE_TYPES_TEXTFILE)
            .setDataSource(InlineDataSource.createStringDataSource(
                    "all_analyzable_types",
                    "121|32761|2147483641|9223372036854775801|123.341|234.561|344.671|345.671|2015-05-10 12:15:31.123456|2015-05-09|ela ma kota|ela ma kot|ela ma    |false|cGllcyBiaW5hcm55|\n" +
                            "127|32767|2147483647|9223372036854775807|123.345|235.567|345.678|345.678|2015-05-10 12:15:35.123456|2015-06-10|ala ma kota|ala ma kot|ala ma    |true|a290IGJpbmFybnk=|\n"))
            .build();

    private static final HiveTableDefinition ALL_TYPES_ALL_NULL_TABLE = HiveTableDefinition.like(ALL_HIVE_SIMPLE_TYPES_TEXTFILE)
            .setDataSource(InlineDataSource.createStringDataSource(
                    "all_analyzable_types_all_null",
                    "\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\n"))
            .build();

    private static final class AllTypesTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return Requirements.compose(
                    mutableTable(ALL_TYPES_TABLE, ALL_TYPES_TABLE_NAME, MutableTableRequirement.State.LOADED),
                    mutableTable(ALL_TYPES_ALL_NULL_TABLE, ALL_TYPES_ALL_NULL_TABLE_NAME, MutableTableRequirement.State.LOADED),
                    mutableTable(ALL_TYPES_TABLE, EMPTY_ALL_TYPES_TABLE_NAME, MutableTableRequirement.State.CREATED));
        }
    }

    private static List<Row> getAllTypesTableStatistics()
    {
        return ImmutableList.of(
                row("c_tinyint", null, 2.0, 0.0, null, "121", "127"),
                row("c_smallint", null, 2.0, 0.0, null, "32761", "32767"),
                row("c_int", null, 2.0, 0.0, null, "2147483641", "2147483647"),
                row("c_bigint", null, 1.0, 0.0, null, "9223372036854775807", "9223372036854775807"),
                row("c_float", null, 2.0, 0.0, null, "123.341", "123.345"),
                row("c_double", null, 2.0, 0.0, null, "234.561", "235.567"),
                row("c_decimal", null, 2.0, 0.0, null, "345.0", "346.0"),
                row("c_decimal_w_params", null, 2.0, 0.0, null, "345.671", "345.678"),
                row("c_timestamp", null, 2.0, 0.0, null, null, null),
                row("c_date", null, 2.0, 0.0, null, "2015-05-09", "2015-06-10"),
                row("c_string", 22.0, 2.0, 0.0, null, null, null),
                row("c_varchar", 20.0, 2.0, 0.0, null, null, null),
                row("c_char", 12.0, 2.0, 0.0, null, null, null),
                row("c_boolean", null, 2.0, 0.0, null, null, null),
                row("c_binary", 23.0, null, 0.0, null, null, null),
                row(null, null, null, null, 2.0, null, null));
    }

    private static List<Row> getAllTypesAllNullTableStatistics()
    {
        return ImmutableList.of(
                row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                row("c_int", 0.0, 0.0, 1.0, null, null, null),
                row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                row("c_float", 0.0, 0.0, 1.0, null, null, null),
                row("c_double", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                row("c_date", 0.0, 0.0, 1.0, null, null, null),
                row("c_string", 0.0, 0.0, 1.0, null, null, null),
                row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                row("c_char", 0.0, 0.0, 1.0, null, null, null),
                row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                row("c_binary", 0.0, null, 1.0, null, null, null),
                row(null, null, null, null, 1.0, null, null));
    }

    private static List<Row> getAllTypesEmptyTableStatistics()
    {
        return ImmutableList.of(
                row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                row("c_int", 0.0, 0.0, 1.0, null, null, null),
                row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                row("c_float", 0.0, 0.0, 1.0, null, null, null),
                row("c_double", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                row("c_date", 0.0, 0.0, 1.0, null, null, null),
                row("c_string", 0.0, 0.0, 1.0, null, null, null),
                row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                row("c_char", 0.0, 0.0, 1.0, null, null, null),
                row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                row("c_binary", 0.0, 0.0, 1.0, null, null, null),
                row(null, null, null, null, 0.0, null, null));
    }

    @Test
    @Requires(UnpartitionedNationTable.class)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testStatisticsForUnpartitionedTable()
    {
        String tableNameInDatabase = mutableTablesState().get(NATION.getName()).getNameInDatabase();

        String showStatsWholeTable = "SHOW STATS FOR " + tableNameInDatabase;

        // table not analyzed

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("n_nationkey", 0.0, 0.0, 1.0, null, null, null),
                row("n_name", 0.0, 0.0, 1.0, null, null, null),
                row("n_regionkey", 0.0, 0.0, 1.0, null, null, null),
                row("n_comment", 0.0, 0.0, 1.0, null, null, null),
                row(null, null, null, null, 0.0, null, null));

        // basic analysis

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS");
        onTrino().executeQuery("CALL system.flush_metadata_cache()");

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("n_nationkey", null, null, null, null, null, null),
                row("n_name", null, null, null, null, null, null),
                row("n_regionkey", null, null, null, null, null, null),
                row("n_comment", null, null, null, null, null, null),
                row(null, null, null, null, 25.0, null, null));

        // column analysis

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS FOR COLUMNS");
        onTrino().executeQuery("CALL system.flush_metadata_cache()");

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("n_nationkey", null, anyOf(19., 25.), 0.0, null, "0", "24"),
                row("n_name", 177.0, anyOf(24., 25.), 0.0, null, null, null),
                row("n_regionkey", null, 5.0, 0.0, null, "0", "4"),
                row("n_comment", 1857.0, 25.0, 0.0, null, null, null),
                row(null, null, null, null, 25.0, null, null));
    }

    @Test
    @Requires(NationPartitionedByBigintTable.class)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testStatisticsForTablePartitionedByBigint()
    {
        String tableNameInDatabase = mutableTablesState().get(NATION_PARTITIONED_BY_BIGINT_REGIONKEY.getName()).getNameInDatabase();

        String showStatsWholeTable = "SHOW STATS FOR " + tableNameInDatabase;
        String showStatsPartitionOne = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 1)";
        String showStatsPartitionTwo = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 2)";

        // table not analyzed

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, null, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, null, null, null));

        // basic analysis for single partition

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey = \"1\") COMPUTE STATISTICS");
        onTrino().executeQuery("CALL system.flush_metadata_cache()");

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 15.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "2", "2"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, null, null, null));

        // basic analysis for all partitions

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey) COMPUTE STATISTICS");
        onTrino().executeQuery("CALL system.flush_metadata_cache()");

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 15.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "2", "2"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        // column analysis for single partition

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey = \"1\") COMPUTE STATISTICS FOR COLUMNS");
        onTrino().executeQuery("CALL system.flush_metadata_cache()");

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 114.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                row("p_comment", 1497.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 15.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 38.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "2", "2"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        // column analysis for all partitions

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey) COMPUTE STATISTICS FOR COLUMNS");
        onTrino().executeQuery("CALL system.flush_metadata_cache()");

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 109.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                row("p_comment", 1197.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 15.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 38.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, anyOf(4., 5.), 0.0, null, "8", "21"),
                row("p_name", 31.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "2", "2"),
                row("p_comment", 351.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 5.0, null, null));
    }

    @Test
    @Requires(NationPartitionedByVarcharTable.class)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testStatisticsForTablePartitionedByVarchar()
    {
        String tableNameInDatabase = mutableTablesState().get(NATION_PARTITIONED_BY_VARCHAR_REGIONKEY.getName()).getNameInDatabase();

        String showStatsWholeTable = "SHOW STATS FOR " + tableNameInDatabase;
        String showStatsPartitionOne = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 'AMERICA')";
        String showStatsPartitionTwo = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 'ASIA')";

        // table not analyzed

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, null, null),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, null, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, null, null),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, null, null, null));

        // basic analysis for single partition

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey = \"AMERICA\") COMPUTE STATISTICS");
        onTrino().executeQuery("CALL system.flush_metadata_cache()");

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", 85.0, 3.0, 0.0, null, null, null),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 15.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", 35.0, 1.0, 0.0, null, null, null),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, null, null),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, null, null, null));

        // basic analysis for all partitions

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey) COMPUTE STATISTICS");
        onTrino().executeQuery("CALL system.flush_metadata_cache()");

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", 85.0, 3.0, 0.0, null, null, null),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 15.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", 35.0, 1.0, 0.0, null, null, null),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", 20.0, 1.0, 0.0, null, null, null),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        // column analysis for single partition

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey = \"AMERICA\") COMPUTE STATISTICS FOR COLUMNS");
        onTrino().executeQuery("CALL system.flush_metadata_cache()");

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 114.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", 85.0, 3.0, 0.0, null, null, null),
                row("p_comment", 1497.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 15.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 38.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", 35.0, 1.0, 0.0, null, null, null),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", 20.0, 1.0, 0.0, null, null, null),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        // column analysis for all partitions

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey) COMPUTE STATISTICS FOR COLUMNS");
        onTrino().executeQuery("CALL system.flush_metadata_cache()");

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 109.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", 85.0, 3.0, 0.0, null, null, null),
                row("p_comment", 1197.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 15.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 38.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", 35.0, 1.0, 0.0, null, null, null),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, anyOf(4., 5.), 0.0, null, "8", "21"),
                row("p_name", 31.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", 20.0, 1.0, 0.0, null, null, null),
                row("p_comment", 351.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 5.0, null, null));
    }

    // This covers also stats calculation for unpartitioned table
    @Test
    @Requires(AllTypesTable.class)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testStatisticsForAllDataTypes()
    {
        String tableNameInDatabase = mutableTablesState().get(ALL_TYPES_TABLE_NAME).getNameInDatabase();

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS");

        assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, null, null, null, null, null),
                row("c_smallint", null, null, null, null, null, null),
                row("c_int", null, null, null, null, null, null),
                row("c_bigint", null, null, null, null, null, null),
                row("c_float", null, null, null, null, null, null),
                row("c_double", null, null, null, null, null, null),
                row("c_decimal", null, null, null, null, null, null),
                row("c_decimal_w_params", null, null, null, null, null, null),
                row("c_timestamp", null, null, null, null, null, null),
                row("c_date", null, null, null, null, null, null),
                row("c_string", null, null, null, null, null, null),
                row("c_varchar", null, null, null, null, null, null),
                row("c_char", null, null, null, null, null, null),
                row("c_boolean", null, null, null, null, null, null),
                row("c_binary", null, null, null, null, null, null),
                row(null, null, null, null, 2.0, null, null));

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS FOR COLUMNS");
        onTrino().executeQuery("CALL system.flush_metadata_cache()");

        // SHOW STATS FORMAT: column_name, data_size, distinct_values_count, nulls_fraction, row_count
        assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, 2.0, 0.0, null, "121", "127"),
                row("c_smallint", null, 2.0, 0.0, null, "32761", "32767"),
                row("c_int", null, 2.0, 0.0, null, "2147483641", "2147483647"),
                row("c_bigint", null, 1.0, 0.0, null, "9223372036854775807", "9223372036854775807"),
                row("c_float", null, 2.0, 0.0, null, "123.341", "123.345"),
                row("c_double", null, 2.0, 0.0, null, "234.561", "235.567"),
                row("c_decimal", null, 2.0, 0.0, null, "345.0", "346.0"),
                row("c_decimal_w_params", null, 2.0, 0.0, null, "345.671", "345.678"),
                row("c_timestamp", null, 2.0, 0.0, null, null, null),
                row("c_date", null, 2.0, 0.0, null, "2015-05-09", "2015-06-10"),
                row("c_string", 22.0, 2.0, 0.0, null, null, null),
                row("c_varchar", 20.0, 2.0, 0.0, null, null, null),
                row("c_char", 12.0, 2.0, 0.0, null, null, null),
                row("c_boolean", null, 2.0, 0.0, null, null, null),
                row("c_binary", 23.0, null, 0.0, null, null, null),
                row(null, null, null, null, 2.0, null, null));
    }

    @Test
    @Requires(AllTypesTable.class)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testStatisticsForAllDataTypesNoData()
    {
        String tableNameInDatabase = mutableTablesState().get(EMPTY_ALL_TYPES_TABLE_NAME).getNameInDatabase();

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS");

        assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                row("c_int", 0.0, 0.0, 1.0, null, null, null),
                row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                row("c_float", 0.0, 0.0, 1.0, null, null, null),
                row("c_double", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                row("c_date", 0.0, 0.0, 1.0, null, null, null),
                row("c_string", 0.0, 0.0, 1.0, null, null, null),
                row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                row("c_char", 0.0, 0.0, 1.0, null, null, null),
                row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                row("c_binary", 0.0, 0.0, 1.0, null, null, null),
                row(null, null, null, null, 0.0, null, null));

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS FOR COLUMNS");
        onTrino().executeQuery("CALL system.flush_metadata_cache()");

        assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                row("c_int", 0.0, 0.0, 1.0, null, null, null),
                row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                row("c_float", 0.0, 0.0, 1.0, null, null, null),
                row("c_double", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                row("c_date", 0.0, 0.0, 1.0, null, null, null),
                row("c_string", 0.0, 0.0, 1.0, null, null, null),
                row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                row("c_char", 0.0, 0.0, 1.0, null, null, null),
                row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                row("c_binary", 0.0, 0.0, 1.0, null, null, null),
                row(null, null, null, null, 0.0, null, null));
    }

    @Test
    @Requires(AllTypesTable.class)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testStatisticsForAllDataTypesOnlyNulls()
    {
        String tableNameInDatabase = mutableTablesState().get(EMPTY_ALL_TYPES_TABLE_NAME).getNameInDatabase();
        onHive().executeQuery("INSERT INTO TABLE " + tableNameInDatabase + " VALUES(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)");

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS");

        assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, null, null, null, null, null),
                row("c_smallint", null, null, null, null, null, null),
                row("c_int", null, null, null, null, null, null),
                row("c_bigint", null, null, null, null, null, null),
                row("c_float", null, null, null, null, null, null),
                row("c_double", null, null, null, null, null, null),
                row("c_decimal", null, null, null, null, null, null),
                row("c_decimal_w_params", null, null, null, null, null, null),
                row("c_timestamp", null, null, null, null, null, null),
                row("c_date", null, null, null, null, null, null),
                row("c_string", null, null, null, null, null, null),
                row("c_varchar", null, null, null, null, null, null),
                row("c_char", null, null, null, null, null, null),
                row("c_boolean", null, null, null, null, null, null),
                row("c_binary", null, null, null, null, null, null),
                row(null, null, null, null, 1.0, null, null));

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS FOR COLUMNS");
        onTrino().executeQuery("CALL system.flush_metadata_cache()");

        assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                row("c_int", 0.0, 0.0, 1.0, null, null, null),
                row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                row("c_float", 0.0, 0.0, 1.0, null, null, null),
                row("c_double", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                row("c_date", 0.0, 0.0, 1.0, null, null, null),
                row("c_string", 0.0, 0.0, 1.0, null, null, null),
                row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                row("c_char", 0.0, 0.0, 1.0, null, null, null),
                row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                row("c_binary", 0.0, null, 1.0, null, null, null),
                row(null, null, null, null, 1.0, null, null));
    }

    @Test
    @Requires(UnpartitionedNationTable.class)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testStatisticsForSkewedTable()
    {
        String tableName = "test_hive_skewed_table_statistics";
        onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onHive().executeQuery("CREATE TABLE " + tableName + " (c_string STRING, c_int INT) SKEWED BY (c_string) ON ('c1')");
        onHive().executeQuery("INSERT INTO TABLE " + tableName + " VALUES ('c1', 1), ('c1', 2)");

        assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableName)).containsOnly(
                row("c_string", null, null, null, null, null, null),
                row("c_int", null, null, null, null, null, null),
                row(null, null, null, null, 2.0, null, null));

        onHive().executeQuery("ANALYZE TABLE " + tableName + " COMPUTE STATISTICS");
        onTrino().executeQuery("CALL system.flush_metadata_cache()");

        assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableName)).containsOnly(
                row("c_string", null, null, null, null, null, null),
                row("c_int", null, null, null, null, null, null),
                row(null, null, null, null, 2.0, null, null));

        onHive().executeQuery("ANALYZE TABLE " + tableName + " COMPUTE STATISTICS FOR COLUMNS");
        onTrino().executeQuery("CALL system.flush_metadata_cache()");
        assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableName)).containsOnly(
                row("c_string", 4.0, 1.0, 0.0, null, null, null),
                row("c_int", null, 2.0, 0.0, null, "1", "2"),
                row(null, null, null, null, 2.0, null, null));
    }

    @Test
    @Requires(UnpartitionedNationTable.class)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testAnalyzesForSkewedTable()
    {
        String tableName = "test_analyze_skewed_table";
        onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onHive().executeQuery("CREATE TABLE " + tableName + " (c_string STRING, c_int INT) SKEWED BY (c_string) ON ('c1')");
        onHive().executeQuery("INSERT INTO TABLE " + tableName + " VALUES ('c1', 1), ('c1', 2)");

        assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableName)).containsOnly(
                row("c_string", null, null, null, null, null, null),
                row("c_int", null, null, null, null, null, null),
                row(null, null, null, null, 2.0, null, null));

        assertThat(onTrino().executeQuery("ANALYZE " + tableName)).containsExactlyInOrder(row(2));
        assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableName)).containsOnly(
                row("c_string", 4.0, 1.0, 0.0, null, null, null),
                row("c_int", null, 2.0, 0.0, null, "1", "2"),
                row(null, null, null, null, 2.0, null, null));
    }

    @Test
    @Requires(UnpartitionedNationTable.class)
    public void testAnalyzeForUnpartitionedTable()
    {
        String tableNameInDatabase = mutableTablesState().get(NATION.getName()).getNameInDatabase();

        String showStatsWholeTable = "SHOW STATS FOR " + tableNameInDatabase;

        // table not analyzed
        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("n_nationkey", 0.0, 0.0, 1.0, null, null, null),
                row("n_name", 0.0, 0.0, 1.0, null, null, null),
                row("n_regionkey", 0.0, 0.0, 1.0, null, null, null),
                row("n_comment", 0.0, 0.0, 1.0, null, null, null),
                row(null, null, null, null, 0.0, null, null));

        assertThat(onTrino().executeQuery("ANALYZE " + tableNameInDatabase)).containsExactlyInOrder(row(25));

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("n_nationkey", null, 25.0, 0.0, null, "0", "24"),
                row("n_name", 177.0, 25.0, 0.0, null, null, null),
                row("n_regionkey", null, 5.0, 0.0, null, "0", "4"),
                row("n_comment", 1857.0, 25.0, 0.0, null, null, null),
                row(null, null, null, null, 25.0, null, null));
    }

    @Test
    @Requires(NationPartitionedByBigintTable.class)
    public void testAnalyzeForTablePartitionedByBigint()
    {
        String tableNameInDatabase = mutableTablesState().get(NATION_PARTITIONED_BY_BIGINT_REGIONKEY.getName()).getNameInDatabase();

        String showStatsWholeTable = "SHOW STATS FOR " + tableNameInDatabase;
        String showStatsPartitionOne = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 1)";
        String showStatsPartitionTwo = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 2)";

        // table not analyzed

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, null, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, null, null, null));

        // analyze for single partition

        assertThat(onTrino().executeQuery("ANALYZE " + tableNameInDatabase + " WITH (partitions = ARRAY[ARRAY['1']])")).containsExactlyInOrder(row(5));

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 114.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                row("p_comment", 1497.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 15.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 38.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "2", "2"),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, null, null, null));

        // analyze for all partitions

        assertThat(onTrino().executeQuery("ANALYZE " + tableNameInDatabase)).containsExactlyInOrder(row(15));

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 109.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                row("p_comment", 1197.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 15.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 38.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "8", "21"),
                row("p_name", 31.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "2", "2"),
                row("p_comment", 351.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 5.0, null, null));
    }

    @Test
    @Requires(NationPartitionedByVarcharTable.class)
    public void testAnalyzeForTablePartitionedByVarchar()
    {
        String tableNameInDatabase = mutableTablesState().get(NATION_PARTITIONED_BY_VARCHAR_REGIONKEY.getName()).getNameInDatabase();

        String showStatsWholeTable = "SHOW STATS FOR " + tableNameInDatabase;
        String showStatsPartitionOne = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 'AMERICA')";
        String showStatsPartitionTwo = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 'ASIA')";

        // table not analyzed

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null, null, null),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, null, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, null, null),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, null, null, null));

        // analyze for single partition

        assertThat(onTrino().executeQuery("ANALYZE " + tableNameInDatabase + " WITH (partitions = ARRAY[ARRAY['AMERICA']])")).containsExactlyInOrder(row(5));

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 114.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", 85.0, 3.0, 0.0, null, null, null),
                row("p_comment", 1497.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 15.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 38.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", 35.0, 1.0, 0.0, null, null, null),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, null, null),
                row("p_comment", null, null, null, null, null, null),
                row(null, null, null, null, null, null, null));

        // column analysis for all partitions

        assertThat(onTrino().executeQuery("ANALYZE " + tableNameInDatabase)).containsExactlyInOrder(row(15));

        assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 109.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", 85.0, 3.0, 0.0, null, null, null),
                row("p_comment", 1197.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 15.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 38.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", 35.0, 1.0, 0.0, null, null, null),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 5.0, null, null));

        assertThat(onTrino().executeQuery(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "8", "21"),
                row("p_name", 31.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", 20.0, 1.0, 0.0, null, null, null),
                row("p_comment", 351.0, 5.0, 0.0, null, null, null),
                row(null, null, null, null, 5.0, null, null));
    }

    // This covers also stats calculation for unpartitioned table
    @Test
    @Requires(AllTypesTable.class)
    public void testAnalyzeForAllDataTypes()
    {
        String tableNameInDatabase = mutableTablesState().get(ALL_TYPES_TABLE_NAME).getNameInDatabase();

        assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                row("c_int", 0.0, 0.0, 1.0, null, null, null),
                row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                row("c_float", 0.0, 0.0, 1.0, null, null, null),
                row("c_double", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                row("c_date", 0.0, 0.0, 1.0, null, null, null),
                row("c_string", 0.0, 0.0, 1.0, null, null, null),
                row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                row("c_char", 0.0, 0.0, 1.0, null, null, null),
                row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                row("c_binary", 0.0, 0.0, 1.0, null, null, null),
                row(null, null, null, null, 0.0, null, null));

        assertThat(onTrino().executeQuery("ANALYZE " + tableNameInDatabase)).containsExactlyInOrder(row(2));

        // SHOW STATS FORMAT: column_name, data_size, distinct_values_count, nulls_fraction, row_count
        assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, 2.0, 0.0, null, "121", "127"),
                row("c_smallint", null, 2.0, 0.0, null, "32761", "32767"),
                row("c_int", null, 2.0, 0.0, null, "2147483641", "2147483647"),
                row("c_bigint", null, 1.0, 0.0, null, "9223372036854775807", "9223372036854775807"),
                row("c_float", null, 2.0, 0.0, null, "123.341", "123.345"),
                row("c_double", null, 2.0, 0.0, null, "234.561", "235.567"),
                row("c_decimal", null, 2.0, 0.0, null, "345.0", "346.0"),
                row("c_decimal_w_params", null, 2.0, 0.0, null, "345.671", "345.678"),
                row("c_timestamp", null, 2.0, 0.0, null, null, null),
                row("c_date", null, 2.0, 0.0, null, "2015-05-09", "2015-06-10"),
                row("c_string", 22.0, 2.0, 0.0, null, null, null),
                row("c_varchar", 20.0, 2.0, 0.0, null, null, null),
                row("c_char", 12.0, 2.0, 0.0, null, null, null),
                row("c_boolean", null, 2.0, 0.0, null, null, null),
                row("c_binary", 23.0, null, 0.0, null, null, null),
                row(null, null, null, null, 2.0, null, null));
    }

    @Test
    @Requires(AllTypesTable.class)
    public void testAnalyzeForAllDataTypesNoData()
    {
        String tableNameInDatabase = mutableTablesState().get(EMPTY_ALL_TYPES_TABLE_NAME).getNameInDatabase();

        assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                row("c_int", 0.0, 0.0, 1.0, null, null, null),
                row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                row("c_float", 0.0, 0.0, 1.0, null, null, null),
                row("c_double", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                row("c_date", 0.0, 0.0, 1.0, null, null, null),
                row("c_string", 0.0, 0.0, 1.0, null, null, null),
                row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                row("c_char", 0.0, 0.0, 1.0, null, null, null),
                row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                row("c_binary", 0.0, 0.0, 1.0, null, null, null),
                row(null, null, null, null, 0.0, null, null));

        assertThat(onTrino().executeQuery("ANALYZE " + tableNameInDatabase)).containsExactlyInOrder(row(0));

        assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                row("c_int", 0.0, 0.0, 1.0, null, null, null),
                row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                row("c_float", 0.0, 0.0, 1.0, null, null, null),
                row("c_double", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                row("c_date", 0.0, 0.0, 1.0, null, null, null),
                row("c_string", 0.0, 0.0, 1.0, null, null, null),
                row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                row("c_char", 0.0, 0.0, 1.0, null, null, null),
                row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                row("c_binary", 0.0, 0.0, 1.0, null, null, null),
                row(null, null, null, null, 0.0, null, null));
    }

    @Test
    @Requires(AllTypesTable.class)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testAnalyzeForAllDataTypesOnlyNulls()
    {
        String tableNameInDatabase = mutableTablesState().get(EMPTY_ALL_TYPES_TABLE_NAME).getNameInDatabase();

        // insert from Hive to prevent Trino collecting statistics on insert
        onHive().executeQuery("INSERT INTO TABLE " + tableNameInDatabase + " VALUES(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)");

        assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", null, null, null, null, null, null),
                row("c_smallint", null, null, null, null, null, null),
                row("c_int", null, null, null, null, null, null),
                row("c_bigint", null, null, null, null, null, null),
                row("c_float", null, null, null, null, null, null),
                row("c_double", null, null, null, null, null, null),
                row("c_decimal", null, null, null, null, null, null),
                row("c_decimal_w_params", null, null, null, null, null, null),
                row("c_timestamp", null, null, null, null, null, null),
                row("c_date", null, null, null, null, null, null),
                row("c_string", null, null, null, null, null, null),
                row("c_varchar", null, null, null, null, null, null),
                row("c_char", null, null, null, null, null, null),
                row("c_boolean", null, null, null, null, null, null),
                row("c_binary", null, null, null, null, null, null),
                row(null, null, null, null, 1.0, null, null));

        assertThat(onTrino().executeQuery("ANALYZE " + tableNameInDatabase)).containsExactlyInOrder(row(1));

        assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableNameInDatabase)).containsOnly(
                row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                row("c_int", 0.0, 0.0, 1.0, null, null, null),
                row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                row("c_float", 0.0, 0.0, 1.0, null, null, null),
                row("c_double", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                row("c_date", 0.0, 0.0, 1.0, null, null, null),
                row("c_string", 0.0, 0.0, 1.0, null, null, null),
                row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                row("c_char", 0.0, 0.0, 1.0, null, null, null),
                row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                row("c_binary", 0.0, null, 1.0, null, null, null),
                row(null, null, null, null, 1.0, null, null));
    }

    @Test
    @Requires(AllTypesTable.class)
    public void testComputeTableStatisticsOnCreateTable()
    {
        String allTypesTable = mutableTablesState().get(ALL_TYPES_TABLE_NAME).getNameInDatabase();
        String emptyAllTypesTable = mutableTablesState().get(EMPTY_ALL_TYPES_TABLE_NAME).getNameInDatabase();
        String allTypesAllNullTable = mutableTablesState().get(ALL_TYPES_ALL_NULL_TABLE_NAME).getNameInDatabase();

        assertComputeTableStatisticsOnCreateTable(allTypesTable, getAllTypesTableStatistics());
        assertComputeTableStatisticsOnCreateTable(emptyAllTypesTable, getAllTypesEmptyTableStatistics());
        assertComputeTableStatisticsOnCreateTable(allTypesAllNullTable, getAllTypesAllNullTableStatistics());
    }

    @Test
    @Requires(AllTypesTable.class)
    public void testComputeTableStatisticsOnInsert()
    {
        String allTypesTable = mutableTablesState().get(ALL_TYPES_TABLE_NAME).getNameInDatabase();
        String emptyAllTypesTable = mutableTablesState().get(EMPTY_ALL_TYPES_TABLE_NAME).getNameInDatabase();
        String allTypesAllNullTable = mutableTablesState().get(ALL_TYPES_ALL_NULL_TABLE_NAME).getNameInDatabase();

        assertComputeTableStatisticsOnInsert(allTypesTable, getAllTypesTableStatistics());
        assertComputeTableStatisticsOnInsert(emptyAllTypesTable, getAllTypesEmptyTableStatistics());
        assertComputeTableStatisticsOnInsert(allTypesAllNullTable, getAllTypesAllNullTableStatistics());

        String tableName = "test_update_table_statistics";
        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        try {
            onTrino().executeQuery(format("CREATE TABLE %s AS SELECT * FROM %s WITH NO DATA", tableName, allTypesTable));
            onTrino().executeQuery(format("INSERT INTO %s SELECT * FROM %s", tableName, allTypesTable));
            onTrino().executeQuery(format("INSERT INTO %s SELECT * FROM %s", tableName, allTypesAllNullTable));
            onTrino().executeQuery(format("INSERT INTO %s SELECT * FROM %s", tableName, allTypesAllNullTable));
            assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableName)).containsOnly(
                    row("c_tinyint", null, 2.0, 0.5, null, "121", "127"),
                    row("c_smallint", null, 2.0, 0.5, null, "32761", "32767"),
                    row("c_int", null, 2.0, 0.5, null, "2147483641", "2147483647"),
                    row("c_bigint", null, 1.0, 0.5, null, "9223372036854775807", "9223372036854775807"),
                    row("c_float", null, 2.0, 0.5, null, "123.341", "123.345"),
                    row("c_double", null, 2.0, 0.5, null, "234.561", "235.567"),
                    row("c_decimal", null, 2.0, 0.5, null, "345.0", "346.0"),
                    row("c_decimal_w_params", null, 2.0, 0.5, null, "345.671", "345.678"),
                    row("c_timestamp", null, 2.0, 0.5, null, null, null),
                    row("c_date", null, 2.0, 0.5, null, "2015-05-09", "2015-06-10"),
                    row("c_string", 22.0, 2.0, 0.5, null, null, null),
                    row("c_varchar", 20.0, 2.0, 0.5, null, null, null),
                    row("c_char", 12.0, 2.0, 0.5, null, null, null),
                    row("c_boolean", null, 2.0, 0.5, null, null, null),
                    row("c_binary", 23.0, null, 0.5, null, null, null),
                    row(null, null, null, null, 4.0, null, null));

            onTrino().executeQuery(format("INSERT INTO %s VALUES( " +
                    "TINYINT '120', " +
                    "SMALLINT '32760', " +
                    "INTEGER '2147483640', " +
                    "BIGINT '9223372036854775800', " +
                    "REAL '123.340', " +
                    "DOUBLE '234.560', " +
                    "CAST(343.0 AS DECIMAL(10, 0)), " +
                    "CAST(345.670 AS DECIMAL(10, 5)), " +
                    "TIMESTAMP '2015-05-10 12:15:30', " +
                    "DATE '2015-05-08', " +
                    "VARCHAR 'ela ma kot', " +
                    "CAST('ela ma ko' AS VARCHAR(10)), " +
                    "CAST('ela m     ' AS CHAR(10)), " +
                    "false, " +
                    "CAST('cGllcyBiaW5hcm54' as VARBINARY))", tableName));

            assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableName)).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 2.0, 0.4, null, "120", "127"),
                    row("c_smallint", null, 2.0, 0.4, null, "32760", "32767"),
                    row("c_int", null, 2.0, 0.4, null, "2147483640", "2147483647"),
                    row("c_bigint", null, 1.0, 0.4, null, "9223372036854775807", "9223372036854775807"),
                    row("c_float", null, 2.0, 0.4, null, "123.34", "123.345"),
                    row("c_double", null, 2.0, 0.4, null, "234.56", "235.567"),
                    row("c_decimal", null, 2.0, 0.4, null, "343.0", "346.0"),
                    row("c_decimal_w_params", null, 2.0, 0.4, null, "345.67", "345.678"),
                    row("c_timestamp", null, 2.0, 0.4, null, null, null),
                    row("c_date", null, 2.0, 0.4, null, "2015-05-08", "2015-06-10"),
                    row("c_string", 32.0, 2.0, 0.4, null, null, null),
                    row("c_varchar", 29.0, 2.0, 0.4, null, null, null),
                    row("c_char", 17.0, 2.0, 0.4, null, null, null),
                    row("c_boolean", null, 2.0, 0.4, null, null, null),
                    row("c_binary", 39.0, null, 0.4, null, null, null),
                    row(null, null, null, null, 5.0, null, null)));
        }
        finally {
            onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    @Requires(AllTypesTable.class)
    public void testComputePartitionStatisticsOnCreateTable()
    {
        String tableName = "test_compute_partition_statistics_on_create_table";
        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        try {
            onTrino().executeQuery(format("CREATE TABLE %s WITH ( " +
                    " partitioned_by = ARRAY['p_bigint', 'p_varchar']" +
                    ") AS " +
                    "SELECT * FROM ( " +
                    "    VALUES " +
                    "        (" +
                    "           TINYINT '120', " +
                    "           SMALLINT '32760', " +
                    "           INTEGER '2147483640', " +
                    "           BIGINT '9223372036854775800', " +
                    "           REAL '123.340', DOUBLE '234.560', " +
                    "           CAST(343.0 AS DECIMAL(10, 0)), " +
                    "           CAST(345.670 AS DECIMAL(10, 5)), " +
                    "           TIMESTAMP '2015-05-10 12:15:30.000', " +
                    "           DATE '2015-05-08', " +
                    "           VARCHAR 'p1 varchar', " +
                    "           CAST('p1 varchar10' AS VARCHAR(10)), " +
                    "           CAST('p1 char10' AS CHAR(10)), false, " +
                    "           CAST('p1 binary' as VARBINARY), " +
                    "           BIGINT '1', " +
                    "           VARCHAR 'partition1'" +
                    "        ), " +
                    "        (null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '1', 'partition1'), " +
                    "        (" +
                    "           TINYINT '99', " +
                    "           SMALLINT '333'," +
                    "           INTEGER '444', " +
                    "           BIGINT '555', " +
                    "           REAL '666.340', " +
                    "           DOUBLE '777.560', " +
                    "           CAST(888.0 AS DECIMAL(10, 0)), " +
                    "           CAST(999.670 AS DECIMAL(10, 5)), " +
                    "           TIMESTAMP '2015-05-10 12:45:30.000', " +
                    "           DATE '2015-05-09', " +
                    "           VARCHAR 'p2 varchar', " +
                    "           CAST('p2 varchar10' AS VARCHAR(10)), " +
                    "           CAST('p2 char10' AS CHAR(10)), " +
                    "           true, " +
                    "           CAST('p2 binary' as VARBINARY), " +
                    "           BIGINT '2', " +
                    "           VARCHAR 'partition2'" +
                    "        ), " +
                    "        (null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '2', 'partition2') " +
                    "" +
                    ") AS t (c_tinyint, c_smallint, c_int, c_bigint, c_float, c_double, c_decimal, c_decimal_w_params, c_timestamp, c_date, c_string, c_varchar, c_char, c_boolean, c_binary, p_bigint, p_varchar)", tableName));

            assertThat(onTrino().executeQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_bigint = 1 AND p_varchar = 'partition1')", tableName))).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "120", "120"),
                    row("c_smallint", null, 1.0, 0.5, null, "32760", "32760"),
                    row("c_int", null, 1.0, 0.5, null, "2147483640", "2147483640"),
                    row("c_bigint", null, 1.0, 0.5, null, "9223372036854775807", "9223372036854775807"),
                    row("c_float", null, 1.0, 0.5, null, "123.34", "123.34"),
                    row("c_double", null, 1.0, 0.5, null, "234.56", "234.56"),
                    row("c_decimal", null, 1.0, 0.5, null, "343.0", "343.0"),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "345.67", "345.67"),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-08", "2015-05-08"),
                    row("c_string", 10.0, 1.0, 0.5, null, null, null),
                    row("c_varchar", 10.0, 1.0, 0.5, null, null, null),
                    row("c_char", 9.0, 1.0, 0.5, null, null, null),
                    row("c_boolean", null, 1.0, 0.5, null, null, null),
                    row("c_binary", 9.0, null, 0.5, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "1", "1"),
                    row("p_varchar", 20.0, 1.0, 0.0, null, null, null),
                    row(null, null, null, null, 2.0, null, null)));

            assertThat(onTrino().executeQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_bigint = 2 AND p_varchar = 'partition2')", tableName))).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "99", "99"),
                    row("c_smallint", null, 1.0, 0.5, null, "333", "333"),
                    row("c_int", null, 1.0, 0.5, null, "444", "444"),
                    row("c_bigint", null, 1.0, 0.5, null, "555", "555"),
                    row("c_float", null, 1.0, 0.5, null, "666.34", "666.34"),
                    row("c_double", null, 1.0, 0.5, null, "777.56", "777.56"),
                    row("c_decimal", null, 1.0, 0.5, null, "888.0", "888.0"),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "999.67", "999.67"),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-09", "2015-05-09"),
                    row("c_string", 10.0, 1.0, 0.5, null, null, null),
                    row("c_varchar", 10.0, 1.0, 0.5, null, null, null),
                    row("c_char", 9.0, 1.0, 0.5, null, null, null),
                    row("c_boolean", null, 1.0, 0.5, null, null, null),
                    row("c_binary", 9.0, null, 0.5, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "2", "2"),
                    row("p_varchar", 20.0, 1.0, 0.0, null, null, null),
                    row(null, null, null, null, 2.0, null, null)));
        }
        finally {
            onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    @Requires(AllTypesTable.class)
    public void testComputePartitionStatisticsOnInsert()
    {
        String tableName = "test_compute_partition_statistics_on_insert";

        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        try {
            onTrino().executeQuery(format("CREATE TABLE %s(" +
                    "c_tinyint            TINYINT, " +
                    "c_smallint           SMALLINT, " +
                    "c_int                INT, " +
                    "c_bigint             BIGINT, " +
                    "c_float              REAL, " +
                    "c_double             DOUBLE, " +
                    "c_decimal            DECIMAL(10,0), " +
                    "c_decimal_w_params   DECIMAL(10,5), " +
                    "c_timestamp          TIMESTAMP, " +
                    "c_date               DATE, " +
                    "c_string             VARCHAR, " +
                    "c_varchar            VARCHAR(10), " +
                    "c_char               CHAR(10), " +
                    "c_boolean            BOOLEAN, " +
                    "c_binary             VARBINARY, " +
                    "p_bigint             BIGINT, " +
                    "p_varchar            VARCHAR " +
                    ") WITH ( " +
                    " partitioned_by = ARRAY['p_bigint', 'p_varchar']" +
                    ")", tableName));

            onTrino().executeQuery(format("INSERT INTO %s VALUES " +
                    "(TINYINT '120', SMALLINT '32760', INTEGER '2147483640', BIGINT '9223372036854775800', REAL '123.340', DOUBLE '234.560', CAST(343.0 AS DECIMAL(10, 0)), CAST(345.670 AS DECIMAL(10, 5)), TIMESTAMP '2015-05-10 12:15:30', DATE '2015-05-08', 'p1 varchar', CAST('p1 varchar10' AS VARCHAR(10)), CAST('p1 char10' AS CHAR(10)), false, CAST('p1 binary' as VARBINARY), BIGINT '1', 'partition1')," +
                    "(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '1', 'partition1')", tableName));

            onTrino().executeQuery(format("INSERT INTO %s VALUES " +
                    "(TINYINT '99', SMALLINT '333', INTEGER '444', BIGINT '555', REAL '666.340', DOUBLE '777.560', CAST(888.0 AS DECIMAL(10, 0)), CAST(999.670 AS DECIMAL(10, 5)), TIMESTAMP '2015-05-10 12:45:30', DATE '2015-05-09', 'p2 varchar', CAST('p2 varchar10' AS VARCHAR(10)), CAST('p2 char10' AS CHAR(10)), true, CAST('p2 binary' as VARBINARY), BIGINT '2', 'partition2')," +
                    "(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '2', 'partition2')", tableName));

            String showStatsPartitionOne = format("SHOW STATS FOR (SELECT * FROM %s WHERE p_bigint = 1 AND p_varchar = 'partition1')", tableName);
            String showStatsPartitionTwo = format("SHOW STATS FOR (SELECT * FROM %s WHERE p_bigint = 2 AND p_varchar = 'partition2')", tableName);

            assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "120", "120"),
                    row("c_smallint", null, 1.0, 0.5, null, "32760", "32760"),
                    row("c_int", null, 1.0, 0.5, null, "2147483640", "2147483640"),
                    row("c_bigint", null, 1.0, 0.5, null, "9223372036854775807", "9223372036854775807"),
                    row("c_float", null, 1.0, 0.5, null, "123.34", "123.34"),
                    row("c_double", null, 1.0, 0.5, null, "234.56", "234.56"),
                    row("c_decimal", null, 1.0, 0.5, null, "343.0", "343.0"),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "345.67", "345.67"),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-08", "2015-05-08"),
                    row("c_string", 10.0, 1.0, 0.5, null, null, null),
                    row("c_varchar", 10.0, 1.0, 0.5, null, null, null),
                    row("c_char", 9.0, 1.0, 0.5, null, null, null),
                    row("c_boolean", null, 1.0, 0.5, null, null, null),
                    row("c_binary", 9.0, null, 0.5, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "1", "1"),
                    row("p_varchar", 20.0, 1.0, 0.0, null, null, null),
                    row(null, null, null, null, 2.0, null, null)));

            assertThat(onTrino().executeQuery(showStatsPartitionTwo)).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "99", "99"),
                    row("c_smallint", null, 1.0, 0.5, null, "333", "333"),
                    row("c_int", null, 1.0, 0.5, null, "444", "444"),
                    row("c_bigint", null, 1.0, 0.5, null, "555", "555"),
                    row("c_float", null, 1.0, 0.5, null, "666.34", "666.34"),
                    row("c_double", null, 1.0, 0.5, null, "777.56", "777.56"),
                    row("c_decimal", null, 1.0, 0.5, null, "888.0", "888.0"),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "999.67", "999.67"),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-09", "2015-05-09"),
                    row("c_string", 10.0, 1.0, 0.5, null, null, null),
                    row("c_varchar", 10.0, 1.0, 0.5, null, null, null),
                    row("c_char", 9.0, 1.0, 0.5, null, null, null),
                    row("c_boolean", null, 1.0, 0.5, null, null, null),
                    row("c_binary", 9.0, null, 0.5, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "2", "2"),
                    row("p_varchar", 20.0, 1.0, 0.0, null, null, null),
                    row(null, null, null, null, 2.0, null, null)));

            onTrino().executeQuery(format("INSERT INTO %s VALUES( TINYINT '119', SMALLINT '32759', INTEGER '2147483639', BIGINT '9223372036854775799', REAL '122.340', DOUBLE '233.560', CAST(342.0 AS DECIMAL(10, 0)), CAST(344.670 AS DECIMAL(10, 5)), TIMESTAMP '2015-05-10 12:15:29', DATE '2015-05-07', 'p1 varchar', CAST('p1 varchar10' AS VARCHAR(10)), CAST('p1 char10' AS CHAR(10)), true, CAST('p1 binary' as VARBINARY), BIGINT '1', 'partition1')", tableName));
            onTrino().executeQuery(format("INSERT INTO %s VALUES( null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '1', 'partition1')", tableName));

            assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "119", "120"),
                    row("c_smallint", null, 1.0, 0.5, null, "32759", "32760"),
                    row("c_int", null, 1.0, 0.5, null, "2147483639", "2147483640"),
                    row("c_bigint", null, 1.0, 0.5, null, "9223372036854775807", "9223372036854775807"),
                    row("c_float", null, 1.0, 0.5, null, "122.34", "123.34"),
                    row("c_double", null, 1.0, 0.5, null, "233.56", "234.56"),
                    row("c_decimal", null, 1.0, 0.5, null, "342.0", "343.0"),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "344.67", "345.67"),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-07", "2015-05-08"),
                    row("c_string", 20.0, 1.0, 0.5, null, null, null),
                    row("c_varchar", 20.0, 1.0, 0.5, null, null, null),
                    row("c_char", 18.0, 1.0, 0.5, null, null, null),
                    row("c_boolean", null, 2.0, 0.5, null, null, null),
                    row("c_binary", 18.0, null, 0.5, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "1", "1"),
                    row("p_varchar", 40.0, 1.0, 0.0, null, null, null),
                    row(null, null, null, null, 4.0, null, null)));

            onTrino().executeQuery(format("INSERT INTO %s VALUES( TINYINT '100', SMALLINT '334', INTEGER '445', BIGINT '556', REAL '667.340', DOUBLE '778.560', CAST(889.0 AS DECIMAL(10, 0)), CAST(1000.670 AS DECIMAL(10, 5)), TIMESTAMP '2015-05-10 12:45:31', DATE '2015-05-10', VARCHAR 'p2 varchar', CAST('p2 varchar10' AS VARCHAR(10)), CAST('p2 char10' AS CHAR(10)), true, CAST('p2 binary' as VARBINARY), BIGINT '2', 'partition2')", tableName));
            onTrino().executeQuery(format("INSERT INTO %s VALUES( null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '2', 'partition2')", tableName));

            assertThat(onTrino().executeQuery(showStatsPartitionTwo)).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "99", "100"),
                    row("c_smallint", null, 1.0, 0.5, null, "333", "334"),
                    row("c_int", null, 1.0, 0.5, null, "444", "445"),
                    row("c_bigint", null, 1.0, 0.5, null, "555", "556"),
                    row("c_float", null, 1.0, 0.5, null, "666.34", "667.34"),
                    row("c_double", null, 1.0, 0.5, null, "777.56", "778.56"),
                    row("c_decimal", null, 1.0, 0.5, null, "888.0", "889.0"),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "999.67", "1000.67"),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-09", "2015-05-10"),
                    row("c_string", 20.0, 1.0, 0.5, null, null, null),
                    row("c_varchar", 20.0, 1.0, 0.5, null, null, null),
                    row("c_char", 18.0, 1.0, 0.5, null, null, null),
                    row("c_boolean", null, 1.0, 0.5, null, null, null),
                    row("c_binary", 18.0, null, 0.5, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "2", "2"),
                    row("p_varchar", 40.0, 1.0, 0.0, null, null, null),
                    row(null, null, null, null, 4.0, null, null)));
        }
        finally {
            onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test(dataProvider = "testComputeFloatingPointStatisticsDataProvider")
    public void testComputeFloatingPointStatistics(String dataType)
    {
        String tableName = "test_compute_floating_point_statistics";
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        try {
            onTrino().executeQuery(format("CREATE TABLE %1$s(c_basic %2$s, c_minmax %2$s, c_inf %2$s, c_ninf %2$s, c_nan %2$s, c_nzero %2$s)", tableName, dataType));
            onTrino().executeQuery("ANALYZE " + tableName); // TODO remove after https://github.com/trinodb/trino/issues/2469

            onTrino().executeQuery(format(
                    "INSERT INTO %1$s(c_basic, c_minmax, c_inf, c_ninf, c_nan, c_nzero) VALUES " +
                            "  (%2$s '42.3', %2$s '576234.567',  %2$s 'Infinity', %2$s '-Infinity', %2$s 'NaN', %2$s '-0')," +
                            "  (%2$s '42.3', %2$s '-1234567.89', %2$s '-15', %2$s '45', %2$s '12345', %2$s '-47'), " +
                            "  (NULL, NULL, NULL, NULL, NULL, NULL)",
                    tableName,
                    dataType));

            List<Row> expectedStatistics = ImmutableList.of(
                    row("c_basic", null, 1., 0.33333333333, null, "42.3", "42.3"),
                    Objects.equals(dataType, "double")
                            ? row("c_minmax", null, 2., 0.33333333333, null, "-1234567.89", "576234.567")
                            : row("c_minmax", null, 2., 0.33333333333, null, "-1234567.9", "576234.56"),
                    row("c_inf", null, 2., 0.33333333333, null, null, null), // -15, +inf
                    row("c_ninf", null, 2., 0.33333333333, null, null, null), // -inf, 45
                    row("c_nan", null, 1., 0.33333333333, null, "12345.0", "12345.0"), // NaN is ignored by min/max
                    row("c_nzero", null, 2., 0.33333333333, null, "-47.0", "0.0"),
                    row(null, null, null, null, 3., null, null));

            assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableName)).containsOnly(expectedStatistics);

            onTrino().executeQuery("ANALYZE " + tableName);
            assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableName)).containsOnly(expectedStatistics);
        }
        finally {
            onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @DataProvider
    public Object[][] testComputeFloatingPointStatisticsDataProvider()
    {
        return new Object[][] {
                {"real"},
                {"double"},
        };
    }

    @Test
    public void testComputeStatisticsForTableWithOnlyDateColumns()
    {
        String tableName = "test_compute_statistics_with_only_date_columns";
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        try {
            onTrino().executeQuery(format("CREATE TABLE %s AS SELECT date'2019-12-02' c_date", tableName));

            List<Row> expectedStatistics = ImmutableList.of(
                    row("c_date", null, 1.0, 0.0, null, "2019-12-02", "2019-12-02"),
                    row(null, null, null, null, 1.0, null, null));

            assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableName)).containsOnly(expectedStatistics);

            onTrino().executeQuery("ANALYZE " + tableName);
            assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableName)).containsOnly(expectedStatistics);
        }
        finally {
            onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testMixedHiveAndPrestoStatistics()
    {
        String tableName = "test_mixed_hive_and_presto_statistics";
        onHive().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        onHive().executeQuery(format("CREATE TABLE %s (a INT) PARTITIONED BY (p INT) STORED AS ORC TBLPROPERTIES ('transactional' = 'false')", tableName));

        try {
            onHive().executeQuery(format("INSERT OVERWRITE TABLE %s PARTITION (p=1) VALUES (1),(2),(3),(4)", tableName));
            onHive().executeQuery(format("INSERT OVERWRITE TABLE %s PARTITION (p=2) VALUES (10),(11),(12)", tableName));

            String showStatsPartitionOne = format("SHOW STATS FOR (SELECT * FROM %s WHERE p = 1)", tableName);
            String showStatsPartitionTwo = format("SHOW STATS FOR (SELECT * FROM %s WHERE p = 2)", tableName);
            String showStatsWholeTable = format("SHOW STATS FOR %s", tableName);

            // drop all stats; which could have been created on insert
            onTrino().executeQuery(format("CALL system.drop_stats('default', '%s')", tableName));

            // sanity check that there are no statistics
            assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                    row("p", null, 1.0, 0.0, null, "1", "1"),
                    row("a", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));
            assertThat(onTrino().executeQuery(showStatsPartitionTwo)).containsOnly(
                    row("p", null, 1.0, 0.0, null, "2", "2"),
                    row("a", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));
            assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                    row("p", null, 2.0, 0.0, null, "1", "2"),
                    row("a", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));

            // analyze first partition with Trino and second with Hive
            onTrino().executeQuery(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['1']])", tableName));
            onHive().executeQuery(format("ANALYZE TABLE %s PARTITION (p = \"2\") COMPUTE STATISTICS", tableName));
            onHive().executeQuery(format("ANALYZE TABLE %s PARTITION (p = \"2\") COMPUTE STATISTICS FOR COLUMNS", tableName));
            onTrino().executeQuery("CALL system.flush_metadata_cache()");

            // we can get stats for individual partitions
            assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                    row("p", null, 1.0, 0.0, null, "1", "1"),
                    row("a", null, 4.0, 0.0, null, "1", "4"),
                    row(null, null, null, null, 4.0, null, null));
            assertThat(onTrino().executeQuery(showStatsPartitionTwo)).containsOnly(
                    row("p", null, 1.0, 0.0, null, "2", "2"),
                    row("a", null, 3.0, 0.0, null, "10", "12"),
                    row(null, null, null, null, 3.0, null, null));

            // as well as for whole table
            assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                    row("p", null, 2.0, 0.0, null, "1", "2"),
                    row("a", null, 4.0, 0.0, null, "1", "12"),
                    row(null, null, null, null, 7.0, null, null));
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testEmptyPartitionedHiveStatistics()
    {
        String tableName = "test_empty_partitioned_hive_" + randomNameSuffix();
        try {
            onHive().executeQuery(format("CREATE TABLE %s (a INT) PARTITIONED BY (p INT)", tableName));

            // disable computation of statistics
            onHive().executeQuery("set hive.stats.autogather=false");

            onHive().executeQuery(format("INSERT INTO TABLE %s PARTITION (p=1) VALUES (11),(12),(13),(14)", tableName));
            onHive().executeQuery(format("INSERT INTO TABLE %s PARTITION (p=2) VALUES (21),(22),(23)", tableName));

            String showStatsPartitionOne = format("SHOW STATS FOR (SELECT * FROM %s WHERE p = 1)", tableName);
            String showStatsPartitionTwo = format("SHOW STATS FOR (SELECT * FROM %s WHERE p = 2)", tableName);
            String showStatsWholeTable = format("SHOW STATS FOR %s", tableName);

            assertThat(onTrino().executeQuery(showStatsPartitionOne)).containsOnly(
                    row("p", null, 1.0, 0.0, null, "1", "1"),
                    row("a", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));
            assertThat(onTrino().executeQuery(showStatsPartitionTwo)).containsOnly(
                    row("p", null, 1.0, 0.0, null, "2", "2"),
                    row("a", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));
            assertThat(onTrino().executeQuery(showStatsWholeTable)).containsOnly(
                    row("p", null, 2.0, 0.0, null, "1", "2"),
                    row("a", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));
        }
        finally {
            // enable computation of statistics
            onHive().executeQuery("set hive.stats.autogather=true");
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    private static void assertComputeTableStatisticsOnCreateTable(String sourceTableName, List<Row> expectedStatistics)
    {
        String copiedTableName = "assert_compute_table_statistics_on_create_table_" + sourceTableName;
        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", copiedTableName));
        try {
            onTrino().executeQuery(format("CREATE TABLE %s AS SELECT * FROM %s", copiedTableName, sourceTableName));
            assertThat(onTrino().executeQuery("SHOW STATS FOR " + copiedTableName)).containsOnly(expectedStatistics);
        }
        finally {
            onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", copiedTableName));
        }
    }

    private static void assertComputeTableStatisticsOnInsert(String sourceTableName, List<Row> expectedStatistics)
    {
        String copiedTableName = "assert_compute_table_statistics_on_insert_" + sourceTableName;
        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", copiedTableName));
        try {
            onTrino().executeQuery(format("CREATE TABLE %s AS SELECT * FROM %s WITH NO DATA", copiedTableName, sourceTableName));
            assertThat(onTrino().executeQuery("SHOW STATS FOR " + copiedTableName)).containsOnly(getAllTypesEmptyTableStatistics());
            onTrino().executeQuery(format("INSERT INTO %s SELECT * FROM %s", copiedTableName, sourceTableName));
            assertThat(onTrino().executeQuery("SHOW STATS FOR " + copiedTableName)).containsOnly(expectedStatistics);
        }
        finally {
            onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", copiedTableName));
        }
    }
}
