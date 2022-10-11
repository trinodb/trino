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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDeltaLakeQueryRunner;
import static java.lang.String.format;

public class TestDeltaLakeTableStatistics
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createDeltaLakeQueryRunner(DELTA_CATALOG, ImmutableMap.of(), ImmutableMap.of("delta.register-table-procedure.enabled", "true"));
    }

    @BeforeClass
    public void registerTables()
    {
        String dataPath = Resources.getResource("databricks/person").toExternalForm();
        getQueryRunner().execute(
                format("CALL system.register_table('%s', 'person', '%s')", getSession().getSchema().orElseThrow(), dataPath));
    }

    @Test
    public void testShowStatsForPersonTable()
    {
        assertQuery(
                "SHOW STATS FOR person",
                "VALUES " +
                        //  column_name | data_size | distinct_values_count | nulls_fraction | row_count | low_value | high_value
                        "('name', null, null, 0.0, null, null, null)," +
                        "('age', null, 6.0, 0.0, null, null, null)," +
                        "('married', null, null, null, null, null, null)," +
                        "('phones', null, null, 0.0, null, null, null)," +
                        "('address', null, null, null, null, null, null)," +
                        "('income', null, null, 0.0, null, 22000.0, 120000.0)," +
                        "('gender', null, null, null, null, null, null)," +
                        "(null, null, null, null, 12.0, null, null)");
    }

    @Test
    public void testShowStatsForTableWithNullsInPartitioningColumn()
    {
        assertUpdate(
                "CREATE TABLE test_null_in_partitioning_column (pk, val_col)" +
                        "WITH(partitioned_by = ARRAY['pk']) " +
                        "AS VALUES " +
                        "('as1', 23), " +
                        "(null, 24) ",
                2);
        assertQuery(
                "SHOW STATS FOR test_null_in_partitioning_column",
                "VALUES " +
                        //  column_name | data_size | distinct_values_count | nulls_fraction | row_count | low_value | high_value
                        "('pk', null, 1.0, 0.5, null, null, null)," +
                        "('val_col', null, 2.0, 0.0, null, 23, 24)," +
                        "(null, null, null, null, 2.0, null, null)");
    }

    @Test
    public void testShowStatsForTableWithTwoPartitioningColumns()
    {
        assertUpdate(
                "CREATE TABLE test_stats_for_table_with_two_part_columns (pk1, pk2, val_col)" +
                        "WITH(partitioned_by = ARRAY['pk1', 'pk2']) " +
                        "AS VALUES " +
                        "('pk1', 'pk21', 23), " +
                        "(null, 'pk22', 24), " +
                        "('pk1', 'pk22', 25), " +
                        "('pk1', 'pk23', 26) ",
                4);
        assertQuery(
                "SHOW STATS FOR test_stats_for_table_with_two_part_columns",
                "VALUES " +
                        //  column_name | data_size | distinct_values_count | nulls_fraction | row_count | low_value | high_value
                        "('pk1', null, 1.0, 0.25, null, null, null)," +
                        "('pk2', null, 3.0, 0.0, null, null, null)," +
                        "('val_col', null, 4.0, 0.0, null, 23, 26)," +
                        "(null, null, null, null, 4.0, null, null)");
    }

    @Test
    public void testShowStatsForPartitioningColumnThatOnlyHasNulls()
    {
        assertUpdate(
                "CREATE TABLE test_stats_for_table_with_nulls_only_partitioning_column (pk1, val_col) " +
                        "WITH(partitioned_by = ARRAY['pk1']) " +
                        "AS VALUES (CAST(null AS VARCHAR), 23), " +
                        "(CAST(null AS VARCHAR), 24)",
                2);
        assertQuery(
                "SHOW STATS FOR test_stats_for_table_with_nulls_only_partitioning_column",
                "VALUES " +
                        //  column_name | data_size | distinct_values_count | nulls_fraction | row_count | low_value | high_value
                        "('pk1', 0.0, 0.0, 1.0, null, null, null)," +
                        "('val_col', null, 2.0, 0.0, null, 23, 24)," +
                        "(null, null, null, null, 2.0, null, null)");
    }

    @Test
    public void testShowStatsForQueryWithWhereClause()
    {
        assertUpdate(
                "CREATE TABLE show_stats_with_where_clause (pk1, pk2, val_col)" +
                        "WITH(partitioned_by = ARRAY['pk1', 'pk2']) " +
                        "AS VALUES " +
                        "('pk1', 'pk21', 23), " +
                        "(null, 'pk22', 24), " +
                        "('pk1', 'pk23', 25), " +
                        "('pk1', 'pk24', 26) ",
                4);
        assertQuery(
                "SHOW STATS FOR (SELECT * FROM show_stats_with_where_clause WHERE pk1 IS NOT NULL)",
                "VALUES " +
                        //  column_name | data_size | distinct_values_count | nulls_fraction | row_count | low_value | high_value
                        "('pk1', null, 1.0, 0.0, null, null, null)," +
                        "('pk2', null, 3.0, 0.0, null, null, null)," +
                        "('val_col', null, 3.0, 0.0, null, 23, 26)," +
                        "(null, null, null, null, 3.0, null, null)");
    }

    @Test
    public void testShowStatsForAllNullColumn()
    {
        assertUpdate("CREATE TABLE show_stats_with_null AS SELECT CAST(NULL AS INT) col", 1);
        assertQuery(
                "SHOW STATS FOR show_stats_with_null",
                "VALUES " +
                        //  column_name | data_size | distinct_values_count | nulls_fraction | row_count | low_value | high_value
                        "('col', 0.0, 0.0, 1.0, null, null, null)," +
                        "(null, null, null, null, 1.0, null, null)");
    }
}
