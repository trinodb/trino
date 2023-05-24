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
package io.trino.plugin.hive;

import io.airlift.units.Duration;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.s3.S3HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestHiveAnalyzeCorruptStatistics
        extends AbstractTestQueryFramework
{
    private HiveMinioDataLake hiveMinioDataLake;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake("test-analyze"));
        hiveMinioDataLake.start();

        return S3HiveQueryRunner.builder(hiveMinioDataLake)
                // Increase timeout because drop_stats doesn't finish with in the default timeout
                .setThriftMetastoreTimeout(new Duration(5, MINUTES))
                .build();
    }

    @Test
    public void testAnalyzeCorruptColumnStatisticsOnEmptyTable()
    {
        String tableName = "test_analyze_corrupt_column_statistics_" + randomNameSuffix();

        // Concurrent ANALYZE statements generate duplicated rows in Thrift metastore's TAB_COL_STATS table when column statistics is empty
        prepareBrokenColumnStatisticsTable(tableName);

        // SHOW STATS should succeed even when the column statistics is broken
        assertQuerySucceeds("SHOW STATS FOR " + tableName);

        // ANALYZE and drop_stats are unsupported for tables having broken column statistics
        assertThatThrownBy(() -> query("ANALYZE " + tableName))
                .hasMessage("%s: Socket is closed by peer.", hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                .hasStackTraceContaining("ThriftHiveMetastore.setTableColumnStatistics");

        assertThatThrownBy(() -> query("CALL system.drop_stats('tpch', '" + tableName + "')"))
                .hasMessageContaining("The query returned more than one instance BUT either unique is set to true or only aggregates are to be returned, so should have returned one result maximum");

        assertUpdate("DROP TABLE " + tableName);
    }

    private void prepareBrokenColumnStatisticsTable(String tableName)
    {
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 col", 1);

        // Insert duplicated row to simulate broken column statistics status https://github.com/trinodb/trino/issues/13787
        assertEquals(onMetastore("SELECT COUNT(1) FROM TAB_COL_STATS WHERE db_name = 'tpch' AND table_name = '" + tableName + "'"), "1");
        onMetastore("INSERT INTO TAB_COL_STATS  " +
                "SELECT cs_id + 1, db_name, table_name, column_name, column_type, tbl_id, long_low_value, long_high_value, double_high_value, double_low_value, big_decimal_low_value, big_decimal_high_value, num_nulls, num_distincts, avg_col_len, max_col_len, num_trues, num_falses, last_analyzed " +
                "FROM TAB_COL_STATS WHERE db_name = 'tpch' AND table_name = '" + tableName + "'");
        assertEquals(onMetastore("SELECT COUNT(1) FROM TAB_COL_STATS WHERE db_name = 'tpch' AND table_name = '" + tableName + "'"), "2");
    }

    @Test
    public void testAnalyzeCorruptPartitionStatisticsOnEmptyTable()
    {
        String tableName = "test_analyze_corrupt_partition_statistics_" + randomNameSuffix();

        // Concurrent ANALYZE statements generate duplicated rows in Thrift metastore's PART_COL_STATS table when partition statistics is empty
        prepareBrokenPartitionStatisticsTable(tableName);

        // SHOW STATS should succeed even when the partition statistics is broken
        assertQuerySucceeds("SHOW STATS FOR " + tableName);

        // ANALYZE succeeds for a partitioned table with corrupt stats unlike a non-partitioned table with corrupt stats
        assertUpdate("ANALYZE " + tableName, 1);
        assertThatThrownBy(() -> query("CALL system.drop_stats('tpch', '" + tableName + "')"))
                .hasMessageContaining("The query returned more than one instance BUT either unique is set to true or only aggregates are to be returned, so should have returned one result maximum");

        assertUpdate("DROP TABLE " + tableName);
    }

    private void prepareBrokenPartitionStatisticsTable(String tableName)
    {
        assertUpdate("CREATE TABLE " + tableName + " WITH(partitioned_by = ARRAY['part']) AS SELECT 1 col, 'test_partition' part", 1);

        // Insert duplicated row to simulate broken partition statistics status https://github.com/trinodb/trino/issues/13787
        assertEquals(onMetastore("SELECT COUNT(1) FROM PART_COL_STATS WHERE db_name = 'tpch' AND table_name = '" + tableName + "'"), "1");
        onMetastore("INSERT INTO PART_COL_STATS  " +
                "SELECT cs_id + 1, db_name, table_name, partition_name, column_name, column_type, part_id, long_low_value, long_high_value, double_high_value, double_low_value, big_decimal_low_value, big_decimal_high_value, num_nulls, num_distincts, avg_col_len, max_col_len, num_trues, num_falses, last_analyzed " +
                "FROM PART_COL_STATS WHERE db_name = 'tpch' AND table_name = '" + tableName + "'");
        assertEquals(onMetastore("SELECT COUNT(1) FROM PART_COL_STATS WHERE db_name = 'tpch' AND table_name = '" + tableName + "'"), "2");
    }

    private String onMetastore(@Language("SQL") String sql)
    {
        return hiveMinioDataLake.getHiveHadoop().runOnMetastore(sql);
    }
}
