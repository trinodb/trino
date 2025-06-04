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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.Session;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.metastore.MetastoreMethod;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.plugin.hive.metastore.MetastoreInvocations.assertMetastoreInvocationsForQuery;
import static io.trino.plugin.hive.metastore.MetastoreMethod.CREATE_TABLE;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_DATABASE;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_PARTITIONS_BY_NAMES;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_PARTITION_COLUMN_STATISTICS;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_PARTITION_NAMES_BY_FILTER;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_TABLE;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_TABLE_COLUMN_STATISTICS;
import static io.trino.plugin.hive.metastore.MetastoreMethod.UPDATE_PARTITION_STATISTICS;
import static io.trino.plugin.hive.metastore.MetastoreMethod.UPDATE_TABLE_STATISTICS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)// metastore invocation counters shares mutable state so can't be run from many threads simultaneously
public class TestHiveMetastoreAccessOperations
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .addHiveProperty("hive.dynamic-filtering.wait-timeout", "1h")
                .build();
    }

    @Test
    public void testUse()
    {
        assertMetastoreInvocations("USE " + getSession().getSchema().orElseThrow(),
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .build());
    }

    @Test
    public void testCreateTable()
    {
        assertMetastoreInvocations("CREATE TABLE test_create(id VARCHAR, age INT)",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(CREATE_TABLE)
                        .add(GET_DATABASE)
                        .add(GET_TABLE)
                        .add(UPDATE_TABLE_STATISTICS)
                        .build());
    }

    @Test
    public void testCreateTableAsSelect()
    {
        assertMetastoreInvocations("CREATE TABLE test_ctas AS SELECT 1 AS age",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(CREATE_TABLE)
                        .add(GET_TABLE)
                        .add(UPDATE_TABLE_STATISTICS)
                        .build());
    }

    @Test
    public void testSelect()
    {
        assertUpdate("CREATE TABLE test_select_from(id VARCHAR, age INT)");

        assertMetastoreInvocations("SELECT * FROM test_select_from",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testSelectPartitionedTable()
    {
        assertUpdate("CREATE TABLE test_select_partition WITH (partitioned_by = ARRAY['part']) AS SELECT 1 AS data, 10 AS part", 1);

        assertMetastoreInvocations("SELECT * FROM test_select_partition",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .add(GET_PARTITION_NAMES_BY_FILTER)
                        .add(GET_PARTITIONS_BY_NAMES)
                        .build());

        assertUpdate("INSERT INTO test_select_partition SELECT 2 AS data, 20 AS part", 1);
        assertMetastoreInvocations("SELECT * FROM test_select_partition",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .add(GET_PARTITION_NAMES_BY_FILTER)
                        .add(GET_PARTITIONS_BY_NAMES)
                        .build());

        // Specify a specific partition
        assertMetastoreInvocations("SELECT * FROM test_select_partition WHERE part = 10",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .add(GET_PARTITION_NAMES_BY_FILTER)
                        .add(GET_PARTITIONS_BY_NAMES)
                        .build());
    }

    @Test
    public void testSelectWithFilter()
    {
        assertUpdate("CREATE TABLE test_select_from_where AS SELECT 2 AS age", 1);

        assertMetastoreInvocations("SELECT * FROM test_select_from_where WHERE age = 2",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testSelectFromView()
    {
        assertUpdate("CREATE TABLE test_select_view_table(id VARCHAR, age INT)");
        assertUpdate("CREATE VIEW test_select_view_view AS SELECT id, age FROM test_select_view_table");

        assertMetastoreInvocations("SELECT * FROM test_select_view_view",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
    }

    @Test
    public void testSelectFromViewWithFilter()
    {
        assertUpdate("CREATE TABLE test_select_view_where_table AS SELECT 2 AS age", 1);
        assertUpdate("CREATE VIEW test_select_view_where_view AS SELECT age FROM test_select_view_where_table");

        assertMetastoreInvocations("SELECT * FROM test_select_view_where_view WHERE age = 2",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
    }

    @Test
    public void testJoin()
    {
        assertUpdate("CREATE TABLE test_join_t1 AS SELECT 2 AS age, 'id1' AS id", 1);
        assertUpdate("CREATE TABLE test_join_t2 AS SELECT 'name1' AS name, 'id1' AS id", 1);

        assertMetastoreInvocations("SELECT name, age FROM test_join_t1 JOIN test_join_t2 ON test_join_t2.id = test_join_t1.id",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 2)
                        .addCopies(GET_TABLE_COLUMN_STATISTICS, 2)
                        .build());
    }

    @Test
    public void testSelfJoin()
    {
        assertUpdate("CREATE TABLE test_self_join_table AS SELECT 2 AS age, 0 parent, 3 AS id", 1);

        assertMetastoreInvocations("SELECT child.age, parent.age FROM test_self_join_table child JOIN test_self_join_table parent ON child.parent = parent.id",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .addCopies(GET_TABLE_COLUMN_STATISTICS, 2)
                        .build());
    }

    @Test
    public void testDynamicPartitionPruning()
    {
        assertUpdate(
                "CREATE TABLE test_dynamic_partition_pruning_table WITH (partitioned_by=ARRAY['suppkey']) AS " +
                        "SELECT orderkey, partkey, suppkey FROM tpch.tiny.lineitem",
                60175);

        Session session = Session.builder(getSession())
                // Avoid caching all partitions metadata during planning through getTableStatistics
                // and force partitions to be fetched during splits generation
                .setCatalogSessionProperty("hive", "partition_statistics_sample_size", "10")
                .build();
        @Language("SQL") String sql = "SELECT * FROM test_dynamic_partition_pruning_table l JOIN tpch.tiny.supplier s ON l.suppkey = s.suppkey " +
                "AND s.name = 'Supplier#000000001'";

        assertMetastoreInvocations(
                Session.builder(session)
                        .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                        .build(),
                sql,
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .addCopies(GET_PARTITIONS_BY_NAMES, 5)
                        .add(GET_PARTITION_COLUMN_STATISTICS)
                        .addCopies(GET_PARTITION_NAMES_BY_FILTER, 2)
                        .build());

        assertQuerySucceeds("CALL system.flush_metadata_cache()");
        assertMetastoreInvocations(
                session,
                sql,
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .add(GET_PARTITIONS_BY_NAMES)
                        .add(GET_PARTITION_COLUMN_STATISTICS)
                        .addCopies(GET_PARTITION_NAMES_BY_FILTER, 2)
                        .build());
    }

    @Test
    public void testExplainSelect()
    {
        assertUpdate("CREATE TABLE test_explain AS SELECT 2 AS age", 1);

        assertMetastoreInvocations("EXPLAIN SELECT * FROM test_explain",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .add(GET_TABLE_COLUMN_STATISTICS)
                        .build());
    }

    @Test
    public void testDescribe()
    {
        assertUpdate("CREATE TABLE test_describe(id VARCHAR, age INT)");

        assertMetastoreInvocations("DESCRIBE test_describe",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testShowStatsForTable()
    {
        assertUpdate("CREATE TABLE test_show_stats AS SELECT 2 AS age", 1);

        assertMetastoreInvocations("SHOW STATS FOR test_show_stats",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .add(GET_TABLE_COLUMN_STATISTICS)
                        .build());
    }

    @Test
    public void testShowStatsForTableWithFilter()
    {
        assertUpdate("CREATE TABLE test_show_stats_with_filter AS SELECT 2 AS age", 1);

        assertMetastoreInvocations("SHOW STATS FOR (SELECT * FROM test_show_stats_with_filter where age >= 2)",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .add(GET_TABLE_COLUMN_STATISTICS)
                        .build());
    }

    @Test
    public void testAnalyze()
    {
        assertUpdate("CREATE TABLE test_analyze AS SELECT 2 AS age", 1);

        assertMetastoreInvocations("ANALYZE test_analyze",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .add(UPDATE_TABLE_STATISTICS)
                        .build());
    }

    @Test
    public void testAnalyzePartitionedTable()
    {
        assertUpdate("CREATE TABLE test_analyze_partition WITH (partitioned_by = ARRAY['part']) AS SELECT 1 AS data, 10 AS part", 1);

        assertMetastoreInvocations("ANALYZE test_analyze_partition",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 1)
                        .add(GET_PARTITION_NAMES_BY_FILTER)
                        .add(GET_PARTITIONS_BY_NAMES)
                        .add(GET_PARTITION_COLUMN_STATISTICS)
                        .add(UPDATE_PARTITION_STATISTICS)
                        .build());

        assertUpdate("INSERT INTO test_analyze_partition SELECT 2 AS data, 20 AS part", 1);

        assertMetastoreInvocations("ANALYZE test_analyze_partition",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .add(GET_PARTITION_NAMES_BY_FILTER)
                        .add(GET_PARTITIONS_BY_NAMES)
                        .add(GET_PARTITION_COLUMN_STATISTICS)
                        .add(UPDATE_PARTITION_STATISTICS)
                        .build());
    }

    @Test
    public void testDropStats()
    {
        assertUpdate("CREATE TABLE drop_stats AS SELECT 2 AS age", 1);

        assertMetastoreInvocations("CALL system.drop_stats(CURRENT_SCHEMA, 'drop_stats')",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .add(UPDATE_TABLE_STATISTICS)
                        .build());
    }

    @Test
    public void testDropStatsPartitionedTable()
    {
        assertUpdate("CREATE TABLE drop_stats_partition WITH (partitioned_by = ARRAY['part']) AS SELECT 1 AS data, 10 AS part", 1);

        assertMetastoreInvocations("CALL system.drop_stats(CURRENT_SCHEMA, 'drop_stats_partition')",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .add(GET_PARTITION_NAMES_BY_FILTER)
                        .add(UPDATE_PARTITION_STATISTICS)
                        .build());

        assertUpdate("INSERT INTO drop_stats_partition SELECT 2 AS data, 20 AS part", 1);

        assertMetastoreInvocations("CALL system.drop_stats(CURRENT_SCHEMA, 'drop_stats_partition')",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .add(GET_PARTITION_NAMES_BY_FILTER)
                        .addCopies(UPDATE_PARTITION_STATISTICS, 2)
                        .build());
    }

    private void assertMetastoreInvocations(@Language("SQL") String query, Multiset<MetastoreMethod> expectedInvocations)
    {
        assertMetastoreInvocations(getSession(), query, expectedInvocations);
    }

    private void assertMetastoreInvocations(Session session, @Language("SQL") String query, Multiset<MetastoreMethod> expectedInvocations)
    {
        assertMetastoreInvocationsForQuery(getDistributedQueryRunner(), session, query, expectedInvocations);
    }
}
