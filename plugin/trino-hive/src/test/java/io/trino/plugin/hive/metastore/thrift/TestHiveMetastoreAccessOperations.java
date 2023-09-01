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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.Session;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastore;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastoreUtil;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.io.File;

import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.CREATE_TABLE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_DATABASE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_PARTITIONS_BY_NAMES;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_PARTITION_NAMES_BY_FILTER;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_PARTITION_STATISTICS;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_TABLE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_TABLE_STATISTICS;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.UPDATE_PARTITION_STATISTICS;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.UPDATE_TABLE_STATISTICS;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.TestingSession.testSessionBuilder;

@Test(singleThreaded = true) // metastore invocation counters shares mutable state so can't be run from many threads simultaneously
public class TestHiveMetastoreAccessOperations
        extends AbstractTestQueryFramework
{
    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog("hive")
            .setSchema("test_schema")
            .build();

    private CountingAccessHiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(TEST_SESSION).build();

        File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive").toFile();
        metastore = new CountingAccessHiveMetastore(createTestingFileHiveMetastore(baseDir));

        queryRunner.installPlugin(new TestingHivePlugin(metastore));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.of());

        queryRunner.execute("CREATE SCHEMA test_schema");
        return queryRunner;
    }

    @Test
    public void testUse()
    {
        assertMetastoreInvocations("USE " + getSession().getSchema().orElseThrow(),
                ImmutableMultiset.builder()
                        .add(GET_DATABASE)
                        .build());
    }

    @Test
    public void testCreateTable()
    {
        assertMetastoreInvocations("CREATE TABLE test_create(id VARCHAR, age INT)",
                ImmutableMultiset.builder()
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
                ImmutableMultiset.builder()
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
                ImmutableMultiset.builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testSelectPartitionedTable()
    {
        assertUpdate("CREATE TABLE test_select_partition WITH (partitioned_by = ARRAY['part']) AS SELECT 1 AS data, 10 AS part", 1);

        assertMetastoreInvocations("SELECT * FROM test_select_partition",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 2)
                        .add(GET_PARTITION_NAMES_BY_FILTER)
                        .add(GET_PARTITIONS_BY_NAMES)
                        .build());

        assertUpdate("INSERT INTO test_select_partition SELECT 2 AS data, 20 AS part", 1);
        assertMetastoreInvocations("SELECT * FROM test_select_partition",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 2)
                        .add(GET_PARTITION_NAMES_BY_FILTER)
                        .add(GET_PARTITIONS_BY_NAMES)
                        .build());

        // Specify a specific partition
        assertMetastoreInvocations("SELECT * FROM test_select_partition WHERE part = 10",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 2)
                        .add(GET_PARTITION_NAMES_BY_FILTER)
                        .add(GET_PARTITIONS_BY_NAMES)
                        .build());
    }

    @Test
    public void testSelectWithFilter()
    {
        assertUpdate("CREATE TABLE test_select_from_where AS SELECT 2 AS age", 1);

        assertMetastoreInvocations("SELECT * FROM test_select_from_where WHERE age = 2",
                ImmutableMultiset.builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testSelectFromView()
    {
        assertUpdate("CREATE TABLE test_select_view_table(id VARCHAR, age INT)");
        assertUpdate("CREATE VIEW test_select_view_view AS SELECT id, age FROM test_select_view_table");

        assertMetastoreInvocations("SELECT * FROM test_select_view_view",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
    }

    @Test
    public void testSelectFromViewWithFilter()
    {
        assertUpdate("CREATE TABLE test_select_view_where_table AS SELECT 2 AS age", 1);
        assertUpdate("CREATE VIEW test_select_view_where_view AS SELECT age FROM test_select_view_where_table");

        assertMetastoreInvocations("SELECT * FROM test_select_view_where_view WHERE age = 2",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
    }

    @Test
    public void testJoin()
    {
        assertUpdate("CREATE TABLE test_join_t1 AS SELECT 2 AS age, 'id1' AS id", 1);
        assertUpdate("CREATE TABLE test_join_t2 AS SELECT 'name1' AS name, 'id1' AS id", 1);

        assertMetastoreInvocations("SELECT name, age FROM test_join_t1 JOIN test_join_t2 ON test_join_t2.id = test_join_t1.id",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 2)
                        .addCopies(GET_TABLE_STATISTICS, 2)
                        .build());
    }

    @Test
    public void testSelfJoin()
    {
        assertUpdate("CREATE TABLE test_self_join_table AS SELECT 2 AS age, 0 parent, 3 AS id", 1);

        assertMetastoreInvocations("SELECT child.age, parent.age FROM test_self_join_table child JOIN test_self_join_table parent ON child.parent = parent.id",
                ImmutableMultiset.builder()
                        .add(GET_TABLE)
                        .add(GET_TABLE_STATISTICS)
                        .build());
    }

    @Test
    public void testExplainSelect()
    {
        assertUpdate("CREATE TABLE test_explain AS SELECT 2 AS age", 1);

        assertMetastoreInvocations("EXPLAIN SELECT * FROM test_explain",
                ImmutableMultiset.builder()
                        .add(GET_TABLE)
                        .add(GET_TABLE_STATISTICS)
                        .build());
    }

    @Test
    public void testDescribe()
    {
        assertUpdate("CREATE TABLE test_describe(id VARCHAR, age INT)");

        assertMetastoreInvocations("DESCRIBE test_describe",
                ImmutableMultiset.builder()
                        .add(GET_DATABASE)
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testShowStatsForTable()
    {
        assertUpdate("CREATE TABLE test_show_stats AS SELECT 2 AS age", 1);

        assertMetastoreInvocations("SHOW STATS FOR test_show_stats",
                ImmutableMultiset.builder()
                        .add(GET_TABLE)
                        .add(GET_TABLE_STATISTICS)
                        .build());
    }

    @Test
    public void testShowStatsForTableWithFilter()
    {
        assertUpdate("CREATE TABLE test_show_stats_with_filter AS SELECT 2 AS age", 1);

        assertMetastoreInvocations("SHOW STATS FOR (SELECT * FROM test_show_stats_with_filter where age >= 2)",
                ImmutableMultiset.builder()
                        .add(GET_TABLE)
                        .add(GET_TABLE_STATISTICS)
                        .build());
    }

    @Test
    public void testAnalyze()
    {
        assertUpdate("CREATE TABLE test_analyze AS SELECT 2 AS age", 1);

        assertMetastoreInvocations("ANALYZE test_analyze",
                ImmutableMultiset.builder()
                        .add(GET_TABLE)
                        .add(UPDATE_TABLE_STATISTICS)
                        .build());
    }

    @Test
    public void testAnalyzePartitionedTable()
    {
        assertUpdate("CREATE TABLE test_analyze_partition WITH (partitioned_by = ARRAY['part']) AS SELECT 1 AS data, 10 AS part", 1);

        assertMetastoreInvocations("ANALYZE test_analyze_partition",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 2)
                        .add(GET_PARTITION_NAMES_BY_FILTER)
                        .add(GET_PARTITIONS_BY_NAMES)
                        .add(GET_PARTITION_STATISTICS)
                        .add(UPDATE_PARTITION_STATISTICS)
                        .build());

        assertUpdate("INSERT INTO test_analyze_partition SELECT 2 AS data, 20 AS part", 1);

        assertMetastoreInvocations("ANALYZE test_analyze_partition",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 2)
                        .add(GET_PARTITION_NAMES_BY_FILTER)
                        .add(GET_PARTITIONS_BY_NAMES)
                        .add(GET_PARTITION_STATISTICS)
                        .add(UPDATE_PARTITION_STATISTICS)
                        .build());
    }

    @Test
    public void testDropStats()
    {
        assertUpdate("CREATE TABLE drop_stats AS SELECT 2 AS age", 1);

        assertMetastoreInvocations("CALL system.drop_stats('test_schema', 'drop_stats')",
                ImmutableMultiset.builder()
                        .add(GET_TABLE)
                        .add(UPDATE_TABLE_STATISTICS)
                        .build());
    }

    @Test
    public void testDropStatsPartitionedTable()
    {
        assertUpdate("CREATE TABLE drop_stats_partition WITH (partitioned_by = ARRAY['part']) AS SELECT 1 AS data, 10 AS part", 1);

        assertMetastoreInvocations("CALL system.drop_stats('test_schema', 'drop_stats_partition')",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 2)
                        .add(GET_PARTITION_NAMES_BY_FILTER)
                        .add(UPDATE_PARTITION_STATISTICS)
                        .build());

        assertUpdate("INSERT INTO drop_stats_partition SELECT 2 AS data, 20 AS part", 1);

        assertMetastoreInvocations("CALL system.drop_stats('test_schema', 'drop_stats_partition')",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 2)
                        .add(GET_PARTITION_NAMES_BY_FILTER)
                        .addCopies(UPDATE_PARTITION_STATISTICS, 2)
                        .build());
    }

    private void assertMetastoreInvocations(@Language("SQL") String query, Multiset<?> expectedInvocations)
    {
        CountingAccessHiveMetastoreUtil.assertMetastoreInvocations(metastore, getQueryRunner(), getQueryRunner().getDefaultSession(), query, expectedInvocations);
    }
}
