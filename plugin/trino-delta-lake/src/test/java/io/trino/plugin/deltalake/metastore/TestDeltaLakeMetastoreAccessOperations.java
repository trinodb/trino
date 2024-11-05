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
package io.trino.plugin.deltalake.metastore;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.Session;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.plugin.deltalake.DeltaLakeQueryRunner;
import io.trino.plugin.deltalake.TestingDeltaLakeUtils;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.MetastoreMethod;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.hive.metastore.MetastoreInvocations.filterInvocations;
import static io.trino.plugin.hive.metastore.MetastoreMethod.CREATE_TABLE;
import static io.trino.plugin.hive.metastore.MetastoreMethod.DROP_TABLE;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_ALL_DATABASES;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_DATABASE;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_TABLE;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_TABLES;
import static io.trino.plugin.hive.metastore.MetastoreMethod.REPLACE_TABLE;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // metastore invocation counters shares mutable state so can't be run from many threads simultaneously
public class TestDeltaLakeMetastoreAccessOperations
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private DeltaLakeTableMetadataScheduler metadataScheduler;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DeltaLakeQueryRunner.builder()
                .addDeltaProperty("delta.register-table-procedure.enabled", "true")
                .addDeltaProperty("delta.metastore.store-table-metadata", "true")
                .addDeltaProperty("delta.metastore.store-table-metadata-threads", "0") // Use the same thread to make the test deterministic
                .addDeltaProperty("delta.metastore.store-table-metadata-interval", "30m") // Use a large interval to avoid interference with the test
                .build();

        metastore = TestingDeltaLakeUtils.getConnectorService(queryRunner, HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
        metadataScheduler = TestingDeltaLakeUtils.getConnectorService(queryRunner, DeltaLakeTableMetadataScheduler.class);

        return queryRunner;
    }

    @Test
    public void testCreateTable()
    {
        assertMetastoreInvocations("CREATE TABLE test_create (id VARCHAR, age INT)",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(CREATE_TABLE)
                        .add(GET_DATABASE)
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testCreateOrReplaceTable()
    {
        assertMetastoreInvocations("CREATE OR REPLACE TABLE test_create_or_replace (id VARCHAR, age INT)",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(GET_TABLE)
                        .add(CREATE_TABLE)
                        .build());
        assertMetastoreInvocations("CREATE OR REPLACE TABLE test_create_or_replace (id VARCHAR, age INT)",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(GET_TABLE)
                        .add(REPLACE_TABLE)
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
                        .build());
    }

    @Test
    public void testCreateOrReplaceTableAsSelect()
    {
        assertMetastoreInvocations(
                "CREATE OR REPLACE TABLE test_cortas AS SELECT 1 AS age",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(GET_TABLE)
                        .add(CREATE_TABLE)
                        .build());

        assertMetastoreInvocations(
                "CREATE OR REPLACE TABLE test_cortas AS SELECT 1 AS age",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(GET_TABLE)
                        .add(REPLACE_TABLE)
                        .build());
    }

    @Test
    public void testSelect()
    {
        assertUpdate("CREATE TABLE test_select_from (id VARCHAR, age INT)");

        assertMetastoreInvocations("SELECT * FROM test_select_from",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testSelectWithFilter()
    {
        assertUpdate("CREATE TABLE test_select_from_where AS SELECT 2 as age", 1);

        assertMetastoreInvocations("SELECT * FROM test_select_from_where WHERE age = 2",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testSelectFromView()
    {
        assertUpdate("CREATE TABLE test_select_view_table (id VARCHAR, age INT)");
        assertUpdate("CREATE VIEW test_select_view_view AS SELECT id, age FROM test_select_view_table");

        assertMetastoreInvocations("SELECT * FROM test_select_view_view",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
    }

    @Test
    public void testSelectFromViewWithFilter()
    {
        assertUpdate("CREATE TABLE test_select_view_where_table AS SELECT 2 as age", 1);
        assertUpdate("CREATE VIEW test_select_view_where_view AS SELECT age FROM test_select_view_where_table");

        assertMetastoreInvocations("SELECT * FROM test_select_view_where_view WHERE age = 2",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
    }

    @Test
    public void testSelectFromMaterializedView()
    {
        assertUpdate("CREATE TABLE test_select_mview_table (id VARCHAR, age INT)");
        assertQueryFails(
                "CREATE MATERIALIZED VIEW test_select_mview_view AS SELECT id, age FROM test_select_mview_table",
                "This connector does not support creating materialized views");
    }

    @Test
    public void testSelectFromMaterializedViewWithFilter()
    {
        assertUpdate("CREATE TABLE test_select_mview_where_table AS SELECT 2 as age", 1);
        assertQueryFails(
                "CREATE MATERIALIZED VIEW test_select_mview_where_view AS SELECT age FROM test_select_mview_where_table",
                "This connector does not support creating materialized views");
    }

    @Test
    public void testRefreshMaterializedView()
    {
        assertUpdate("CREATE TABLE test_refresh_mview_table (id VARCHAR, age INT)");
        assertQueryFails(
                "CREATE MATERIALIZED VIEW test_refresh_mview_view AS SELECT id, age FROM test_refresh_mview_table",
                "This connector does not support creating materialized views");
    }

    @Test
    public void testJoin()
    {
        assertUpdate("CREATE TABLE test_join_t1 AS SELECT 2 as age, 'id1' AS id", 1);
        assertUpdate("CREATE TABLE test_join_t2 AS SELECT 'name1' as name, 'id1' AS id", 1);

        assertMetastoreInvocations("SELECT name, age FROM test_join_t1 JOIN test_join_t2 ON test_join_t2.id = test_join_t1.id",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
    }

    @Test
    public void testSelfJoin()
    {
        assertUpdate("CREATE TABLE test_self_join_table AS SELECT 2 as age, 0 parent, 3 AS id", 1);

        assertMetastoreInvocations("SELECT child.age, parent.age FROM test_self_join_table child JOIN test_self_join_table parent ON child.parent = parent.id",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testExplainSelect()
    {
        assertUpdate("CREATE TABLE test_explain AS SELECT 2 as age", 1);

        assertMetastoreInvocations("EXPLAIN SELECT * FROM test_explain",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testShowStatsForTable()
    {
        assertUpdate("CREATE TABLE test_show_stats AS SELECT 2 as age", 1);

        assertMetastoreInvocations("SHOW STATS FOR test_show_stats",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testShowStatsForTableWithFilter()
    {
        assertUpdate("CREATE TABLE test_show_stats_with_filter AS SELECT 2 as age", 1);

        assertMetastoreInvocations("SHOW STATS FOR (SELECT * FROM test_show_stats_with_filter where age >= 2)",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testDropTable()
    {
        assertUpdate("CREATE TABLE test_drop_table AS SELECT 20050910 as a_number", 1);

        assertMetastoreInvocations("DROP TABLE test_drop_table",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .add(DROP_TABLE)
                        .build());
    }

    @Test
    public void testShowTables()
    {
        assertMetastoreInvocations("SHOW TABLES",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_TABLES)
                        .build());
    }

    @Test
    public void testSelectWithoutMetadataInMetastore()
    {
        assertUpdate("CREATE TABLE test_select_without_cache (id VARCHAR, age INT)");

        removeMetadataCachingPropertiesFromMetastore("test_select_without_cache");
        assertMetastoreInvocations(
                getSession(),
                "SELECT * FROM test_select_without_cache",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .build(),
                asyncInvocations(true)); // async invocations happen because the table metadata is not stored
        assertMetastoreInvocations("SELECT * FROM test_select_without_cache",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testUnionWithoutMetadataInMetastore()
    {
        assertUpdate("CREATE TABLE test_union_without_cache (id VARCHAR, age INT)");
        assertMetastoreInvocations("SELECT * FROM test_union_without_cache UNION ALL SELECT * FROM test_union_without_cache",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .build());

        removeMetadataCachingPropertiesFromMetastore("test_union_without_cache");
        assertMetastoreInvocations(
                getSession(),
                "SELECT * FROM test_union_without_cache UNION ALL SELECT * FROM test_union_without_cache",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .build(),
                asyncInvocations(true)); // async invocations happen because the table metadata is not stored
        assertMetastoreInvocations("SELECT * FROM test_union_without_cache UNION ALL SELECT * FROM test_union_without_cache",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testSelectVersionedWithoutMetadataInMetastore()
    {
        assertUpdate("CREATE TABLE test_select_versioned_without_cache AS SELECT 2 as age", 1);

        // Time travel query should not cache the metadata because the definition might be different from the latest verion
        removeMetadataCachingPropertiesFromMetastore("test_select_versioned_without_cache");
        assertMetastoreInvocations("SELECT * FROM test_select_versioned_without_cache FOR VERSION AS OF 0",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testStoreMetastoreCreateOrReplaceTable()
    {
        testStoreMetastoreCreateOrReplaceTable(true);
        testStoreMetastoreCreateOrReplaceTable(false);
    }

    private void testStoreMetastoreCreateOrReplaceTable(boolean storeTableMetadata)
    {
        Session session = sessionWithStoreTableMetadata(storeTableMetadata);

        assertMetastoreInvocations(session, "CREATE OR REPLACE TABLE test_create_or_replace_without_cache (id VARCHAR, age INT)",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(GET_TABLE)
                        .add(storeTableMetadata ? CREATE_TABLE : REPLACE_TABLE)
                        .build());
        removeMetadataCachingPropertiesFromMetastore("test_create_or_replace_without_cache");
        assertMetastoreInvocations(
                session,
                "CREATE OR REPLACE TABLE test_create_or_replace_without_cache (id VARCHAR, age INT)",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(GET_TABLE)
                        .add(REPLACE_TABLE)
                        .build(),
                asyncInvocations(storeTableMetadata));
    }

    @Test
    public void testStoreMetastoreCreateTableOrReplaceTableAsSelect()
    {
        testStoreMetastoreCreateTableOrReplaceTableAsSelect(true);
        testStoreMetastoreCreateTableOrReplaceTableAsSelect(false);
    }

    private void testStoreMetastoreCreateTableOrReplaceTableAsSelect(boolean storeTableMetadata)
    {
        Session session = sessionWithStoreTableMetadata(storeTableMetadata);

        assertMetastoreInvocations(session, "CREATE OR REPLACE TABLE test_ctas_without_cache AS SELECT 1 AS age",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(storeTableMetadata ? CREATE_TABLE : REPLACE_TABLE)
                        .add(GET_TABLE)
                        .build());
        removeMetadataCachingPropertiesFromMetastore("test_ctas_without_cache");
        assertMetastoreInvocations(session, "CREATE OR REPLACE TABLE test_ctas_without_cache AS SELECT 1 AS age", ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(GET_TABLE)
                        .add(REPLACE_TABLE)
                        .build(),
                asyncInvocations(storeTableMetadata));
    }

    @Test
    public void testStoreMetastoreCommentTable()
    {
        testStoreMetastoreCommentTable(true);
        testStoreMetastoreCommentTable(false);
    }

    private void testStoreMetastoreCommentTable(boolean storeTableMetadata)
    {
        Session session = sessionWithStoreTableMetadata(storeTableMetadata);
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_cache_metastore", "(col int)")) {
            assertMetastoreInvocations(session, "COMMENT ON TABLE " + table.getName() + " IS 'test comment'", ImmutableMultiset.of(GET_TABLE), asyncInvocations(storeTableMetadata));
        }
    }

    @Test
    public void testStoreMetastoreCommentColumn()
    {
        testStoreMetastoreCommentColumn(true);
        testStoreMetastoreCommentColumn(false);
    }

    private void testStoreMetastoreCommentColumn(boolean storeTableMetadata)
    {
        Session session = sessionWithStoreTableMetadata(storeTableMetadata);
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_cache_metastore", "(col int COMMENT 'test comment')")) {
            assertMetastoreInvocations(session, "COMMENT ON COLUMN " + table.getName() + ".col IS 'new test comment'", ImmutableMultiset.of(GET_TABLE), asyncInvocations(storeTableMetadata));
        }
    }

    @Test
    public void testStoreMetastoreAlterColumn()
    {
        testStoreMetastoreAlterColumn(true);
        testStoreMetastoreAlterColumn(false);
    }

    private void testStoreMetastoreAlterColumn(boolean storeTableMetadata)
    {
        Session session = sessionWithStoreTableMetadata(storeTableMetadata);

        // Use 'name' column mapping mode to allow renaming columns
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_cache_metastore", "(col int NOT NULL) WITH (column_mapping_mode = 'name')")) {
            assertMetastoreInvocations(session, "ALTER TABLE " + table.getName() + " ALTER COLUMN col DROP NOT NULL", ImmutableMultiset.of(GET_TABLE), asyncInvocations(storeTableMetadata));
            assertMetastoreInvocations(session, "ALTER TABLE " + table.getName() + " ADD COLUMN new_col int COMMENT 'test comment'", ImmutableMultiset.of(GET_TABLE), asyncInvocations(storeTableMetadata));
            assertMetastoreInvocations(session, "ALTER TABLE " + table.getName() + " RENAME COLUMN new_col TO renamed_col", ImmutableMultiset.of(GET_TABLE), asyncInvocations(storeTableMetadata));
            assertMetastoreInvocations(session, "ALTER TABLE " + table.getName() + " DROP COLUMN renamed_col", ImmutableMultiset.of(GET_TABLE), asyncInvocations(storeTableMetadata));
            // Update the following test once the connector supports changing column types
            assertQueryFails(session, "ALTER TABLE " + table.getName() + " ALTER COLUMN col SET DATA TYPE bigint", "This connector does not support setting column types");
        }
    }

    @Test
    public void testStoreMetastoreSetTableProperties()
    {
        testStoreMetastoreSetTableProperties(true);
        testStoreMetastoreSetTableProperties(false);
    }

    private void testStoreMetastoreSetTableProperties(boolean storeTableMetadata)
    {
        Session session = sessionWithStoreTableMetadata(storeTableMetadata);
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_cache_metastore", "(col int)")) {
            assertMetastoreInvocations(session, "ALTER TABLE " + table.getName() + " SET PROPERTIES change_data_feed_enabled = true", ImmutableMultiset.of(GET_TABLE), asyncInvocations(storeTableMetadata));
        }
    }

    @Test
    public void testStoreMetastoreOptimize()
    {
        testStoreMetastoreOptimize(true);
        testStoreMetastoreOptimize(false);
    }

    private void testStoreMetastoreOptimize(boolean storeTableMetadata)
    {
        Session session = sessionWithStoreTableMetadata(storeTableMetadata);
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_cache_metastore", "(col int)")) {
            assertMetastoreInvocations(session, "ALTER TABLE " + table.getName() + " EXECUTE optimize", ImmutableMultiset.of(GET_TABLE), asyncInvocations(storeTableMetadata));
        }
    }

    @Test
    public void testStoreMetastoreVacuum()
    {
        testStoreMetastoreVacuum(true);
        testStoreMetastoreVacuum(false);
    }

    private void testStoreMetastoreVacuum(boolean storeTableMetadata)
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "store_table_metadata", Boolean.toString(storeTableMetadata))
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "vacuum_min_retention", "0s")
                .build();

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_cache_metastore", "AS SELECT 1 a")) {
            assertUpdate("UPDATE " + table.getName() + " SET a = 2", 1);
            assertMetastoreInvocations(
                    session,
                    "CALL system.vacuum(schema_name => CURRENT_SCHEMA, table_name => '" + table.getName() + "', retention => '0s')",
                    ImmutableMultiset.of(GET_TABLE));
        }
    }

    @Test
    public void testStoreMetastoreRegisterTable()
    {
        testStoreMetastoreRegisterTable(true);
        testStoreMetastoreRegisterTable(false);
    }

    private void testStoreMetastoreRegisterTable(boolean storeTableMetadata)
    {
        Session session = sessionWithStoreTableMetadata(storeTableMetadata);
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_cache_metastore", "(col int) COMMENT 'test comment'")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            String tableLocation = metastore.getTable(TPCH_SCHEMA, table.getName()).orElseThrow().getStorage().getLocation();
            metastore.dropTable(TPCH_SCHEMA, table.getName(), false);

            assertMetastoreInvocations(
                    session,
                    "CALL system.register_table('%s', '%s', '%s')".formatted(TPCH_SCHEMA, table.getName(), tableLocation),
                    ImmutableMultiset.of(GET_DATABASE, CREATE_TABLE));
        }
    }

    @Test
    public void testStoreMetastoreDataManipulation()
    {
        testStoreMetastoreDataManipulation(true);
        testStoreMetastoreDataManipulation(false);
    }

    private void testStoreMetastoreDataManipulation(boolean storeTableMetadata)
    {
        Session session = sessionWithStoreTableMetadata(storeTableMetadata);
        String schemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"col\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}";

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_cache_metastore", "(col int)")) {
            assertThat(metastore.getTable(TPCH_SCHEMA, table.getName()).orElseThrow().getParameters())
                    .contains(entry("trino_last_transaction_version", "0"), entry("trino_metadata_schema_string", schemaString));

            assertMetastoreInvocations(session, "INSERT INTO " + table.getName() + " VALUES 1", ImmutableMultiset.of(GET_TABLE), asyncInvocations(storeTableMetadata));
            assertMetastoreInvocations(session, "UPDATE " + table.getName() + " SET col = 2", ImmutableMultiset.of(GET_TABLE), asyncInvocations(storeTableMetadata));
            assertMetastoreInvocations(session, "MERGE INTO " + table.getName() + " t " +
                            "USING (SELECT * FROM (VALUES 2)) AS s(col) " +
                            "ON (t.col = s.col) " +
                            "WHEN MATCHED THEN UPDATE SET col = 3",
                    ImmutableMultiset.of(GET_TABLE), asyncInvocations(storeTableMetadata));
            assertMetastoreInvocations(session, "DELETE FROM " + table.getName() + " WHERE col = 3", ImmutableMultiset.of(GET_TABLE), asyncInvocations(storeTableMetadata)); // row level delete
            assertMetastoreInvocations(session, "DELETE FROM " + table.getName(), ImmutableMultiset.of(GET_TABLE), asyncInvocations(storeTableMetadata)); // metadata delete
        }
    }

    @Test
    public void testStoreMetastoreTruncateTable()
    {
        testStoreMetastoreTruncateTable(true);
        testStoreMetastoreTruncateTable(false);
    }

    private void testStoreMetastoreTruncateTable(boolean storeTableMetadata)
    {
        Session session = sessionWithStoreTableMetadata(storeTableMetadata);
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_cache_metastore", "AS SELECT 1 col")) {
            assertMetastoreInvocations(session, "TRUNCATE TABLE " + table.getName(), ImmutableMultiset.of(GET_TABLE), asyncInvocations(storeTableMetadata));
        }
    }

    private void removeMetadataCachingPropertiesFromMetastore(String tableName)
    {
        Table table = metastore.getTable(getSession().getSchema().orElseThrow(), tableName).orElseThrow();
        Table newMetastoreTable = Table.builder(table)
                .setParameters(Maps.filterKeys(table.getParameters(), key -> !key.equals("trino_last_transaction_version")))
                .build();
        metastore.replaceTable(table.getDatabaseName(), table.getTableName(), newMetastoreTable, buildInitialPrivilegeSet(table.getOwner().orElseThrow()));
    }

    private Session sessionWithStoreTableMetadata(boolean storeTableMetadata)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "store_table_metadata", Boolean.toString(storeTableMetadata))
                .build();
    }

    private void assertMetastoreInvocations(@Language("SQL") String query, Multiset<MetastoreMethod> expectedInvocations)
    {
        assertMetastoreInvocations(getSession(), query, expectedInvocations, ImmutableMultiset.of());
    }

    private void assertMetastoreInvocations(Session session, @Language("SQL") String query, Multiset<MetastoreMethod> expectedInvocations)
    {
        assertMetastoreInvocations(session, query, expectedInvocations, ImmutableMultiset.of());
    }

    private void assertMetastoreInvocations(Session session, @Language("SQL") String query, Multiset<MetastoreMethod> expectedInvocations, Multiset<MetastoreMethod> asyncInvocations)
    {
        assertUpdate("CALL system.flush_metadata_cache()");

        metadataScheduler.clear();
        assertMetastoreInvocationsForQuery(getDistributedQueryRunner(), session, query, expectedInvocations, () -> metadataScheduler.process(), asyncInvocations);
    }

    private static Multiset<MetastoreMethod> asyncInvocations(boolean storeTableParameter)
    {
        return storeTableParameter ? ImmutableMultiset.of(GET_TABLE, REPLACE_TABLE) : ImmutableMultiset.of();
    }

    private static void assertMetastoreInvocationsForQuery(
            QueryRunner queryRunner,
            Session session,
            @Language("SQL") String query,
            Multiset<MetastoreMethod> expectedInvocations,
            Runnable asyncOperation,
            Multiset<MetastoreMethod> expectedInvocationsAfterAsync)
    {
        queryRunner.execute(session, query);
        List<SpanData> spansBeforeAsync = queryRunner.getSpans();

        asyncOperation.run();
        Set<SpanData> spansAfterAsync = Sets.difference(new HashSet<>(queryRunner.getSpans()), new HashSet<>(spansBeforeAsync));

        Multiset<MetastoreMethod> invocations = filterInvocations(spansBeforeAsync);
        assertMultisetsEqual(invocations, expectedInvocations);

        Multiset<MetastoreMethod> asyncInvocations = filterInvocations(spansAfterAsync.stream().collect(toImmutableList()));
        assertMultisetsEqual(asyncInvocations, expectedInvocationsAfterAsync);
    }
}
