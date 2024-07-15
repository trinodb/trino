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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.Session;
import io.trino.plugin.hive.metastore.MetastoreMethod;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;

import static io.trino.plugin.hive.metastore.MetastoreInvocations.assertMetastoreInvocationsForQuery;
import static io.trino.plugin.hive.metastore.MetastoreMethod.CREATE_TABLE;
import static io.trino.plugin.hive.metastore.MetastoreMethod.DROP_TABLE;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_DATABASE;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_TABLE;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_TABLES;
import static io.trino.plugin.hive.metastore.MetastoreMethod.REPLACE_TABLE;
import static io.trino.plugin.iceberg.IcebergSessionProperties.COLLECT_EXTENDED_STATISTICS_ON_WRITE;
import static io.trino.plugin.iceberg.TableType.DATA;
import static io.trino.plugin.iceberg.TableType.FILES;
import static io.trino.plugin.iceberg.TableType.HISTORY;
import static io.trino.plugin.iceberg.TableType.MANIFESTS;
import static io.trino.plugin.iceberg.TableType.MATERIALIZED_VIEW_STORAGE;
import static io.trino.plugin.iceberg.TableType.METADATA_LOG_ENTRIES;
import static io.trino.plugin.iceberg.TableType.PARTITIONS;
import static io.trino.plugin.iceberg.TableType.PROPERTIES;
import static io.trino.plugin.iceberg.TableType.REFS;
import static io.trino.plugin.iceberg.TableType.SNAPSHOTS;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD) // metastore invocation counters shares mutable state so can't be run from many threads simultaneously
public class TestIcebergMetastoreAccessOperations
        extends AbstractTestQueryFramework
{
    private static final int MAX_PREFIXES_COUNT = 10;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .build();
    }

    @Test
    public void testUse()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        Session session = Session.builder(getSession())
                .setCatalog(Optional.empty())
                .setSchema(Optional.empty())
                .build();
        assertMetastoreInvocations(session, "USE %s.%s".formatted(catalog, schema),
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .build());
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
                        .add(CREATE_TABLE)
                        .add(GET_DATABASE)
                        .add(GET_TABLE)
                        .build());
        assertMetastoreInvocations("CREATE OR REPLACE TABLE test_create_or_replace (id VARCHAR, age INT)",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(REPLACE_TABLE)
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testCreateTableAsSelect()
    {
        assertMetastoreInvocations(
                withStatsOnWrite(getSession(), false),
                "CREATE TABLE test_ctas AS SELECT 1 AS age",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(CREATE_TABLE)
                        .add(GET_TABLE)
                        .build());

        assertMetastoreInvocations(
                withStatsOnWrite(getSession(), true),
                "CREATE TABLE test_ctas_with_stats AS SELECT 1 AS age",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(CREATE_TABLE)
                        .addCopies(GET_TABLE, 4)
                        .add(REPLACE_TABLE)
                        .build());
    }

    @Test
    public void testCreateOrReplaceTableAsSelect()
    {
        assertMetastoreInvocations(
                "CREATE OR REPLACE TABLE test_cortas AS SELECT 1 AS age",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(CREATE_TABLE)
                        .addCopies(GET_TABLE, 4)
                        .add(REPLACE_TABLE)
                        .build());

        assertMetastoreInvocations(
                "CREATE OR REPLACE TABLE test_cortas AS SELECT 1 AS age",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .addCopies(GET_TABLE, 3)
                        .addCopies(REPLACE_TABLE, 2)
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
        assertUpdate("CREATE MATERIALIZED VIEW test_select_mview_view AS SELECT id, age FROM test_select_mview_table");

        assertMetastoreInvocations("SELECT * FROM test_select_mview_view",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
    }

    @Test
    public void testSelectFromMaterializedViewWithFilter()
    {
        assertUpdate("CREATE TABLE test_select_mview_where_table AS SELECT 2 as age", 1);
        assertUpdate("CREATE MATERIALIZED VIEW test_select_mview_where_view AS SELECT age FROM test_select_mview_where_table");

        assertMetastoreInvocations("SELECT * FROM test_select_mview_where_view WHERE age = 2",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
    }

    @Test
    public void testRefreshMaterializedView()
    {
        assertUpdate("CREATE TABLE test_refresh_mview_table (id VARCHAR, age INT)");
        assertUpdate("CREATE MATERIALIZED VIEW test_refresh_mview_view AS SELECT id, age FROM test_refresh_mview_table");

        assertMetastoreInvocations("REFRESH MATERIALIZED VIEW test_refresh_mview_view",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 2)
                        .addCopies(REPLACE_TABLE, 1)
                        .build());
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
    public void testSelectSystemTable()
    {
        assertUpdate("CREATE TABLE test_select_snapshots AS SELECT 2 AS age", 1);

        // select from $history
        assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots$history\"",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 1)
                        .build());

        // select from $metadata_log_entries
        assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots$metadata_log_entries\"",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .build());

        // select from $snapshots
        assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots$snapshots\"",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 1)
                        .build());

        // select from $manifests
        assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots$manifests\"",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 1)
                        .build());

        // select from $partitions
        assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots$partitions\"",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 1)
                        .build());

        // select from $files
        assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots$files\"",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 1)
                        .build());

        // select from $properties
        assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots$properties\"",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 1)
                        .build());

        assertQueryFails("SELECT * FROM \"test_select_snapshots$materialized_view_storage\"",
                "Table 'tpch.test_select_snapshots\\$materialized_view_storage' not found");

        // This test should get updated if a new system table is added.
        assertThat(TableType.values())
                .containsExactly(DATA, HISTORY, METADATA_LOG_ENTRIES, SNAPSHOTS, MANIFESTS, PARTITIONS, FILES, PROPERTIES, REFS, MATERIALIZED_VIEW_STORAGE);
    }

    @Test
    public void testUnregisterTable()
    {
        assertUpdate("CREATE TABLE test_unregister_table AS SELECT 2 as age", 1);

        assertMetastoreInvocations("CALL system.unregister_table(CURRENT_SCHEMA, 'test_unregister_table')",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(GET_TABLE)
                        .add(DROP_TABLE)
                        .build());
    }

    @ParameterizedTest
    @MethodSource("metadataQueriesTestTableCountDataProvider")
    public void testInformationSchemaColumns(int tables)
    {
        String schemaName = "test_i_s_columns_schema" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        Session session = Session.builder(getSession())
                .setSchema(schemaName)
                .build();

        for (int i = 0; i < tables; i++) {
            assertUpdate(session, "CREATE TABLE test_select_i_s_columns" + i + "(id varchar, age integer)");
            // Produce multiple snapshots and metadata files
            assertUpdate(session, "INSERT INTO test_select_i_s_columns" + i + " VALUES ('abc', 11)", 1);
            assertUpdate(session, "INSERT INTO test_select_i_s_columns" + i + " VALUES ('xyz', 12)", 1);

            assertUpdate(session, "CREATE TABLE test_other_select_i_s_columns" + i + "(id varchar, age integer)"); // won't match the filter
        }

        // Bulk retrieval
        assertMetastoreInvocations(session, "SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name LIKE 'test_select_i_s_columns%'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLES)
                        .addCopies(GET_TABLE, tables * 2)
                        .build());

        // Pointed lookup
        assertMetastoreInvocations(session, "SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name = 'test_select_i_s_columns0'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .build());

        // Pointed lookup via DESCRIBE (which does some additional things before delegating to information_schema.columns)
        assertMetastoreInvocations(session, "DESCRIBE test_select_i_s_columns0",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(GET_TABLE)
                        .build());

        for (int i = 0; i < tables; i++) {
            assertUpdate(session, "DROP TABLE test_select_i_s_columns" + i);
            assertUpdate(session, "DROP TABLE test_other_select_i_s_columns" + i);
        }
    }

    @ParameterizedTest
    @MethodSource("metadataQueriesTestTableCountDataProvider")
    public void testSystemMetadataTableComments(int tables)
    {
        String schemaName = "test_s_m_table_comments" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        Session session = Session.builder(getSession())
                .setSchema(schemaName)
                .build();

        for (int i = 0; i < tables; i++) {
            assertUpdate(session, "CREATE TABLE test_select_s_m_t_comments" + i + "(id varchar, age integer)");
            // Produce multiple snapshots and metadata files
            assertUpdate(session, "INSERT INTO test_select_s_m_t_comments" + i + " VALUES ('abc', 11)", 1);
            assertUpdate(session, "INSERT INTO test_select_s_m_t_comments" + i + " VALUES ('xyz', 12)", 1);

            assertUpdate(session, "CREATE TABLE test_other_select_s_m_t_comments" + i + "(id varchar, age integer)"); // won't match the filter
        }

        // Bulk retrieval
        assertMetastoreInvocations(session, "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA AND table_name LIKE 'test_select_s_m_t_comments%'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLES)
                        .addCopies(GET_TABLE, tables * 2)
                        .build());

        // Bulk retrieval for two schemas
        assertMetastoreInvocations(session, "SELECT * FROM system.metadata.table_comments WHERE schema_name IN (CURRENT_SCHEMA, 'non_existent') AND table_name LIKE 'test_select_s_m_t_comments%'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLES, 2)
                        .addCopies(GET_TABLE, tables * 2)
                        .build());

        // Pointed lookup
        assertMetastoreInvocations(session, "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA AND table_name = 'test_select_s_m_t_comments0'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 1)
                        .build());

        for (int i = 0; i < tables; i++) {
            assertUpdate(session, "DROP TABLE test_select_s_m_t_comments" + i);
            assertUpdate(session, "DROP TABLE test_other_select_s_m_t_comments" + i);
        }
    }

    public Object[][] metadataQueriesTestTableCountDataProvider()
    {
        return new Object[][] {
                {3},
                {MAX_PREFIXES_COUNT},
                {MAX_PREFIXES_COUNT + 3},
        };
    }

    @Test
    public void testSystemMetadataMaterializedViews()
    {
        String schemaName = "test_materialized_views_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        Session session = Session.builder(getSession())
                .setSchema(schemaName)
                .build();

        assertUpdate(session, "CREATE TABLE test_table1 AS SELECT 1 a", 1);
        assertUpdate(session, "CREATE TABLE test_table2 AS SELECT 1 a", 1);

        assertUpdate(session, "CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM test_table1 JOIN test_table2 USING (a)");
        assertUpdate(session, "REFRESH MATERIALIZED VIEW mv1", 1);

        assertUpdate(session, "CREATE MATERIALIZED VIEW mv2 AS SELECT count(*) c FROM test_table1 JOIN test_table2 USING (a)");
        assertUpdate(session, "REFRESH MATERIALIZED VIEW mv2", 1);

        // Bulk retrieval
        assertMetastoreInvocations(session, "SELECT * FROM system.metadata.materialized_views WHERE schema_name = CURRENT_SCHEMA",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLES)
                        .addCopies(GET_TABLE, 4)
                        .build());

        // Bulk retrieval without selecting freshness
        assertMetastoreInvocations(session, "SELECT schema_name, name FROM system.metadata.materialized_views WHERE schema_name = CURRENT_SCHEMA",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLES)
                        .build());

        // Bulk retrieval for two schemas
        assertMetastoreInvocations(session, "SELECT * FROM system.metadata.materialized_views WHERE schema_name IN (CURRENT_SCHEMA, 'non_existent')",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLES, 2)
                        .addCopies(GET_TABLE, 4)
                        .build());

        // Pointed lookup
        assertMetastoreInvocations(session, "SELECT * FROM system.metadata.materialized_views WHERE schema_name = CURRENT_SCHEMA AND name = 'mv1'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 3)
                        .build());

        // Pointed lookup without selecting freshness
        assertMetastoreInvocations(session, "SELECT schema_name, name FROM system.metadata.materialized_views WHERE schema_name = CURRENT_SCHEMA AND name = 'mv1'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLE)
                        .build());

        assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
    }

    @Test
    public void testShowTables()
    {
        assertMetastoreInvocations("SHOW TABLES",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(GET_TABLES)
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

    private static Session withStatsOnWrite(Session session, boolean enabled)
    {
        String catalog = session.getCatalog().orElseThrow();
        return Session.builder(session)
                .setCatalogSessionProperty(catalog, COLLECT_EXTENDED_STATISTICS_ON_WRITE, Boolean.toString(enabled))
                .build();
    }
}
