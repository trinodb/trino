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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.airlift.log.Logger;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.Session;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.plugin.iceberg.TableType;
import io.trino.plugin.iceberg.util.FileOperationUtils.FileOperation;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.CREATE_TABLE;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.GET_DATABASE;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.GET_TABLE;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.GET_TABLES;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.UPDATE_TABLE;
import static io.trino.plugin.iceberg.IcebergSessionProperties.COLLECT_EXTENDED_STATISTICS_ON_WRITE;
import static io.trino.plugin.iceberg.TableType.ALL_ENTRIES;
import static io.trino.plugin.iceberg.TableType.ALL_MANIFESTS;
import static io.trino.plugin.iceberg.TableType.DATA;
import static io.trino.plugin.iceberg.TableType.ENTRIES;
import static io.trino.plugin.iceberg.TableType.FILES;
import static io.trino.plugin.iceberg.TableType.HISTORY;
import static io.trino.plugin.iceberg.TableType.MANIFESTS;
import static io.trino.plugin.iceberg.TableType.MATERIALIZED_VIEW_STORAGE;
import static io.trino.plugin.iceberg.TableType.METADATA_LOG_ENTRIES;
import static io.trino.plugin.iceberg.TableType.PARTITIONS;
import static io.trino.plugin.iceberg.TableType.PROPERTIES;
import static io.trino.plugin.iceberg.TableType.REFS;
import static io.trino.plugin.iceberg.TableType.SNAPSHOTS;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.METADATA_JSON;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.fromFilePath;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/*
 * The test currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
@Execution(SAME_THREAD)
public class TestIcebergGlueCatalogAccessOperations
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestIcebergGlueCatalogAccessOperations.class);

    private static final int MAX_PREFIXES_COUNT = 5;
    private final String testSchema = "test_schema_" + randomNameSuffix();

    private GlueMetastoreStats glueStats;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder(testSchema)
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .addIcebergProperty("iceberg.catalog.type", "glue")
                .addIcebergProperty("hive.metastore.glue.default-warehouse-dir", "local:///glue")
                .setSchemaInitializer(SchemaInitializer.builder().withSchemaName(testSchema).build())
                .build();
        glueStats = ((IcebergConnector) queryRunner.getCoordinator().getConnector("iceberg")).getInjector().getInstance(GlueMetastoreStats.class);
        return queryRunner;
    }

    @AfterAll
    public void cleanUpSchema()
    {
        getQueryRunner().execute("DROP SCHEMA " + testSchema);
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
        assertGlueMetastoreApiInvocations(session, "USE %s.%s".formatted(catalog, schema),
                ImmutableMultiset.<GlueMetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .build());
    }

    @Test
    public void testCreateTable()
    {
        try {
            assertGlueMetastoreApiInvocations("CREATE TABLE test_create (id VARCHAR, age INT)",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(CREATE_TABLE)
                            .addCopies(GET_DATABASE, 2)
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_create");
        }
    }

    @Test
    public void testCreateTableAsSelect()
    {
        try {
            assertGlueMetastoreApiInvocations(
                    withStatsOnWrite(getSession(), false),
                    "CREATE TABLE test_ctas AS SELECT 1 AS age",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .addCopies(GET_DATABASE, 2)
                            .add(CREATE_TABLE)
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_ctas");
        }

        try {
            assertGlueMetastoreApiInvocations(
                    withStatsOnWrite(getSession(), true),
                    "CREATE TABLE test_ctas_with_stats AS SELECT 1 AS age",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .addCopies(GET_DATABASE, 2)
                            .add(CREATE_TABLE)
                            .addCopies(GET_TABLE, 5)
                            .add(UPDATE_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_ctas_with_stats");
        }
    }

    @Test
    public void testSelect()
    {
        try {
            assertUpdate("CREATE TABLE test_select_from (id VARCHAR, age INT)");

            assertGlueMetastoreApiInvocations("SELECT * FROM test_select_from",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_from");
        }
    }

    @Test
    public void testSelectWithFilter()
    {
        try {
            assertUpdate("CREATE TABLE test_select_from_where AS SELECT 2 as age", 1);

            assertGlueMetastoreApiInvocations("SELECT * FROM test_select_from_where WHERE age = 2",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_from_where");
        }
    }

    @Test
    public void testSelectFromView()
    {
        try {
            assertUpdate("CREATE TABLE test_select_view_table (id VARCHAR, age INT)");
            assertUpdate("CREATE VIEW test_select_view_view AS SELECT id, age FROM test_select_view_table");

            assertGlueMetastoreApiInvocations("SELECT * FROM test_select_view_view",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .addCopies(GET_TABLE, 2)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP VIEW IF EXISTS test_select_view_view");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_view_table");
        }
    }

    @Test
    public void testSelectFromViewWithFilter()
    {
        try {
            assertUpdate("CREATE TABLE test_select_view_where_table AS SELECT 2 as age", 1);
            assertUpdate("CREATE VIEW test_select_view_where_view AS SELECT age FROM test_select_view_where_table");

            assertGlueMetastoreApiInvocations("SELECT * FROM test_select_view_where_view WHERE age = 2",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .addCopies(GET_TABLE, 2)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_view_where_table");
            getQueryRunner().execute("DROP VIEW IF EXISTS test_select_view_where_view");
        }
    }

    @Test
    public void testSelectFromMaterializedView()
    {
        try {
            assertUpdate("CREATE TABLE test_select_mview_table (id VARCHAR, age INT)");
            assertUpdate("CREATE MATERIALIZED VIEW test_select_mview_view AS SELECT id, age FROM test_select_mview_table");

            assertGlueMetastoreApiInvocations("SELECT * FROM test_select_mview_view",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .addCopies(GET_TABLE, 2)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP MATERIALIZED VIEW IF EXISTS test_select_mview_view");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_mview_table");
        }
    }

    @Test
    public void testSelectFromMaterializedViewWithFilter()
    {
        try {
            assertUpdate("CREATE TABLE test_select_mview_where_table AS SELECT 2 as age", 1);
            assertUpdate("CREATE MATERIALIZED VIEW test_select_mview_where_view AS SELECT age FROM test_select_mview_where_table");

            assertGlueMetastoreApiInvocations("SELECT * FROM test_select_mview_where_view WHERE age = 2",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .addCopies(GET_TABLE, 2)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP MATERIALIZED VIEW IF EXISTS test_select_mview_where_view");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_mview_where_table");
        }
    }

    @Test
    public void testRefreshMaterializedView()
    {
        try {
            assertUpdate("CREATE TABLE test_refresh_mview_table (id VARCHAR, age INT)");
            assertUpdate("CREATE MATERIALIZED VIEW test_refresh_mview_view AS SELECT id, age FROM test_refresh_mview_table");

            assertGlueMetastoreApiInvocations("REFRESH MATERIALIZED VIEW test_refresh_mview_view",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .addCopies(GET_TABLE, 4)
                            .add(UPDATE_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP MATERIALIZED VIEW IF EXISTS test_refresh_mview_view");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_refresh_mview_table");
        }
    }

    @Test
    public void testMaterializedViewMetadata()
    {
        try {
            assertUpdate("CREATE TABLE test_mview_metadata_table (id VARCHAR, age INT)");
            assertUpdate("CREATE MATERIALIZED VIEW test_mview_metadata_view AS SELECT id, age FROM test_mview_metadata_table");

            // listing
            assertGlueMetastoreApiInvocations(
                    "SELECT * FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLES)
                            .add(GET_TABLE)
                            .build());

            // pointed lookup
            assertGlueMetastoreApiInvocations(
                    "SELECT * FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = 'test_mview_metadata_view'",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // just names
            assertGlueMetastoreApiInvocations(
                    "SELECT name FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLES)
                            .build());

            // getting relations with their types, like some tools do
            assertGlueMetastoreApiInvocations(
                    """
                            SELECT table_name, IF(mv.name IS NOT NULL, 'MATERIALIZED VIEW', table_type) AS table_type
                            FROM information_schema.tables t
                            JOIN system.metadata.materialized_views mv ON t.table_schema = mv.schema_name AND t.table_name = mv.name
                            WHERE t.table_schema = CURRENT_SCHEMA AND mv.catalog_name = CURRENT_CATALOG
                            """,
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .addCopies(GET_TABLES, 2)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP MATERIALIZED VIEW IF EXISTS test_mview_metadata_view");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_mview_metadata_table");
        }
    }

    @Test
    public void testJoin()
    {
        try {
            assertUpdate("CREATE TABLE test_join_t1 AS SELECT 2 as age, 'id1' AS id", 1);
            assertUpdate("CREATE TABLE test_join_t2 AS SELECT 'name1' as name, 'id1' AS id", 1);

            assertGlueMetastoreApiInvocations("SELECT name, age FROM test_join_t1 JOIN test_join_t2 ON test_join_t2.id = test_join_t1.id",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .addCopies(GET_TABLE, 2)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_join_t1");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_join_t2");
        }
    }

    @Test
    public void testSelfJoin()
    {
        try {
            assertUpdate("CREATE TABLE test_self_join_table AS SELECT 2 as age, 0 parent, 3 AS id", 1);

            assertGlueMetastoreApiInvocations("SELECT child.age, parent.age FROM test_self_join_table child JOIN test_self_join_table parent ON child.parent = parent.id",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_self_join_table");
        }
    }

    @Test
    public void testExplainSelect()
    {
        try {
            assertUpdate("CREATE TABLE test_explain AS SELECT 2 as age", 1);

            assertGlueMetastoreApiInvocations("EXPLAIN SELECT * FROM test_explain",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_explain");
        }
    }

    @Test
    public void testShowStatsForTable()
    {
        try {
            assertUpdate("CREATE TABLE test_show_stats AS SELECT 2 as age", 1);

            assertGlueMetastoreApiInvocations("SHOW STATS FOR test_show_stats",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_show_stats");
        }
    }

    @Test
    public void testShowStatsForTableWithFilter()
    {
        try {
            assertUpdate("CREATE TABLE test_show_stats_with_filter AS SELECT 2 as age", 1);

            assertGlueMetastoreApiInvocations("SHOW STATS FOR (SELECT * FROM test_show_stats_with_filter where age >= 2)",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_show_stats_with_filter");
        }
    }

    @Test
    public void testSelectSystemTable()
    {
        try {
            assertUpdate("CREATE TABLE test_select_snapshots AS SELECT 2 AS age", 1);

            // select from $history
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$history\"",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // select from $metadata_log_entries
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$metadata_log_entries\"",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // select from $snapshots
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$snapshots\"",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // select from $all_manifests
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$all_manifests\"",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // select from $manifests
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$manifests\"",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // select from $partitions
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$partitions\"",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // select from $files
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$files\"",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // select from $all_entries
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$all_entries\"",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // select from $entries
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$entries\"",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // select from $properties
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$properties\"",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // select from $refs
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$refs\"",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            assertQueryFails("SELECT * FROM \"test_select_snapshots$materialized_view_storage\"",
                    "Table '" + testSchema + ".test_select_snapshots\\$materialized_view_storage' not found");

            // This test should get updated if a new system table is added.
            assertThat(TableType.values())
                    .containsExactly(DATA, HISTORY, METADATA_LOG_ENTRIES, SNAPSHOTS, ALL_MANIFESTS, MANIFESTS, PARTITIONS, FILES, ALL_ENTRIES, ENTRIES, PROPERTIES, REFS, MATERIALIZED_VIEW_STORAGE);
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_snapshots");
        }
    }

    @Test
    public void testInformationSchemaTableAndColumns()
    {
        String schemaName = "test_i_s_columns_schema" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        try {
            Session session = Session.builder(getSession())
                    .setSchema(schemaName)
                    .build();
            int tablesCreated = 0;
            try {
                // Do not use @DataProvider to save test setup time which may be considerable
                for (int tables : List.of(2, MAX_PREFIXES_COUNT, MAX_PREFIXES_COUNT + 2)) {
                    log.info("testInformationSchemaColumns: Testing with %s tables", tables);
                    checkState(tablesCreated < tables);

                    for (int i = tablesCreated; i < tables; i++) {
                        tablesCreated++;
                        assertUpdate(session, "CREATE TABLE test_select_i_s_columns" + i + "(id varchar, age integer)");
                        // Produce multiple snapshots and metadata files
                        assertUpdate(session, "INSERT INTO test_select_i_s_columns" + i + " VALUES ('abc', 11)", 1);
                        assertUpdate(session, "INSERT INTO test_select_i_s_columns" + i + " VALUES ('xyz', 12)", 1);

                        assertUpdate(session, "CREATE TABLE test_other_select_i_s_columns" + i + "(id varchar, age integer)"); // won't match the filter
                    }

                    // Bulk columns retrieval
                    assertInvocations(
                            session,
                            "SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name LIKE 'test_select_i_s_columns%'",
                            ImmutableMultiset.<GlueMetastoreMethod>builder()
                                    .add(GET_TABLES)
                                    .build(),
                            ImmutableMultiset.of());
                }

                // Tables listing
                assertInvocations(
                        session,
                        "SELECT * FROM information_schema.tables WHERE table_schema = CURRENT_SCHEMA",
                        ImmutableMultiset.<GlueMetastoreMethod>builder()
                                .add(GET_TABLES)
                                .build(),
                        ImmutableMultiset.of());

                // Pointed columns lookup
                assertInvocations(
                        session,
                        "SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name = 'test_select_i_s_columns0'",
                        ImmutableMultiset.<GlueMetastoreMethod>builder()
                                .add(GET_TABLE)
                                .build(),
                        ImmutableMultiset.<FileOperation>builder()
                                .add(new FileOperation(METADATA_JSON, "InputFile.length"))
                                .add(new FileOperation(METADATA_JSON, "InputFile.newInput"))
                                .build());

                // Pointed columns lookup via DESCRIBE (which does some additional things before delegating to information_schema.columns)
                assertInvocations(
                        session,
                        "DESCRIBE test_select_i_s_columns1",
                        ImmutableMultiset.<GlueMetastoreMethod>builder()
                                .add(GET_DATABASE)
                                .add(GET_TABLE)
                                .build(),
                        ImmutableMultiset.<FileOperation>builder()
                                .add(new FileOperation(METADATA_JSON, "InputFile.length"))
                                .add(new FileOperation(METADATA_JSON, "InputFile.newInput"))
                                .build());
            }
            finally {
                for (int i = 0; i < tablesCreated; i++) {
                    assertUpdate(session, "DROP TABLE IF EXISTS test_select_i_s_columns" + i);
                    assertUpdate(session, "DROP TABLE IF EXISTS test_other_select_i_s_columns" + i);
                }
            }
        }
        finally {
            assertUpdate("DROP SCHEMA " + schemaName);
        }
    }

    @Test
    public void testSystemMetadataTableComments()
    {
        String schemaName = "test_s_m_table_comments" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        try {
            Session session = Session.builder(getSession())
                    .setSchema(schemaName)
                    .build();
            int tablesCreated = 0;
            try {
                // Do not use @DataProvider to save test setup time which may be considerable
                for (int tables : List.of(2, MAX_PREFIXES_COUNT, MAX_PREFIXES_COUNT + 2)) {
                    log.info("testSystemMetadataTableComments: Testing with %s tables", tables);
                    checkState(tablesCreated < tables);

                    for (int i = tablesCreated; i < tables; i++) {
                        tablesCreated++;
                        assertUpdate(session, "CREATE TABLE test_select_s_m_t_comments" + i + "(id varchar, age integer)");
                        // Produce multiple snapshots and metadata files
                        assertUpdate(session, "INSERT INTO test_select_s_m_t_comments" + i + " VALUES ('abc', 11)", 1);
                        assertUpdate(session, "INSERT INTO test_select_s_m_t_comments" + i + " VALUES ('xyz', 12)", 1);

                        assertUpdate(session, "CREATE TABLE test_other_select_s_m_t_comments" + i + "(id varchar, age integer)"); // won't match the filter
                    }

                    // Bulk retrieval
                    assertInvocations(
                            session,
                            "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA AND table_name LIKE 'test_select_s_m_t_comments%'",
                            ImmutableMultiset.<GlueMetastoreMethod>builder()
                                    .add(GET_TABLES)
                                    .build(),
                            ImmutableMultiset.of());
                }

                // Pointed lookup
                assertInvocations(
                        session,
                        "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA AND table_name = 'test_select_s_m_t_comments0'",
                        ImmutableMultiset.<GlueMetastoreMethod>builder()
                                .add(GET_TABLE)
                                .build(),
                        ImmutableMultiset.<FileOperation>builder()
                                .add(new FileOperation(METADATA_JSON, "InputFile.length"))
                                .add(new FileOperation(METADATA_JSON, "InputFile.newInput"))
                                .build());
            }
            finally {
                for (int i = 0; i < tablesCreated; i++) {
                    assertUpdate(session, "DROP TABLE IF EXISTS test_select_s_m_t_comments" + i);
                    assertUpdate(session, "DROP TABLE IF EXISTS test_other_select_s_m_t_comments" + i);
                }
            }
        }
        finally {
            assertUpdate("DROP SCHEMA " + schemaName);
        }
    }

    @Test
    public void testShowTables()
    {
        assertGlueMetastoreApiInvocations("SHOW TABLES",
                ImmutableMultiset.<GlueMetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(GET_TABLES)
                        .build());
    }

    private void assertGlueMetastoreApiInvocations(@Language("SQL") String query, Multiset<GlueMetastoreMethod> expectedInvocations)
    {
        assertGlueMetastoreApiInvocations(getSession(), query, expectedInvocations);
    }

    private void assertGlueMetastoreApiInvocations(Session session, @Language("SQL") String query, Multiset<GlueMetastoreMethod> expectedInvocations)
    {
        assertInvocations(
                session,
                query,
                expectedInvocations,
                Optional.empty());
    }

    private void assertInvocations(
            Session session,
            @Language("SQL") String query,
            Multiset<GlueMetastoreMethod> expectedGlueInvocations,
            Multiset<FileOperation> expectedFileOperations)
    {
        assertInvocations(session, query, expectedGlueInvocations, Optional.of(expectedFileOperations));
    }

    private void assertInvocations(
            Session session,
            @Language("SQL") String query,
            Multiset<GlueMetastoreMethod> expectedGlueInvocations,
            Optional<Multiset<FileOperation>> expectedFileOperations)
    {
        Map<GlueMetastoreMethod, Integer> countsBefore = Arrays.stream(GlueMetastoreMethod.values())
                .collect(toImmutableMap(Function.identity(), method -> method.getInvocationCount(glueStats)));

        getQueryRunner().execute(session, query);
        List<SpanData> spans = getQueryRunner().getSpans();

        Map<GlueMetastoreMethod, Integer> countsAfter = Arrays.stream(GlueMetastoreMethod.values())
                .collect(toImmutableMap(Function.identity(), method -> method.getInvocationCount(glueStats)));
        Multiset<FileOperation> fileOperations = getFileOperations(spans);

        Multiset<GlueMetastoreMethod> actualGlueInvocations = Arrays.stream(GlueMetastoreMethod.values())
                .collect(toImmutableMultiset(Function.identity(), method -> requireNonNull(countsAfter.get(method)) - requireNonNull(countsBefore.get(method))));

        assertMultisetsEqual(actualGlueInvocations, expectedGlueInvocations);
        expectedFileOperations.ifPresent(expected -> assertMultisetsEqual(fileOperations, expected));
    }

    private Multiset<FileOperation> getFileOperations(List<SpanData> spans)
    {
        return spans.stream()
                .filter(span -> span.getName().startsWith("InputFile."))
                .map(span -> new FileOperation(fromFilePath(span.getAttributes().get(FILE_LOCATION)), span.getName()))
                .collect(toCollection(HashMultiset::create));
    }

    private static Session withStatsOnWrite(Session session, boolean enabled)
    {
        String catalog = session.getCatalog().orElseThrow();
        return Session.builder(session)
                .setCatalogSessionProperty(catalog, COLLECT_EXTENDED_STATISTICS_ON_WRITE, Boolean.toString(enabled))
                .build();
    }
}
