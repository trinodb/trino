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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.filesystem.TrackingFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.TableType;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_NEW_STREAM;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.iceberg.IcebergSessionProperties.COLLECT_EXTENDED_STATISTICS_ON_WRITE;
import static io.trino.plugin.iceberg.TableType.DATA;
import static io.trino.plugin.iceberg.TableType.FILES;
import static io.trino.plugin.iceberg.TableType.HISTORY;
import static io.trino.plugin.iceberg.TableType.MANIFESTS;
import static io.trino.plugin.iceberg.TableType.PARTITIONS;
import static io.trino.plugin.iceberg.TableType.PROPERTIES;
import static io.trino.plugin.iceberg.TableType.REFS;
import static io.trino.plugin.iceberg.TableType.SNAPSHOTS;
import static io.trino.plugin.iceberg.catalog.glue.GlueMetastoreMethod.CREATE_TABLE;
import static io.trino.plugin.iceberg.catalog.glue.GlueMetastoreMethod.GET_DATABASE;
import static io.trino.plugin.iceberg.catalog.glue.GlueMetastoreMethod.GET_TABLE;
import static io.trino.plugin.iceberg.catalog.glue.GlueMetastoreMethod.GET_TABLES;
import static io.trino.plugin.iceberg.catalog.glue.GlueMetastoreMethod.UPDATE_TABLE;
import static io.trino.plugin.iceberg.catalog.glue.TestIcebergGlueCatalogAccessOperations.FileType.METADATA_JSON;
import static io.trino.plugin.iceberg.catalog.glue.TestIcebergGlueCatalogAccessOperations.FileType.fromFilePath;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;

/*
 * The test currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
@Test(singleThreaded = true) // metastore invocation counters shares mutable state so can't be run from many threads simultaneously
public class TestIcebergGlueCatalogAccessOperations
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestIcebergGlueCatalogAccessOperations.class);

    private static final int MAX_PREFIXES_COUNT = 5;
    private final String testSchema = "test_schema_" + randomNameSuffix();
    private final Session testSession = testSessionBuilder()
            .setCatalog("iceberg")
            .setSchema(testSchema)
            .build();

    private GlueMetastoreStats glueStats;
    private TrackingFileSystemFactory trackingFileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File tmp = Files.createTempDirectory("test_iceberg").toFile();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSession)
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .build();

        trackingFileSystemFactory = new TrackingFileSystemFactory(new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS));

        queryRunner.installPlugin(new TestingIcebergPlugin(
                Optional.empty(),
                Optional.of(trackingFileSystemFactory),
                EMPTY_MODULE));
        queryRunner.createCatalog("iceberg", "iceberg",
                ImmutableMap.of(
                        "iceberg.catalog.type", "glue",
                        "hive.metastore.glue.default-warehouse-dir", tmp.getAbsolutePath()));

        queryRunner.execute("CREATE SCHEMA " + testSchema);

        glueStats = ((IcebergConnector) queryRunner.getCoordinator().getConnector("iceberg")).getInjector().getInstance(GlueMetastoreStats.class);

        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
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
                ImmutableMultiset.builder()
                        .add(GET_DATABASE)
                        .build());
    }

    @Test
    public void testCreateTable()
    {
        try {
            assertGlueMetastoreApiInvocations("CREATE TABLE test_create (id VARCHAR, age INT)",
                    ImmutableMultiset.builder()
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
                    ImmutableMultiset.builder()
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
                    ImmutableMultiset.builder()
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
                    ImmutableMultiset.builder()
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
                    ImmutableMultiset.builder()
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
                    ImmutableMultiset.builder()
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
                    ImmutableMultiset.builder()
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
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 3)
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
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 3)
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
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 5)
                            .addCopies(UPDATE_TABLE, 1)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP MATERIALIZED VIEW IF EXISTS test_refresh_mview_view");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_refresh_mview_table");
        }
    }

    @Test
    public void testJoin()
    {
        try {
            assertUpdate("CREATE TABLE test_join_t1 AS SELECT 2 as age, 'id1' AS id", 1);
            assertUpdate("CREATE TABLE test_join_t2 AS SELECT 'name1' as name, 'id1' AS id", 1);

            assertGlueMetastoreApiInvocations("SELECT name, age FROM test_join_t1 JOIN test_join_t2 ON test_join_t2.id = test_join_t1.id",
                    ImmutableMultiset.builder()
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
                    ImmutableMultiset.builder()
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
                    ImmutableMultiset.builder()
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
                    ImmutableMultiset.builder()
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
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 1)
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
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 1)
                            .build());

            // select from $snapshots
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$snapshots\"",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 1)
                            .build());

            // select from $manifests
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$manifests\"",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 1)
                            .build());

            // select from $partitions
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$partitions\"",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 1)
                            .build());

            // select from $files
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$files\"",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 1)
                            .build());

            // select from $properties
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$properties\"",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 1)
                            .build());

            // select from $refs
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$refs\"",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 1)
                            .build());

            // This test should get updated if a new system table is added.
            assertThat(TableType.values())
                    .containsExactly(DATA, HISTORY, SNAPSHOTS, MANIFESTS, PARTITIONS, FILES, PROPERTIES, REFS);
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_snapshots");
        }
    }

    @Test
    public void testInformationSchemaColumns()
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

                    // Bulk retrieval
                    assertInvocations(
                            session,
                            "SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name LIKE 'test_select_i_s_columns%'",
                            ImmutableMultiset.<GlueMetastoreMethod>builder()
                                    .add(GET_TABLES)
                                    .build(),
                            ImmutableMultiset.of());
                }

                // Pointed lookup
                assertInvocations(
                        session,
                        "SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name = 'test_select_i_s_columns0'",
                        ImmutableMultiset.<GlueMetastoreMethod>builder()
                                .add(GET_TABLE)
                                .build(),
                        ImmutableMultiset.<FileOperation>builder()
                                .add(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM))
                                .build());

                // Pointed lookup via DESCRIBE (which does some additional things before delegating to information_schema.columns)
                assertInvocations(
                        session,
                        "DESCRIBE test_select_i_s_columns0",
                        ImmutableMultiset.<GlueMetastoreMethod>builder()
                                .add(GET_DATABASE)
                                .add(GET_TABLE)
                                .build(),
                        ImmutableMultiset.<FileOperation>builder()
                                .add(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM))
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
                                    .addCopies(GET_TABLES, 1)
                                    .build(),
                            ImmutableMultiset.of());
                }

                // Pointed lookup
                assertInvocations(
                        session,
                        "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA AND table_name = 'test_select_s_m_t_comments0'",
                        ImmutableMultiset.<GlueMetastoreMethod>builder()
                                .addCopies(GET_TABLE, 1)
                                .build(),
                        ImmutableMultiset.<FileOperation>builder()
                                .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
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

    private void assertGlueMetastoreApiInvocations(@Language("SQL") String query, Multiset<?> expectedInvocations)
    {
        assertGlueMetastoreApiInvocations(getSession(), query, expectedInvocations);
    }

    private void assertGlueMetastoreApiInvocations(Session session, @Language("SQL") String query, Multiset<?> expectedInvocations)
    {
        assertInvocations(
                session,
                query,
                expectedInvocations.stream()
                        .map(GlueMetastoreMethod.class::cast)
                        .collect(toImmutableMultiset()),
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
        trackingFileSystemFactory.reset();

        getQueryRunner().execute(session, query);

        Map<GlueMetastoreMethod, Integer> countsAfter = Arrays.stream(GlueMetastoreMethod.values())
                .collect(toImmutableMap(Function.identity(), method -> method.getInvocationCount(glueStats)));
        Multiset<FileOperation> fileOperations = getFileOperations();

        Multiset<GlueMetastoreMethod> actualGlueInvocations = Arrays.stream(GlueMetastoreMethod.values())
                .collect(toImmutableMultiset(Function.identity(), method -> requireNonNull(countsAfter.get(method)) - requireNonNull(countsBefore.get(method))));

        assertMultisetsEqual(actualGlueInvocations, expectedGlueInvocations);
        expectedFileOperations.ifPresent(expected -> assertMultisetsEqual(fileOperations, expected));
    }

    private Multiset<FileOperation> getFileOperations()
    {
        return trackingFileSystemFactory.getOperationCounts()
                .entrySet().stream()
                .flatMap(entry -> nCopies(entry.getValue(), new FileOperation(
                        fromFilePath(entry.getKey().location().toString()),
                        entry.getKey().operationType())).stream())
                .collect(toCollection(HashMultiset::create));
    }

    private static Session withStatsOnWrite(Session session, boolean enabled)
    {
        String catalog = session.getCatalog().orElseThrow();
        return Session.builder(session)
                .setCatalogSessionProperty(catalog, COLLECT_EXTENDED_STATISTICS_ON_WRITE, Boolean.toString(enabled))
                .build();
    }

    private record FileOperation(FileType fileType, TrackingFileSystemFactory.OperationType operationType)
    {
        public FileOperation
        {
            requireNonNull(fileType, "fileType is null");
            requireNonNull(operationType, "operationType is null");
        }
    }

    enum FileType
    {
        METADATA_JSON,
        SNAPSHOT,
        MANIFEST,
        STATS,
        DATA,
        /**/;

        public static FileType fromFilePath(String path)
        {
            if (path.endsWith("metadata.json")) {
                return METADATA_JSON;
            }
            if (path.contains("/snap-")) {
                return SNAPSHOT;
            }
            if (path.endsWith("-m0.avro")) {
                return MANIFEST;
            }
            if (path.endsWith(".stats")) {
                return STATS;
            }
            if (path.contains("/data/") && (path.endsWith(".orc") || path.endsWith(".parquet"))) {
                return DATA;
            }
            throw new IllegalArgumentException("File not recognized: " + path);
        }
    }
}
