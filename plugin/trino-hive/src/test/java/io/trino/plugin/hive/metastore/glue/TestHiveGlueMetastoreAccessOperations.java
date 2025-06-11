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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static com.google.common.collect.Iterators.getOnlyElement;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.CREATE_PARTITIONS;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.CREATE_TABLE;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.DELETE_COLUMN_STATISTICS_FOR_PARTITION;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.DELETE_COLUMN_STATISTICS_FOR_TABLE;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.GET_COLUMN_STATISTICS_FOR_PARTITION;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.GET_COLUMN_STATISTICS_FOR_TABLE;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.GET_DATABASE;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.GET_PARTITION;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.GET_PARTITIONS;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.GET_PARTITION_NAMES;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.GET_TABLE;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.GET_TABLES;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.UPDATE_COLUMN_STATISTICS_FOR_TABLE;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.UPDATE_PARTITION;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.UPDATE_TABLE;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // glueStats is shared mutable state
public class TestHiveGlueMetastoreAccessOperations
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestHiveGlueMetastoreAccessOperations.class);

    private static final int MAX_PREFIXES_COUNT = 5;
    private final String testSchema = "test_schema_" + randomNameSuffix();

    private GlueMetastoreStats glueStats;
    private Path schemaDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = HiveQueryRunner.builder(testSessionBuilder()
                        .setCatalog("hive")
                        .setSchema(testSchema)
                        .build())
                .addHiveProperty("hive.metastore", "glue")
                .addHiveProperty("hive.metastore.glue.default-warehouse-dir", "local:///glue")
                .addHiveProperty("hive.security", "allow-all")
                .setCreateTpchSchemas(false)
                .build();
        queryRunner.execute("CREATE SCHEMA " + testSchema);
        schemaDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").resolve("glue").resolve(testSchema);
        glueStats = getConnectorService(queryRunner, GlueHiveMetastore.class).getStats();
        return queryRunner;
    }

    @AfterAll
    public void cleanUpSchema()
    {
        getQueryRunner().execute("DROP SCHEMA " + testSchema + " CASCADE");
    }

    @Test
    void testInsertOverwriteStatisticsDisabled()
    {
        String tableName = "test_insert_overwrite_" + randomNameSuffix();

        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INT, part INT) WITH (partitioned_by = ARRAY['part'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1), (2, 1)", 2);

            Session insertOverwriteSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setCatalogSessionProperty("hive", "insert_existing_partitions_behavior", "OVERWRITE")
                    .setCatalogSessionProperty("hive", "collect_column_statistics_on_write", "false")
                    .setCatalogSessionProperty("hive", "statistics_enabled", "false")
                    .build();
            assertInvocations(insertOverwriteSession, "INSERT INTO " + tableName + " VALUES (3, 1)",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(DELETE_COLUMN_STATISTICS_FOR_PARTITION)
                            .add(GET_COLUMN_STATISTICS_FOR_PARTITION)
                            .add(GET_PARTITION)
                            .add(GET_TABLE)
                            .add(UPDATE_PARTITION)
                            .build(),
                    // We can't disable partition cache in glue v2, see GlueMetastoreModule#createGlueCache
                    // there is maybe 1 time additional call for the GET_PARTITION
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_PARTITION)
                            .build());
            assertQuery("SELECT * FROM " + tableName, "VALUES (3, 1)");
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    void testSyncPartitionMetadataProcedure()
            throws IOException
    {
        String tableName = "test_sync_partition_metadata_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INT, part INT) WITH (partitioned_by = ARRAY['part'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1)", 1);
            Path tableDir = schemaDir.resolve(tableName);
            Path sourcePartitionDir = tableDir.resolve("part=1");
            Path sourceFile;
            try (Stream<Path> paths = Files.list(sourcePartitionDir)) {
                sourceFile = getOnlyElement(paths.iterator());
            }

            // prepare partition to sync
            Path targetPartitionDir = tableDir.resolve("part=2");
            Files.createDirectories(targetPartitionDir);
            Files.copy(sourceFile, targetPartitionDir.resolve("data"));

            // the sync_partition_metadata doesn't call UPDATE_COLUMN_STATISTICS_FOR_PARTITION
            assertInvocations("CALL system.sync_partition_metadata('%s', '%s', 'FULL')".formatted(testSchema, tableName),
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .add(CREATE_PARTITIONS)
                            .add(DELETE_COLUMN_STATISTICS_FOR_PARTITION)
                            .addCopies(GET_PARTITION_NAMES, 5)
                            .build());

            // the partition is successfully synced
            assertQuery("SELECT * FROM " + tableName + " WHERE part = 2", "VALUES (1, 2)");
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
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
        assertInvocations(session, "USE %s.%s".formatted(catalog, schema),
                ImmutableMultiset.<GlueMetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .build());
    }

    @Test
    public void testCreateTable()
    {
        try {
            assertInvocations("CREATE TABLE test_create (id VARCHAR, age INT)",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_DATABASE)
                            .addCopies(GET_TABLE, 2)
                            .add(CREATE_TABLE)
                            .add(UPDATE_TABLE)
                            .addCopies(GET_COLUMN_STATISTICS_FOR_TABLE, 2)
                            .addCopies(DELETE_COLUMN_STATISTICS_FOR_TABLE, 2)
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
            assertInvocations(
                    "CREATE TABLE test_ctas AS SELECT 1 AS age",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_DATABASE)
                            .addCopies(GET_TABLE, 2)
                            .add(CREATE_TABLE)
                            .add(UPDATE_TABLE)
                            .add(UPDATE_COLUMN_STATISTICS_FOR_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_ctas");
        }
    }

    @Test
    public void testSelect()
    {
        try {
            assertUpdate("CREATE TABLE test_select_from (id VARCHAR, age INT)");

            assertInvocations("SELECT * FROM test_select_from",
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

            assertInvocations("SELECT * FROM test_select_from_where WHERE age = 2",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_from_where");
        }
    }

    @Test
    public void testSelectFromPartitionedWithFilter()
    {
        try {
            assertUpdate(
                    """
                            CREATE TABLE test_select_from_partitioned_where WITH (partitioned_by = ARRAY['regionkey']) AS
                            SELECT nationkey, name, regionkey FROM tpch.tiny.nation
                            UNION ALL SELECT nationkey, name, regionkey + 10 AS regionkey FROM tpch.tiny.nation
                            """,
                    50);

            assertInvocations("SELECT * FROM test_select_from_partitioned_where WHERE regionkey IN (2, 3)",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .add(GET_PARTITIONS)
                            // TODO this is a bulk call, it should suffice to do it once
                            .addCopies(GET_PARTITION_NAMES, 5)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_from_partitioned_where");
        }
    }

    @Test
    public void testSelectFromView()
    {
        try {
            assertUpdate("CREATE TABLE test_select_view_table (id VARCHAR, age INT)");
            assertUpdate("CREATE VIEW test_select_view_view AS SELECT id, age FROM test_select_view_table");

            assertInvocations("SELECT * FROM test_select_view_view",
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

            assertInvocations("SELECT * FROM test_select_view_where_view WHERE age = 2",
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
    public void testJoin()
    {
        try {
            assertUpdate("CREATE TABLE test_join_t1 AS SELECT 2 as age, 'id1' AS id", 1);
            assertUpdate("CREATE TABLE test_join_t2 AS SELECT 'name1' as name, 'id1' AS id", 1);

            assertInvocations("SELECT name, age FROM test_join_t1 JOIN test_join_t2 ON test_join_t2.id = test_join_t1.id",
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

            assertInvocations("SELECT child.age, parent.age FROM test_self_join_table child JOIN test_self_join_table parent ON child.parent = parent.id",
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

            assertInvocations("EXPLAIN SELECT * FROM test_explain",
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

            assertInvocations("SHOW STATS FOR test_show_stats",
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

            assertInvocations("SHOW STATS FOR (SELECT * FROM test_show_stats_with_filter where age >= 2)",
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
            assertUpdate(
                    """
                            CREATE TABLE test_select_system_table WITH (partitioned_by = ARRAY['regionkey']) AS
                            SELECT nationkey, name, regionkey FROM tpch.tiny.nation
                            UNION ALL SELECT nationkey, name, regionkey + 10 AS regionkey FROM tpch.tiny.nation
                            """,
                    50);

            // select from $partitions
            assertInvocations("SELECT * FROM \"test_select_system_table$partitions\"",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            // TODO this is a bulk call, it should suffice to do it once
                            .addCopies(GET_PARTITION_NAMES, 5)
                            .build());

            // select from $properties
            assertInvocations("SELECT * FROM \"test_select_system_table$properties\"",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_system_table");
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
                                    .addCopies(GET_TABLE, tables * 2)
                                    .build());
                }

                // Tables listing
                assertInvocations(
                        session,
                        "SELECT * FROM information_schema.tables WHERE table_schema = CURRENT_SCHEMA",
                        ImmutableMultiset.<GlueMetastoreMethod>builder()
                                .add(GET_TABLES)
                                .build());

                // Pointed columns lookup
                assertInvocations(
                        session,
                        "SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name = 'test_select_i_s_columns0'",
                        ImmutableMultiset.<GlueMetastoreMethod>builder()
                                .add(GET_TABLE)
                                .build());

                // Pointed columns lookup via DESCRIBE (which does some additional things before delegating to information_schema.columns)
                assertInvocations(
                        session,
                        "DESCRIBE test_select_i_s_columns0",
                        ImmutableMultiset.<GlueMetastoreMethod>builder()
                                .add(GET_DATABASE)
                                .add(GET_TABLE)
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
                                    .addCopies(GET_TABLE, tables * 2)
                                    .build());
                }

                // Pointed lookup
                assertInvocations(
                        session,
                        "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA AND table_name = 'test_select_s_m_t_comments0'",
                        ImmutableMultiset.<GlueMetastoreMethod>builder()
                                .add(GET_TABLE)
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
        assertInvocations("SHOW TABLES",
                ImmutableMultiset.<GlueMetastoreMethod>builder()
                        .add(GET_DATABASE)
                        .add(GET_TABLES)
                        .build());
    }

    private void assertInvocations(@Language("SQL") String query, Multiset<GlueMetastoreMethod> expectedGlueInvocations)
    {
        assertInvocations(getSession(), query, expectedGlueInvocations);
    }

    private void assertInvocations(Session session, @Language("SQL") String query, Multiset<GlueMetastoreMethod> expectedGlueInvocations)
    {
        assertMultisetsEqual(getActualInvocations(session, query), expectedGlueInvocations);
    }

    private void assertInvocations(
            Session session, @Language("SQL") String query,
            Multiset<GlueMetastoreMethod> determinedExpectedGlueInvocations,
            Multiset<GlueMetastoreMethod> possibleExpectedGlueInvocations)
    {
        Multiset<GlueMetastoreMethod> actualInvocations = getActualInvocations(session, query);
        if (!mismatchMultisets(determinedExpectedGlueInvocations, actualInvocations).isEmpty()) {
            Multiset<GlueMetastoreMethod> expectedGlueInvocations = ImmutableMultiset.<GlueMetastoreMethod>builder()
                    .addAll(determinedExpectedGlueInvocations)
                    .addAll(possibleExpectedGlueInvocations)
                    .build();
            assertMultisetsEqual(expectedGlueInvocations, actualInvocations);
        }
    }

    private Multiset<GlueMetastoreMethod> getActualInvocations(Session session, @Language("SQL") String query)
    {
        Map<GlueMetastoreMethod, Integer> countsBefore = Arrays.stream(GlueMetastoreMethod.values())
                .collect(toImmutableMap(Function.identity(), method -> method.getInvocationCount(glueStats)));

        getQueryRunner().execute(session, query);

        Map<GlueMetastoreMethod, Integer> countsAfter = Arrays.stream(GlueMetastoreMethod.values())
                .collect(toImmutableMap(Function.identity(), method -> method.getInvocationCount(glueStats)));

        return Arrays.stream(GlueMetastoreMethod.values())
                .collect(toImmutableMultiset(Function.identity(), method -> requireNonNull(countsAfter.get(method)) - requireNonNull(countsBefore.get(method))));
    }

    private static List<String> mismatchMultisets(Multiset<?> actual, Multiset<?> expected)
    {
        if (expected.equals(actual)) {
            return ImmutableList.of();
        }

        return Sets.union(expected.elementSet(), actual.elementSet()).stream()
                .filter(key -> expected.count(key) != actual.count(key))
                .flatMap(key -> {
                    int expectedCount = expected.count(key);
                    int actualCount = actual.count(key);
                    if (actualCount < expectedCount) {
                        return Stream.of(format("%s more occurrences of %s", expectedCount - actualCount, key));
                    }
                    if (actualCount > expectedCount) {
                        return Stream.of(format("%s fewer occurrences of %s", actualCount - expectedCount, key));
                    }
                    return Stream.of();
                })
                .collect(toImmutableList());
    }
}
