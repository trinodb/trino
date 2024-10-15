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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.iceberg.util.FileOperationUtils;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.ThreadPools;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static io.trino.SystemSessionProperties.MIN_INPUT_SIZE_PER_TASK;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergSessionProperties.COLLECT_EXTENDED_STATISTICS_ON_WRITE;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.util.EqualityDeleteUtils.writeEqualityDeleteForTable;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileOperation;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.DATA;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.DELETE;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.MANIFEST;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.METADATA_JSON;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.SNAPSHOT;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.STATS;
import static io.trino.plugin.iceberg.util.FileOperationUtils.Scope.ALL_FILES;
import static io.trino.plugin.iceberg.util.FileOperationUtils.Scope.METADATA_FILES;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.Math.min;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
public class TestIcebergFileOperations
        extends AbstractTestQueryFramework
{
    private static final int MAX_PREFIXES_COUNT = 10;

    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("iceberg")
                .setSchema("test_schema")
                // It is essential to disable DeterminePartitionCount rule since all queries in this test scans small
                // amount of data which makes them run with single hash partition count. However, this test requires them
                // to run over multiple nodes.
                .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "0MB")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                // the delete test must run with a single task, so we can verify the delete file is read once per task
                // currently the only way to achieve this is to set worker count to 0
                .setWorkerCount(0)
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .build();

        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        dataDirectory.toFile().mkdirs();
        queryRunner.installPlugin(new TestingIcebergPlugin(dataDirectory));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", ImmutableMap.<String, String>builder()
                .put("iceberg.split-manager-threads", "0")
                // FS accesses with metadata cache are tested separately in io.trino.plugin.iceberg.TestIcebergMemoryCacheFileOperations
                .put("iceberg.metadata-cache.enabled", "false")
                .buildOrThrow());

        metastore = ((IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_CATALOG)).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.execute("CREATE SCHEMA test_schema");

        return queryRunner;
    }

    @BeforeAll
    public void initFileSystemFactory()
    {
        fileSystemFactory = getFileSystemFactory(getDistributedQueryRunner());
    }

    @Test
    public void testCreateTable()
    {
        assertFileSystemAccesses("CREATE TABLE test_create (id VARCHAR, age INT)",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "OutputFile.create"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "OutputFile.create"))
                        .build());
    }

    @Test
    public void testCreateOrReplaceTable()
    {
        assertFileSystemAccesses("CREATE OR REPLACE TABLE test_create_or_replace (id VARCHAR, age INT)",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "OutputFile.create"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "OutputFile.create"))
                        .build());
        assertFileSystemAccesses("CREATE OR REPLACE TABLE test_create_or_replace (id VARCHAR, age INT)",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "OutputFile.create"))
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "OutputFile.create"))
                        .build());
    }

    @Test
    public void testCreateTableAsSelect()
    {
        assertFileSystemAccesses(
                withStatsOnWrite(getSession(), false),
                "CREATE TABLE test_create_as_select AS SELECT 1 col_name",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "OutputFile.create"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "OutputFile.create"))
                        .add(new FileOperation(MANIFEST, "OutputFile.create"))
                        .build());

        assertFileSystemAccesses(
                withStatsOnWrite(getSession(), true),
                "CREATE TABLE test_create_as_select_with_stats AS SELECT 1 col_name",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .addCopies(new FileOperation(METADATA_JSON, "OutputFile.create"), 2) // TODO (https://github.com/trinodb/trino/issues/15439): it would be good to publish data and stats in one commit
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "OutputFile.create"))
                        .add(new FileOperation(MANIFEST, "OutputFile.create"))
                        .add(new FileOperation(STATS, "OutputFile.create"))
                        .build());
    }

    @Test
    public void testCreateOrReplaceTableAsSelect()
    {
        assertFileSystemAccesses(
                "CREATE OR REPLACE TABLE test_create_or_replace_as_select AS SELECT 1 col_name",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, "OutputFile.create"), 2)
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "OutputFile.create"))
                        .add(new FileOperation(MANIFEST, "OutputFile.create"))
                        .add(new FileOperation(STATS, "OutputFile.create"))
                        .build());

        assertFileSystemAccesses(
                "CREATE OR REPLACE TABLE test_create_or_replace_as_select AS SELECT 1 col_name",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, "OutputFile.create"), 2)
                        .addCopies(new FileOperation(METADATA_JSON, "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(SNAPSHOT, "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(SNAPSHOT, "InputFile.length"), 2)
                        .add(new FileOperation(SNAPSHOT, "OutputFile.create"))
                        .add(new FileOperation(MANIFEST, "OutputFile.create"))
                        .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                        .add(new FileOperation(STATS, "OutputFile.create"))
                        .build());
    }

    @Test
    public void testSelect()
    {
        assertUpdate("CREATE TABLE test_select AS SELECT 1 col_name", 1);
        assertFileSystemAccesses("SELECT * FROM test_select",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                        .build());
    }

    @ParameterizedTest
    @MethodSource("testSelectWithLimitDataProvider")
    public void testSelectWithLimit(int numberOfFiles)
    {
        assertUpdate("DROP TABLE IF EXISTS test_select_with_limit"); // test is parameterized

        // Create table with multiple files
        assertUpdate("CREATE TABLE test_select_with_limit(k varchar, v integer) WITH (partitioning=ARRAY['truncate(k, 1)'])");
        // 2 files per partition, numberOfFiles files in total, in numberOfFiles separate manifests (due to fastAppend)
        for (int i = 0; i < numberOfFiles; i++) {
            String k = Integer.toString(10 + i * 5);
            assertUpdate("INSERT INTO test_select_with_limit VALUES ('" + k + "', " + i + ")", 1);
        }

        // org.apache.iceberg.util.ParallelIterable, even if used with a direct executor, schedules 2 * ThreadPools.WORKER_THREAD_POOL_SIZE upfront
        int icebergManifestPrefetching = 2 * ThreadPools.WORKER_THREAD_POOL_SIZE;

        assertFileSystemAccesses("SELECT * FROM test_select_with_limit LIMIT 3",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), min(icebergManifestPrefetching, numberOfFiles))
                        .build());

        assertFileSystemAccesses("EXPLAIN SELECT * FROM test_select_with_limit LIMIT 3",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), numberOfFiles)
                        .build());

        assertFileSystemAccesses("EXPLAIN ANALYZE SELECT * FROM test_select_with_limit LIMIT 3",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), numberOfFiles + min(icebergManifestPrefetching, numberOfFiles))
                        .build());

        assertUpdate("DROP TABLE test_select_with_limit");
    }

    public Object[][] testSelectWithLimitDataProvider()
    {
        return new Object[][] {
                {10},
                {50},
                // 2 * ThreadPools.WORKER_THREAD_POOL_SIZE manifest is always read, so include one more data point to show this is a constant number
                {2 * 2 * ThreadPools.WORKER_THREAD_POOL_SIZE + 6},
        };
    }

    @Test
    public void testReadWholePartition()
    {
        assertUpdate("DROP TABLE IF EXISTS test_read_part_key");

        assertUpdate("CREATE TABLE test_read_part_key(key varchar, data varchar) WITH (partitioning=ARRAY['key'])");

        // Create multiple files per partition
        assertUpdate("INSERT INTO test_read_part_key(key, data) VALUES ('p1', '1-abc'), ('p1', '1-def'), ('p2', '2-abc'), ('p2', '2-def')", 4);
        assertUpdate("INSERT INTO test_read_part_key(key, data) VALUES ('p1', '1-baz'), ('p2', '2-baz')", 2);

        // Read partition and data columns
        assertFileSystemAccesses(
                "SELECT key, max(data) FROM test_read_part_key GROUP BY key",
                ALL_FILES,
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .addCopies(new FileOperation(DATA, "InputFile.newInput"), 4)
                        .build());

        // Read partition column only
        assertFileSystemAccesses(
                "SELECT key, count(*) FROM test_read_part_key GROUP BY key",
                ALL_FILES,
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .build());

        // Read partition column only, one partition only
        assertFileSystemAccesses(
                "SELECT count(*) FROM test_read_part_key WHERE key = 'p1'",
                ALL_FILES,
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .build());

        // Read partition and synthetic columns
        assertFileSystemAccesses(
                "SELECT count(*), array_agg(\"$path\"), max(\"$file_modified_time\") FROM test_read_part_key GROUP BY key",
                ALL_FILES,
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        // TODO return synthetic columns without opening the data files
                        .addCopies(new FileOperation(DATA, "InputFile.newInput"), 4)
                        .addCopies(new FileOperation(DATA, "InputFile.lastModified"), 4)
                        .build());

        // Read only row count
        assertFileSystemAccesses(
                "SELECT count(*) FROM test_read_part_key",
                ALL_FILES,
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .build());

        assertUpdate("DROP TABLE test_read_part_key");
    }

    @Test
    public void testReadWholePartitionSplittableFile()
    {
        String catalog = getSession().getCatalog().orElseThrow();

        assertUpdate("DROP TABLE IF EXISTS test_read_whole_splittable_file");
        assertUpdate("CREATE TABLE test_read_whole_splittable_file(key varchar, data varchar) WITH (partitioning=ARRAY['key'])");

        assertUpdate(
                Session.builder(getSession())
                        .setSystemProperty(SystemSessionProperties.WRITER_SCALING_MIN_DATA_PROCESSED, "1PB")
                        .setCatalogSessionProperty(catalog, "parquet_writer_block_size", "1kB")
                        .setCatalogSessionProperty(catalog, "orc_writer_max_stripe_size", "1kB")
                        .setCatalogSessionProperty(catalog, "orc_writer_max_stripe_rows", "1000")
                        .build(),
                "INSERT INTO test_read_whole_splittable_file SELECT 'single partition', comment FROM tpch.tiny.orders", 15000);

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, IcebergSessionProperties.SPLIT_SIZE, "1kB")
                .build();

        // Read partition column only
        assertFileSystemAccesses(
                session,
                "SELECT key, count(*) FROM test_read_whole_splittable_file GROUP BY key",
                ALL_FILES,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .build());

        // Read only row count
        assertFileSystemAccesses(
                session,
                "SELECT count(*) FROM test_read_whole_splittable_file",
                ALL_FILES,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .build());

        assertUpdate("DROP TABLE test_read_whole_splittable_file");
    }

    @Test
    public void testSelectFromVersionedTable()
    {
        String tableName = "test_select_from_versioned_table";
        assertUpdate("CREATE TABLE " + tableName + " (id int, age int)");
        long v1SnapshotId = getLatestSnapshotId(tableName);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 20)", 1);
        long v2SnapshotId = getLatestSnapshotId(tableName);
        assertUpdate("INSERT INTO " + tableName + "  VALUES (3, 30)", 1);
        long v3SnapshotId = getLatestSnapshotId(tableName);
        assertFileSystemAccesses("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v1SnapshotId,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v2SnapshotId,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v3SnapshotId,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                        .build());
    }

    @Test
    public void testSelectFromVersionedTableWithSchemaEvolution()
    {
        String tableName = "test_select_from_versioned_table_with_schema_evolution";
        assertUpdate("CREATE TABLE " + tableName + " (id int, age int)");
        long v1SnapshotId = getLatestSnapshotId(tableName);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 20)", 1);
        long v2SnapshotId = getLatestSnapshotId(tableName);
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN address varchar");
        assertUpdate("INSERT INTO " + tableName + "  VALUES (3, 30, 'London')", 1);
        long v3SnapshotId = getLatestSnapshotId(tableName);
        assertFileSystemAccesses("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v1SnapshotId,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v2SnapshotId,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v3SnapshotId,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                        .build());
    }

    @Test
    public void testSelectWithFilter()
    {
        assertUpdate("CREATE TABLE test_select_with_filter AS SELECT 1 col_name", 1);
        assertFileSystemAccesses("SELECT * FROM test_select_with_filter WHERE col_name = 1",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                        .build());
    }

    @Test
    public void testJoin()
    {
        assertUpdate("CREATE TABLE test_join_t1 AS SELECT 2 AS age, 'id1' AS id", 1);
        assertUpdate("CREATE TABLE test_join_t2 AS SELECT 'name1' AS name, 'id1' AS id", 1);

        assertFileSystemAccesses("SELECT name, age FROM test_join_t1 JOIN test_join_t2 ON test_join_t2.id = test_join_t1.id",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(SNAPSHOT, "InputFile.length"), 2)
                        .addCopies(new FileOperation(SNAPSHOT, "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 4)
                        .build());
    }

    @Test
    public void testSelfJoinStatistics()
    {
        assertUpdate("CREATE TABLE test_self_join AS SELECT 'name1' AS name, 2 AS age, 'id1' AS id", 1);

        // We use column statistics for all three columns from t1 and single column from t2.
        // IcebergMetadata#tableStatisticsCache should be able to avoid multiple reads by re-using stats from t1.
        assertFileSystemAccesses("EXPLAIN SELECT t1.name, t1.age FROM test_self_join t1 JOIN test_self_join t2 ON t1.id = t2.id",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                        .build());

        // Same columns projected from both t1 and t2, but with different predicate which prevents reuse of statistics from IcebergMetadata#tableStatisticsCache
        assertFileSystemAccesses("EXPLAIN SELECT t1.age FROM test_self_join t1 JOIN test_self_join t2 ON t1.id = t2.id WHERE t2.age > 0",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                        .build());

        // Different columns projected from t1 and t2 prevents reuse of statistics from IcebergMetadata#tableStatisticsCache
        assertFileSystemAccesses("EXPLAIN SELECT t1.name FROM test_self_join t1 JOIN test_self_join t2 ON t1.name = t2.id",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                        .build());
    }

    @Test
    public void testJoinWithPartitionedTable()
    {
        assertUpdate("CREATE TABLE test_join_partitioned_t1 (a BIGINT, b TIMESTAMP(6) with time zone) WITH (partitioning = ARRAY['a', 'day(b)'])");
        assertUpdate("CREATE TABLE test_join_partitioned_t2 (foo BIGINT)");
        assertUpdate("INSERT INTO test_join_partitioned_t2 VALUES(123)", 1);
        assertUpdate("INSERT INTO test_join_partitioned_t1 VALUES(123, current_date)", 1);

        assertFileSystemAccesses("SELECT count(*) FROM test_join_partitioned_t1 t1 join test_join_partitioned_t2 t2 on t1.a = t2.foo",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(SNAPSHOT, "InputFile.length"), 2)
                        .addCopies(new FileOperation(SNAPSHOT, "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 4)
                        .build());
    }

    @Test
    public void testExplainSelect()
    {
        assertUpdate("CREATE TABLE test_explain AS SELECT 2 AS age", 1);

        assertFileSystemAccesses("EXPLAIN SELECT * FROM test_explain",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                        .build());
    }

    @Test
    public void testShowStatsForTable()
    {
        assertUpdate("CREATE TABLE test_show_stats AS SELECT 2 AS age", 1);

        assertFileSystemAccesses("SHOW STATS FOR test_show_stats",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                        .build());
    }

    @Test
    public void testShowStatsForPartitionedTable()
    {
        assertUpdate("CREATE TABLE test_show_stats_partitioned " +
                "WITH (partitioning = ARRAY['regionkey']) " +
                "AS SELECT * FROM tpch.tiny.nation", 25);

        assertFileSystemAccesses("SHOW STATS FOR test_show_stats_partitioned",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                        .build());
    }

    @Test
    public void testShowStatsForTableWithFilter()
    {
        assertUpdate("CREATE TABLE test_show_stats_with_filter AS SELECT 2 AS age", 1);

        assertFileSystemAccesses("SHOW STATS FOR (SELECT * FROM test_show_stats_with_filter WHERE age >= 2)",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                        .build());
    }

    @Test
    public void testPredicateWithVarcharCastToDate()
    {
        assertUpdate("CREATE TABLE test_varchar_as_date_predicate(a varchar) WITH (partitioning=ARRAY['truncate(a, 4)'])");
        assertUpdate("INSERT INTO test_varchar_as_date_predicate VALUES '2001-01-31'", 1);
        assertUpdate("INSERT INTO test_varchar_as_date_predicate VALUES '2005-09-10'", 1);

        assertFileSystemAccesses("SELECT * FROM test_varchar_as_date_predicate",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                        .build());

        // CAST to date and comparison
        assertFileSystemAccesses("SELECT * FROM test_varchar_as_date_predicate WHERE CAST(a AS date) >= DATE '2005-01-01'",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(MANIFEST, "InputFile.newStream")) // fewer than without filter
                        .build());

        // CAST to date and BETWEEN
        assertFileSystemAccesses("SELECT * FROM test_varchar_as_date_predicate WHERE CAST(a AS date) BETWEEN DATE '2005-01-01' AND DATE '2005-12-31'",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(MANIFEST, "InputFile.newStream")) // fewer than without filter
                        .build());

        // conversion to date as a date function
        assertFileSystemAccesses("SELECT * FROM test_varchar_as_date_predicate WHERE date(a) >= DATE '2005-01-01'",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(MANIFEST, "InputFile.newStream")) // fewer than without filter
                        .build());

        assertUpdate("DROP TABLE test_varchar_as_date_predicate");
    }

    @Test
    public void testRemoveOrphanFiles()
    {
        String tableName = "test_remove_orphan_files_" + randomNameSuffix();
        Session sessionWithShortRetentionUnlocked = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "remove_orphan_files_min_retention", "0s")
                .build();
        assertUpdate("CREATE TABLE " + tableName + " (key varchar, value integer)");
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('two', 2), ('three', 3)", 2);
        assertUpdate("DELETE FROM " + tableName + " WHERE key = 'two'", 1);

        assertFileSystemAccesses(
                sessionWithShortRetentionUnlocked,
                "ALTER TABLE " + tableName + " EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .addCopies(new FileOperation(SNAPSHOT, "InputFile.length"), 4)
                        .addCopies(new FileOperation(SNAPSHOT, "InputFile.newStream"), 4)
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 5)
                        .build());

        assertUpdate("DROP TABLE " + tableName);
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
        assertFileSystemAccesses(session, "SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name LIKE 'test_select_i_s_columns%'",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, "InputFile.newStream"), tables * 2)
                        .build());

        // Pointed lookup
        assertFileSystemAccesses(session, "SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name = 'test_select_i_s_columns0'",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .build());

        // Pointed lookup via DESCRIBE (which does some additional things before delegating to information_schema.columns)
        assertFileSystemAccesses(session, "DESCRIBE test_select_i_s_columns0",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
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
        assertFileSystemAccesses(session, "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA AND table_name LIKE 'test_select_s_m_t_comments%'",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, "InputFile.newStream"), tables * 2)
                        .build());

        // Bulk retrieval for two schemas
        assertFileSystemAccesses(session, "SELECT * FROM system.metadata.table_comments WHERE schema_name IN (CURRENT_SCHEMA, 'non_existent') AND table_name LIKE 'test_select_s_m_t_comments%'",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, "InputFile.newStream"), tables * 2)
                        .build());

        // Pointed lookup
        assertFileSystemAccesses(session, "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA AND table_name = 'test_select_s_m_t_comments0'",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
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
        assertFileSystemAccesses(session, "SELECT * FROM system.metadata.materialized_views WHERE schema_name = CURRENT_SCHEMA",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, "InputFile.newStream"), 4)
                        .build());

        // Bulk retrieval without selecting freshness
        assertFileSystemAccesses(
                session,
                "SELECT schema_name, name FROM system.metadata.materialized_views WHERE schema_name = CURRENT_SCHEMA",
                ImmutableMultiset.of());

        // Bulk retrieval for two schemas
        assertFileSystemAccesses(session, "SELECT * FROM system.metadata.materialized_views WHERE schema_name IN (CURRENT_SCHEMA, 'non_existent')",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, "InputFile.newStream"), 4)
                        .build());

        // Pointed lookup
        assertFileSystemAccesses(session, "SELECT * FROM system.metadata.materialized_views WHERE schema_name = CURRENT_SCHEMA AND name = 'mv1'",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, "InputFile.newStream"), 3)
                        .build());

        // Pointed lookup without selecting freshness
        assertFileSystemAccesses(
                session,
                "SELECT schema_name, name FROM system.metadata.materialized_views WHERE schema_name = CURRENT_SCHEMA AND name = 'mv1'",
                ImmutableMultiset.of());

        assertFileSystemAccesses(
                session,
                "SELECT * FROM iceberg.information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name = 'mv1'",
                ImmutableMultiset.of());

        assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
    }

    @Test
    public void testV2TableEnsureEqualityDeleteFilesAreReadOnce()
            throws Exception
    {
        String tableName = "test_equality_deletes_ensure_delete_read_count" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, age INT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 20), (3, 30)", 2);
        // change the schema and do another insert to force at least 2 splits
        // use the same ID in both files so the delete file doesn't get optimized away by statstics
        assertUpdate("INSERT INTO " + tableName + "  VALUES (2, 22)", 1);
        Table icebergTable = IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "iceberg", "test_schema");

        // Delete only 1 row in the file so the data file is not pruned completely
        writeEqualityDeleteForTable(icebergTable,
                fileSystemFactory,
                Optional.of(icebergTable.spec()),
                Optional.empty(),
                ImmutableMap.of("id", 2),
                Optional.empty());

        ImmutableMultiset<FileOperation> expectedAccesses = ImmutableMultiset.<FileOperationUtils.FileOperation>builder()
                .addCopies(new FileOperationUtils.FileOperation(DATA, "InputFile.newInput"), 2)
                .addCopies(new FileOperationUtils.FileOperation(DELETE, "InputFile.newInput"), 1)
                .build();

        QueryRunner.MaterializedResultWithPlan queryResult = getDistributedQueryRunner().executeWithPlan(getSession(), "SELECT * FROM " + tableName);
        assertThat(queryResult.result().getRowCount())
                .describedAs("query result row count")
                .isEqualTo(1);
        assertMultisetsEqual(
                FileOperationUtils.getOperations(getDistributedQueryRunner().getSpans()).stream()
                        .filter(operation -> ImmutableSet.of(DATA, DELETE).contains(operation.fileType()))
                        .collect(toImmutableMultiset()),
                expectedAccesses);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testShowTables()
    {
        assertFileSystemAccesses("SHOW TABLES", ImmutableMultiset.of());
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<FileOperation> expectedAccesses)
    {
        assertFileSystemAccesses(query, METADATA_FILES, expectedAccesses);
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, FileOperationUtils.Scope scope, Multiset<FileOperation> expectedAccesses)
    {
        assertFileSystemAccesses(getSession(), query, scope, expectedAccesses);
    }

    private void assertFileSystemAccesses(Session session, @Language("SQL") String query, Multiset<FileOperation> expectedAccesses)
    {
        assertFileSystemAccesses(session, query, METADATA_FILES, expectedAccesses);
    }

    private synchronized void assertFileSystemAccesses(Session session, @Language("SQL") String query, FileOperationUtils.Scope scope, Multiset<FileOperationUtils.FileOperation> expectedAccesses)
    {
        getDistributedQueryRunner().executeWithPlan(session, query);
        assertMultisetsEqual(
                FileOperationUtils.getOperations(getDistributedQueryRunner().getSpans()).stream()
                        .filter(scope)
                        .collect(toImmutableMultiset()),
                expectedAccesses);
    }

    private long getLatestSnapshotId(String tableName)
    {
        return (long) computeScalar(format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES", tableName));
    }

    private static Session withStatsOnWrite(Session session, boolean enabled)
    {
        String catalog = session.getCatalog().orElseThrow();
        return Session.builder(session)
                .setCatalogSessionProperty(catalog, COLLECT_EXTENDED_STATISTICS_ON_WRITE, Boolean.toString(enabled))
                .build();
    }
}
