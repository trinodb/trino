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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import com.google.inject.Key;
import io.trino.Session;
import io.trino.filesystem.TrackingFileSystemFactory;
import io.trino.filesystem.TrackingFileSystemFactory.OperationType;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.iceberg.catalog.file.TestingIcebergFileMetastoreCatalogModule;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.apache.iceberg.util.ThreadPools;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.SystemSessionProperties.MIN_INPUT_SIZE_PER_TASK;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_GET_LENGTH;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_NEW_STREAM;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.OUTPUT_FILE_CREATE;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.OUTPUT_FILE_CREATE_OR_OVERWRITE;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.OUTPUT_FILE_LOCATION;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.hive.util.MultisetAssertions.assertMultisetsEqual;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergSessionProperties.COLLECT_EXTENDED_STATISTICS_ON_WRITE;
import static io.trino.plugin.iceberg.TestIcebergMetadataFileOperations.FileType.DATA;
import static io.trino.plugin.iceberg.TestIcebergMetadataFileOperations.FileType.MANIFEST;
import static io.trino.plugin.iceberg.TestIcebergMetadataFileOperations.FileType.METADATA_JSON;
import static io.trino.plugin.iceberg.TestIcebergMetadataFileOperations.FileType.SNAPSHOT;
import static io.trino.plugin.iceberg.TestIcebergMetadataFileOperations.FileType.STATS;
import static io.trino.plugin.iceberg.TestIcebergMetadataFileOperations.FileType.fromFilePath;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

@Test(singleThreaded = true) // e.g. trackingFileSystemFactory is shared mutable state
public class TestIcebergMetadataFileOperations
        extends AbstractTestQueryFramework
{
    private TrackingFileSystemFactory trackingFileSystemFactory;

    @Override
    protected DistributedQueryRunner createQueryRunner()
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

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                // Tests that inspect MBean attributes need to run with just one node, otherwise
                // the attributes may come from the bound class instance in non-coordinator node
                .setNodeCount(1)
                .build();

        File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").toFile();
        HiveMetastore metastore = createTestingFileHiveMetastore(baseDir);

        trackingFileSystemFactory = new TrackingFileSystemFactory(new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS));
        queryRunner.installPlugin(new TestingIcebergPlugin(
                Optional.of(new TestingIcebergFileMetastoreCatalogModule(metastore)),
                Optional.of(trackingFileSystemFactory),
                binder -> {
                    newOptionalBinder(binder, Key.get(boolean.class, AsyncIcebergSplitProducer.class))
                            .setBinding().toInstance(false);
                }));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg");
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.execute("CREATE SCHEMA test_schema");

        return queryRunner;
    }

    @Test
    public void testCreateTable()
    {
        assertFileSystemAccesses("CREATE TABLE test_create (id VARCHAR, age INT)",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, OUTPUT_FILE_CREATE), 1)
                        .addCopies(new FileOperation(METADATA_JSON, OUTPUT_FILE_LOCATION), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, OUTPUT_FILE_CREATE_OR_OVERWRITE), 1)
                        .addCopies(new FileOperation(SNAPSHOT, OUTPUT_FILE_LOCATION), 2)
                        .build());
    }

    @Test
    public void testCreateTableAsSelect()
    {
        assertFileSystemAccesses(
                withStatsOnWrite(getSession(), false),
                "CREATE TABLE test_create_as_select AS SELECT 1 col_name",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, OUTPUT_FILE_CREATE), 1)
                        .addCopies(new FileOperation(METADATA_JSON, OUTPUT_FILE_LOCATION), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, OUTPUT_FILE_CREATE_OR_OVERWRITE), 1)
                        .addCopies(new FileOperation(SNAPSHOT, OUTPUT_FILE_LOCATION), 2)
                        .addCopies(new FileOperation(MANIFEST, OUTPUT_FILE_CREATE_OR_OVERWRITE), 1)
                        .addCopies(new FileOperation(MANIFEST, OUTPUT_FILE_LOCATION), 1)
                        .build());

        assertFileSystemAccesses(
                withStatsOnWrite(getSession(), true),
                "CREATE TABLE test_create_as_select_with_stats AS SELECT 1 col_name",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(METADATA_JSON, OUTPUT_FILE_CREATE), 2) // TODO (https://github.com/trinodb/trino/issues/15439): it would be good to publish data and stats in one commit
                        .addCopies(new FileOperation(METADATA_JSON, OUTPUT_FILE_LOCATION), 2)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, OUTPUT_FILE_CREATE_OR_OVERWRITE), 1)
                        .addCopies(new FileOperation(SNAPSHOT, OUTPUT_FILE_LOCATION), 2)
                        .addCopies(new FileOperation(MANIFEST, OUTPUT_FILE_CREATE_OR_OVERWRITE), 1)
                        .addCopies(new FileOperation(MANIFEST, OUTPUT_FILE_LOCATION), 1)
                        .addCopies(new FileOperation(STATS, OUTPUT_FILE_CREATE), 1)
                        .build());
    }

    @Test
    public void testSelect()
    {
        assertUpdate("CREATE TABLE test_select AS SELECT 1 col_name", 1);
        assertFileSystemAccesses("SELECT * FROM test_select",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 1)
                        .build());
    }

    @Test(dataProvider = "testSelectWithLimitDataProvider")
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
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), min(icebergManifestPrefetching, numberOfFiles))
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), min(icebergManifestPrefetching, numberOfFiles))
                        .build());

        assertFileSystemAccesses("EXPLAIN SELECT * FROM test_select_with_limit LIMIT 3",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), numberOfFiles)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), numberOfFiles)
                        .build());

        assertFileSystemAccesses("EXPLAIN ANALYZE SELECT * FROM test_select_with_limit LIMIT 3",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), numberOfFiles + min(icebergManifestPrefetching, numberOfFiles))
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), numberOfFiles + min(icebergManifestPrefetching, numberOfFiles))
                        .build());

        assertUpdate("DROP TABLE test_select_with_limit");
    }

    @DataProvider
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
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v2SnapshotId,
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 1)
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v3SnapshotId,
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 2)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 2)
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName,
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 2)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 2)
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
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v2SnapshotId,
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 1)
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v3SnapshotId,
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 2)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 2)
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName,
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 2)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 2)
                        .build());
    }

    @Test
    public void testSelectWithFilter()
    {
        assertUpdate("CREATE TABLE test_select_with_filter AS SELECT 1 col_name", 1);
        assertFileSystemAccesses("SELECT * FROM test_select_with_filter WHERE col_name = 1",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 1)
                        .build());
    }

    @Test
    public void testJoin()
    {
        assertUpdate("CREATE TABLE test_join_t1 AS SELECT 2 AS age, 'id1' AS id", 1);
        assertUpdate("CREATE TABLE test_join_t2 AS SELECT 'name1' AS name, 'id1' AS id", 1);

        assertFileSystemAccesses("SELECT name, age FROM test_join_t1 JOIN test_join_t2 ON test_join_t2.id = test_join_t1.id",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 2)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 4)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 4)
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
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 2)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 4)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 4)
                        .build());
    }

    @Test
    public void testExplainSelect()
    {
        assertUpdate("CREATE TABLE test_explain AS SELECT 2 AS age", 1);

        assertFileSystemAccesses("EXPLAIN SELECT * FROM test_explain",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 1)
                        .build());
    }

    @Test
    public void testShowStatsForTable()
    {
        assertUpdate("CREATE TABLE test_show_stats AS SELECT 2 AS age", 1);

        assertFileSystemAccesses("SHOW STATS FOR test_show_stats",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 1)
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
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 1)
                        .build());
    }

    @Test
    public void testShowStatsForTableWithFilter()
    {
        assertUpdate("CREATE TABLE test_show_stats_with_filter AS SELECT 2 AS age", 1);

        assertFileSystemAccesses("SHOW STATS FOR (SELECT * FROM test_show_stats_with_filter WHERE age >= 2)",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 1)
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
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 2)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 2)
                        .build());

        // CAST to date and comparison
        assertFileSystemAccesses("SELECT * FROM test_varchar_as_date_predicate WHERE CAST(a AS date) >= DATE '2005-01-01'",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 1) // fewer than without filter
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 1) // fewer than without filter
                        .build());

        // CAST to date and BETWEEN
        assertFileSystemAccesses("SELECT * FROM test_varchar_as_date_predicate WHERE CAST(a AS date) BETWEEN DATE '2005-01-01' AND DATE '2005-12-31'",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 1) // fewer than without filter
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 1) // fewer than without filter
                        .build());

        // conversion to date as a date function
        assertFileSystemAccesses("SELECT * FROM test_varchar_as_date_predicate WHERE date(a) >= DATE '2005-01-01'",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 1) // fewer than without filter
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 1) // fewer than without filter
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
                        .add(new FileOperation(METADATA_JSON, INPUT_FILE_NEW_STREAM))
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_GET_LENGTH), 4)
                        .addCopies(new FileOperation(SNAPSHOT, INPUT_FILE_NEW_STREAM), 4)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_GET_LENGTH), 5)
                        .addCopies(new FileOperation(MANIFEST, INPUT_FILE_NEW_STREAM), 5)
                        .build());

        assertUpdate("DROP TABLE " + tableName);
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<FileOperation> expectedAccesses)
    {
        assertFileSystemAccesses(getSession(), query, expectedAccesses);
    }

    private void assertFileSystemAccesses(Session session, @Language("SQL") String query, Multiset<FileOperation> expectedAccesses)
    {
        resetCounts();
        getDistributedQueryRunner().executeWithQueryId(session, query);
        assertMultisetsEqual(
                getOperations().stream()
                        .filter(operation -> operation.fileType() != DATA)
                        .collect(toImmutableMultiset()),
                expectedAccesses);
    }

    private void resetCounts()
    {
        trackingFileSystemFactory.reset();
    }

    private Multiset<FileOperation> getOperations()
    {
        return trackingFileSystemFactory.getOperationCounts()
                .entrySet().stream()
                .flatMap(entry -> nCopies(entry.getValue(), new FileOperation(
                        fromFilePath(entry.getKey().getLocation().toString()),
                        entry.getKey().getOperationType())).stream())
                .collect(toCollection(HashMultiset::create));
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

    private record FileOperation(FileType fileType, OperationType operationType)
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
            if (path.contains("/data/") && path.endsWith(".orc")) {
                return DATA;
            }
            throw new IllegalArgumentException("File not recognized: " + path);
        }
    }
}
