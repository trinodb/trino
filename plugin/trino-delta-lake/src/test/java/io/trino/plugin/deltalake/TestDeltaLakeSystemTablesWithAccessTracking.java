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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.deltalake.metastore.TestingDeltaLakeMetastoreModule;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestDeltaLakeSystemTablesWithAccessTracking
        extends AbstractTestQueryFramework
{
    protected static final String SCHEMA = "test_delta_lake_system_tables_tracking_" + randomNameSuffix();

    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog("delta_lake")
            .setSchema(SCHEMA)
            .build();

    private File dataDirectory;
    private HiveMetastore metastore;
    private AccessTrackingFileSystemFactory fileSystemFactory;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(TEST_SESSION).build();

        dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake").toFile();
        metastore = createTestingFileHiveMetastore(dataDirectory);
        fileSystemFactory = new AccessTrackingFileSystemFactory(new HdfsFileSystemFactory(HDFS_ENVIRONMENT));

        queryRunner.installPlugin(new TestingDeltaLakePlugin(Optional.of(new TestingDeltaLakeMetastoreModule(metastore)), Optional.of(fileSystemFactory), EMPTY_MODULE));
        queryRunner.createCatalog(
                "delta_lake",
                "delta_lake",
                ImmutableMap.of("delta.enable-non-concurrent-writes", "true"));

        queryRunner.execute("CREATE SCHEMA " + SCHEMA);
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        if (metastore != null) {
            metastore.dropDatabase(SCHEMA, false);
            deleteRecursively(dataDirectory.toPath(), ALLOW_INSECURE);
        }
    }

    @Test
    public void testHistoryTableVersionFilterPushdown()
    {
        try {
            assertUpdate("CREATE TABLE test_simple_table_pushdown_version_filter (_bigint BIGINT)");
            assertUpdate("INSERT INTO test_simple_table_pushdown_version_filter VALUES 1", 1);
            assertUpdate("INSERT INTO test_simple_table_pushdown_version_filter VALUES 2", 1);
            assertUpdate("INSERT INTO test_simple_table_pushdown_version_filter VALUES 3", 1);
            assertUpdate("INSERT INTO test_simple_table_pushdown_version_filter VALUES 4", 1);
            assertQuerySucceeds("ALTER TABLE test_simple_table_pushdown_version_filter EXECUTE OPTIMIZE");

            fileSystemFactory.resetOpenCount();
            // equal
            assertThat(query("SELECT version, operation FROM \"test_simple_table_pushdown_version_filter$history\" WHERE version = 0 AND operation = 'CREATE TABLE'"))
                    .matches("VALUES (BIGINT '0', VARCHAR 'CREATE TABLE')");
            assertThat(fileSystemFactory.getOpenCount()).containsExactlyInAnyOrderEntriesOf(
                    ImmutableMap.of(
                            "00000000000000000000.json", 1,
                            "00000000000000000005.checkpoint.parquet", 1,
                            "00000000000000000006.json", 16,
                            "_last_checkpoint", 17));
            fileSystemFactory.resetOpenCount();
            // less than
            assertThat(query("SELECT version, operation FROM \"test_simple_table_pushdown_version_filter$history\" WHERE version < 1"))
                    .matches("VALUES (BIGINT '0', VARCHAR 'CREATE TABLE')");
            assertThat(fileSystemFactory.getOpenCount()).containsExactlyInAnyOrderEntriesOf(
                    ImmutableMap.of(
                            "00000000000000000000.json", 1,
                            "00000000000000000006.json", 16,
                            "_last_checkpoint", 17));
            fileSystemFactory.resetOpenCount();
            // less than value greater than the number of transactions
            assertThat(query("SELECT version, operation FROM \"test_simple_table_pushdown_version_filter$history\" WHERE version < 10"))
                    .matches("""
                            VALUES
                                (BIGINT '0', VARCHAR 'CREATE TABLE'),
                                (BIGINT '1', VARCHAR 'WRITE'),
                                (BIGINT '2', VARCHAR 'WRITE'),
                                (BIGINT '3', VARCHAR 'WRITE'),
                                (BIGINT '4', VARCHAR 'WRITE'),
                                (BIGINT '5', VARCHAR 'OPTIMIZE')
                            """);
            assertThat(fileSystemFactory.getOpenCount()).containsExactlyInAnyOrderEntriesOf(
                    ImmutableMap.of(
                            "00000000000000000000.json", 1,
                            "00000000000000000001.json", 1,
                            "00000000000000000002.json", 1,
                            "00000000000000000003.json", 1,
                            "00000000000000000004.json", 1,
                            "00000000000000000005.json", 1,
                            "00000000000000000006.json", 17,
                            "_last_checkpoint", 17));
            fileSystemFactory.resetOpenCount();
            // less than or equal
            assertThat(query("SELECT version, operation FROM \"test_simple_table_pushdown_version_filter$history\" WHERE version <= 1"))
                    .matches("""
                            VALUES
                                (BIGINT '0', VARCHAR 'CREATE TABLE'),
                                (BIGINT '1', VARCHAR 'WRITE')
                            """);
            assertThat(fileSystemFactory.getOpenCount()).containsExactlyInAnyOrderEntriesOf(
                    ImmutableMap.of(
                            "00000000000000000000.json", 1,
                            "00000000000000000001.json", 1,
                            "00000000000000000006.json", 16,
                            "_last_checkpoint", 17));
            fileSystemFactory.resetOpenCount();
            // greater than
            assertThat(query("SELECT version, operation FROM \"test_simple_table_pushdown_version_filter$history\" WHERE version > 2"))
                    .matches("""
                            VALUES
                                (BIGINT '3', VARCHAR 'WRITE'),
                                (BIGINT '4', VARCHAR 'WRITE'),
                                (BIGINT '5', VARCHAR 'OPTIMIZE')
                            """);
            assertThat(fileSystemFactory.getOpenCount()).containsExactlyInAnyOrderEntriesOf(
                    ImmutableMap.of(
                            "00000000000000000003.json", 1,
                            "00000000000000000004.json", 1,
                            "00000000000000000005.json", 1,
                            "00000000000000000006.json", 17,
                            "_last_checkpoint", 17));
            fileSystemFactory.resetOpenCount();
            // greater than or equal
            assertThat(query("SELECT version, operation FROM \"test_simple_table_pushdown_version_filter$history\" WHERE version >= 5"))
                    .matches("VALUES (BIGINT '5', VARCHAR 'OPTIMIZE')");
            assertThat(fileSystemFactory.getOpenCount()).containsExactlyInAnyOrderEntriesOf(
                    ImmutableMap.of(
                            "00000000000000000005.json", 1,
                            "00000000000000000006.json", 17,
                            "_last_checkpoint", 17));
            fileSystemFactory.resetOpenCount();
            // between
            assertThat(query("SELECT version, operation FROM \"test_simple_table_pushdown_version_filter$history\" WHERE version BETWEEN 1 AND 2"))
                    .matches("""
                            VALUES
                                (BIGINT '1', VARCHAR 'WRITE'),
                                (BIGINT '2', VARCHAR 'WRITE')
                            """);
            assertThat(fileSystemFactory.getOpenCount()).containsExactlyInAnyOrderEntriesOf(
                    ImmutableMap.of(
                            "00000000000000000001.json", 1,
                            "00000000000000000002.json", 1,
                            "00000000000000000006.json", 16,
                            "_last_checkpoint", 17));
            fileSystemFactory.resetOpenCount();
            assertThat(query("SELECT version, operation FROM \"test_simple_table_pushdown_version_filter$history\" WHERE version >= 1 AND version <= 2"))
                    .matches("""
                            VALUES
                                (BIGINT '1', VARCHAR 'WRITE'),
                                (BIGINT '2', VARCHAR 'WRITE')
                            """);
            assertThat(fileSystemFactory.getOpenCount()).containsExactlyInAnyOrderEntriesOf(
                    ImmutableMap.of(
                            "00000000000000000001.json", 1,
                            "00000000000000000002.json", 1,
                            "00000000000000000006.json", 16,
                            "_last_checkpoint", 17));
            fileSystemFactory.resetOpenCount();
            // multiple ranges
            assertThat(query("SELECT version, operation FROM \"test_simple_table_pushdown_version_filter$history\" WHERE version = 1 OR version = 2 OR version = 4"))
                    .matches("""
                            VALUES
                                (BIGINT '1', VARCHAR 'WRITE'),
                                (BIGINT '2', VARCHAR 'WRITE'),
                                (BIGINT '4', VARCHAR 'WRITE')
                            """);
            assertThat(fileSystemFactory.getOpenCount()).containsExactlyInAnyOrderEntriesOf(
                    ImmutableMap.of(
                            "00000000000000000000.json", 1,
                            "00000000000000000001.json", 1,
                            "00000000000000000002.json", 1,
                            "00000000000000000003.json", 1,
                            "00000000000000000004.json", 1,
                            "00000000000000000005.json", 1,
                            "00000000000000000006.json", 17,
                            "_last_checkpoint", 17));
            fileSystemFactory.resetOpenCount();
            // none domain
            assertThat(query("SELECT version, operation FROM \"test_simple_table_pushdown_version_filter$history\" WHERE version IS NULL"))
                    .returnsEmptyResult();
            assertThat(fileSystemFactory.getOpenCount()).containsExactlyInAnyOrderEntriesOf(
                    ImmutableMap.of(
                            "00000000000000000000.json", 1,
                            "00000000000000000001.json", 1,
                            "00000000000000000002.json", 1,
                            "00000000000000000003.json", 1,
                            "00000000000000000004.json", 1,
                            "00000000000000000005.json", 1,
                            "00000000000000000006.json", 17,
                            "_last_checkpoint", 17));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_simple_table_pushdown_version_filter");
        }
    }
}
