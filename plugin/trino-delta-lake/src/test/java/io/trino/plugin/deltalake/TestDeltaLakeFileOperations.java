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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.Session;
import io.trino.filesystem.TrackingFileSystemFactory;
import io.trino.filesystem.TrackingFileSystemFactory.OperationType;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_GET_LENGTH;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_NEW_STREAM;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.CDF_DATA;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.DATA;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.LAST_CHECKPOINT;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.TRANSACTION_LOG_JSON;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.TRINO_EXTENDED_STATS_JSON;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.hive.util.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.Math.toIntExact;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

// single-threaded AccessTrackingFileSystemFactory is shared mutable state
@Test(singleThreaded = true)
public class TestDeltaLakeFileOperations
        extends AbstractTestQueryFramework
{
    private TrackingFileSystemFactory trackingFileSystemFactory;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("delta_lake")
                .setSchema("default")
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .build();
        try {
            String metastoreDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_metastore").toFile().getAbsoluteFile().toURI().toString();
            trackingFileSystemFactory = new TrackingFileSystemFactory(new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS));

            queryRunner.installPlugin(new TestingDeltaLakePlugin(Optional.empty(), Optional.of(trackingFileSystemFactory), EMPTY_MODULE));
            queryRunner.createCatalog(
                    "delta_lake",
                    "delta_lake",
                    Map.of(
                            "hive.metastore", "file",
                            "hive.metastore.catalog.dir", metastoreDirectory,
                            "delta.enable-non-concurrent-writes", "true"));

            queryRunner.execute("CREATE SCHEMA " + session.getSchema().orElseThrow());
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    public void testReadWholePartition()
    {
        assertUpdate("DROP TABLE IF EXISTS test_read_part_key");
        assertUpdate("CREATE TABLE test_read_part_key(key varchar, data varchar) WITH (partitioned_by=ARRAY['key'])");

        // Create multiple files per partition
        assertUpdate("INSERT INTO test_read_part_key(key, data) VALUES ('p1', '1-abc'), ('p1', '1-def'), ('p2', '2-abc'), ('p2', '2-def')", 4);
        assertUpdate("INSERT INTO test_read_part_key(key, data) VALUES ('p1', '1-baz'), ('p2', '2-baz')", 2);

        // Read partition and data columns
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_read_part_key')");
        assertFileSystemAccesses(
                "SELECT key, max(data) FROM test_read_part_key GROUP BY key",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 3) // TODO (https://github.com/trinodb/trino/issues/16782) should be checked once per query
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM), 3) // TODO (https://github.com/trinodb/trino/issues/16780) why is last transaction log accessed more times than others?
                        .addCopies(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(DATA, "key=p1/", INPUT_FILE_GET_LENGTH), 2)
                        .addCopies(new FileOperation(DATA, "key=p2/", INPUT_FILE_GET_LENGTH), 2)
                        .addCopies(new FileOperation(DATA, "key=p1/", INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(DATA, "key=p2/", INPUT_FILE_NEW_STREAM), 2)
                        .build());

        // Read partition column only
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_read_part_key')");
        assertFileSystemAccesses(
                "SELECT key, count(*) FROM test_read_part_key GROUP BY key",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 3) // TODO (https://github.com/trinodb/trino/issues/16782) should be checked once per query
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM), 3) // TODO (https://github.com/trinodb/trino/issues/16780) why is last transaction log accessed more times than others?
                        .addCopies(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM), 1)
                        .build());

        // Read partition column only, one partition only
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_read_part_key')");
        assertFileSystemAccesses(
                "SELECT count(*) FROM test_read_part_key WHERE key = 'p1'",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 2) // TODO (https://github.com/trinodb/trino/issues/16782) should be checked once per query
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM), 2) // TODO (https://github.com/trinodb/trino/issues/16780) why is last transaction log accessed more times than others?
                        .build());

        // Read partition and synthetic columns
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_read_part_key')");
        assertFileSystemAccesses(
                "SELECT count(*), array_agg(\"$path\"), max(\"$file_modified_time\") FROM test_read_part_key GROUP BY key",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 3) // TODO (https://github.com/trinodb/trino/issues/16782) should be checked once per query
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM), 3) // TODO (https://github.com/trinodb/trino/issues/16780) why is last transaction log accessed more times than others?
                        .addCopies(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM), 1)
                        .build());

        assertUpdate("DROP TABLE test_read_part_key");
    }

    @Test
    public void testTableChangesFileSystemAccess()
            throws URISyntaxException
    {
        assertUpdate("CREATE TABLE table_changes_file_system_access (page_url VARCHAR, key VARCHAR, views INTEGER) WITH (change_data_feed_enabled = true, partitioned_by=ARRAY['key'])");
        assertUpdate("INSERT INTO table_changes_file_system_access VALUES('url1', 'domain1', 1)", 1);
        assertUpdate("INSERT INTO table_changes_file_system_access VALUES('url2', 'domain2', 2)", 1);
        assertUpdate("INSERT INTO table_changes_file_system_access VALUES('url3', 'domain3', 3)", 1);
        assertUpdate("UPDATE table_changes_file_system_access SET page_url = 'url22' WHERE key = 'domain2'", 1);
        assertUpdate("UPDATE table_changes_file_system_access SET page_url = 'url33' WHERE views = 3", 1);
        assertUpdate("DELETE FROM table_changes_file_system_access WHERE page_url = 'url1'", 1);

        // The difference comes from the fact that during UPDATE queries there is no guarantee that rows that are going to be deleted and
        // rows that are going to be inserted come on the same worker to io.trino.plugin.deltalake.DeltaLakeMergeSink.storeMergedRows
        int cdfFilesForDomain2 = countCdfFilesForKey("domain2");
        int cdfFilesForDomain3 = countCdfFilesForKey("domain3");
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'table_changes_file_system_access')");
        assertFileSystemAccesses("SELECT * FROM TABLE(system.table_changes('default', 'table_changes_file_system_access'))",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000005.json", INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000006.json", INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000007.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(CDF_DATA, "key=domain1/", INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(CDF_DATA, "key=domain2/", INPUT_FILE_GET_LENGTH), cdfFilesForDomain2)
                        .addCopies(new FileOperation(CDF_DATA, "key=domain3/", INPUT_FILE_GET_LENGTH), cdfFilesForDomain3)
                        .addCopies(new FileOperation(CDF_DATA, "key=domain1/", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(CDF_DATA, "key=domain2/", INPUT_FILE_NEW_STREAM), cdfFilesForDomain2)
                        .addCopies(new FileOperation(CDF_DATA, "key=domain3/", INPUT_FILE_NEW_STREAM), cdfFilesForDomain3)
                        .addCopies(new FileOperation(DATA, "key=domain1/", INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(DATA, "key=domain2/", INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(DATA, "key=domain3/", INPUT_FILE_GET_LENGTH), 1)
                        .addCopies(new FileOperation(DATA, "key=domain1/", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(DATA, "key=domain2/", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(DATA, "key=domain3/", INPUT_FILE_NEW_STREAM), 1)
                        .build());
    }

    private int countCdfFilesForKey(String partitionValue)
            throws URISyntaxException
    {
        String path = (String) computeScalar("SELECT \"$path\" FROM table_changes_file_system_access WHERE key = '" + partitionValue + "'");
        String partitionKey = "key=" + partitionValue;
        String tableLocation = path.substring(0, path.lastIndexOf(partitionKey));
        String partitionCdfFolder = new URI(tableLocation).getPath() + "_change_data/" + partitionKey + "/";
        return toIntExact(Arrays.stream(new File(partitionCdfFolder).list()).filter(file -> !file.contains(".crc")).count());
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<FileOperation> expectedAccesses)
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        trackingFileSystemFactory.reset();
        queryRunner.executeWithQueryId(queryRunner.getDefaultSession(), query);
        assertMultisetsEqual(getOperations(), expectedAccesses);
    }

    private Multiset<FileOperation> getOperations()
    {
        return trackingFileSystemFactory.getOperationCounts()
                .entrySet().stream()
                .flatMap(entry -> nCopies(entry.getValue(), FileOperation.create(
                        entry.getKey().getLocation().path(),
                        entry.getKey().getOperationType())).stream())
                .collect(toCollection(HashMultiset::create));
    }

    private record FileOperation(FileType fileType, String fileId, OperationType operationType)
    {
        public static FileOperation create(String path, OperationType operationType)
        {
            Pattern dataFilePattern = Pattern.compile(".*/(?<partition>key=[^/]*/)(?<queryId>\\d{8}_\\d{6}_\\d{5}_\\w{5})-(?<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");
            String fileName = path.replaceFirst(".*/", "");
            if (path.matches(".*/_delta_log/_last_checkpoint")) {
                return new FileOperation(LAST_CHECKPOINT, fileName, operationType);
            }
            if (path.matches(".*/_delta_log/\\d+\\.json")) {
                return new FileOperation(TRANSACTION_LOG_JSON, fileName, operationType);
            }
            if (path.matches(".*/_delta_log/_trino_meta/extended_stats.json")) {
                return new FileOperation(TRINO_EXTENDED_STATS_JSON, fileName, operationType);
            }
            if (path.matches(".*/_change_data/.*")) {
                Matcher matcher = dataFilePattern.matcher(path);
                if (matcher.matches()) {
                    return new FileOperation(CDF_DATA, matcher.group("partition"), operationType);
                }
            }
            if (!path.contains("_delta_log")) {
                Matcher matcher = dataFilePattern.matcher(path);
                if (matcher.matches()) {
                    return new FileOperation(DATA, matcher.group("partition"), operationType);
                }
            }
            throw new IllegalArgumentException("File not recognized: " + path);
        }

        public FileOperation
        {
            requireNonNull(fileType, "fileType is null");
            requireNonNull(fileId, "fileId is null");
            requireNonNull(operationType, "operationType is null");
        }
    }

    enum FileType
    {
        LAST_CHECKPOINT,
        TRANSACTION_LOG_JSON,
        TRINO_EXTENDED_STATS_JSON,
        DATA,
        CDF_DATA,
        /**/;
    }
}
