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

import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_EXISTS;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_GET_LENGTH;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_NEW_STREAM;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.DATA;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.LAST_CHECKPOINT;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.TRANSACTION_LOG_JSON;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.TRINO_EXTENDED_STATS_JSON;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;

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
            trackingFileSystemFactory = new TrackingFileSystemFactory(new HdfsFileSystemFactory(HDFS_ENVIRONMENT));

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
                        .addCopies(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_EXISTS), 1)
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
                        .addCopies(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_EXISTS), 1)
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
                        .addCopies(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_EXISTS), 1)
                        .addCopies(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM), 1)
                        .build());

        assertUpdate("DROP TABLE test_read_part_key");
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<FileOperation> expectedAccesses)
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        trackingFileSystemFactory.reset();
        queryRunner.executeWithQueryId(queryRunner.getDefaultSession(), query);
        assertThat(getOperations())
                .containsExactlyInAnyOrderElementsOf(expectedAccesses);
    }

    private Multiset<FileOperation> getOperations()
    {
        return trackingFileSystemFactory.getOperationCounts()
                .entrySet().stream()
                .flatMap(entry -> nCopies(entry.getValue(), FileOperation.create(
                        entry.getKey().getFilePath(),
                        entry.getKey().getOperationType())).stream())
                .collect(toCollection(HashMultiset::create));
    }

    private record FileOperation(FileType fileType, String fileId, OperationType operationType)
    {
        public static FileOperation create(String path, OperationType operationType)
        {
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
            if (!path.contains("_delta_log")) {
                Matcher matcher = Pattern.compile(".*/(?<partition>key=[^/]*/)(?<queryId>\\d{8}_\\d{6}_\\d{5}_\\w{5})-(?<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})")
                        .matcher(path);
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
        /**/;
    }
}
