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
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.alluxio.AlluxioFileSystemCacheConfig;
import io.trino.filesystem.alluxio.AlluxioFileSystemCacheModule;
import io.trino.filesystem.alluxio.TestingAlluxioFileSystemCache;
import io.trino.filesystem.cache.CacheFileSystemFactory;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.filesystem.cache.NoneCachingHostAddressProvider;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.deltalake.cache.DeltaLakeCacheKeyProvider;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.inject.Scopes.SINGLETON;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_GET_LENGTH;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_NEW_STREAM;
import static io.trino.filesystem.alluxio.TestingAlluxioFileSystemCache.OperationType.CACHE_READ;
import static io.trino.filesystem.alluxio.TestingAlluxioFileSystemCache.OperationType.EXTERNAL_READ;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.deltalake.TestDeltaLakeAlluxioCacheFileOperations.FileType.CDF_DATA;
import static io.trino.plugin.deltalake.TestDeltaLakeAlluxioCacheFileOperations.FileType.CHECKPOINT;
import static io.trino.plugin.deltalake.TestDeltaLakeAlluxioCacheFileOperations.FileType.DATA;
import static io.trino.plugin.deltalake.TestDeltaLakeAlluxioCacheFileOperations.FileType.LAST_CHECKPOINT;
import static io.trino.plugin.deltalake.TestDeltaLakeAlluxioCacheFileOperations.FileType.TRANSACTION_LOG_JSON;
import static io.trino.plugin.deltalake.TestDeltaLakeAlluxioCacheFileOperations.FileType.TRINO_EXTENDED_STATS_JSON;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

// single-threaded AccessTrackingFileSystemFactory is shared mutable state
@Execution(ExecutionMode.SAME_THREAD)
public class TestDeltaLakeAlluxioCacheFileOperations
        extends AbstractTestQueryFramework
{
    private TrackingFileSystemFactory trackingFileSystemFactory;
    private TestingAlluxioFileSystemCache alluxioFileSystemCache;

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
            File metastoreDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_metastore").toFile().getAbsoluteFile();
            trackingFileSystemFactory = new TrackingFileSystemFactory(new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS));
            AlluxioFileSystemCacheConfig alluxioFileSystemCacheConfiguration = new AlluxioFileSystemCacheConfig()
                    .setCacheDirectories(metastoreDirectory.getAbsolutePath() + "/cache")
                    .disableTTL()
                    .setMaxCacheSizes("100MB");
            alluxioFileSystemCache = new TestingAlluxioFileSystemCache(AlluxioFileSystemCacheModule.getAlluxioConfiguration(alluxioFileSystemCacheConfiguration), new DeltaLakeCacheKeyProvider());
            TrinoFileSystemFactory fileSystemFactory = new CacheFileSystemFactory(trackingFileSystemFactory, alluxioFileSystemCache, alluxioFileSystemCache.getCacheKeyProvider());

            Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data");
            queryRunner.installPlugin(new TestingDeltaLakePlugin(dataDirectory, Optional.empty(), Optional.of(fileSystemFactory), binder -> binder.bind(CachingHostAddressProvider.class).to(NoneCachingHostAddressProvider.class).in(SINGLETON)));
            queryRunner.createCatalog(
                    "delta_lake",
                    "delta_lake",
                    Map.of(
                            "hive.metastore", "file",
                            "hive.metastore.catalog.dir", metastoreDirectory.toURI().toString(),
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
    public void testCacheFileOperations()
    {
        assertUpdate("DROP TABLE IF EXISTS test_cache_file_operations");
        assertUpdate("CREATE TABLE test_cache_file_operations(key varchar, data varchar) with (partitioned_by=ARRAY['key'])");
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p1', '1-abc')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p2', '2-xyz')", 1);
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_cache_file_operations')");
        alluxioFileSystemCache.clear();
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(DATA, "key=p1/", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(DATA, "key=p2/", INPUT_FILE_NEW_STREAM), 1)
                        // All data cached when reading parquet file footers to collect statistics when writing
                        .build(),
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation(EXTERNAL_READ, "key=p1/"), 1)
                        .addCopies(new CacheOperation(EXTERNAL_READ, "key=p2/"), 1)
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM), 1)
                        .build(),
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation(CACHE_READ, "key=p1/"), 1)
                        .addCopies(new CacheOperation(CACHE_READ, "key=p2/"), 1)
                        .build());
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p3', '3-xyz')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p4', '4-xyz')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p5', '5-xyz')", 1);
        alluxioFileSystemCache.clear();
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000005.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000006.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(DATA, "key=p1/", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(DATA, "key=p2/", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(DATA, "key=p3/", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(DATA, "key=p4/", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(DATA, "key=p5/", INPUT_FILE_NEW_STREAM), 1)
                        // All data cached when reading parquet file footers to collect statistics when writing
                        .build(),
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation(EXTERNAL_READ, "key=p1/"), 1)
                        .addCopies(new CacheOperation(EXTERNAL_READ, "key=p2/"), 1)
                        .addCopies(new CacheOperation(EXTERNAL_READ, "key=p3/"), 1)
                        .addCopies(new CacheOperation(EXTERNAL_READ, "key=p4/"), 1)
                        .addCopies(new CacheOperation(EXTERNAL_READ, "key=p5/"), 1)
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000005.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000006.json", INPUT_FILE_NEW_STREAM), 1)
                        .build(),
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation(CACHE_READ, "key=p1/"), 1)
                        .addCopies(new CacheOperation(CACHE_READ, "key=p2/"), 1)
                        .addCopies(new CacheOperation(CACHE_READ, "key=p3/"), 1)
                        .addCopies(new CacheOperation(CACHE_READ, "key=p4/"), 1)
                        .addCopies(new CacheOperation(CACHE_READ, "key=p5/"), 1)
                        .build());
    }

    @Test
    public void testCacheCheckpointFileOperations()
    {
        assertUpdate("DROP TABLE IF EXISTS test_checkpoint_file_operations");
        assertUpdate("CREATE TABLE test_checkpoint_file_operations(key varchar, data varchar) with (checkpoint_interval = 2, partitioned_by=ARRAY['key'])");
        assertUpdate("INSERT INTO test_checkpoint_file_operations VALUES ('p1', '1-abc')", 1);
        assertUpdate("INSERT INTO test_checkpoint_file_operations VALUES ('p2', '2-xyz')", 1);
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_checkpoint_file_operations')");
        alluxioFileSystemCache.clear();
        assertFileSystemAccesses(
                "SELECT * FROM test_checkpoint_file_operations",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", INPUT_FILE_GET_LENGTH), 4)
                        .addCopies(new FileOperation(DATA, "key=p1/", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(DATA, "key=p2/", INPUT_FILE_NEW_STREAM), 1)
                        // All data cached when reading parquet file footers to collect statistics when writing
                        .build(),
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation(EXTERNAL_READ, "key=p1/"), 1)
                        .addCopies(new CacheOperation(EXTERNAL_READ, "key=p2/"), 1)
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_checkpoint_file_operations",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", INPUT_FILE_GET_LENGTH), 4)
                        .build(),
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation(CACHE_READ, "key=p1/"), 1)
                        .addCopies(new CacheOperation(CACHE_READ, "key=p2/"), 1)
                        .build());
        assertUpdate("INSERT INTO test_checkpoint_file_operations VALUES ('p3', '3-xyz')", 1);
        assertUpdate("INSERT INTO test_checkpoint_file_operations VALUES ('p4', '4-xyz')", 1);
        assertUpdate("INSERT INTO test_checkpoint_file_operations VALUES ('p5', '5-xyz')", 1);
        alluxioFileSystemCache.clear();
        assertFileSystemAccesses(
                "SELECT * FROM test_checkpoint_file_operations",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000005.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000006.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000004.checkpoint.parquet", INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000004.checkpoint.parquet", INPUT_FILE_GET_LENGTH), 4)
                        .addCopies(new FileOperation(DATA, "key=p1/", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(DATA, "key=p2/", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(DATA, "key=p3/", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(DATA, "key=p4/", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(DATA, "key=p5/", INPUT_FILE_NEW_STREAM), 1)
                        // All data cached when reading parquet file footers to collect statistics when writing
                        .build(),
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation(EXTERNAL_READ, "key=p1/"), 1)
                        .addCopies(new CacheOperation(EXTERNAL_READ, "key=p2/"), 1)
                        .addCopies(new CacheOperation(EXTERNAL_READ, "key=p3/"), 1)
                        .addCopies(new CacheOperation(EXTERNAL_READ, "key=p4/"), 1)
                        .addCopies(new CacheOperation(EXTERNAL_READ, "key=p5/"), 1)
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_checkpoint_file_operations",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000005.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000006.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000004.checkpoint.parquet", INPUT_FILE_NEW_STREAM), 2)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000004.checkpoint.parquet", INPUT_FILE_GET_LENGTH), 4)
                        .build(),
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation(CACHE_READ, "key=p1/"), 1)
                        .addCopies(new CacheOperation(CACHE_READ, "key=p2/"), 1)
                        .addCopies(new CacheOperation(CACHE_READ, "key=p3/"), 1)
                        .addCopies(new CacheOperation(CACHE_READ, "key=p4/"), 1)
                        .addCopies(new CacheOperation(CACHE_READ, "key=p5/"), 1)
                        .build());
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<FileOperation> expectedAccesses, Multiset<CacheOperation> expectedCacheAccesses)
    {
        assertUpdate("CALL system.flush_metadata_cache()");
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        trackingFileSystemFactory.reset();
        alluxioFileSystemCache.reset();
        queryRunner.executeWithPlan(queryRunner.getDefaultSession(), query);
        assertMultisetsEqual(getOperations(), expectedAccesses);
        assertMultisetsEqual(getCacheOperations(), expectedCacheAccesses);
    }

    private Multiset<CacheOperation> getCacheOperations()
    {
        return alluxioFileSystemCache.getOperationCounts()
                .entrySet().stream()
                .filter(entry -> {
                    String path = entry.getKey().location().path();
                    return !path.endsWith(".trinoSchema") && !path.contains(".trinoPermissions");
                })
                .flatMap(entry -> nCopies((int) entry.getValue().stream().filter(l -> l > 0).count(), CacheOperation.create(
                        entry.getKey().type(),
                        entry.getKey().location().path())).stream())
                .collect(toCollection(HashMultiset::create));
    }

    private static Pattern dataFilePattern = Pattern.compile(".*?/(?<partition>key=[^/]*/)?(?<queryId>\\d{8}_\\d{6}_\\d{5}_\\w{5})_(?<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");

    private record CacheOperation(TestingAlluxioFileSystemCache.OperationType type, String fileId)
    {
        public static CacheOperation create(TestingAlluxioFileSystemCache.OperationType operationType, String path)
        {
            String fileName = path.replaceFirst(".*/", "");
            if (!path.contains("_delta_log") && !path.contains("/.trino")) {
                Matcher matcher = dataFilePattern.matcher(path);
                if (matcher.matches()) {
                    return new CacheOperation(operationType, matcher.group("partition"));
                }
            }
            else {
                return new CacheOperation(operationType, fileName);
            }
            throw new IllegalArgumentException("File not recognized: " + path);
        }
    }

    private Multiset<FileOperation> getOperations()
    {
        return trackingFileSystemFactory.getOperationCounts()
                .entrySet().stream()
                .filter(entry -> {
                    String path = entry.getKey().location().path();
                    return !path.endsWith(".trinoSchema") && !path.contains(".trinoPermissions");
                })
                .flatMap(entry -> nCopies(entry.getValue(), FileOperation.create(
                        entry.getKey().location().path(),
                        entry.getKey().operationType())).stream())
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
            if (path.matches(".*/_delta_log/\\d+\\.checkpoint.parquet")) {
                return new FileOperation(CHECKPOINT, fileName, operationType);
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
        CHECKPOINT,
        TRINO_EXTENDED_STATS_JSON,
        DATA,
        CDF_DATA,
        /**/;
    }
}
