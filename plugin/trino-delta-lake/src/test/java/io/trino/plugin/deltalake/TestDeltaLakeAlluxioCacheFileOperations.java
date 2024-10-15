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
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import com.google.common.io.Resources;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.CacheOperation;
import static io.trino.plugin.deltalake.DeltaLakeAlluxioCacheTestUtils.getCacheOperations;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.copyDirectoryContents;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.lang.String.format;

@Execution(ExecutionMode.SAME_THREAD)
public class TestDeltaLakeAlluxioCacheFileOperations
        extends AbstractTestQueryFramework
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Path cacheDirectory = Files.createTempDirectory("cache");
        closeAfterClass(() -> deleteRecursively(cacheDirectory, ALLOW_INSECURE));

        return DeltaLakeQueryRunner.builder()
                .setCoordinatorProperties(ImmutableMap.of("node-scheduler.include-coordinator", "false"))
                .setDeltaProperties(ImmutableMap.<String, String>builder()
                        .put("fs.cache.enabled", "true")
                        .put("fs.cache.directories", cacheDirectory.toAbsolutePath().toString())
                        .put("fs.cache.max-sizes", "100MB")
                        .put("delta.enable-non-concurrent-writes", "true")
                        .put("delta.register-table-procedure.enabled", "true")
                        .buildOrThrow())
                .setWorkerCount(1)
                .build();
    }

    private URL getResourceLocation(String resourcePath)
    {
        return getClass().getClassLoader().getResource(resourcePath);
    }

    private void registerTable(String name, String resourcePath)
    {
        String dataPath = getResourceLocation(resourcePath).toExternalForm();
        getQueryRunner().execute(format("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')", name, dataPath));
    }

    @Test
    public void testCacheFileOperations()
    {
        assertUpdate("DROP TABLE IF EXISTS test_cache_file_operations");
        assertUpdate("CREATE TABLE test_cache_file_operations(key varchar, data varchar) with (partitioned_by=ARRAY['key'])");
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p1', '1-abc')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p2', '2-xyz')", 1);
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_cache_file_operations')");
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 794))
                        .add(new CacheOperation("InputFile.length", "00000000000000000000.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 636))
                        .add(new CacheOperation("InputFile.length", "00000000000000000001.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.json", 0, 636))
                        .add(new CacheOperation("InputFile.length", "00000000000000000002.json"))
                        .add(new CacheOperation("Alluxio.readExternalStream", "00000000000000000002.json", 0, 636))
                        .add(new CacheOperation("InputFile.newStream", "00000000000000000002.json"))
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000002.json", 0, 636))
                        .add(new CacheOperation("InputFile.length", "00000000000000000003.json"))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 220))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 220))
                        .add(new CacheOperation("Input.readFully", "key=p1/", 0, 220))
                        .add(new CacheOperation("Input.readFully", "key=p2/", 0, 220))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p1/", 0, 220))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p2/", 0, 220))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 794))
                        .add(new CacheOperation("InputFile.length", "00000000000000000000.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 636))
                        .add(new CacheOperation("InputFile.length", "00000000000000000001.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.json", 0, 636))
                        .add(new CacheOperation("InputFile.length", "00000000000000000002.json"))
                        .add(new CacheOperation("InputFile.length", "00000000000000000003.json"))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 220))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 220))
                        .build());
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p3', '3-xyz')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p4', '4-xyz')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p5', '5-xyz')", 1);
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 794))
                        .add(new CacheOperation("InputFile.length", "00000000000000000000.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 636))
                        .add(new CacheOperation("InputFile.length", "00000000000000000001.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.json", 0, 636))
                        .add(new CacheOperation("InputFile.length", "00000000000000000002.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000003.json", 0, 636))
                        .add(new CacheOperation("InputFile.length", "00000000000000000003.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000004.json", 0, 636))
                        .add(new CacheOperation("InputFile.length", "00000000000000000004.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000005.json", 0, 636))
                        .add(new CacheOperation("Alluxio.readExternalStream", "00000000000000000005.json", 0, 636))
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000005.json", 0, 636))
                        .add(new CacheOperation("InputFile.newStream", "00000000000000000005.json"))
                        .add(new CacheOperation("InputFile.length", "00000000000000000005.json"))
                        .add(new CacheOperation("InputFile.length", "00000000000000000006.json"))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 220))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 220))
                        .add(new CacheOperation("Alluxio.readCached", "key=p3/", 0, 220))
                        .add(new CacheOperation("Alluxio.readCached", "key=p4/", 0, 220))
                        .add(new CacheOperation("Alluxio.readCached", "key=p5/", 0, 220))
                        .add(new CacheOperation("Input.readFully", "key=p3/", 0, 220))
                        .add(new CacheOperation("Input.readFully", "key=p4/", 0, 220))
                        .add(new CacheOperation("Input.readFully", "key=p5/", 0, 220))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p3/", 0, 220))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p4/", 0, 220))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p5/", 0, 220))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 794))
                        .add(new CacheOperation("InputFile.length", "00000000000000000000.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 636))
                        .add(new CacheOperation("InputFile.length", "00000000000000000001.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.json", 0, 636))
                        .add(new CacheOperation("InputFile.length", "00000000000000000002.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000003.json", 0, 636))
                        .add(new CacheOperation("InputFile.length", "00000000000000000003.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000004.json", 0, 636))
                        .add(new CacheOperation("InputFile.length", "00000000000000000004.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000005.json", 0, 636))
                        .add(new CacheOperation("InputFile.length", "00000000000000000005.json"))
                        .add(new CacheOperation("InputFile.length", "00000000000000000006.json"))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .addCopies(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 220), 1)
                        .addCopies(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 220), 1)
                        .addCopies(new CacheOperation("Alluxio.readCached", "key=p3/", 0, 220), 1)
                        .addCopies(new CacheOperation("Alluxio.readCached", "key=p4/", 0, 220), 1)
                        .addCopies(new CacheOperation("Alluxio.readCached", "key=p5/", 0, 220), 1)
                        .build());
    }

    @Test
    public void testCacheCheckpointAndExtendedStatsFileOperations()
    {
        registerTable("checkpoint_and_extended_stats", "trino432/partition_values_parsed");
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'checkpoint_and_extended_stats')");
        assertFileSystemAccesses(
                "SELECT * FROM checkpoint_and_extended_stats",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000003.checkpoint.parquet", 0, 7077), 2)
                        .addCopies(new CacheOperation("InputFile.length", "00000000000000000003.checkpoint.parquet"), 2)
                        .add(new CacheOperation("InputFile.length", "00000000000000000004.json"))
                        .addAll(Stream.of("int_part=10/string_part=part1/", "int_part=20/string_part=part2/", "int_part=__HIVE_DEFAULT_PARTITION__/string_part=__HIVE_DEFAULT_PARTITION__/")
                                .flatMap(fileId ->
                                        Stream.of(
                                                new CacheOperation("Alluxio.readCached", fileId, 0, 199),
                                                new CacheOperation("Input.readFully", fileId, 0, 199),
                                                new CacheOperation("Alluxio.writeCache", fileId, 0, 199)))
                                .iterator())
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .build());
        assertFileSystemAccesses(
                "EXPLAIN ANALYZE SELECT * FROM checkpoint_and_extended_stats",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000003.checkpoint.parquet", 0, 7077), 3)
                        .addCopies(new CacheOperation("InputFile.length", "00000000000000000003.checkpoint.parquet"), 3)
                        .add(new CacheOperation("InputFile.length", "00000000000000000004.json"))
                        .addAll(Stream.of("int_part=10/string_part=part1/", "int_part=20/string_part=part2/", "int_part=__HIVE_DEFAULT_PARTITION__/string_part=__HIVE_DEFAULT_PARTITION__/")
                                .flatMap(fileId -> Stream.of(new CacheOperation("Alluxio.readCached", fileId, 0, 199)))
                                .iterator())
                        .add(new CacheOperation("InputFile.newStream", "extended_stats.json"))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .build());
    }

    @Test
    public void testCacheDeletionVectorsFileOperations()
    {
        registerTable("deletion_vectors", "databricks122/deletion_vectors");
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'deletion_vectors')");
        assertFileSystemAccesses(
                "SELECT * FROM deletion_vectors",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 924))
                        .add(new CacheOperation("InputFile.length", "00000000000000000000.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 851))
                        .add(new CacheOperation("InputFile.length", "00000000000000000001.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.json", 0, 1607))
                        .add(new CacheOperation("InputFile.length", "00000000000000000002.json"))
                        .add(new CacheOperation("InputFile.length", "00000000000000000003.json"))
                        .add(new CacheOperation("Alluxio.readCached", "data", 0, 796))
                        .add(new CacheOperation("Input.readFully", "data", 0, 796))
                        .add(new CacheOperation("Alluxio.writeCache", "data", 0, 796))
                        .add(new CacheOperation("Alluxio.readCached", "deletion_vector", 1, 42))
                        .add(new CacheOperation("Input.readFully", "deletion_vector", 0, 43))
                        .add(new CacheOperation("InputFile.length", "deletion_vector"))
                        .add(new CacheOperation("Alluxio.writeCache", "deletion_vector", 0, 43))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .build());
        assertFileSystemAccesses(
                "EXPLAIN ANALYZE SELECT * FROM deletion_vectors",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 924))
                        .add(new CacheOperation("InputFile.length", "00000000000000000000.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 851))
                        .add(new CacheOperation("InputFile.length", "00000000000000000001.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.json", 0, 1607))
                        .add(new CacheOperation("InputFile.length", "00000000000000000002.json"))
                        .add(new CacheOperation("InputFile.length", "00000000000000000003.json"))
                        .add(new CacheOperation("Alluxio.readCached", "data", 0, 796))
                        .add(new CacheOperation("Alluxio.readCached", "deletion_vector", 1, 42))
                        .add(new CacheOperation("InputFile.length", "deletion_vector"))
                        .add(new CacheOperation("InputFile.newStream", "extended_stats.json"))
                        .add(new CacheOperation("InputFile.newStream", "extendeded_stats.json"))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .build());
    }

    @Test
    public void testChangeDataFileOperations()
    {
        registerTable("cdc_table", "trino/cdc_table");
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'cdc_table')");
        assertFileSystemAccesses(
                "SELECT * FROM TABLE(system.table_changes(schema_name=>CURRENT_SCHEMA, table_name=>'cdc_table', since_version=>0))",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 1117))
                        .add(new CacheOperation("InputFile.length", "00000000000000000000.json"))
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 1100), 2)
                        .addCopies(new CacheOperation("InputFile.length", "00000000000000000001.json"), 2)
                        .add(new CacheOperation("InputFile.length", "00000000000000000002.json"))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .add(new CacheOperation("Alluxio.readCached", "change_data/key=1/", 0, 389))
                        .add(new CacheOperation("Alluxio.readCached", "change_data/key=1/", 4, 74))
                        .add(new CacheOperation("Input.readFully", "change_data/key=1/", 0, 389))
                        .add(new CacheOperation("Alluxio.writeCache", "change_data/key=1/", 0, 389))
                        .add(new CacheOperation("Alluxio.readCached", "change_data/key=2/", 0, 394))
                        .add(new CacheOperation("Alluxio.readCached", "change_data/key=2/", 4, 75))
                        .add(new CacheOperation("Input.readFully", "change_data/key=2/", 0, 394))
                        .add(new CacheOperation("Alluxio.writeCache", "change_data/key=2/", 0, 394))
                        .build());
        assertFileSystemAccesses(
                "EXPLAIN ANALYZE SELECT * FROM TABLE(system.table_changes(schema_name=>CURRENT_SCHEMA, table_name=>'cdc_table', since_version=>0))",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 1117))
                        .add(new CacheOperation("InputFile.length", "00000000000000000000.json"))
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 1100), 2)
                        .addCopies(new CacheOperation("InputFile.length", "00000000000000000001.json"), 2)
                        .add(new CacheOperation("InputFile.length", "00000000000000000002.json"))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .add(new CacheOperation("Alluxio.readCached", "change_data/key=1/", 0, 389))
                        .add(new CacheOperation("Alluxio.readCached", "change_data/key=1/", 4, 74))
                        .add(new CacheOperation("Alluxio.readCached", "change_data/key=2/", 0, 394))
                        .add(new CacheOperation("Alluxio.readCached", "change_data/key=2/", 4, 75))
                        .build());
    }

    @Test
    public void testTimeTravelWithLastCheckpoint()
    {
        // Version 2 has a checkpoint file
        registerTable("time_travel_with_last_checkpoint", "trino440/time_travel");
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'time_travel_with_last_checkpoint')");

        assertFileSystemAccesses(
                "SELECT * FROM time_travel_with_last_checkpoint FOR VERSION AS OF 1",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readExternalStream", "00000000000000000000.json", 0, 1015))
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000000.json", 0, 1015))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 1015))
                        .add(new CacheOperation("InputFile.length", "00000000000000000000.json"))
                        .add(new CacheOperation("InputFile.newStream", "00000000000000000000.json"))
                        .add(new CacheOperation("Alluxio.readExternalStream", "00000000000000000001.json", 0, 613))
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000001.json", 0, 613))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 613))
                        .add(new CacheOperation("InputFile.length", "00000000000000000001.json"))
                        .add(new CacheOperation("InputFile.exists", "00000000000000000001.json"))
                        .add(new CacheOperation("InputFile.newStream", "00000000000000000001.json"))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .addCopies(new CacheOperation("Input.readFully", "data", 0, 199), 2)
                        .addCopies(new CacheOperation("Alluxio.writeCache", "data", 0, 199), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", "data", 0, 199), 2)
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM time_travel_with_last_checkpoint FOR VERSION AS OF 2",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000002.checkpoint.parquet", 0, 5884), 2)
                        .addCopies(new CacheOperation("InputFile.length", "00000000000000000002.checkpoint.parquet"), 2)
                        .add(new CacheOperation("InputFile.exists", "00000000000000000002.json"))
                        .add(new CacheOperation("Input.readFully", "data", 0, 199))
                        .add(new CacheOperation("Alluxio.writeCache", "data", 0, 199))
                        .addCopies(new CacheOperation("Alluxio.readCached", "data", 0, 199), 3)
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM time_travel_with_last_checkpoint FOR VERSION AS OF 2",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000002.checkpoint.parquet", 0, 5884), 2)
                        .addCopies(new CacheOperation("InputFile.length", "00000000000000000002.checkpoint.parquet"), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", "data", 0, 199), 3)
                        .add(new CacheOperation("InputFile.exists", "00000000000000000002.json"))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .build());

        assertUpdate("DROP TABLE time_travel_with_last_checkpoint");
    }

    @Test
    public void testTimeTravelWithoutLastCheckpoint()
            throws Exception
    {
        // Version 2 has a checkpoint file
        Path tableLocation = Files.createTempFile("time_travel", null);
        copyDirectoryContents(new File(Resources.getResource("trino440/time_travel").toURI()).toPath(), tableLocation);
        Files.delete(tableLocation.resolve("_delta_log/_last_checkpoint"));

        getQueryRunner().execute("CALL system.register_table(CURRENT_SCHEMA, 'time_travel_without_last_checkpoint', '" + tableLocation.toUri() + "')");
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'time_travel_without_last_checkpoint')");

        assertFileSystemAccesses(
                "SELECT * FROM time_travel_without_last_checkpoint FOR VERSION AS OF 1",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 1015))
                        .add(new CacheOperation("InputFile.length", "00000000000000000000.json"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 613))
                        .add(new CacheOperation("InputFile.length", "00000000000000000001.json"))
                        .add(new CacheOperation("InputFile.exists", "00000000000000000001.json"))
                        .addCopies(new CacheOperation("Input.readFully", "data", 0, 199), 2)
                        .addCopies(new CacheOperation("Alluxio.writeCache", "data", 0, 199), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", "data", 0, 199), 2)
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM time_travel_without_last_checkpoint FOR VERSION AS OF 2",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Input.readFully", "00000000000000000002.checkpoint.parquet", 0, 5884))
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000002.checkpoint.parquet", 0, 5884))
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000002.checkpoint.parquet", 0, 5884), 2)
                        .addCopies(new CacheOperation("InputFile.length", "00000000000000000002.checkpoint.parquet"), 2)
                        .add(new CacheOperation("InputFile.exists", "00000000000000000002.json"))
                        .add(new CacheOperation("Input.readFully", "data", 0, 199))
                        .add(new CacheOperation("Alluxio.writeCache", "data", 0, 199))
                        .addCopies(new CacheOperation("Alluxio.readCached", "data", 0, 199), 3)
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM time_travel_without_last_checkpoint FOR VERSION AS OF 2",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000002.checkpoint.parquet", 0, 5884), 2)
                        .addCopies(new CacheOperation("InputFile.length", "00000000000000000002.checkpoint.parquet"), 2)
                        .add(new CacheOperation("InputFile.exists", "00000000000000000002.json"))
                        .addCopies(new CacheOperation("Alluxio.readCached", "data", 0, 199), 3)
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .build());

        assertUpdate("DROP TABLE time_travel_without_last_checkpoint");
    }

    @Test
    public void testReadV2CheckpointJson()
    {
        String dataPath = getResourceLocation("deltalake/v2_checkpoint_json").toExternalForm();
        assertFileSystemAccesses(
                format("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')", "v2_checkpoint_json", dataPath),
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.73a4ddb8-2bfc-40d8-b09f-1b6a0abdfb04.json", 0, 765))
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000001.checkpoint.73a4ddb8-2bfc-40d8-b09f-1b6a0abdfb04.json", 0, 765))
                        .add(new CacheOperation("Alluxio.readExternalStream", "00000000000000000001.checkpoint.73a4ddb8-2bfc-40d8-b09f-1b6a0abdfb04.json", 0, 765))
                        .add(new CacheOperation("InputFile.newStream", "00000000000000000001.checkpoint.73a4ddb8-2bfc-40d8-b09f-1b6a0abdfb04.json"))
                        .add(new CacheOperation("InputFile.length", "00000000000000000001.checkpoint.73a4ddb8-2bfc-40d8-b09f-1b6a0abdfb04.json"))
                        .add(new CacheOperation("InputFile.length", "00000000000000000002.json"))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .build());
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'v2_checkpoint_json')");
        assertFileSystemAccesses(
                "SELECT * FROM v2_checkpoint_json",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.73a4ddb8-2bfc-40d8-b09f-1b6a0abdfb04.json", 0, 765), 2)
                        .addCopies(new CacheOperation("InputFile.length", "00000000000000000001.checkpoint.73a4ddb8-2bfc-40d8-b09f-1b6a0abdfb04.json"), 2)
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.90cf4e21-dbaa-41d6-8ae5-6709cfbfbfe0.parquet", 0, 9176))
                        .add(new CacheOperation("Input.readFully", "00000000000000000001.checkpoint.0000000001.0000000001.90cf4e21-dbaa-41d6-8ae5-6709cfbfbfe0.parquet", 0, 9176))
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000001.checkpoint.0000000001.0000000001.90cf4e21-dbaa-41d6-8ae5-6709cfbfbfe0.parquet", 0, 9176))
                        .add(new CacheOperation("InputFile.length", "00000000000000000001.checkpoint.0000000001.0000000001.90cf4e21-dbaa-41d6-8ae5-6709cfbfbfe0.parquet"))
                        .add(new CacheOperation("InputFile.length", "00000000000000000002.json"))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .add(new CacheOperation("Alluxio.writeCache", "data", 0, 666))
                        .add(new CacheOperation("Alluxio.readCached", "data", 0, 666))
                        .add(new CacheOperation("Input.readFully", "data", 0, 666))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM v2_checkpoint_json",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.73a4ddb8-2bfc-40d8-b09f-1b6a0abdfb04.json", 0, 765), 2)
                        .addCopies(new CacheOperation("InputFile.length", "00000000000000000001.checkpoint.73a4ddb8-2bfc-40d8-b09f-1b6a0abdfb04.json"), 2)
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.90cf4e21-dbaa-41d6-8ae5-6709cfbfbfe0.parquet", 0, 9176))
                        .add(new CacheOperation("InputFile.length", "00000000000000000001.checkpoint.0000000001.0000000001.90cf4e21-dbaa-41d6-8ae5-6709cfbfbfe0.parquet"))
                        .add(new CacheOperation("InputFile.length", "00000000000000000002.json"))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .add(new CacheOperation("Alluxio.readCached", "data", 0, 666))
                        .build());
    }

    @Test
    public void testReadV2CheckpointParquet()
    {
        String dataPath = getResourceLocation("deltalake/v2_checkpoint_parquet").toExternalForm();
        assertFileSystemAccesses(
                format("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')", "v2_checkpoint_parquet", dataPath),
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("InputFile.length", "00000000000000000002.json"))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", 0, 19019))
                        .add(new CacheOperation("Input.readFully", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", 0, 19019))
                        .addCopies(new CacheOperation("InputFile.length", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet"), 2)
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", 0, 19019))
                        .build());

        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'v2_checkpoint_parquet')");
        assertFileSystemAccesses(
                "SELECT * FROM v2_checkpoint_parquet",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", 0, 19019), 2)
                        .addCopies(new CacheOperation("InputFile.length", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet"), 4)
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.03288d7e-af16-44ed-829c-196064a71812.parquet", 0, 9415))
                        .add(new CacheOperation("InputFile.length", "00000000000000000001.checkpoint.0000000001.0000000001.03288d7e-af16-44ed-829c-196064a71812.parquet"))
                        .add(new CacheOperation("Input.readFully", "00000000000000000001.checkpoint.0000000001.0000000001.03288d7e-af16-44ed-829c-196064a71812.parquet", 0, 9415))
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000001.checkpoint.0000000001.0000000001.03288d7e-af16-44ed-829c-196064a71812.parquet", 0, 9415))
                        .add(new CacheOperation("InputFile.length", "00000000000000000002.json"))
                        .add(new CacheOperation("Alluxio.writeCache", "data", 0, 666))
                        .add(new CacheOperation("Alluxio.readCached", "data", 0, 666))
                        .add(new CacheOperation("Input.readFully", "data", 0, 666))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM v2_checkpoint_parquet",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", 0, 19019), 2)
                        .addCopies(new CacheOperation("InputFile.length", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet"), 4)
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.03288d7e-af16-44ed-829c-196064a71812.parquet", 0, 9415))
                        .add(new CacheOperation("InputFile.length", "00000000000000000001.checkpoint.0000000001.0000000001.03288d7e-af16-44ed-829c-196064a71812.parquet"))
                        .add(new CacheOperation("InputFile.length", "00000000000000000002.json"))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .add(new CacheOperation("Alluxio.readCached", "data", 0, 666))
                        .build());
    }

    @Test
    public void testCreateOrReplaceTable()
    {
        assertFileSystemAccesses("CREATE OR REPLACE TABLE test_create_or_replace (id VARCHAR, age INT)", ImmutableMultiset.of());

        assertFileSystemAccesses("CREATE OR REPLACE TABLE test_create_or_replace (id VARCHAR, age INT)",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 799))
                        .add(new CacheOperation("Alluxio.readExternalStream", "00000000000000000000.json", 0, 799))
                        .add(new CacheOperation("InputFile.newStream", "00000000000000000000.json"))
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000000.json", 0, 799))
                        .add(new CacheOperation("InputFile.length", "00000000000000000000.json"))
                        .add(new CacheOperation("InputFile.exists", "00000000000000000001.json"))
                        .add(new CacheOperation("InputFile.length", "00000000000000000001.json"))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .add(new CacheOperation("InputFile.exists", "extended_stats.json"))
                        .build());
        assertUpdate("DROP TABLE test_create_or_replace");
    }

    @Test
    public void testCreateOrReplaceTableAsSelect()
    {
        assertFileSystemAccesses(
                "CREATE OR REPLACE TABLE test_create_or_replace_as_select AS SELECT 1 col_name",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("InputFile.exists", "extendeded_stats.json"))
                        .add(new CacheOperation("InputFile.newStream", "extendeded_stats.json"))
                        .add(new CacheOperation("InputFile.newStream", "extended_stats.json"))
                        .build());

        assertFileSystemAccesses(
                "CREATE OR REPLACE TABLE test_create_or_replace_as_select AS SELECT 1 col_name",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 1041))
                        .add(new CacheOperation("Alluxio.readExternalStream", "00000000000000000000.json", 0, 1041))
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000000.json", 0, 1041))
                        .add(new CacheOperation("InputFile.newStream", "00000000000000000000.json"))
                        .add(new CacheOperation("InputFile.length", "00000000000000000000.json"))
                        .add(new CacheOperation("InputFile.length", "00000000000000000001.json"))
                        .add(new CacheOperation("InputFile.exists", "00000000000000000001.json"))
                        .add(new CacheOperation("InputFile.exists", "extendeded_stats.json"))
                        .add(new CacheOperation("InputFile.newStream", "extended_stats.json"))
                        .add(new CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .build());

        assertUpdate("DROP TABLE test_create_or_replace_as_select");
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<CacheOperation> expectedCacheAccesses)
    {
        assertUpdate("CALL system.flush_metadata_cache()");
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        queryRunner.executeWithPlan(queryRunner.getDefaultSession(), query);
        assertMultisetsEqual(getCacheOperations(queryRunner), expectedCacheAccesses);
    }
}
