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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import com.google.common.io.Resources;
import io.opentelemetry.api.common.Attributes;
import io.trino.Session;
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
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_LOCATION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_SIZE;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_SIZE;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.copyDirectoryContents;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

@Execution(ExecutionMode.SAME_THREAD)
public class TestDeltaLakeAlluxioCacheFileOperations
        extends AbstractTestQueryFramework
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Path cacheDirectory = Files.createTempDirectory("cache");
        cacheDirectory.toFile().deleteOnExit();
        Path metastoreDirectory = Files.createTempDirectory(DELTA_CATALOG);
        metastoreDirectory.toFile().deleteOnExit();

        Session session = testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema("default")
                .build();

        Map<String, String> deltaLakeProperties = ImmutableMap.<String, String>builder()
                .put("fs.cache.enabled", "true")
                .put("fs.cache.directories", cacheDirectory.toAbsolutePath().toString())
                .put("fs.cache.max-sizes", "100MB")
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", metastoreDirectory.toUri().toString())
                .put("delta.enable-non-concurrent-writes", "true")
                .put("delta.register-table-procedure.enabled", "true")
                .buildOrThrow();

        DistributedQueryRunner queryRunner = DeltaLakeQueryRunner.builder(session)
                .setCoordinatorProperties(ImmutableMap.of("node-scheduler.include-coordinator", "false"))
                .setDeltaProperties(deltaLakeProperties)
                .setWorkerCount(1)
                .build();

        queryRunner.execute("CREATE SCHEMA " + session.getSchema().orElseThrow());
        return queryRunner;
    }

    private URL getResourceLocation(String resourcePath)
    {
        return getClass().getClassLoader().getResource(resourcePath);
    }

    private void registerTable(String name, String resourcePath)
    {
        String dataPath = getResourceLocation(resourcePath).toExternalForm();
        getQueryRunner().execute(format("CALL system.register_table('%s', '%s', '%s')", getSession().getSchema().orElseThrow(), name, dataPath));
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
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 757))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 636))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.json", 0, 636))
                        .add(new CacheOperation("Alluxio.readExternal", "00000000000000000002.json", 0, 636))
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000002.json", 0, 636))
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 218))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p1/", 0, 218))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p2/", 0, 218))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p1/", 0, 218))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p2/", 0, 218))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 757))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 636))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.json", 0, 636))
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 218))
                        .build());
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p3', '3-xyz')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p4', '4-xyz')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p5', '5-xyz')", 1);
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 757))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 636))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.json", 0, 636))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000003.json", 0, 636))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000004.json", 0, 636))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000005.json", 0, 636))
                        .add(new CacheOperation("Alluxio.readExternal", "00000000000000000005.json", 0, 636))
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000005.json", 0, 636))
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p3/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p4/", 0, 218))
                        .add(new CacheOperation("Alluxio.readCached", "key=p5/", 0, 218))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p3/", 0, 218))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p4/", 0, 218))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p5/", 0, 218))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p3/", 0, 218))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p4/", 0, 218))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p5/", 0, 218))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 757))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 636))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.json", 0, 636))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000003.json", 0, 636))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000004.json", 0, 636))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000005.json", 0, 636))
                        .addCopies(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 218), 1)
                        .addCopies(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 218), 1)
                        .addCopies(new CacheOperation("Alluxio.readCached", "key=p3/", 0, 218), 1)
                        .addCopies(new CacheOperation("Alluxio.readCached", "key=p4/", 0, 218), 1)
                        .addCopies(new CacheOperation("Alluxio.readCached", "key=p5/", 0, 218), 1)
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
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000003.checkpoint.parquet", 706, 972), 1)
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000003.checkpoint.parquet", 4, 624), 1)
                        .addAll(Stream.of("int_part=10/string_part=part1/", "int_part=20/string_part=part2/", "int_part=__HIVE_DEFAULT_PARTITION__/string_part=__HIVE_DEFAULT_PARTITION__/")
                                .flatMap(fileId ->
                                        Stream.of(
                                                new CacheOperation("Alluxio.readCached", fileId, 0, 199),
                                                new CacheOperation("Alluxio.readExternal", fileId, 0, 199),
                                                new CacheOperation("Alluxio.writeCache", fileId, 0, 199)))
                                .iterator())
                        .build());
        assertFileSystemAccesses(
                "EXPLAIN ANALYZE SELECT * FROM checkpoint_and_extended_stats",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000003.checkpoint.parquet", 0, 7077), 3)
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000003.checkpoint.parquet", 706, 972), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000003.checkpoint.parquet", 4, 624), 1)
                        .addAll(Stream.of("int_part=10/string_part=part1/", "int_part=20/string_part=part2/", "int_part=__HIVE_DEFAULT_PARTITION__/string_part=__HIVE_DEFAULT_PARTITION__/")
                                .flatMap(fileId -> Stream.of(new CacheOperation("Alluxio.readCached", fileId, 0, 199)))
                                .iterator())
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
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 851))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.json", 0, 1607))
                        .add(new CacheOperation("Alluxio.readCached", "data", 0, 796))
                        .add(new CacheOperation("Alluxio.readExternal", "data", 0, 796))
                        .add(new CacheOperation("Alluxio.writeCache", "data", 0, 796))
                        .add(new CacheOperation("Alluxio.readCached", "deletion_vector", 5, 34))
                        .add(new CacheOperation("Alluxio.readCached", "deletion_vector", 1, 4))
                        .add(new CacheOperation("Alluxio.readExternal", "deletion_vector", 38, 1))
                        .add(new CacheOperation("Alluxio.readExternal", "deletion_vector", 1, 4))
                        .addCopies(new CacheOperation("Alluxio.writeCache", "deletion_vector", 0, 43), 2)
                        .build());
        assertFileSystemAccesses(
                "EXPLAIN ANALYZE SELECT * FROM deletion_vectors",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 924))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 851))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.json", 0, 1607))
                        .add(new CacheOperation("Alluxio.readCached", "data", 0, 796))
                        .add(new CacheOperation("Alluxio.readCached", "deletion_vector", 5, 34))
                        .add(new CacheOperation("Alluxio.readCached", "deletion_vector", 1, 4))
                        .build());
    }

    @Test
    public void testChangeDataFileOperations()
    {
        registerTable("cdc_table", "trino/cdc_table");
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'cdc_table')");
        assertFileSystemAccesses(
                "SELECT * FROM TABLE(system.table_changes(schema_name=>'default', table_name=>'cdc_table', since_version=>0))",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 1117))
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 1100), 2)
                        .add(new CacheOperation("Alluxio.readCached", "change_data/key=1/", 0, 389))
                        .add(new CacheOperation("Alluxio.readCached", "change_data/key=1/", 4, 74))
                        .add(new CacheOperation("Alluxio.readExternal", "change_data/key=1/", 0, 389))
                        .add(new CacheOperation("Alluxio.writeCache", "change_data/key=1/", 0, 389))

                        .add(new CacheOperation("Alluxio.readCached", "change_data/key=2/", 0, 394))
                        .add(new CacheOperation("Alluxio.readCached", "change_data/key=2/", 4, 75))
                        .add(new CacheOperation("Alluxio.readExternal", "change_data/key=2/", 0, 394))
                        .add(new CacheOperation("Alluxio.writeCache", "change_data/key=2/", 0, 394))
                        .build());
        assertFileSystemAccesses(
                "EXPLAIN ANALYZE SELECT * FROM TABLE(system.table_changes(schema_name=>'default', table_name=>'cdc_table', since_version=>0))",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 1117))
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 1100), 2)
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
                        .add(new CacheOperation("Alluxio.readExternal", "00000000000000000000.json", 0, 1015))
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000000.json", 0, 1015))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 1015))
                        .add(new CacheOperation("Alluxio.readExternal", "00000000000000000001.json", 0, 613))
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000001.json", 0, 613))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 613))
                        .addCopies(new CacheOperation("Alluxio.readExternal", "data", 0, 199), 2)
                        .addCopies(new CacheOperation("Alluxio.writeCache", "data", 0, 199), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", "data", 0, 199), 2)
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM time_travel_with_last_checkpoint FOR VERSION AS OF 2",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.checkpoint.parquet", 4, 561))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.checkpoint.parquet", 643, 767))
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000002.checkpoint.parquet", 0, 5884), 2)
                        .add(new CacheOperation("Alluxio.readExternal", "data", 0, 199))
                        .add(new CacheOperation("Alluxio.writeCache", "data", 0, 199))
                        .addCopies(new CacheOperation("Alluxio.readCached", "data", 0, 199), 3)
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM time_travel_with_last_checkpoint FOR VERSION AS OF 2",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.checkpoint.parquet", 4, 561))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.checkpoint.parquet", 643, 767))
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000002.checkpoint.parquet", 0, 5884), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", "data", 0, 199), 3)
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
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.json", 0, 613))
                        .addCopies(new CacheOperation("Alluxio.readExternal", "data", 0, 199), 2)
                        .addCopies(new CacheOperation("Alluxio.writeCache", "data", 0, 199), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", "data", 0, 199), 2)
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM time_travel_without_last_checkpoint FOR VERSION AS OF 2",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readExternal", "00000000000000000002.checkpoint.parquet", 0, 5884))
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000002.checkpoint.parquet", 0, 5884))
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000002.checkpoint.parquet", 0, 5884), 2)
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.checkpoint.parquet", 4, 561))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.checkpoint.parquet", 643, 767))
                        .add(new CacheOperation("Alluxio.readExternal", "data", 0, 199))
                        .add(new CacheOperation("Alluxio.writeCache", "data", 0, 199))
                        .addCopies(new CacheOperation("Alluxio.readCached", "data", 0, 199), 3)
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM time_travel_without_last_checkpoint FOR VERSION AS OF 2",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000002.checkpoint.parquet", 0, 5884), 2)
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.checkpoint.parquet", 4, 561))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000002.checkpoint.parquet", 643, 767))
                        .addCopies(new CacheOperation("Alluxio.readCached", "data", 0, 199), 3)
                        .build());

        assertUpdate("DROP TABLE time_travel_without_last_checkpoint");
    }

    @Test
    public void testReadV2CheckpointJson()
    {
        registerTable("v2_checkpoint_json", "deltalake/v2_checkpoint_json");
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'v2_checkpoint_json')");
        assertFileSystemAccesses(
                "SELECT * FROM v2_checkpoint_json",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.73a4ddb8-2bfc-40d8-b09f-1b6a0abdfb04.json", 0, 765), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.90cf4e21-dbaa-41d6-8ae5-6709cfbfbfe0.parquet", 0, 9176), 2)
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.90cf4e21-dbaa-41d6-8ae5-6709cfbfbfe0.parquet", 1224, 143))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.90cf4e21-dbaa-41d6-8ae5-6709cfbfbfe0.parquet", 4, 727))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.90cf4e21-dbaa-41d6-8ae5-6709cfbfbfe0.parquet", 1994, 188))
                        .add(new CacheOperation("Alluxio.writeCache", "data", 0, 666))
                        .add(new CacheOperation("Alluxio.readCached", "data", 0, 666))
                        .add(new CacheOperation("Alluxio.readExternal", "data", 0, 666))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM v2_checkpoint_json",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.73a4ddb8-2bfc-40d8-b09f-1b6a0abdfb04.json", 0, 765), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.90cf4e21-dbaa-41d6-8ae5-6709cfbfbfe0.parquet", 0, 9176), 2)
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.90cf4e21-dbaa-41d6-8ae5-6709cfbfbfe0.parquet", 1224, 143))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.90cf4e21-dbaa-41d6-8ae5-6709cfbfbfe0.parquet", 4, 727))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.90cf4e21-dbaa-41d6-8ae5-6709cfbfbfe0.parquet", 1994, 188))
                        .add(new CacheOperation("Alluxio.readCached", "data", 0, 666))
                        .build());
    }

    @Test
    public void testReadV2CheckpointParquet()
    {
        registerTable("v2_checkpoint_parquet", "deltalake/v2_checkpoint_parquet");
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'v2_checkpoint_parquet')");
        assertFileSystemAccesses(
                "SELECT * FROM v2_checkpoint_parquet",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", 0, 19019), 2)
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", 100, 2470))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", 1304, 1266))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", 3155, 87))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", 3829, 143))
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.03288d7e-af16-44ed-829c-196064a71812.parquet", 0, 9415), 2)
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.03288d7e-af16-44ed-829c-196064a71812.parquet", 4, 758))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.03288d7e-af16-44ed-829c-196064a71812.parquet", 1255, 143))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.03288d7e-af16-44ed-829c-196064a71812.parquet", 2040, 199))
                        .add(new CacheOperation("Alluxio.writeCache", "data", 0, 666))
                        .add(new CacheOperation("Alluxio.readCached", "data", 0, 666))
                        .add(new CacheOperation("Alluxio.readExternal", "data", 0, 666))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM v2_checkpoint_parquet",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", 0, 19019), 2)
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", 100, 2470))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", 1304, 1266))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", 3155, 87))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", 3829, 143))
                        .addCopies(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.03288d7e-af16-44ed-829c-196064a71812.parquet", 0, 9415), 2)
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.03288d7e-af16-44ed-829c-196064a71812.parquet", 4, 758))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.03288d7e-af16-44ed-829c-196064a71812.parquet", 1255, 143))
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000001.checkpoint.0000000001.0000000001.03288d7e-af16-44ed-829c-196064a71812.parquet", 2040, 199))
                        .add(new CacheOperation("Alluxio.readCached", "data", 0, 666))
                        .build());
    }

    @Test
    public void testCreateOrReplaceTable()
    {
        assertFileSystemAccesses("CREATE OR REPLACE TABLE test_create_or_replace (id VARCHAR, age INT)", ImmutableMultiset.of());

        assertFileSystemAccesses("CREATE OR REPLACE TABLE test_create_or_replace (id VARCHAR, age INT)",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 762))
                        .add(new CacheOperation("Alluxio.readExternal", "00000000000000000000.json", 0, 762))
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000000.json", 0, 762))
                        .build());
        assertUpdate("DROP TABLE test_create_or_replace");
    }

    @Test
    public void testCreateOrReplaceTableAsSelect()
    {
        assertFileSystemAccesses(
                "CREATE OR REPLACE TABLE test_create_or_replace_as_select AS SELECT 1 col_name", ImmutableMultiset.of());

        assertFileSystemAccesses(
                "CREATE OR REPLACE TABLE test_create_or_replace_as_select AS SELECT 1 col_name",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "00000000000000000000.json", 0, 1004))
                        .add(new CacheOperation("Alluxio.readExternal", "00000000000000000000.json", 0, 1004))
                        .add(new CacheOperation("Alluxio.writeCache", "00000000000000000000.json", 0, 1004))
                        .build());

        assertUpdate("DROP TABLE test_create_or_replace_as_select");
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<CacheOperation> expectedCacheAccesses)
    {
        assertUpdate("CALL system.flush_metadata_cache()");
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        queryRunner.executeWithPlan(queryRunner.getDefaultSession(), query);
        assertMultisetsEqual(getCacheOperations(), expectedCacheAccesses);
    }

    private Multiset<CacheOperation> getCacheOperations()
    {
        return getQueryRunner().getSpans().stream()
                .filter(span -> span.getName().startsWith("Alluxio."))
                .filter(span -> !isTrinoSchemaOrPermissions(requireNonNull(span.getAttributes().get(CACHE_FILE_LOCATION))))
                .map(span -> CacheOperation.create(span.getName(), span.getAttributes()))
                .collect(toCollection(HashMultiset::create));
    }

    private static Pattern dataFilePattern = Pattern.compile(".*?/(?<partition>((\\w+)=[^/]*/)*)(?<queryId>\\d{8}_\\d{6}_\\d{5}_\\w{5})_(?<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");

    private record CacheOperation(String operationName, String fileId, long position, long length)
    {
        public static CacheOperation create(String operationName, Attributes attributes)
        {
            String path = requireNonNull(attributes.get(CACHE_FILE_LOCATION));
            String fileName = path.replaceFirst(".*/", "");

            long position = switch (operationName) {
                case "Alluxio.readCached" -> requireNonNull(attributes.get(CACHE_FILE_READ_POSITION));
                case "Alluxio.readExternal" -> requireNonNull(attributes.get(CACHE_FILE_READ_POSITION));
                case "Alluxio.writeCache" -> requireNonNull(attributes.get(CACHE_FILE_WRITE_POSITION));
                default -> throw new IllegalArgumentException("Unexpected operation name: " + operationName);
            };

            long length = switch (operationName) {
                case "Alluxio.readCached" -> requireNonNull(attributes.get(CACHE_FILE_READ_SIZE));
                case "Alluxio.readExternal" -> requireNonNull(attributes.get(CACHE_FILE_READ_SIZE));
                case "Alluxio.writeCache" -> requireNonNull(attributes.get(CACHE_FILE_WRITE_SIZE));
                default -> throw new IllegalArgumentException("Unexpected operation name: " + operationName);
            };

            if (!path.contains("_delta_log") && !path.contains("/.trino")) {
                Matcher matcher = dataFilePattern.matcher(path);
                if (matcher.matches()) {
                    String changeData = path.contains("/_change_data/") ? "change_data/" : "";
                    if (!path.contains("=")) {
                        return new CacheOperation(operationName, "data", position, length);
                    }
                    return new CacheOperation(operationName, changeData + matcher.group("partition"), position, length);
                }
                if (path.contains("/part-00000-")) {
                    return new CacheOperation(operationName, "data", position, length);
                }
                if (path.contains("/deletion_vector_")) {
                    return new CacheOperation(operationName, "deletion_vector", position, length);
                }
            }
            else {
                return new CacheOperation(operationName, fileName, position, length);
            }
            throw new IllegalArgumentException("File not recognized: " + path);
        }
    }

    private static boolean isTrinoSchemaOrPermissions(String path)
    {
        return path.endsWith(".trinoSchema") || path.contains(".trinoPermissions");
    }
}
