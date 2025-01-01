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
import com.google.common.io.Resources;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.Table;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableMetadataScheduler;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.CDF_DATA;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.CHECKPOINT;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.DATA;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.DELETION_VECTOR;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.LAST_CHECKPOINT;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.STARBURST_EXTENDED_STATS_JSON;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.TRANSACTION_LOG_JSON;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.TRINO_EXTENDED_STATS_JSON;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.copyDirectoryContents;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

// single-threaded as DistributedQueryRunner.spans is shared mutable state
@Execution(ExecutionMode.SAME_THREAD)
public class TestDeltaLakeFileOperations
        extends AbstractTestQueryFramework
{
    private static final int MAX_PREFIXES_COUNT = 10;

    private HiveMetastore metastore;
    // TODO: Consider waiting for scheduled task completion instead of manual triggering
    private DeltaLakeTableMetadataScheduler metadataScheduler;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path catalogDir = Files.createTempDirectory("catalog-dir");
        closeAfterClass(() -> deleteRecursively(catalogDir, ALLOW_INSECURE));

        DistributedQueryRunner queryRunner = DeltaLakeQueryRunner.builder()
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .addDeltaProperty("hive.metastore.catalog.dir", catalogDir.toUri().toString())
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .addDeltaProperty("delta.register-table-procedure.enabled", "true")
                .addDeltaProperty("delta.metastore.store-table-metadata", "true")
                .addDeltaProperty("delta.metastore.store-table-metadata-threads", "0") // Use the same thread to make the test deterministic
                .addDeltaProperty("delta.metastore.store-table-metadata-interval", "30m") // Use a large interval to avoid interference with the test
                .build();
        metastore = TestingDeltaLakeUtils.getConnectorService(queryRunner, HiveMetastoreFactory.class).createMetastore(Optional.empty());
        metadataScheduler = TestingDeltaLakeUtils.getConnectorService(queryRunner, DeltaLakeTableMetadataScheduler.class);
        return queryRunner;
    }

    @Test
    public void testCreateTableAsSelect()
    {
        assertFileSystemAccesses(
                "CREATE TABLE test_create_as_select AS SELECT 1 col_name",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(STARBURST_EXTENDED_STATS_JSON, "extendeded_stats.json", "InputFile.newStream"))
                        .add(new FileOperation(STARBURST_EXTENDED_STATS_JSON, "extendeded_stats.json", "InputFile.exists"))
                        .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", "InputFile.newStream"))
                        .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", "OutputFile.createOrOverwrite"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "OutputFile.create"))
                        .add(new FileOperation(DATA, "no partition", "OutputFile.create"))
                        .build());
        assertUpdate("DROP TABLE test_create_as_select");

        assertFileSystemAccesses(
                "CREATE TABLE test_create_partitioned_as_select WITH (partitioned_by=ARRAY['key']) AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(key, col)",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(STARBURST_EXTENDED_STATS_JSON, "extendeded_stats.json", "InputFile.newStream"))
                        .add(new FileOperation(STARBURST_EXTENDED_STATS_JSON, "extendeded_stats.json", "InputFile.exists"))
                        .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", "InputFile.newStream"))
                        .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", "OutputFile.createOrOverwrite"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "OutputFile.create"))
                        .add(new FileOperation(DATA, "key=1/", "OutputFile.create"))
                        .add(new FileOperation(DATA, "key=2/", "OutputFile.create"))
                        .build());
        assertUpdate("DROP TABLE test_create_partitioned_as_select");
    }

    @Test
    public void testCreateOrReplaceTable()
    {
        assertFileSystemAccesses("CREATE OR REPLACE TABLE test_create_or_replace (id VARCHAR, age INT)",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "OutputFile.create"))
                        .build());

        assertFileSystemAccesses("CREATE OR REPLACE TABLE test_create_or_replace (id VARCHAR, age INT)",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "OutputFile.createOrOverwrite"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.exists"))
                        .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", "InputFile.exists"))
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .build());
        assertUpdate("DROP TABLE test_create_or_replace");
    }

    @Test
    public void testCreateOrReplaceTableAsSelect()
    {
        assertFileSystemAccesses(
                "CREATE OR REPLACE TABLE test_create_or_replace_as_select AS SELECT 1 col_name",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(STARBURST_EXTENDED_STATS_JSON, "extendeded_stats.json", "InputFile.newStream"))
                        .add(new FileOperation(STARBURST_EXTENDED_STATS_JSON, "extendeded_stats.json", "InputFile.exists"))
                        .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", "InputFile.newStream"))
                        .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", "OutputFile.createOrOverwrite"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "OutputFile.create"))
                        .add(new FileOperation(DATA, "no partition", "OutputFile.create"))
                        .build());

        assertFileSystemAccesses(
                "CREATE OR REPLACE TABLE test_create_or_replace_as_select AS SELECT 1 col_name",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(STARBURST_EXTENDED_STATS_JSON, "extendeded_stats.json", "InputFile.exists"))
                        .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", "InputFile.newStream"))
                        .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", "OutputFile.createOrOverwrite"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "OutputFile.createOrOverwrite"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.exists"))
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(DATA, "no partition", "OutputFile.create"))
                        .build());

        assertUpdate("DROP TABLE test_create_or_replace_as_select");
    }

    @Test
    public void testReadUnpartitionedTable()
    {
        assertUpdate("DROP TABLE IF EXISTS test_read_unpartitioned");
        assertUpdate("CREATE TABLE test_read_unpartitioned(key varchar, data varchar)");

        // Create multiple files
        assertUpdate("INSERT INTO test_read_unpartitioned(key, data) VALUES ('p1', '1-abc'), ('p1', '1-def'), ('p2', '2-abc'), ('p2', '2-def')", 4);
        assertUpdate("INSERT INTO test_read_unpartitioned(key, data) VALUES ('p1', '1-baz'), ('p2', '2-baz')", 2);

        // Read all columns
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_read_unpartitioned')");
        assertFileSystemAccesses(
                "TABLE test_read_unpartitioned",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .addCopies(new FileOperation(DATA, "no partition", "InputFile.newInput"), 2)
                        .build());

        // Read with aggregation (this may involve fetching stats so may incur more file system accesses)
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_read_unpartitioned')");
        assertFileSystemAccesses(
                "SELECT key, max(data) FROM test_read_unpartitioned GROUP BY key",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", "InputFile.newStream"))
                        .addCopies(new FileOperation(DATA, "no partition", "InputFile.newInput"), 2)
                        .build());

        assertUpdate("DROP TABLE test_read_unpartitioned");
    }

    @Test
    public void testReadTableCheckpointInterval()
    {
        assertUpdate("DROP TABLE IF EXISTS test_read_checkpoint");

        assertUpdate("CREATE TABLE test_read_checkpoint(key varchar, data varchar) WITH (checkpoint_interval = 2)");
        assertUpdate("INSERT INTO test_read_checkpoint(key, data) VALUES ('p1', '1-abc'), ('p1', '1-def'), ('p2', '2-abc'), ('p2', '2-def')", 4);
        assertUpdate("INSERT INTO test_read_checkpoint(key, data) VALUES ('p1', '1-baz'), ('p2', '2-baz')", 2);

        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_read_checkpoint')");
        assertFileSystemAccesses(
                "TABLE test_read_checkpoint",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", "InputFile.length"), 2) // TODO (https://github.com/trinodb/trino/issues/18916) should be checked once per query
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", "InputFile.newInput"), 2) // TODO (https://github.com/trinodb/trino/issues/18916) should be checked once per query
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .addCopies(new FileOperation(DATA, "no partition", "InputFile.newInput"), 2)
                        .build());

        assertUpdate("DROP TABLE test_read_checkpoint");
    }

    @Test
    public void testReadPartitionTableWithCheckpointFiltering()
    {
        assertUpdate("DROP TABLE IF EXISTS test_checkpoint_filtering");

        assertUpdate("CREATE TABLE test_checkpoint_filtering(key varchar, data varchar) WITH (partitioned_by = ARRAY['key'], checkpoint_interval = 2)");
        assertUpdate("INSERT INTO test_checkpoint_filtering(key, data) VALUES ('p1', '1-abc'), ('p1', '1-def'), ('p2', '2-abc'), ('p2', '2-def')", 4);
        assertUpdate("INSERT INTO test_checkpoint_filtering(key, data) VALUES ('p1', '1-baz'), ('p2', '2-baz')", 2);

        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_checkpoint_filtering')");
        assertFileSystemAccesses(
                "TABLE test_checkpoint_filtering",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", "InputFile.length"), 2) // TODO (https://github.com/trinodb/trino/issues/18916) should be checked once per query
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", "InputFile.newInput"), 2) // TODO (https://github.com/trinodb/trino/issues/18916) should be checked once per query
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .addCopies(new FileOperation(DATA, "key=p1/", "InputFile.newInput"), 2)
                        .addCopies(new FileOperation(DATA, "key=p2/", "InputFile.newInput"), 2)
                        .build());

        assertUpdate("DROP TABLE test_checkpoint_filtering");
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
                "TABLE test_read_part_key",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .addCopies(new FileOperation(DATA, "key=p1/", "InputFile.newInput"), 2)
                        .addCopies(new FileOperation(DATA, "key=p2/", "InputFile.newInput"), 2)
                        .build());

        // Read with aggregation (this may involve fetching stats so may incur more file system accesses)
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_read_part_key')");
        assertFileSystemAccesses(
                "SELECT key, max(data) FROM test_read_part_key GROUP BY key",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", "InputFile.newStream"))
                        .addCopies(new FileOperation(DATA, "key=p1/", "InputFile.newInput"), 2)
                        .addCopies(new FileOperation(DATA, "key=p2/", "InputFile.newInput"), 2)
                        .build());

        // Read partition column only
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_read_part_key')");
        assertFileSystemAccesses(
                "SELECT key, count(*) FROM test_read_part_key GROUP BY key",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", "InputFile.newStream"))
                        .build());

        // Read partition column only, one partition only
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_read_part_key')");
        assertFileSystemAccesses(
                "SELECT count(*) FROM test_read_part_key WHERE key = 'p1'",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .build());

        // Read partition and synthetic columns
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_read_part_key')");
        assertFileSystemAccesses(
                "SELECT count(*), array_agg(\"$path\"), max(\"$file_modified_time\") FROM test_read_part_key GROUP BY key",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", "InputFile.newStream"))
                        .build());

        // Read only row count
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_read_part_key')");
        assertFileSystemAccesses(
                "SELECT count(*) FROM test_read_part_key",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .build());

        assertUpdate("DROP TABLE test_read_part_key");
    }

    @Test
    public void testReadWholePartitionSplittableFile()
    {
        String catalog = getSession().getCatalog().orElseThrow();

        assertUpdate("DROP TABLE IF EXISTS test_read_whole_splittable_file");
        assertUpdate("CREATE TABLE test_read_whole_splittable_file(key varchar, data varchar) WITH (partitioned_by=ARRAY['key'])");

        assertUpdate(
                Session.builder(getSession())
                        .setSystemProperty(SystemSessionProperties.WRITER_SCALING_MIN_DATA_PROCESSED, "1PB")
                        .build(),
                "INSERT INTO test_read_whole_splittable_file SELECT 'single partition', comment FROM tpch.tiny.orders", 15000);

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, DeltaLakeSessionProperties.MAX_SPLIT_SIZE, "1kB")
                .build();

        // Read partition column only
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_read_whole_splittable_file')");
        assertFileSystemAccesses(
                session,
                "SELECT key, count(*) FROM test_read_whole_splittable_file GROUP BY key",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", "InputFile.newStream"))
                        .build());

        // Read only row count
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_read_whole_splittable_file')");
        assertFileSystemAccesses(
                session,
                "SELECT count(*) FROM test_read_whole_splittable_file",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .build());

        assertUpdate("DROP TABLE test_read_whole_splittable_file");
    }

    @Test
    public void testSelfJoin()
    {
        assertUpdate("CREATE TABLE test_self_join_table AS SELECT 2 as age, 0 parent, 3 AS id", 1);
        Session sessionWithoutDynamicFiltering = Session.builder(getSession())
                // Disable dynamic filtering so that the second table data scan is not being pruned
                .setSystemProperty("enable_dynamic_filtering", "false")
                .build();

        assertFileSystemAccesses(
                sessionWithoutDynamicFiltering,
                "SELECT child.age, parent.age FROM test_self_join_table child JOIN test_self_join_table parent ON child.parent = parent.id",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", "InputFile.newStream"))
                        .addCopies(new FileOperation(DATA, "no partition", "InputFile.newInput"), 2)
                        .build());

        assertUpdate("DROP TABLE test_self_join_table");
    }

    @Test
    public void testSelectFromVersionedTable()
    {
        String tableName = "test_select_from_versioned_table" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(id int)");
        for (int i = 0; i < 25; i++) {
            assertUpdate("INSERT INTO " + tableName + " VALUES " + i, 1);
        }

        assertFileSystemAccesses("SELECT * FROM " + tableName + " FOR VERSION AS OF 0",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.exists"))
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName + " FOR VERSION AS OF 1",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.exists"))
                        .add(new FileOperation(DATA, "no partition", "InputFile.newInput"))
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName + " FOR VERSION AS OF 2",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.exists"))
                        .addCopies(new FileOperation(DATA, "no partition", "InputFile.newInput"), 2)
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName + " FOR VERSION AS OF 8",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000005.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000006.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000007.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000008.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000005.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000006.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000007.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000008.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000008.json", "InputFile.exists"))
                        .addCopies(new FileOperation(DATA, "no partition", "InputFile.newInput"), 8)
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName + " FOR VERSION AS OF 13",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000010.checkpoint.parquet", "InputFile.length"), 2)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000010.checkpoint.parquet", "InputFile.newInput"), 2)
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000011.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000012.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000013.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000011.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000012.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000013.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000013.json", "InputFile.exists"))
                        .addCopies(new FileOperation(DATA, "no partition", "InputFile.newInput"), 13)
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName + " FOR VERSION AS OF 20",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000020.json", "InputFile.exists"))
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000020.checkpoint.parquet", "InputFile.length"), 2)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000020.checkpoint.parquet", "InputFile.newInput"), 2)
                        .addCopies(new FileOperation(DATA, "no partition", "InputFile.newInput"), 20)
                        .build());
        assertFileSystemAccesses("SELECT * FROM " + tableName + " FOR VERSION AS OF 23",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000020.checkpoint.parquet", "InputFile.length"), 2)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000020.checkpoint.parquet", "InputFile.newInput"), 2)
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000021.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000022.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000023.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000021.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000022.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000023.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000023.json", "InputFile.exists"))
                        .addCopies(new FileOperation(DATA, "no partition", "InputFile.newInput"), 23)
                        .build());

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDeleteWholePartition()
    {
        assertUpdate("DROP TABLE IF EXISTS test_delete_part_key");
        assertUpdate("CREATE TABLE test_delete_part_key(key varchar, data varchar) WITH (partitioned_by=ARRAY['key'])");

        // Create multiple files per partition
        assertUpdate("INSERT INTO test_delete_part_key(key, data) VALUES ('p1', '1-abc'), ('p1', '1-def'), ('p2', '2-abc'), ('p2', '2-def')", 4);
        assertUpdate("INSERT INTO test_delete_part_key(key, data) VALUES ('p1', '1-baz'), ('p2', '2-baz')", 2);

        // Delete partition column only
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_delete_part_key')");
        assertFileSystemAccesses(
                "DELETE FROM test_delete_part_key WHERE key = 'p1'",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.exists"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "OutputFile.createOrOverwrite"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .build());

        assertUpdate("DROP TABLE test_delete_part_key");
    }

    @Test
    public void testDeleteWholeTable()
    {
        assertUpdate("DROP TABLE IF EXISTS test_delete_whole_table");
        assertUpdate("CREATE TABLE test_delete_whole_table(key varchar, data varchar)");

        // Create multiple files per partition
        assertUpdate("INSERT INTO test_delete_whole_table(key, data) VALUES ('p1', '1-abc'), ('p1', '1-def'), ('p2', '2-abc'), ('p2', '2-def')", 4);
        assertUpdate("INSERT INTO test_delete_whole_table(key, data) VALUES ('p1', '1-baz'), ('p2', '2-baz')", 2);

        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_delete_whole_table')");
        assertFileSystemAccesses(
                "DELETE FROM test_delete_whole_table WHERE true",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.exists"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "OutputFile.createOrOverwrite"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .build());

        assertUpdate("DROP TABLE test_delete_whole_table");
    }

    @Test
    public void testDeleteWithNonPartitionFilter()
    {
        assertUpdate("CREATE TABLE test_delete_with_non_partition_filter (page_url VARCHAR, key VARCHAR, views INTEGER) WITH (partitioned_by=ARRAY['key'])");
        assertUpdate("INSERT INTO test_delete_with_non_partition_filter VALUES('url1', 'domain1', 1)", 1);
        assertUpdate("INSERT INTO test_delete_with_non_partition_filter VALUES('url2', 'domain2', 2)", 1);
        assertUpdate("INSERT INTO test_delete_with_non_partition_filter VALUES('url3', 'domain3', 3)", 1);

        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_delete_with_non_partition_filter')");
        assertFileSystemAccesses(
                "DELETE FROM test_delete_with_non_partition_filter WHERE page_url ='url1'",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.exists"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "OutputFile.createOrOverwrite"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.length"))
                        .addCopies(new FileOperation(DATA, "key=domain1/", "InputFile.newInput"), 2)
                        .add(new FileOperation(DATA, "key=domain1/", "InputFile.length"))
                        .add(new FileOperation(DATA, "key=domain1/", "OutputFile.create"))
                        .build());

        assertUpdate("DROP TABLE test_delete_with_non_partition_filter");
    }

    @Test
    public void testHistorySystemTable()
    {
        assertUpdate("CREATE TABLE test_history_system_table (a INT, b INT)");
        assertUpdate("INSERT INTO test_history_system_table VALUES (1, 2)", 1);
        assertUpdate("INSERT INTO test_history_system_table VALUES (2, 3)", 1);
        assertUpdate("INSERT INTO test_history_system_table VALUES (3, 4)", 1);
        assertUpdate("INSERT INTO test_history_system_table VALUES (4, 5)", 1);

        assertFileSystemAccesses("SELECT * FROM \"test_history_system_table$history\"",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.length"), 2)
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000005.json", "InputFile.length"))
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .build());

        assertFileSystemAccesses("SELECT * FROM \"test_history_system_table$history\" WHERE version = 3",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.newStream"), 2)
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"), 2)
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000005.json", "InputFile.length"))
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .build());

        assertFileSystemAccesses("SELECT * FROM \"test_history_system_table$history\" WHERE version > 3",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.newStream"))
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.newStream"), 2)
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.length"), 2)
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000005.json", "InputFile.length"))
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .build());

        assertFileSystemAccesses("SELECT * FROM \"test_history_system_table$history\" WHERE version >= 3 OR version = 1",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.newStream"), 2)
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.length"), 2)
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000005.json", "InputFile.length"))
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .build());

        assertFileSystemAccesses("SELECT * FROM \"test_history_system_table$history\" WHERE version >= 1 AND version < 3",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"), 2)
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"), 2)
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000005.json", "InputFile.length"))
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .build());

        assertFileSystemAccesses("SELECT * FROM \"test_history_system_table$history\" WHERE version > 1 AND version < 2",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000005.json", "InputFile.length"))
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .build());
    }

    @Test
    public void testTableChangesFileSystemAccess()
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
        assertFileSystemAccesses("SELECT * FROM TABLE(system.table_changes(CURRENT_SCHEMA, 'table_changes_file_system_access'))",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000005.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000006.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.length"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000005.json", "InputFile.length"), 2)
                        .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000006.json", "InputFile.length"), 2)
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000007.json", "InputFile.length"))
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(CDF_DATA, "key=domain1/", "InputFile.newInput"))
                        .addCopies(new FileOperation(CDF_DATA, "key=domain2/", "InputFile.newInput"), cdfFilesForDomain2)
                        .addCopies(new FileOperation(CDF_DATA, "key=domain3/", "InputFile.newInput"), cdfFilesForDomain3)
                        .add(new FileOperation(DATA, "key=domain1/", "InputFile.newInput"))
                        .add(new FileOperation(DATA, "key=domain2/", "InputFile.newInput"))
                        .add(new FileOperation(DATA, "key=domain3/", "InputFile.newInput"))
                        .build());
    }

    @Test
    public void testInformationSchemaColumns()
    {
        testInformationSchemaColumns(true);
        testInformationSchemaColumns(false);
    }

    private void testInformationSchemaColumns(boolean removeCachedProperties)
    {
        for (int tables : Arrays.asList(3, MAX_PREFIXES_COUNT, MAX_PREFIXES_COUNT + 3)) {
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

                if (removeCachedProperties) {
                    removeLastTransactionVersionFromMetastore(schemaName, "test_select_i_s_columns" + i);
                    removeLastTransactionVersionFromMetastore(schemaName, "test_other_select_i_s_columns" + i);
                    metadataScheduler.clear();
                }
            }

            // Store table metadata in metastore for making the file access counts deterministic
            metadataScheduler.process();

            // Bulk retrieval
            assertFileSystemAccesses(session, "SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA", removeCachedProperties ?
                    ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"), tables * 2)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"), tables * 2)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"), tables * 2)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"), tables * 2)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"), tables)
                            .build() :
                    ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.exists"), tables)
                            .build());

            // Repeat bulk retrieval. The above query should store column definitions in metastore and the below query should not open any files.
            metadataScheduler.process();
            assertFileSystemAccesses(session, "SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA",
                    ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.exists"), tables)
                            .build());

            assertFileSystemAccesses(session, "SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name LIKE 'test_select_i_s_columns%'",
                    ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.exists"), tables)
                            .build());

            // Pointed lookup
            assertFileSystemAccesses(session, "SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name = 'test_select_i_s_columns0'",
                    ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                            .build());

            // Pointed lookup with LIKE predicate (as if unintentional)
            assertFileSystemAccesses(session, "SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name LIKE 'test_select_i_s_columns0'",
                    ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.exists"), tables)
                            .build());

            // Pointed lookup via DESCRIBE (which does some additional things before delegating to information_schema.columns)
            assertFileSystemAccesses(session, "DESCRIBE test_select_i_s_columns0",
                    ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                            .build());

            for (int i = 0; i < tables; i++) {
                assertUpdate(session, "DROP TABLE test_select_i_s_columns" + i);
                assertUpdate(session, "DROP TABLE test_other_select_i_s_columns" + i);
            }
        }
    }

    @Test
    public void testSystemMetadataTableComments()
    {
        testSystemMetadataTableComments(true);
        testSystemMetadataTableComments(false);
    }

    private void testSystemMetadataTableComments(boolean removeCachedProperties)
    {
        for (int tables : Arrays.asList(3, MAX_PREFIXES_COUNT, MAX_PREFIXES_COUNT + 3)) {
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

                if (removeCachedProperties) {
                    removeLastTransactionVersionFromMetastore(schemaName, "test_select_s_m_t_comments" + i);
                    removeLastTransactionVersionFromMetastore(schemaName, "test_other_select_s_m_t_comments" + i);
                    metadataScheduler.clear();
                }
            }

            // Store table metadata in metastore for making the file access counts deterministic
            metadataScheduler.process();

            // Bulk retrieval
            assertFileSystemAccesses(session, "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA", removeCachedProperties ?
                    ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"), tables * 2)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"), tables * 2)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"), tables * 2)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"), tables * 2)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"), tables)
                            .build() :
                    ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.exists"), tables)
                            .build());

            // Repeat bulk retrieval. The above query should store table comments in metastore and the below query should not open any files.
            metadataScheduler.process();
            assertFileSystemAccesses(session, "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA",
                    ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.exists"), tables)
                            .build());

            assertFileSystemAccesses(session, "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA AND table_name LIKE 'test_select_s_m_t_comments%'",
                    ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.exists"), tables)
                            .build());

            // Bulk retrieval for two schemas
            assertFileSystemAccesses(session, "SELECT * FROM system.metadata.table_comments WHERE schema_name IN (CURRENT_SCHEMA, 'non_existent') AND table_name LIKE 'test_select_s_m_t_comments%'",
                    ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.exists"), tables)
                            .build());

            // Pointed lookup
            assertFileSystemAccesses(session, "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA AND table_name = 'test_select_s_m_t_comments0'",
                    ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                            .build());

            // Pointed lookup with LIKE predicate (as if unintentional)
            assertFileSystemAccesses(session, "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA AND table_name LIKE 'test_select_s_m_t_comments0'",
                    ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.exists"), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.exists"), tables)
                            .build());

            for (int i = 0; i < tables; i++) {
                assertUpdate(session, "DROP TABLE test_select_s_m_t_comments" + i);
                assertUpdate(session, "DROP TABLE test_other_select_s_m_t_comments" + i);
            }
        }
    }

    @Test
    public void testShowTables()
    {
        assertFileSystemAccesses("SHOW TABLES", ImmutableMultiset.of());
    }

    @Test
    public void testReadMultipartCheckpoint()
            throws Exception
    {
        String tableName = "test_multipart_checkpoint_" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(new File(Resources.getResource("deltalake/multipart_checkpoint").toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()));
        assertFileSystemAccesses("SELECT * FROM " + tableName,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000006.checkpoint.0000000001.0000000002.parquet", "InputFile.length"), 2) // TODO (https://github.com/trinodb/trino/issues/18916) should be checked once per query
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000006.checkpoint.0000000001.0000000002.parquet", "InputFile.newInput"), 2) // TODO (https://github.com/trinodb/trino/issues/18916) should be checked once per query
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000006.checkpoint.0000000002.0000000002.parquet", "InputFile.length"), 2) // TODO (https://github.com/trinodb/trino/issues/18916) should be checked once per query
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000006.checkpoint.0000000002.0000000002.parquet", "InputFile.newInput"), 2) // TODO (https://github.com/trinodb/trino/issues/18916) should be checked once per query
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000007.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000007.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000008.json", "InputFile.length"))
                        .addCopies(new FileOperation(DATA, "no partition", "InputFile.newInput"), 7)
                        .build());
    }

    @Test
    public void testV2CheckpointJson()
            throws Exception
    {
        String tableName = "test_v2_checkpoint_json_" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(new File(Resources.getResource("deltalake/v2_checkpoint_json").toURI()).toPath(), tableLocation);

        assertFileSystemAccesses("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()),
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(CHECKPOINT, "00000000000000000001.checkpoint.73a4ddb8-2bfc-40d8-b09f-1b6a0abdfb04.json", "InputFile.length"))
                        .add(new FileOperation(CHECKPOINT, "00000000000000000001.checkpoint.73a4ddb8-2bfc-40d8-b09f-1b6a0abdfb04.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length")) // TransactionLogTail.getEntriesFromJson access non-existing file as end of tail
                        .build());

        assertFileSystemAccesses("SELECT * FROM " + tableName,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000001.checkpoint.73a4ddb8-2bfc-40d8-b09f-1b6a0abdfb04.json", "InputFile.length"), 2) // TODO (https://github.com/trinodb/trino/issues/18916) should be checked once per query
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000001.checkpoint.73a4ddb8-2bfc-40d8-b09f-1b6a0abdfb04.json", "InputFile.newStream"), 2) // TODO (https://github.com/trinodb/trino/issues/18916) should be checked once per query
                        .add(new FileOperation(CHECKPOINT, "00000000000000000001.checkpoint.0000000001.0000000001.90cf4e21-dbaa-41d6-8ae5-6709cfbfbfe0.parquet", "InputFile.length"))
                        .add(new FileOperation(CHECKPOINT, "00000000000000000001.checkpoint.0000000001.0000000001.90cf4e21-dbaa-41d6-8ae5-6709cfbfbfe0.parquet", "InputFile.newInput"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length")) // TransactionLogTail.getEntriesFromJson access non-existing file as end of tail
                        .add(new FileOperation(DATA, "no partition", "InputFile.newInput"))
                        .build());

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testV2CheckpointParquet()
            throws Exception
    {
        String tableName = "test_v2_checkpoint_parquet_" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(new File(Resources.getResource("deltalake/v2_checkpoint_parquet").toURI()).toPath(), tableLocation);

        assertFileSystemAccesses("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(tableName, tableLocation.toUri()),
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", "InputFile.length"), 2)
                        .add(new FileOperation(CHECKPOINT, "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", "InputFile.newInput"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length")) // TransactionLogTail.getEntriesFromJson access non-existing file as end of tail
                        .build());

        assertFileSystemAccesses("SELECT * FROM " + tableName,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", "InputFile.length"), 4) // TODO (https://github.com/trinodb/trino/issues/18916) should be checked once per query
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet", "InputFile.newInput"), 2) // TODO (https://github.com/trinodb/trino/issues/18916) should be checked once per query
                        .add(new FileOperation(CHECKPOINT, "00000000000000000001.checkpoint.0000000001.0000000001.03288d7e-af16-44ed-829c-196064a71812.parquet", "InputFile.length"))
                        .add(new FileOperation(CHECKPOINT, "00000000000000000001.checkpoint.0000000001.0000000001.03288d7e-af16-44ed-829c-196064a71812.parquet", "InputFile.newInput"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length")) // TransactionLogTail.getEntriesFromJson access non-existing file as end of tail
                        .add(new FileOperation(DATA, "no partition", "InputFile.newInput"))
                        .build());

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDeletionVectors()
    {
        String tableName = "test_deletion_vectors_" + randomNameSuffix();
        registerTable(tableName, "databricks122/deletion_vectors");
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => '%s')".formatted(tableName));
        assertFileSystemAccesses(
                "SELECT * FROM " + tableName,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(DELETION_VECTOR, "deletion_vector_a52eda8c-0a57-4636-814b-9c165388f7ca.bin", "InputFile.newInput"))
                        .add(new FileOperation(DATA, "no partition", "InputFile.newInput"))
                        .build());
        assertFileSystemAccesses(
                "EXPLAIN ANALYZE SELECT * FROM " + tableName,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .add(new FileOperation(STARBURST_EXTENDED_STATS_JSON, "extendeded_stats.json", "InputFile.newStream"))
                        .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", "InputFile.newStream"))
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(DELETION_VECTOR, "deletion_vector_a52eda8c-0a57-4636-814b-9c165388f7ca.bin", "InputFile.newInput"))
                        .add(new FileOperation(DATA, "no partition", "InputFile.newInput"))
                        .build());

        assertUpdate("DROP TABLE " + tableName);
    }

    private int countCdfFilesForKey(String partitionValue)
    {
        String path = (String) computeScalar("SELECT \"$path\" FROM table_changes_file_system_access WHERE key = '" + partitionValue + "'");
        String partitionKey = "key=" + partitionValue;
        String tableLocation = path.substring(0, path.lastIndexOf(partitionKey));
        String partitionCdfFolder = URI.create(tableLocation).getPath() + "_change_data/" + partitionKey + "/";
        return toIntExact(Arrays.stream(new File(partitionCdfFolder).list()).filter(file -> !file.contains(".crc")).count());
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<FileOperation> expectedAccesses)
    {
        assertFileSystemAccesses(getSession(), query, expectedAccesses);
    }

    private void assertFileSystemAccesses(Session session, @Language("SQL") String query, Multiset<FileOperation> expectedAccesses)
    {
        assertUpdate("CALL system.flush_metadata_cache()");

        getDistributedQueryRunner().executeWithPlan(session, query);
        List<SpanData> spanData = getDistributedQueryRunner().getSpans();
        assertMultisetsEqual(getOperations(spanData), expectedAccesses);
    }

    private Multiset<FileOperation> getOperations(List<SpanData> spanData)
    {
        return spanData.stream()
                .filter(span -> span.getName().startsWith("InputFile.") || span.getName().startsWith("OutputFile."))
                .filter(span -> {
                    String path = requireNonNull(span.getAttributes().get(FILE_LOCATION), "FILE_LOCATION attribute is empty");
                    return !path.endsWith(".trinoSchema") && !path.contains(".trinoPermissions");
                })
                .map(span -> FileOperation.create(span.getAttributes().get(FILE_LOCATION), span.getName()))
                .collect(toCollection(HashMultiset::create));
    }

    private record FileOperation(FileType fileType, String fileId, String operationType)
    {
        public static FileOperation create(String path, String operationType)
        {
            String fileName = path.replaceFirst(".*/", "");
            if (path.matches(".*/_delta_log/_last_checkpoint")) {
                return new FileOperation(LAST_CHECKPOINT, fileName, operationType);
            }
            if (path.matches(".*/_delta_log/.*.checkpoint.*")) {
                return new FileOperation(CHECKPOINT, fileName, operationType);
            }
            if (path.matches(".*/_delta_log/\\d+\\.json")) {
                return new FileOperation(TRANSACTION_LOG_JSON, fileName, operationType);
            }
            if (path.matches(".*/_delta_log/_trino_meta/extended_stats.json")) {
                return new FileOperation(TRINO_EXTENDED_STATS_JSON, fileName, operationType);
            }
            if (path.matches(".*/_delta_log/_starburst_meta/extendeded_stats.json")) {
                return new FileOperation(STARBURST_EXTENDED_STATS_JSON, fileName, operationType);
            }
            if (path.contains("/deletion_vector_")) {
                return new FileOperation(DELETION_VECTOR, fileName, operationType);
            }
            Pattern dataFilePattern = Pattern.compile(".*?/(?<partition>key=[^/]*/)?[^/]+");
            if (path.matches(".*/_change_data/.*")) {
                Matcher matcher = dataFilePattern.matcher(path);
                if (matcher.matches()) {
                    return new FileOperation(CDF_DATA, matcher.group("partition"), operationType);
                }
            }
            if (!path.contains("_delta_log")) {
                Matcher matcher = dataFilePattern.matcher(path);
                if (matcher.matches()) {
                    return new FileOperation(DATA, firstNonNull(matcher.group("partition"), "no partition"), operationType);
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
        CHECKPOINT,
        TRANSACTION_LOG_JSON,
        TRINO_EXTENDED_STATS_JSON,
        STARBURST_EXTENDED_STATS_JSON,
        DATA,
        CDF_DATA,
        DELETION_VECTOR,
        /**/;
    }

    private void registerTable(String name, String resourcePath)
    {
        String dataPath = getResourceLocation(resourcePath).toExternalForm();
        getQueryRunner().execute(format("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')", name, dataPath));
    }

    private URL getResourceLocation(String resourcePath)
    {
        return getClass().getClassLoader().getResource(resourcePath);
    }

    private void removeLastTransactionVersionFromMetastore(String schemaName, String tableName)
    {
        Table table = metastore.getTable(schemaName, tableName).orElseThrow();
        Table newMetastoreTable = Table.builder(table)
                .setParameters(filterKeys(table.getParameters(), key -> !key.equals("trino_last_transaction_version")))
                .build();
        metastore.replaceTable(table.getDatabaseName(), table.getTableName(), newMetastoreTable, buildInitialPrivilegeSet(table.getOwner().orElseThrow()));
    }
}
