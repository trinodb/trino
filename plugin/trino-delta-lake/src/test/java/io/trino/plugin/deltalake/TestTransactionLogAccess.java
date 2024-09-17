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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import io.airlift.units.Duration;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TypeManager;
import io.trino.testing.TestingConnectorContext;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingTelemetry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.collect.Sets.union;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractColumnMetadata;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.LAST_CHECKPOINT_FILENAME;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.TRANSACTION_LOG_DIRECTORY;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // e.g. DistributedQueryRunner.spans is shared mutable state
public class TestTransactionLogAccess
{
    private static final Set<String> EXPECTED_ADD_FILE_PATHS = ImmutableSet.of(
            "age=42/part-00000-b26c891a-7288-4d96-9d3b-bef648f12a34.c000.snappy.parquet",
            "age=25/part-00001-aceaf062-1cd1-45cb-8f83-277ffebe995c.c000.snappy.parquet",
            "age=25/part-00000-b7fbbe31-c7f9-44ed-8757-5c47d10c3e81.c000.snappy.parquet",
            "age=42/part-00003-0f53cae3-3e34-4876-b651-e1db9584dbc3.c000.snappy.parquet",
            "age=30/part-00000-63c2205d-84a3-4a66-bd7c-f69f5af55bbc.c000.snappy.parquet",
            "age=25/part-00000-22a101a1-8f09-425e-847e-cbbe4f894eea.c000.snappy.parquet",
            "age=21/part-00001-290f0f26-19cf-4772-821e-36d55d9b7872.c000.snappy.parquet",
            "age=30/part-00002-5800be2e-2373-47d8-8b86-776a8ea9d69f.c000.snappy.parquet",
            "age=21/part-00000-3d546786-bedc-407f-b9f7-e97aa12cce0f.c000.snappy.parquet",
            "age=30/part-00000-37ccfcd3-b44b-4d04-a1e6-d2837da75f7a.c000.snappy.parquet",
            "age=28/part-00000-40dd1707-1d42-4328-a59a-21f5c945fe60.c000.snappy.parquet",
            "age=29/part-00000-3794c463-cb0c-4beb-8d07-7cc1e3b5920f.c000.snappy.parquet");

    private static final Set<RemoveFileEntry> EXPECTED_REMOVE_ENTRIES = ImmutableSet.of(
            new RemoveFileEntry("age=30/part-00000-7e43a3c3-ea26-4ae7-8eac-8f60cbb4df03.c000.snappy.parquet", null, 1579190163932L, false, Optional.empty()),
            new RemoveFileEntry("age=30/part-00000-72a56c23-01ba-483a-9062-dd0accc86599.c000.snappy.parquet", null, 1579190163932L, false, Optional.empty()),
            new RemoveFileEntry("age=42/part-00000-951068bd-bcf4-4094-bb94-536f3c41d31f.c000.snappy.parquet", null, 1579190155406L, false, Optional.empty()),
            new RemoveFileEntry("age=25/part-00000-609e34b1-5466-4dbc-a780-2708166e7adb.c000.snappy.parquet", null, 1579190163932L, false, Optional.empty()),
            new RemoveFileEntry("age=42/part-00000-6aed618a-2beb-4edd-8466-653e67a9b380.c000.snappy.parquet", null, 1579190155406L, false, Optional.empty()),
            new RemoveFileEntry("age=42/part-00000-b82d8859-84a0-4f05-872c-206b07dd54f0.c000.snappy.parquet", null, 1579190163932L, false, Optional.empty()));

    private final TestingTelemetry testingTelemetry = TestingTelemetry.create("transaction-log-access");
    private final TracingFileSystemFactory tracingFileSystemFactory = new TracingFileSystemFactory(testingTelemetry.getTracer(), new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS));

    private TransactionLogAccess transactionLogAccess;
    private TableSnapshot tableSnapshot;

    private void setupTransactionLogAccessFromResources(String tableName, String resourcePath)
            throws Exception
    {
        setupTransactionLogAccess(tableName, getClass().getClassLoader().getResource(resourcePath).toString());
    }

    private void setupTransactionLogAccess(String tableName, String tableLocation)
            throws IOException
    {
        setupTransactionLogAccess(tableName, tableLocation, new DeltaLakeConfig(), Optional.empty());
    }

    private void setupTransactionLogAccess(String tableName, String tableLocation, DeltaLakeConfig deltaLakeConfig, Optional<Long> endVersion)
            throws IOException
    {
        TestingConnectorContext context = new TestingConnectorContext();
        TypeManager typeManager = context.getTypeManager();

        FileFormatDataSourceStats fileFormatDataSourceStats = new FileFormatDataSourceStats();

        transactionLogAccess = new TransactionLogAccess(
                typeManager,
                new CheckpointSchemaManager(typeManager),
                deltaLakeConfig,
                fileFormatDataSourceStats,
                tracingFileSystemFactory,
                new ParquetReaderConfig());

        DeltaLakeTableHandle tableHandle = new DeltaLakeTableHandle(
                "schema",
                tableName,
                true,
                "location",
                new MetadataEntry("id", "test", "description", null, "", ImmutableList.of(), ImmutableMap.of(), 0),
                new ProtocolEntry(1, 2, Optional.empty(), Optional.empty()),
                TupleDomain.none(),
                TupleDomain.none(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                0,
                false);

        tableSnapshot = transactionLogAccess.loadSnapshot(SESSION, tableHandle.getSchemaTableName(), tableLocation, endVersion);
    }

    @Test
    public void testGetMetadataEntry()
            throws Exception
    {
        setupTransactionLogAccessFromResources("person", "databricks73/person");

        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);

        assertThat(metadataEntry.getCreatedTime()).isEqualTo(1579190100722L);
        assertThat(metadataEntry.getId()).isEqualTo("b6aeffad-da73-4dde-b68e-937e468b1fdf");
        assertThat(metadataEntry.getOriginalPartitionColumns()).containsOnly("age");
        assertThat(metadataEntry.getLowercasePartitionColumns()).containsOnly("age");

        MetadataEntry.Format format = metadataEntry.getFormat();
        assertThat(format.options().keySet()).isEmpty();
        assertThat(format.provider()).isEqualTo("parquet");

        assertThat(tableSnapshot.getCachedMetadata()).isEqualTo(Optional.of(metadataEntry));
    }

    @Test
    public void testGetMetadataEntryUppercase()
            throws Exception
    {
        setupTransactionLogAccessFromResources("uppercase_columns", "databricks73/uppercase_columns");
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);
        assertThat(metadataEntry.getOriginalPartitionColumns()).containsOnly("ALA");
        assertThat(metadataEntry.getLowercasePartitionColumns()).containsOnly("ala");
        assertThat(tableSnapshot.getCachedMetadata()).isEqualTo(Optional.of(metadataEntry));
    }

    @Test
    public void testGetActiveAddEntries()
            throws Exception
    {
        setupTransactionLogAccessFromResources("person", "databricks73/person");

        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(SESSION, tableSnapshot);
        List<AddFileEntry> addFileEntries;
        try (Stream<AddFileEntry> addFileEntriesStream = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
            addFileEntries = addFileEntriesStream.collect(toImmutableList());
        }
        Set<String> paths = addFileEntries
                .stream()
                .map(AddFileEntry::getPath)
                .collect(Collectors.toSet());
        assertThat(paths).isEqualTo(EXPECTED_ADD_FILE_PATHS);

        try (Stream<AddFileEntry> checkpointFilteredAddFileEntriesStream = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
            assertThat(checkpointFilteredAddFileEntriesStream.collect(toImmutableList())).isEqualTo(addFileEntries);
        }

        AddFileEntry addFileEntry = addFileEntries
                .stream()
                .filter(entry -> entry.getPath().equals("age=42/part-00000-b26c891a-7288-4d96-9d3b-bef648f12a34.c000.snappy.parquet"))
                .collect(onlyElement());

        assertThat(addFileEntry.getPartitionValues())
                .hasSize(1)
                .containsEntry("age", "42");
        assertThat(addFileEntry.getCanonicalPartitionValues())
                .hasSize(1)
                .containsEntry("age", Optional.of("42"));

        assertThat(addFileEntry.getSize()).isEqualTo(2687);
        assertThat(addFileEntry.getModificationTime()).isEqualTo(1579190188000L);
        assertThat(addFileEntry.isDataChange()).isFalse();
    }

    @Test
    public void testAddFileEntryUppercase()
            throws Exception
    {
        setupTransactionLogAccessFromResources("uppercase_columns", "databricks73/uppercase_columns");

        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(SESSION, tableSnapshot);

        try (Stream<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
            AddFileEntry addFileEntry = addFileEntries
                    .filter(entry -> entry.getPath().equals("ALA=1/part-00000-20a863e0-890d-4776-8825-f9dccc8973ba.c000.snappy.parquet"))
                    .collect(onlyElement());

            assertThat(addFileEntry.getPartitionValues())
                    .hasSize(1)
                    .containsEntry("ALA", "1");
            assertThat(addFileEntry.getCanonicalPartitionValues())
                    .hasSize(1)
                    .containsEntry("ALA", Optional.of("1"));
        }
    }

    @Test
    public void testAddEntryPruning()
            throws Exception
    {
        // Test data contains two add entries which should be pruned:
        // - Added in the parquet checkpoint but removed in a JSON commit
        // - Added in a JSON commit and removed in a later JSON commit
        setupTransactionLogAccessFromResources("person_test_pruning", "databricks73/person_test_pruning");
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(SESSION, tableSnapshot);
        try (Stream<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
            Set<String> paths = addFileEntries
                    .map(AddFileEntry::getPath)
                    .collect(Collectors.toSet());

            assertThat(paths.contains("age=25/part-00001-aceaf062-1cd1-45cb-8f83-277ffebe995c.c000.snappy.parquet")).isFalse();
            assertThat(paths.contains("age=29/part-00000-3794c463-cb0c-4beb-8d07-7cc1e3b5920f.c000.snappy.parquet")).isFalse();
        }
    }

    @Test
    public void testAddEntryOverrides()
            throws Exception
    {
        setupTransactionLogAccessFromResources("person_test_pruning", "databricks73/person_test_pruning");
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(SESSION, tableSnapshot);
        List<AddFileEntry> addFileEntries;
        try (Stream<AddFileEntry> addFileEntryStream = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
            addFileEntries = addFileEntryStream.collect(toImmutableList());
        }

        // Test data contains two entries which are added multiple times, the most up to date one should be the only one in the active list
        List<String> overwrittenPaths = ImmutableList.of(
                "age=42/part-00000-b26c891a-7288-4d96-9d3b-bef648f12a34.c000.snappy.parquet",
                "age=28/part-00000-40dd1707-1d42-4328-a59a-21f5c945fe60.c000.snappy.parquet");

        for (String path : overwrittenPaths) {
            List<AddFileEntry> activeEntries = addFileEntries.stream()
                    .filter(addFileEntry -> addFileEntry.getPath().equals(path))
                    .toList();
            assertThat(activeEntries.size()).isEqualTo(1);
            assertThat(activeEntries.get(0).getModificationTime()).isEqualTo(9999999L);
        }
    }

    @Test
    public void testAddRemoveAdd()
            throws Exception
    {
        setupTransactionLogAccessFromResources("person_test_pruning", "databricks73/person_test_pruning");
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(SESSION, tableSnapshot);
        try (Stream<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
            // Test data contains an entry added by the parquet checkpoint, removed by a JSON action, and then added back by a later JSON action
            List<AddFileEntry> activeEntries = addFileEntries
                    .filter(addFileEntry -> addFileEntry.getPath().equals("age=30/part-00002-5800be2e-2373-47d8-8b86-776a8ea9d69f.c000.snappy.parquet"))
                    .toList();

            assertThat(activeEntries.size()).isEqualTo(1);
            assertThat(activeEntries.get(0).getModificationTime()).isEqualTo(9999999L);
        }
    }

    @Test
    public void testGetRemoveEntries()
            throws Exception
    {
        setupTransactionLogAccessFromResources("person", "databricks73/person");

        try (Stream<RemoveFileEntry> removeEntries = transactionLogAccess.getRemoveEntries(SESSION, tableSnapshot)) {
            Set<RemoveFileEntry> removedEntries = removeEntries.collect(Collectors.toSet());
            assertThat(removedEntries).isEqualTo(EXPECTED_REMOVE_ENTRIES);
        }
    }

    @Test
    public void testAllGetMetadataEntry()
            throws Exception
    {
        testAllGetMetadataEntry("person", "databricks73/person");
        testAllGetMetadataEntry("person_without_last_checkpoint", "databricks73/person_without_last_checkpoint");
        testAllGetMetadataEntry("person_without_old_jsons", "databricks73/person_without_old_jsons");
        testAllGetMetadataEntry("person_without_checkpoints", "databricks73/person_without_checkpoints");
    }

    private void testAllGetMetadataEntry(String tableName, String resourcePath)
            throws Exception
    {
        setupTransactionLogAccessFromResources(tableName, resourcePath);

        transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);

        assertThat(metadataEntry.getOriginalPartitionColumns()).containsOnly("age");

        MetadataEntry.Format format = metadataEntry.getFormat();
        assertThat(format.options().keySet()).isEmpty();
        assertThat(format.provider()).isEqualTo("parquet");
    }

    @Test
    public void testAllGetActiveAddEntries()
            throws Exception
    {
        testAllGetActiveAddEntries("person", "databricks73/person");
        testAllGetActiveAddEntries("person_without_last_checkpoint", "databricks73/person_without_last_checkpoint");
        testAllGetActiveAddEntries("person_without_old_jsons", "databricks73/person_without_old_jsons");
        testAllGetActiveAddEntries("person_without_checkpoints", "databricks73/person_without_checkpoints");
    }

    private void testAllGetActiveAddEntries(String tableName, String resourcePath)
            throws Exception
    {
        setupTransactionLogAccessFromResources(tableName, resourcePath);
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(SESSION, tableSnapshot);
        try (Stream<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
            Set<String> paths = addFileEntries
                    .map(AddFileEntry::getPath)
                    .collect(Collectors.toSet());

            assertThat(paths).isEqualTo(EXPECTED_ADD_FILE_PATHS);
        }
    }

    @Test
    public void testAllGetRemoveEntries()
            throws Exception
    {
        testAllGetRemoveEntries("person", "databricks73/person");
        testAllGetRemoveEntries("person_without_last_checkpoint", "databricks73/person_without_last_checkpoint");
        testAllGetRemoveEntries("person_without_old_jsons", "databricks73/person_without_old_jsons");
        testAllGetRemoveEntries("person_without_checkpoints", "databricks73/person_without_checkpoints");
    }

    private void testAllGetRemoveEntries(String tableName, String resourcePath)
            throws Exception
    {
        setupTransactionLogAccessFromResources(tableName, resourcePath);

        try (Stream<RemoveFileEntry> removeEntries = transactionLogAccess.getRemoveEntries(SESSION, tableSnapshot)) {
            Set<String> removedPaths = removeEntries.map(RemoveFileEntry::path).collect(Collectors.toSet());
            Set<String> expectedPaths = EXPECTED_REMOVE_ENTRIES.stream().map(RemoveFileEntry::path).collect(Collectors.toSet());

            assertThat(removedPaths).isEqualTo(expectedPaths);
        }
    }

    @Test
    public void testAllGetProtocolEntries()
            throws Exception
    {
        testAllGetProtocolEntries("person", "databricks73/person");
        testAllGetProtocolEntries("person_without_last_checkpoint", "databricks73/person_without_last_checkpoint");
        testAllGetProtocolEntries("person_without_old_jsons", "databricks73/person_without_old_jsons");
        testAllGetProtocolEntries("person_without_checkpoints", "databricks73/person_without_checkpoints");
    }

    private void testAllGetProtocolEntries(String tableName, String resourcePath)
            throws Exception
    {
        setupTransactionLogAccessFromResources(tableName, resourcePath);

        try (Stream<ProtocolEntry> protocolEntryStream = transactionLogAccess.getProtocolEntries(SESSION, tableSnapshot)) {
            List<ProtocolEntry> protocolEntries = protocolEntryStream.toList();
            assertThat(protocolEntries.size()).isEqualTo(1);
            assertThat(protocolEntries.get(0).minReaderVersion()).isEqualTo(1);
            assertThat(protocolEntries.get(0).minWriterVersion()).isEqualTo(2);
        }
    }

    @Test
    public void testMetadataCacheUpdates()
            throws Exception
    {
        String tableName = "person";
        File tempDir = Files.createTempDirectory(null).toFile();
        File tableDir = new File(tempDir, tableName);
        File transactionLogDir = new File(tableDir, TRANSACTION_LOG_DIRECTORY);
        transactionLogDir.mkdirs();

        java.nio.file.Path resourceDir = java.nio.file.Paths.get(getClass().getClassLoader().getResource("databricks73/person/_delta_log").toURI());
        for (int i = 0; i < 12; i++) {
            String extension = i == 10 ? ".checkpoint.parquet" : ".json";
            String fileName = format("%020d%s", i, extension);
            Files.copy(resourceDir.resolve(fileName), new File(transactionLogDir, fileName).toPath());
        }
        Files.copy(resourceDir.resolve(LAST_CHECKPOINT_FILENAME), new File(transactionLogDir, LAST_CHECKPOINT_FILENAME).toPath());

        setupTransactionLogAccess(tableName, tableDir.toURI().toString());
        assertThat(tableSnapshot.getVersion()).isEqualTo(11L);

        String lastTransactionName = format("%020d.json", 12);
        Files.copy(resourceDir.resolve(lastTransactionName), new File(transactionLogDir, lastTransactionName).toPath());
        TableSnapshot updatedSnapshot = transactionLogAccess.loadSnapshot(SESSION, new SchemaTableName("schema", tableName), tableDir.toURI().toString(), Optional.empty());
        assertThat(updatedSnapshot.getVersion()).isEqualTo(12);
    }

    @Test
    public void testUpdatingTailEntriesNoCheckpoint()
            throws Exception
    {
        String tableName = "person";
        File tempDir = Files.createTempDirectory(null).toFile();
        File tableDir = new File(tempDir, tableName);
        File transactionLogDir = new File(tableDir, TRANSACTION_LOG_DIRECTORY);
        transactionLogDir.mkdirs();

        File resourceDir = new File(getClass().getClassLoader().getResource("databricks73/person/_delta_log").toURI());
        copyTransactionLogEntry(0, 7, resourceDir, transactionLogDir);
        setupTransactionLogAccess(tableName, tableDir.toURI().toString());
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(SESSION, tableSnapshot);
        try (Stream<AddFileEntry> activeDataFiles = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
            Set<String> dataFiles = ImmutableSet.of(
                    "age=42/part-00000-b82d8859-84a0-4f05-872c-206b07dd54f0.c000.snappy.parquet",
                    "age=30/part-00000-72a56c23-01ba-483a-9062-dd0accc86599.c000.snappy.parquet",
                    "age=25/part-00000-609e34b1-5466-4dbc-a780-2708166e7adb.c000.snappy.parquet",
                    "age=30/part-00000-7e43a3c3-ea26-4ae7-8eac-8f60cbb4df03.c000.snappy.parquet",
                    "age=21/part-00000-3d546786-bedc-407f-b9f7-e97aa12cce0f.c000.snappy.parquet",
                    "age=21/part-00001-290f0f26-19cf-4772-821e-36d55d9b7872.c000.snappy.parquet");

            assertEqualsIgnoreOrder(activeDataFiles.map(AddFileEntry::getPath).collect(Collectors.toSet()), dataFiles);
        }

        copyTransactionLogEntry(7, 9, resourceDir, transactionLogDir);
        TableSnapshot updatedSnapshot = transactionLogAccess.loadSnapshot(SESSION, new SchemaTableName("schema", tableName), tableDir.toURI().toString(), Optional.empty());
        try (Stream<AddFileEntry> activeDataFiles = transactionLogAccess.getActiveFiles(SESSION, updatedSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
            Set<String> dataFiles = ImmutableSet.of(
                    "age=21/part-00000-3d546786-bedc-407f-b9f7-e97aa12cce0f.c000.snappy.parquet",
                    "age=21/part-00001-290f0f26-19cf-4772-821e-36d55d9b7872.c000.snappy.parquet",
                    "age=30/part-00000-63c2205d-84a3-4a66-bd7c-f69f5af55bbc.c000.snappy.parquet",
                    "age=25/part-00001-aceaf062-1cd1-45cb-8f83-277ffebe995c.c000.snappy.parquet",
                    "age=30/part-00002-5800be2e-2373-47d8-8b86-776a8ea9d69f.c000.snappy.parquet",
                    "age=42/part-00003-0f53cae3-3e34-4876-b651-e1db9584dbc3.c000.snappy.parquet",
                    "age=25/part-00000-b7fbbe31-c7f9-44ed-8757-5c47d10c3e81.c000.snappy.parquet");

            assertEqualsIgnoreOrder(activeDataFiles.map(AddFileEntry::getPath).collect(Collectors.toSet()), dataFiles);
        }
    }

    @Test
    public void testLoadingTailEntriesPastCheckpoint()
            throws Exception
    {
        String tableName = "person";
        File tempDir = Files.createTempDirectory(null).toFile();
        File tableDir = new File(tempDir, tableName);
        File transactionLogDir = new File(tableDir, TRANSACTION_LOG_DIRECTORY);
        transactionLogDir.mkdirs();

        File resourceDir = new File(getClass().getClassLoader().getResource("databricks73/person/_delta_log").toURI());
        copyTransactionLogEntry(0, 8, resourceDir, transactionLogDir);
        setupTransactionLogAccess(tableName, tableDir.toURI().toString());

        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(SESSION, tableSnapshot);
        try (Stream<AddFileEntry> activeDataFiles = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
            Set<String> dataFiles = ImmutableSet.of(
                    "age=21/part-00000-3d546786-bedc-407f-b9f7-e97aa12cce0f.c000.snappy.parquet",
                    "age=21/part-00001-290f0f26-19cf-4772-821e-36d55d9b7872.c000.snappy.parquet",
                    "age=30/part-00000-63c2205d-84a3-4a66-bd7c-f69f5af55bbc.c000.snappy.parquet",
                    "age=25/part-00001-aceaf062-1cd1-45cb-8f83-277ffebe995c.c000.snappy.parquet",
                    "age=30/part-00002-5800be2e-2373-47d8-8b86-776a8ea9d69f.c000.snappy.parquet",
                    "age=42/part-00003-0f53cae3-3e34-4876-b651-e1db9584dbc3.c000.snappy.parquet");

            assertEqualsIgnoreOrder(activeDataFiles.map(AddFileEntry::getPath).collect(Collectors.toSet()), dataFiles);
        }

        copyTransactionLogEntry(8, 12, resourceDir, transactionLogDir);
        Files.copy(new File(resourceDir, LAST_CHECKPOINT_FILENAME).toPath(), new File(transactionLogDir, LAST_CHECKPOINT_FILENAME).toPath());
        TableSnapshot updatedSnapshot = transactionLogAccess.loadSnapshot(SESSION, new SchemaTableName("schema", tableName), tableDir.toURI().toString(), Optional.empty());
        try (Stream<AddFileEntry> activeDataFiles = transactionLogAccess.getActiveFiles(SESSION, updatedSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
            Set<String> dataFiles = ImmutableSet.of(
                    "age=21/part-00000-3d546786-bedc-407f-b9f7-e97aa12cce0f.c000.snappy.parquet",
                    "age=21/part-00001-290f0f26-19cf-4772-821e-36d55d9b7872.c000.snappy.parquet",
                    "age=30/part-00000-63c2205d-84a3-4a66-bd7c-f69f5af55bbc.c000.snappy.parquet",
                    "age=25/part-00001-aceaf062-1cd1-45cb-8f83-277ffebe995c.c000.snappy.parquet",
                    "age=30/part-00002-5800be2e-2373-47d8-8b86-776a8ea9d69f.c000.snappy.parquet",
                    "age=42/part-00003-0f53cae3-3e34-4876-b651-e1db9584dbc3.c000.snappy.parquet",
                    "age=25/part-00000-b7fbbe31-c7f9-44ed-8757-5c47d10c3e81.c000.snappy.parquet",
                    "age=25/part-00000-22a101a1-8f09-425e-847e-cbbe4f894eea.c000.snappy.parquet",
                    "age=42/part-00000-b26c891a-7288-4d96-9d3b-bef648f12a34.c000.snappy.parquet",
                    "age=30/part-00000-37ccfcd3-b44b-4d04-a1e6-d2837da75f7a.c000.snappy.parquet");

            assertEqualsIgnoreOrder(activeDataFiles.map(AddFileEntry::getPath).collect(Collectors.toSet()), dataFiles);
        }
    }

    @Test
    public void testIncrementalCacheUpdates()
            throws Exception
    {
        setupTransactionLogAccessFromResources("person", "databricks73/person");

        String tableName = "person";
        File tempDir = Files.createTempDirectory(null).toFile();
        File tableDir = new File(tempDir, tableName);
        File transactionLogDir = new File(tableDir, TRANSACTION_LOG_DIRECTORY);
        transactionLogDir.mkdirs();

        File resourceDir = new File(getClass().getClassLoader().getResource("databricks73/person/_delta_log").toURI());
        copyTransactionLogEntry(0, 12, resourceDir, transactionLogDir);
        Files.copy(new File(resourceDir, LAST_CHECKPOINT_FILENAME).toPath(), new File(transactionLogDir, LAST_CHECKPOINT_FILENAME).toPath());
        ConnectorSession activeDataFileCacheSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new DeltaLakeSessionProperties(
                        new DeltaLakeConfig().setCheckpointFilteringEnabled(false),
                        new ParquetReaderConfig(),
                        new ParquetWriterConfig())
                        .getSessionProperties())
                .build();
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(activeDataFileCacheSession, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(activeDataFileCacheSession, tableSnapshot);

        Set<String> originalDataFiles = ImmutableSet.of(
                "age=42/part-00000-b26c891a-7288-4d96-9d3b-bef648f12a34.c000.snappy.parquet",
                "age=25/part-00001-aceaf062-1cd1-45cb-8f83-277ffebe995c.c000.snappy.parquet",
                "age=25/part-00000-b7fbbe31-c7f9-44ed-8757-5c47d10c3e81.c000.snappy.parquet",
                "age=42/part-00003-0f53cae3-3e34-4876-b651-e1db9584dbc3.c000.snappy.parquet",
                "age=30/part-00000-63c2205d-84a3-4a66-bd7c-f69f5af55bbc.c000.snappy.parquet",
                "age=25/part-00000-22a101a1-8f09-425e-847e-cbbe4f894eea.c000.snappy.parquet",
                "age=21/part-00001-290f0f26-19cf-4772-821e-36d55d9b7872.c000.snappy.parquet",
                "age=30/part-00002-5800be2e-2373-47d8-8b86-776a8ea9d69f.c000.snappy.parquet",
                "age=21/part-00000-3d546786-bedc-407f-b9f7-e97aa12cce0f.c000.snappy.parquet",
                "age=30/part-00000-37ccfcd3-b44b-4d04-a1e6-d2837da75f7a.c000.snappy.parquet");

        assertFileSystemAccesses(
                () -> {
                    setupTransactionLogAccess(tableName, tableDir.toURI().toString());
                    try (Stream<AddFileEntry> activeDataFiles = transactionLogAccess.getActiveFiles(activeDataFileCacheSession, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
                        assertEqualsIgnoreOrder(activeDataFiles.map(AddFileEntry::getPath).collect(Collectors.toSet()), originalDataFiles);
                    }
                },
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000010.checkpoint.parquet", "InputFile.length"))
                        .add(new FileOperation("00000000000000000010.checkpoint.parquet", "InputFile.newInput"))
                        .add(new FileOperation("00000000000000000011.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000012.json", "InputFile.newStream"))
                        .build());

        copyTransactionLogEntry(12, 14, resourceDir, transactionLogDir);
        Set<String> newDataFiles = ImmutableSet.of(
                "age=28/part-00000-40dd1707-1d42-4328-a59a-21f5c945fe60.c000.snappy.parquet",
                "age=29/part-00000-3794c463-cb0c-4beb-8d07-7cc1e3b5920f.c000.snappy.parquet");
        assertFileSystemAccesses(
                () -> {
                    TableSnapshot updatedTableSnapshot = transactionLogAccess.loadSnapshot(activeDataFileCacheSession, new SchemaTableName("schema", tableName), tableDir.toURI().toString(), Optional.empty());
                    try (Stream<AddFileEntry> activeDataFiles = transactionLogAccess.getActiveFiles(activeDataFileCacheSession, updatedTableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
                        assertEqualsIgnoreOrder(activeDataFiles.map(AddFileEntry::getPath).collect(Collectors.toSet()), union(originalDataFiles, newDataFiles));
                    }
                },
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("_last_checkpoint", "InputFile.newStream"))
                        .addCopies(new FileOperation("00000000000000000012.json", "InputFile.newStream"), 2)
                        .addCopies(new FileOperation("00000000000000000013.json", "InputFile.newStream"), 2)
                        .add(new FileOperation("00000000000000000014.json", "InputFile.newStream"))
                        .build());
    }

    @Test
    public void testSnapshotsAreConsistent()
            throws Exception
    {
        String tableName = "person";
        File tempDir = Files.createTempDirectory(null).toFile();
        File tableDir = new File(tempDir, tableName);
        File transactionLogDir = new File(tableDir, TRANSACTION_LOG_DIRECTORY);
        transactionLogDir.mkdirs();

        File resourceDir = new File(getClass().getClassLoader().getResource("databricks73/person/_delta_log").toURI());
        copyTransactionLogEntry(0, 12, resourceDir, transactionLogDir);
        Files.copy(new File(resourceDir, LAST_CHECKPOINT_FILENAME).toPath(), new File(transactionLogDir, LAST_CHECKPOINT_FILENAME).toPath());

        setupTransactionLogAccess(tableName, tableDir.toURI().toString());
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(SESSION, tableSnapshot);
        List<AddFileEntry> expectedDataFiles;
        try (Stream<AddFileEntry> addFileEntryStream = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
            expectedDataFiles = addFileEntryStream.collect(toImmutableList());
        }

        copyTransactionLogEntry(12, 14, resourceDir, transactionLogDir);
        Set<String> newDataFiles = ImmutableSet.of(
                "age=28/part-00000-40dd1707-1d42-4328-a59a-21f5c945fe60.c000.snappy.parquet",
                "age=29/part-00000-3794c463-cb0c-4beb-8d07-7cc1e3b5920f.c000.snappy.parquet");
        TableSnapshot updatedTableSnapshot = transactionLogAccess.loadSnapshot(SESSION, new SchemaTableName("schema", tableName), tableDir.toURI().toString(), Optional.empty());
        List<AddFileEntry> allDataFiles;
        try (Stream<AddFileEntry> addFileEntryStream = transactionLogAccess.getActiveFiles(SESSION, updatedTableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
            allDataFiles = addFileEntryStream.collect(toImmutableList());
        }
        List<AddFileEntry> dataFilesWithFixedVersion;
        try (Stream<AddFileEntry> addFileEntryStream = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
            dataFilesWithFixedVersion = addFileEntryStream.collect(toImmutableList());
        }
        for (String newFilePath : newDataFiles) {
            assertThat(allDataFiles.stream().anyMatch(entry -> entry.getPath().equals(newFilePath))).isTrue();
            assertThat(dataFilesWithFixedVersion.stream().noneMatch(entry -> entry.getPath().equals(newFilePath))).isTrue();
        }

        assertThat(expectedDataFiles.size()).isEqualTo(dataFilesWithFixedVersion.size());
        List<ColumnMetadata> columns = extractColumnMetadata(transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot), transactionLogAccess.getProtocolEntry(SESSION, tableSnapshot), TESTING_TYPE_MANAGER);
        for (int i = 0; i < expectedDataFiles.size(); i++) {
            AddFileEntry expected = expectedDataFiles.get(i);
            AddFileEntry actual = dataFilesWithFixedVersion.get(i);

            assertThat(expected.getPath()).isEqualTo(actual.getPath());
            assertThat(expected.getPartitionValues()).isEqualTo(actual.getPartitionValues());
            assertThat(expected.getSize()).isEqualTo(actual.getSize());
            assertThat(expected.getModificationTime()).isEqualTo(actual.getModificationTime());
            assertThat(expected.isDataChange()).isEqualTo(actual.isDataChange());
            assertThat(expected.getTags()).isEqualTo(actual.getTags());

            assertThat(expected.getStats().isPresent()).isTrue();
            assertThat(actual.getStats().isPresent()).isTrue();

            for (ColumnMetadata column : columns) {
                DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(column.getName(), column.getType(), OptionalInt.empty(), column.getName(), column.getType(), REGULAR, Optional.empty());
                assertThat(expected.getStats().get().getMinColumnValue(columnHandle)).isEqualTo(actual.getStats().get().getMinColumnValue(columnHandle));
                assertThat(expected.getStats().get().getMaxColumnValue(columnHandle)).isEqualTo(actual.getStats().get().getMaxColumnValue(columnHandle));
                assertThat(expected.getStats().get().getNullCount(columnHandle.baseColumnName())).isEqualTo(actual.getStats().get().getNullCount(columnHandle.baseColumnName()));
                assertThat(expected.getStats().get().getNumRecords()).isEqualTo(actual.getStats().get().getNumRecords());
            }
        }
    }

    @Test
    public void testAddNewTransactionLogs()
            throws Exception
    {
        String tableName = "person";
        File tempDir = Files.createTempDirectory(null).toFile();
        File tableDir = new File(tempDir, tableName);
        File transactionLogDir = new File(tableDir, TRANSACTION_LOG_DIRECTORY);
        transactionLogDir.mkdirs();

        String tableLocation = tableDir.toURI().toString();
        SchemaTableName schemaTableName = new SchemaTableName("schema", tableName);

        File resourceDir = new File(getClass().getClassLoader().getResource("databricks73/person/_delta_log").toURI());
        copyTransactionLogEntry(0, 1, resourceDir, transactionLogDir);

        setupTransactionLogAccess(tableName, tableLocation);
        assertThat(tableSnapshot.getVersion()).isEqualTo(0L);

        copyTransactionLogEntry(1, 2, resourceDir, transactionLogDir);
        TableSnapshot firstUpdate = transactionLogAccess.loadSnapshot(SESSION, schemaTableName, tableLocation, Optional.empty());
        assertThat(firstUpdate.getVersion()).isEqualTo(1L);

        copyTransactionLogEntry(2, 3, resourceDir, transactionLogDir);
        TableSnapshot secondUpdate = transactionLogAccess.loadSnapshot(SESSION, schemaTableName, tableLocation, Optional.empty());
        assertThat(secondUpdate.getVersion()).isEqualTo(2L);
    }

    @Test
    public void testParquetStructStatistics()
            throws Exception
    {
        // See README.md for table contents
        String tableName = "parquet_struct_statistics";
        setupTransactionLogAccess(tableName, getClass().getClassLoader().getResource("databricks73/pruning/" + tableName).toURI().toString());

        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(SESSION, tableSnapshot);
        List<AddFileEntry> addFileEntries;
        try (Stream<AddFileEntry> addFileEntryStream = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
            addFileEntries = addFileEntryStream.collect(toImmutableList());
        }

        AddFileEntry addFileEntry = addFileEntries.stream()
                .filter(entry -> entry.getPath().equalsIgnoreCase("part-00000-0e22455f-5650-442f-a094-e1a8b7ed2271-c000.snappy.parquet"))
                .collect(onlyElement());

        assertThat(addFileEntry.getStats()).isPresent();
        DeltaLakeFileStatistics fileStats = addFileEntry.getStats().get();

        BigDecimal decValue = BigDecimal.valueOf(999999999999123L).movePointLeft(3);
        Map<String, Object> statsValues = ImmutableMap.<String, Object>builder()
                .put("ts", DateTimeEncoding.packDateTimeWithZone(LocalDateTime.parse("2960-10-31T01:00:00").toInstant(UTC).toEpochMilli(), UTC_KEY))
                .put("str", utf8Slice("a"))
                .put("dec_short", 101L)
                .put("dec_long", Decimals.valueOf(decValue))
                .put("l", 10000000L)
                .put("in", 20000000L)
                .put("sh", 123L)
                .put("byt", 42L)
                .put("fl", (long) Float.floatToIntBits(0.123f))
                .put("dou", 0.321)
                .put("dat", LocalDate.parse("5000-01-01").toEpochDay())
                .buildOrThrow();

        for (String columnName : statsValues.keySet()) {
            // Types would need to be specified properly if stats were being read from JSON but are not can be ignored when reading parsed stats from parquet,
            // so it is safe to use INTEGER as a placeholder
            assertThat(fileStats.getMinColumnValue(new DeltaLakeColumnHandle(columnName, IntegerType.INTEGER, OptionalInt.empty(), columnName, IntegerType.INTEGER, REGULAR, Optional.empty()))).isEqualTo(Optional.of(statsValues.get(columnName)));

            assertThat(fileStats.getMaxColumnValue(new DeltaLakeColumnHandle(columnName, IntegerType.INTEGER, OptionalInt.empty(), columnName, IntegerType.INTEGER, REGULAR, Optional.empty()))).isEqualTo(Optional.of(statsValues.get(columnName)));
        }
    }

    @Test
    public void testTableSnapshotsCacheDisabled()
            throws Exception
    {
        String tableName = "person";
        String tableDir = getClass().getClassLoader().getResource("databricks73/" + tableName).toURI().toString();
        DeltaLakeConfig cacheDisabledConfig = new DeltaLakeConfig();
        cacheDisabledConfig.setMetadataCacheTtl(new Duration(0, TimeUnit.SECONDS));

        assertFileSystemAccesses(
                () -> {
                    setupTransactionLogAccess(tableName, tableDir, cacheDisabledConfig, Optional.empty());
                },
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000011.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000012.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000013.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000014.json", "InputFile.newStream"))
                        .build());

        // With the transaction log cache disabled, when loading the snapshot again, all the needed files will be opened again
        assertFileSystemAccesses(
                () -> {
                    transactionLogAccess.loadSnapshot(SESSION, new SchemaTableName("schema", tableName), tableDir, Optional.empty());
                },
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000011.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000012.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000013.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000014.json", "InputFile.newStream"))
                        .build());
    }

    @Test
    public void testTableSnapshotsActiveDataFilesCache()
            throws Exception
    {
        setupTransactionLogAccessFromResources("person", "databricks73/person");

        String tableName = "person";
        String tableDir = getClass().getClassLoader().getResource("databricks73/" + tableName).toURI().toString();
        DeltaLakeConfig shortLivedActiveDataFilesCacheConfig = new DeltaLakeConfig();
        shortLivedActiveDataFilesCacheConfig.setDataFileCacheTtl(new Duration(10, TimeUnit.MINUTES));
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(SESSION, tableSnapshot);

        ConnectorSession activeDataFileCacheSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new DeltaLakeSessionProperties(
                        new DeltaLakeConfig().setCheckpointFilteringEnabled(false),
                        new ParquetReaderConfig(),
                        new ParquetWriterConfig())
                        .getSessionProperties())
                .build();
        assertFileSystemAccesses(
                () -> {
                    setupTransactionLogAccess(tableName, tableDir, shortLivedActiveDataFilesCacheConfig, Optional.empty());
                    try (Stream<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(activeDataFileCacheSession, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
                        assertThat(addFileEntries.count()).isEqualTo(12L);
                    }
                },
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000011.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000012.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000013.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000014.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000010.checkpoint.parquet", "InputFile.length"))
                        .add(new FileOperation("00000000000000000010.checkpoint.parquet", "InputFile.newInput"))
                        .build());

        // The internal data cache should still contain the data files for the table
        assertFileSystemAccesses(
                () -> {
                    try (Stream<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(activeDataFileCacheSession, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
                        assertThat(addFileEntries.count()).isEqualTo(12L);
                    }
                },
                ImmutableMultiset.of());
    }

    @Test
    public void testFlushSnapshotAndActiveFileCache()
            throws Exception
    {
        setupTransactionLogAccessFromResources("person", "databricks73/person");

        String tableName = "person";
        String tableDir = getClass().getClassLoader().getResource("databricks73/" + tableName).toURI().toString();
        DeltaLakeConfig shortLivedActiveDataFilesCacheConfig = new DeltaLakeConfig();
        shortLivedActiveDataFilesCacheConfig.setDataFileCacheTtl(new Duration(10, TimeUnit.MINUTES));
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(SESSION, tableSnapshot);

        assertFileSystemAccesses(
                () -> {
                    setupTransactionLogAccess(tableName, tableDir, shortLivedActiveDataFilesCacheConfig, Optional.empty());
                    try (Stream<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
                        assertThat(addFileEntries.count()).isEqualTo(12L);
                    }
                },
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000011.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000012.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000013.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000014.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000010.checkpoint.parquet", "InputFile.length"))
                        .add(new FileOperation("00000000000000000010.checkpoint.parquet", "InputFile.newInput"))
                        .build());

        // Flush all cache and then load snapshot and get active files
        transactionLogAccess.flushCache();
        assertFileSystemAccesses(
                () -> {
                    transactionLogAccess.loadSnapshot(SESSION, new SchemaTableName("schema", tableName), tableDir, Optional.empty());
                    try (Stream<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
                        assertThat(addFileEntries.count()).isEqualTo(12L);
                    }
                },
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000011.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000012.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000013.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000014.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000010.checkpoint.parquet", "InputFile.length"))
                        .add(new FileOperation("00000000000000000010.checkpoint.parquet", "InputFile.newInput"))
                        .build());
    }

    @Test
    public void testTableSnapshotsActiveDataFilesCacheDisabled()
            throws Exception
    {
        setupTransactionLogAccessFromResources("person", "databricks73/person");

        String tableName = "person";
        String tableDir = getClass().getClassLoader().getResource("databricks73/" + tableName).toURI().toString();
        DeltaLakeConfig shortLivedActiveDataFilesCacheConfig = new DeltaLakeConfig();
        shortLivedActiveDataFilesCacheConfig.setDataFileCacheTtl(new Duration(0, TimeUnit.SECONDS));
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(SESSION, tableSnapshot);

        assertFileSystemAccesses(
                () -> {
                    setupTransactionLogAccess(tableName, tableDir, shortLivedActiveDataFilesCacheConfig, Optional.empty());
                    try (Stream<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
                        assertThat(addFileEntries.count()).isEqualTo(12L);
                    }
                },
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000011.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000012.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000013.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000014.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000010.checkpoint.parquet", "InputFile.length"))
                        .add(new FileOperation("00000000000000000010.checkpoint.parquet", "InputFile.newInput"))
                        .build());

        // With no caching for the transaction log entries, when loading the snapshot again,
        // the checkpoint file will be read again
        assertFileSystemAccesses(
                () -> {
                    try (Stream<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
                        assertThat(addFileEntries.count()).isEqualTo(12L);
                    }
                },
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("00000000000000000010.checkpoint.parquet", "InputFile.length"))
                        .add(new FileOperation("00000000000000000010.checkpoint.parquet", "InputFile.newInput"))
                        .build());
    }

    @Test
    public void testLoadSnapshotWithEndVersion()
            throws Exception
    {
        String tableName = "person";
        String tableDir = getClass().getClassLoader().getResource("databricks73/" + tableName).toURI().toString();
        DeltaLakeConfig shortLivedActiveDataFilesCacheConfig = new DeltaLakeConfig();
        shortLivedActiveDataFilesCacheConfig.setDataFileCacheTtl(new Duration(10, TimeUnit.MINUTES));
        setupTransactionLogAccess("person", getClass().getClassLoader().getResource("databricks73/person").toString(), new DeltaLakeConfig(), Optional.of(9L));
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(SESSION, tableSnapshot);

        // Version 10 has a checkpoint file
        transactionLogAccess.flushCache();
        assertFileSystemAccesses(
                () -> {
                    transactionLogAccess.loadSnapshot(SESSION, new SchemaTableName("schema", tableName), tableDir, Optional.of(9L));
                    try (Stream<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
                        assertThat(addFileEntries.count()).isEqualTo(8L);
                    }
                },
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000003.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000004.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000005.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000006.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000007.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000008.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000009.json", "InputFile.newStream"))
                        .build());

        setupTransactionLogAccess("person", getClass().getClassLoader().getResource("databricks73/person").toString(), new DeltaLakeConfig(), Optional.of(10L));
        transactionLogAccess.flushCache();
        assertFileSystemAccesses(
                () -> {
                    transactionLogAccess.loadSnapshot(SESSION, new SchemaTableName("schema", tableName), tableDir, Optional.of(10L));
                    try (Stream<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
                        assertThat(addFileEntries.count()).isEqualTo(9L);
                    }
                },
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000010.checkpoint.parquet", "InputFile.length"))
                        .add(new FileOperation("00000000000000000010.checkpoint.parquet", "InputFile.newInput"))
                        .build());

        setupTransactionLogAccess("person", getClass().getClassLoader().getResource("databricks73/person").toString(), new DeltaLakeConfig(), Optional.of(11L));
        transactionLogAccess.flushCache();
        assertFileSystemAccesses(
                () -> {
                    transactionLogAccess.loadSnapshot(SESSION, new SchemaTableName("schema", tableName), tableDir, Optional.of(11L));
                    try (Stream<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(SESSION, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
                        assertThat(addFileEntries.count()).isEqualTo(10L);
                    }
                },
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000010.checkpoint.parquet", "InputFile.length"))
                        .add(new FileOperation("00000000000000000010.checkpoint.parquet", "InputFile.newInput"))
                        .add(new FileOperation("00000000000000000011.json", "InputFile.newStream"))
                        .build());
    }

    private void copyTransactionLogEntry(int startVersion, int endVersion, File sourceDir, File targetDir)
            throws IOException
    {
        for (int i = startVersion; i < endVersion; i++) {
            if (i % 10 == 0 && i != 0) {
                String checkpointFileName = format("%020d.checkpoint.parquet", i);
                Files.copy(new File(sourceDir, checkpointFileName).toPath(), new File(targetDir, checkpointFileName).toPath());
            }
            String lastTransactionName = format("%020d.json", i);
            Files.copy(new File(sourceDir, lastTransactionName).toPath(), new File(targetDir, lastTransactionName).toPath());
        }
    }

    private void assertFileSystemAccesses(TestingTelemetry.CheckedRunnable<?> callback, Multiset<FileOperation> expectedAccesses)
            throws Exception
    {
        List<SpanData> spanData = testingTelemetry.captureSpans(callback::run);
        assertMultisetsEqual(getOperations(spanData), expectedAccesses);
    }

    private Multiset<FileOperation> getOperations(List<SpanData> spans)
    {
        return spans.stream()
                .filter(span -> span.getName().startsWith("InputFile."))
                .map(span -> new FileOperation(span.getAttributes().get(FILE_LOCATION).replaceFirst(".*/_delta_log/", ""), span.getName()))
                .collect(toCollection(HashMultiset::create));
    }

    private record FileOperation(String path, String operationType)
    {
        FileOperation
        {
            requireNonNull(path, "path is null");
            requireNonNull(operationType, "operationType is null");
        }
    }
}
