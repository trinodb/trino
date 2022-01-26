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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TypeManager;
import io.trino.testing.TestingConnectorContext;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.Sets.union;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.LAST_CHECKPOINT_FILENAME;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.TRANSACTION_LOG_DIRECTORY;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
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
            new RemoveFileEntry("age=30/part-00000-7e43a3c3-ea26-4ae7-8eac-8f60cbb4df03.c000.snappy.parquet", 1579190163932L, false),
            new RemoveFileEntry("age=30/part-00000-72a56c23-01ba-483a-9062-dd0accc86599.c000.snappy.parquet", 1579190163932L, false),
            new RemoveFileEntry("age=42/part-00000-951068bd-bcf4-4094-bb94-536f3c41d31f.c000.snappy.parquet", 1579190155406L, false),
            new RemoveFileEntry("age=25/part-00000-609e34b1-5466-4dbc-a780-2708166e7adb.c000.snappy.parquet", 1579190163932L, false),
            new RemoveFileEntry("age=42/part-00000-6aed618a-2beb-4edd-8466-653e67a9b380.c000.snappy.parquet", 1579190155406L, false),
            new RemoveFileEntry("age=42/part-00000-b82d8859-84a0-4f05-872c-206b07dd54f0.c000.snappy.parquet", 1579190163932L, false));

    private TrackingTransactionLogAccess transactionLogAccess;
    private TableSnapshot tableSnapshot;

    private void setupTransactionLogAccess(String tableName)
            throws Exception
    {
        setupTransactionLogAccess(tableName, new Path(getClass().getClassLoader().getResource("databricks/" + tableName).toURI()));
    }

    private void setupTransactionLogAccess(String tableName, Path tableLocation)
            throws IOException
    {
        TestingConnectorContext context = new TestingConnectorContext();
        TypeManager typeManager = context.getTypeManager();

        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
        FileFormatDataSourceStats fileFormatDataSourceStats = new FileFormatDataSourceStats();

        transactionLogAccess = new TrackingTransactionLogAccess(
                tableName,
                tableLocation,
                SESSION,
                typeManager,
                new CheckpointSchemaManager(typeManager),
                new DeltaLakeConfig(),
                fileFormatDataSourceStats,
                hdfsEnvironment,
                new ParquetReaderConfig());

        DeltaLakeTableHandle tableHandle = new DeltaLakeTableHandle(
                "schema",
                tableName,
                "location",
                Optional.empty(), // ignored
                TupleDomain.none(),
                TupleDomain.none(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                0);

        tableSnapshot = transactionLogAccess.loadSnapshot(tableHandle.getSchemaTableName(), tableLocation, SESSION);
    }

    @Test
    public void testGetMetadataEntry()
            throws Exception
    {
        setupTransactionLogAccess("person");

        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(tableSnapshot, SESSION).get();

        assertEquals(metadataEntry.getCreatedTime(), 1579190100722L);
        assertEquals(metadataEntry.getId(), "b6aeffad-da73-4dde-b68e-937e468b1fdf");
        assertThat(metadataEntry.getOriginalPartitionColumns()).containsOnly("age");
        assertThat(metadataEntry.getCanonicalPartitionColumns()).containsOnly("age");

        MetadataEntry.Format format = metadataEntry.getFormat();
        assertEquals(format.getOptions().keySet().size(), 0);
        assertEquals(format.getProvider(), "parquet");

        assertEquals(tableSnapshot.getCachedMetadata(), Optional.of(metadataEntry));
    }

    @Test
    public void testGetMetadataEntryUppercase()
            throws Exception
    {
        setupTransactionLogAccess("uppercase_columns");
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(tableSnapshot, SESSION).get();
        assertThat(metadataEntry.getOriginalPartitionColumns()).containsOnly("ALA");
        assertThat(metadataEntry.getCanonicalPartitionColumns()).containsOnly("ala");
        assertEquals(tableSnapshot.getCachedMetadata(), Optional.of(metadataEntry));
    }

    @Test
    public void testGetActiveAddEntries()
            throws Exception
    {
        setupTransactionLogAccess("person");

        List<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(tableSnapshot, SESSION);
        Set<String> paths = addFileEntries
                .stream()
                .map(AddFileEntry::getPath)
                .collect(Collectors.toSet());
        assertEquals(paths, EXPECTED_ADD_FILE_PATHS);

        AddFileEntry addFileEntry = addFileEntries
                .stream()
                .filter(entry -> entry.getPath().equals("age=42/part-00000-b26c891a-7288-4d96-9d3b-bef648f12a34.c000.snappy.parquet"))
                .findFirst()
                .get();

        assertThat(addFileEntry.getPartitionValues())
                .hasSize(1)
                .containsEntry("age", "42");
        assertThat(addFileEntry.getCanonicalPartitionValues())
                .hasSize(1)
                .containsEntry("age", Optional.of("42"));

        assertEquals(addFileEntry.getSize(), 2687);
        assertEquals(addFileEntry.getModificationTime(), 1579190188000L);
        assertFalse(addFileEntry.isDataChange());
    }

    @Test
    public void testAddFileEntryUppercase()
            throws Exception
    {
        setupTransactionLogAccess("uppercase_columns");

        List<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(tableSnapshot, SESSION);
        AddFileEntry addFileEntry = addFileEntries
                .stream()
                .filter(entry -> entry.getPath().equals("ALA=1/part-00000-20a863e0-890d-4776-8825-f9dccc8973ba.c000.snappy.parquet"))
                .findFirst()
                .get();

        assertThat(addFileEntry.getPartitionValues())
                .hasSize(1)
                .containsEntry("ALA", "1");
        assertThat(addFileEntry.getCanonicalPartitionValues())
                .hasSize(1)
                .containsEntry("ala", Optional.of("1"));
    }

    @Test
    public void testAddEntryPruning()
            throws Exception
    {
        // Test data contains two add entries which should be pruned:
        // - Added in the parquet checkpoint but removed in a JSON commit
        // - Added in a JSON commit and removed in a later JSON commit
        setupTransactionLogAccess("person_test_pruning");
        List<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(tableSnapshot, SESSION);
        Set<String> paths = addFileEntries
                .stream()
                .map(AddFileEntry::getPath)
                .collect(Collectors.toSet());

        assertFalse(paths.contains("age=25/part-00001-aceaf062-1cd1-45cb-8f83-277ffebe995c.c000.snappy.parquet"));
        assertFalse(paths.contains("age=29/part-00000-3794c463-cb0c-4beb-8d07-7cc1e3b5920f.c000.snappy.parquet"));
    }

    @Test
    public void testAddEntryOverrides()
            throws Exception
    {
        setupTransactionLogAccess("person_test_pruning");
        List<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(tableSnapshot, SESSION);

        // Test data contains two entries which are added multiple times, the most up to date one should be the only one in the active list
        List<String> overwrittenPaths = ImmutableList.of(
                "age=42/part-00000-b26c891a-7288-4d96-9d3b-bef648f12a34.c000.snappy.parquet",
                "age=28/part-00000-40dd1707-1d42-4328-a59a-21f5c945fe60.c000.snappy.parquet");

        for (String path : overwrittenPaths) {
            List<AddFileEntry> activeEntries = addFileEntries
                    .stream()
                    .filter(addFileEntry -> addFileEntry.getPath().equals(path))
                    .collect(Collectors.toList());

            assertEquals(activeEntries.size(), 1);
            assertEquals(activeEntries.get(0).getModificationTime(), 9999999L);
        }
    }

    @Test
    public void testAddRemoveAdd()
            throws Exception
    {
        setupTransactionLogAccess("person_test_pruning");
        List<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(tableSnapshot, SESSION);

        // Test data contains an entry added by the parquet checkpoint, removed by a JSON action, and then added back by a later JSON action
        List<AddFileEntry> activeEntries = addFileEntries
                .stream()
                .filter(addFileEntry -> addFileEntry.getPath().equals("age=30/part-00002-5800be2e-2373-47d8-8b86-776a8ea9d69f.c000.snappy.parquet"))
                .collect(Collectors.toList());

        assertEquals(activeEntries.size(), 1);
        assertEquals(activeEntries.get(0).getModificationTime(), 9999999L);
    }

    @Test
    public void testGetRemoveEntries()
            throws Exception
    {
        setupTransactionLogAccess("person");

        try (Stream<RemoveFileEntry> removeEntries = transactionLogAccess.getRemoveEntries(tableSnapshot, SESSION)) {
            Set<RemoveFileEntry> removedEntries = removeEntries.collect(Collectors.toSet());
            assertEquals(removedEntries, EXPECTED_REMOVE_ENTRIES);
        }
    }

    @Test
    public void testGetCommitInfoEntries()
            throws Exception
    {
        setupTransactionLogAccess("person");
        try (Stream<CommitInfoEntry> commitInfoEntries = transactionLogAccess.getCommitInfoEntries(tableSnapshot, SESSION)) {
            Set<CommitInfoEntry> entrySet = commitInfoEntries.collect(Collectors.toSet());
            assertEquals(
                    entrySet,
                    ImmutableSet.of(
                            new CommitInfoEntry(0, 1579190200860L, "671960514434781", "michal.slizak@starburstdata.com", "WRITE",
                                    ImmutableMap.of("mode", "Append", "partitionBy", "[\"age\"]"), null, new CommitInfoEntry.Notebook("3040849856940931"),
                                    "0116-154224-guppy476", 10L, "WriteSerializable", true),
                            new CommitInfoEntry(0, 1579190206644L, "671960514434781", "michal.slizak@starburstdata.com", "WRITE",
                                    ImmutableMap.of("mode", "Append", "partitionBy", "[\"age\"]"), null, new CommitInfoEntry.Notebook("3040849856940931"),
                                    "0116-154224-guppy476", 11L, "WriteSerializable", true),
                            new CommitInfoEntry(0, 1579190210571L, "671960514434781", "michal.slizak@starburstdata.com", "WRITE",
                                    ImmutableMap.of("mode", "Append", "partitionBy", "[\"age\"]"), null, new CommitInfoEntry.Notebook("3040849856940931"),
                                    "0116-154224-guppy476", 12L, "WriteSerializable", true)));
        }
    }

    // Broader tests which validate common attributes across the wider data set
    @DataProvider
    public Object[][] tableNames()
    {
        return new Object[][] {
                {"person"},
                {"person_without_last_checkpoint"},
                {"person_without_old_jsons"},
                {"person_without_checkpoints"}
        };
    }

    @Test(dataProvider = "tableNames")
    public void testAllGetMetadataEntry(String tableName)
            throws Exception
    {
        setupTransactionLogAccess(tableName);

        transactionLogAccess.getMetadataEntry(tableSnapshot, SESSION);
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(tableSnapshot, SESSION).get();

        assertThat(metadataEntry.getOriginalPartitionColumns()).containsOnly("age");

        MetadataEntry.Format format = metadataEntry.getFormat();
        assertEquals(format.getOptions().keySet().size(), 0);
        assertEquals(format.getProvider(), "parquet");
    }

    @Test(dataProvider = "tableNames")
    public void testAllGetActiveAddEntries(String tableName)
            throws Exception
    {
        setupTransactionLogAccess(tableName);

        List<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(tableSnapshot, SESSION);
        Set<String> paths = addFileEntries
                .stream()
                .map(AddFileEntry::getPath)
                .collect(Collectors.toSet());

        assertEquals(paths, EXPECTED_ADD_FILE_PATHS);
    }

    @Test(dataProvider = "tableNames")
    public void testAllGetRemoveEntries(String tableName)
            throws Exception
    {
        setupTransactionLogAccess(tableName);

        try (Stream<RemoveFileEntry> removeEntries = transactionLogAccess.getRemoveEntries(tableSnapshot, SESSION)) {
            Set<String> removedPaths = removeEntries.map(RemoveFileEntry::getPath).collect(Collectors.toSet());
            Set<String> expectedPaths = EXPECTED_REMOVE_ENTRIES.stream().map(RemoveFileEntry::getPath).collect(Collectors.toSet());

            assertEquals(removedPaths, expectedPaths);
        }
    }

    @Test(dataProvider = "tableNames")
    public void testAllGetProtocolEntries(String tableName)
            throws Exception
    {
        setupTransactionLogAccess(tableName);

        try (Stream<ProtocolEntry> protocolEntryStream = transactionLogAccess.getProtocolEntries(tableSnapshot, SESSION)) {
            List<ProtocolEntry> protocolEntries = protocolEntryStream.collect(Collectors.toList());

            assertEquals(protocolEntries.size(), 1);
            assertEquals(protocolEntries.get(0).getMinReaderVersion(), 1);
            assertEquals(protocolEntries.get(0).getMinWriterVersion(), 2);
        }
    }

    @Test
    public void testMetadataCacheUpdates()
            throws Exception
    {
        String tableName = "person";
        // setupTransactionLogAccess(tableName, new Path(getClass().getClassLoader().getResource("databricks/" + tableName).toURI()));
        File tempDir = Files.createTempDir();
        File tableDir = new File(tempDir, tableName);
        File transactionLogDir = new File(tableDir, TRANSACTION_LOG_DIRECTORY);
        transactionLogDir.mkdirs();

        java.nio.file.Path resourceDir = java.nio.file.Paths.get(getClass().getClassLoader().getResource("databricks/person/_delta_log").toURI());
        for (int i = 0; i < 12; i++) {
            String extension = i == 10 ? ".checkpoint.parquet" : ".json";
            String fileName = format("%020d%s", i, extension);
            Files.copy(resourceDir.resolve(fileName).toFile(), new File(transactionLogDir, fileName));
        }
        Files.copy(resourceDir.resolve(LAST_CHECKPOINT_FILENAME).toFile(), new File(transactionLogDir, LAST_CHECKPOINT_FILENAME));

        setupTransactionLogAccess(tableName, new Path(tableDir.toURI()));
        assertEquals(tableSnapshot.getVersion(), 11L);

        String lastTransactionName = format("%020d.json", 12);
        Files.copy(resourceDir.resolve(lastTransactionName).toFile(), new File(transactionLogDir, lastTransactionName));
        TableSnapshot updatedSnapshot = transactionLogAccess.loadSnapshot(new SchemaTableName("schema", tableName), new Path(tableDir.toURI()), SESSION);
        assertEquals(updatedSnapshot.getVersion(), 12);
    }

    @Test
    public void testUpdatingTailEntriesNoCheckpoint()
            throws Exception
    {
        String tableName = "person";
        File tempDir = Files.createTempDir();
        File tableDir = new File(tempDir, tableName);
        File transactionLogDir = new File(tableDir, TRANSACTION_LOG_DIRECTORY);
        transactionLogDir.mkdirs();

        File resourceDir = new File(getClass().getClassLoader().getResource("databricks/person/_delta_log").toURI());
        copyTransactionLogEntry(0, 7, resourceDir, transactionLogDir);
        setupTransactionLogAccess(tableName, new Path(tableDir.toURI()));

        List<AddFileEntry> activeDataFiles = transactionLogAccess.getActiveFiles(tableSnapshot, SESSION);

        Set<String> dataFiles = ImmutableSet.of(
                "age=42/part-00000-b82d8859-84a0-4f05-872c-206b07dd54f0.c000.snappy.parquet",
                "age=30/part-00000-72a56c23-01ba-483a-9062-dd0accc86599.c000.snappy.parquet",
                "age=25/part-00000-609e34b1-5466-4dbc-a780-2708166e7adb.c000.snappy.parquet",
                "age=30/part-00000-7e43a3c3-ea26-4ae7-8eac-8f60cbb4df03.c000.snappy.parquet",
                "age=21/part-00000-3d546786-bedc-407f-b9f7-e97aa12cce0f.c000.snappy.parquet",
                "age=21/part-00001-290f0f26-19cf-4772-821e-36d55d9b7872.c000.snappy.parquet");

        assertEqualsIgnoreOrder(activeDataFiles.stream().map(AddFileEntry::getPath).collect(Collectors.toSet()), dataFiles);

        copyTransactionLogEntry(7, 9, resourceDir, transactionLogDir);
        TableSnapshot updatedSnapshot = transactionLogAccess.loadSnapshot(new SchemaTableName("schema", tableName), new Path(tableDir.toURI()), SESSION);
        activeDataFiles = transactionLogAccess.getActiveFiles(updatedSnapshot, SESSION);

        dataFiles = ImmutableSet.of(
                "age=21/part-00000-3d546786-bedc-407f-b9f7-e97aa12cce0f.c000.snappy.parquet",
                "age=21/part-00001-290f0f26-19cf-4772-821e-36d55d9b7872.c000.snappy.parquet",
                "age=30/part-00000-63c2205d-84a3-4a66-bd7c-f69f5af55bbc.c000.snappy.parquet",
                "age=25/part-00001-aceaf062-1cd1-45cb-8f83-277ffebe995c.c000.snappy.parquet",
                "age=30/part-00002-5800be2e-2373-47d8-8b86-776a8ea9d69f.c000.snappy.parquet",
                "age=42/part-00003-0f53cae3-3e34-4876-b651-e1db9584dbc3.c000.snappy.parquet",
                "age=25/part-00000-b7fbbe31-c7f9-44ed-8757-5c47d10c3e81.c000.snappy.parquet");

        assertEqualsIgnoreOrder(activeDataFiles.stream().map(AddFileEntry::getPath).collect(Collectors.toSet()), dataFiles);
    }

    @Test
    public void testLoadingTailEntriesPastCheckpoint()
            throws Exception
    {
        String tableName = "person";
        File tempDir = Files.createTempDir();
        File tableDir = new File(tempDir, tableName);
        File transactionLogDir = new File(tableDir, TRANSACTION_LOG_DIRECTORY);
        transactionLogDir.mkdirs();

        File resourceDir = new File(getClass().getClassLoader().getResource("databricks/person/_delta_log").toURI());
        copyTransactionLogEntry(0, 8, resourceDir, transactionLogDir);
        setupTransactionLogAccess(tableName, new Path(tableDir.toURI()));

        List<AddFileEntry> activeDataFiles = transactionLogAccess.getActiveFiles(tableSnapshot, SESSION);

        Set<String> dataFiles = ImmutableSet.of(
                "age=21/part-00000-3d546786-bedc-407f-b9f7-e97aa12cce0f.c000.snappy.parquet",
                "age=21/part-00001-290f0f26-19cf-4772-821e-36d55d9b7872.c000.snappy.parquet",
                "age=30/part-00000-63c2205d-84a3-4a66-bd7c-f69f5af55bbc.c000.snappy.parquet",
                "age=25/part-00001-aceaf062-1cd1-45cb-8f83-277ffebe995c.c000.snappy.parquet",
                "age=30/part-00002-5800be2e-2373-47d8-8b86-776a8ea9d69f.c000.snappy.parquet",
                "age=42/part-00003-0f53cae3-3e34-4876-b651-e1db9584dbc3.c000.snappy.parquet");

        assertEqualsIgnoreOrder(activeDataFiles.stream().map(AddFileEntry::getPath).collect(Collectors.toSet()), dataFiles);

        copyTransactionLogEntry(8, 12, resourceDir, transactionLogDir);
        Files.copy(new File(resourceDir, LAST_CHECKPOINT_FILENAME), new File(transactionLogDir, LAST_CHECKPOINT_FILENAME));
        TableSnapshot updatedSnapshot = transactionLogAccess.loadSnapshot(new SchemaTableName("schema", tableName), new Path(tableDir.toURI()), SESSION);
        activeDataFiles = transactionLogAccess.getActiveFiles(updatedSnapshot, SESSION);

        dataFiles = ImmutableSet.of(
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

        assertEqualsIgnoreOrder(activeDataFiles.stream().map(AddFileEntry::getPath).collect(Collectors.toSet()), dataFiles);
    }

    @Test
    public void testIncrementalCacheUpdates()
            throws Exception
    {
        String tableName = "person";
        File tempDir = Files.createTempDir();
        File tableDir = new File(tempDir, tableName);
        File transactionLogDir = new File(tableDir, TRANSACTION_LOG_DIRECTORY);
        transactionLogDir.mkdirs();

        File resourceDir = new File(getClass().getClassLoader().getResource("databricks/person/_delta_log").toURI());
        copyTransactionLogEntry(0, 12, resourceDir, transactionLogDir);
        Files.copy(new File(resourceDir, LAST_CHECKPOINT_FILENAME), new File(transactionLogDir, LAST_CHECKPOINT_FILENAME));

        setupTransactionLogAccess(tableName, new Path(tableDir.toURI()));
        List<AddFileEntry> activeDataFiles = transactionLogAccess.getActiveFiles(tableSnapshot, SESSION);

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

        assertEqualsIgnoreOrder(activeDataFiles.stream().map(AddFileEntry::getPath).collect(Collectors.toSet()), originalDataFiles);
        assertEquals(
                transactionLogAccess.getAccessTrackingFileSystem().getOpenCount(),
                ImmutableMap.of(
                        "_last_checkpoint", 1,
                        // the file is accessed once when the transaction log tail is created
                        // and then it tries to access the following file but that file does not
                        // exist so it knows that it has reached the end of the tail
                        "00000000000000000011.json", 1,
                        "00000000000000000012.json", 1));

        copyTransactionLogEntry(12, 14, resourceDir, transactionLogDir);
        Set<String> newDataFiles = ImmutableSet.of(
                "age=28/part-00000-40dd1707-1d42-4328-a59a-21f5c945fe60.c000.snappy.parquet",
                "age=29/part-00000-3794c463-cb0c-4beb-8d07-7cc1e3b5920f.c000.snappy.parquet");
        TableSnapshot updatedTableSnapshot = transactionLogAccess.loadSnapshot(new SchemaTableName("schema", tableName), new Path(tableDir.toURI()), SESSION);
        activeDataFiles = transactionLogAccess.getActiveFiles(updatedTableSnapshot, SESSION);
        assertEqualsIgnoreOrder(activeDataFiles.stream().map(AddFileEntry::getPath).collect(Collectors.toSet()), union(originalDataFiles, newDataFiles));
        assertEquals(
                transactionLogAccess.getAccessTrackingFileSystem().getOpenCount(),
                ImmutableMap.of(
                        "_last_checkpoint", 2,
                        "00000000000000000011.json", 1,
                        "00000000000000000012.json", 3,
                        "00000000000000000013.json", 2,
                        "00000000000000000014.json", 1));
    }

    @Test
    public void testSnapshotsAreConsistent()
            throws Exception
    {
        String tableName = "person";
        File tempDir = Files.createTempDir();
        File tableDir = new File(tempDir, tableName);
        File transactionLogDir = new File(tableDir, TRANSACTION_LOG_DIRECTORY);
        transactionLogDir.mkdirs();

        File resourceDir = new File(getClass().getClassLoader().getResource("databricks/person/_delta_log").toURI());
        copyTransactionLogEntry(0, 12, resourceDir, transactionLogDir);
        Files.copy(new File(resourceDir, LAST_CHECKPOINT_FILENAME), new File(transactionLogDir, LAST_CHECKPOINT_FILENAME));

        setupTransactionLogAccess(tableName, new Path(tableDir.toURI()));
        List<AddFileEntry> expectedDataFiles = transactionLogAccess.getActiveFiles(tableSnapshot, SESSION);

        copyTransactionLogEntry(12, 14, resourceDir, transactionLogDir);
        Set<String> newDataFiles = ImmutableSet.of(
                "age=28/part-00000-40dd1707-1d42-4328-a59a-21f5c945fe60.c000.snappy.parquet",
                "age=29/part-00000-3794c463-cb0c-4beb-8d07-7cc1e3b5920f.c000.snappy.parquet");
        TableSnapshot updatedTableSnapshot = transactionLogAccess.loadSnapshot(new SchemaTableName("schema", tableName), new Path(tableDir.toURI()), SESSION);
        List<AddFileEntry> allDataFiles = transactionLogAccess.getActiveFiles(updatedTableSnapshot, SESSION);
        List<AddFileEntry> dataFilesWithFixedVersion = transactionLogAccess.getActiveFiles(tableSnapshot, SESSION);
        for (String newFilePath : newDataFiles) {
            assertTrue(allDataFiles.stream().anyMatch(entry -> entry.getPath().equals(newFilePath)));
            assertTrue(dataFilesWithFixedVersion.stream().noneMatch(entry -> entry.getPath().equals(newFilePath)));
        }

        assertEquals(expectedDataFiles.size(), dataFilesWithFixedVersion.size());
        List<ColumnMetadata> columns = extractSchema(transactionLogAccess.getMetadataEntry(tableSnapshot, SESSION).get(), TESTING_TYPE_MANAGER);
        for (int i = 0; i < expectedDataFiles.size(); i++) {
            AddFileEntry expected = expectedDataFiles.get(i);
            AddFileEntry actual = dataFilesWithFixedVersion.get(i);

            assertEquals(expected.getPath(), actual.getPath());
            assertEquals(expected.getPartitionValues(), actual.getPartitionValues());
            assertEquals(expected.getSize(), actual.getSize());
            assertEquals(expected.getModificationTime(), actual.getModificationTime());
            assertEquals(expected.isDataChange(), actual.isDataChange());
            assertEquals(expected.getTags(), actual.getTags());

            assertTrue(expected.getStats().isPresent());
            assertTrue(actual.getStats().isPresent());

            for (ColumnMetadata column : columns) {
                DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(column.getName(), column.getType(), REGULAR);
                assertEquals(expected.getStats().get().getMinColumnValue(columnHandle), actual.getStats().get().getMinColumnValue(columnHandle));
                assertEquals(expected.getStats().get().getMaxColumnValue(columnHandle), actual.getStats().get().getMaxColumnValue(columnHandle));
                assertEquals(expected.getStats().get().getNullCount(columnHandle.getName()), actual.getStats().get().getNullCount(columnHandle.getName()));
                assertEquals(expected.getStats().get().getNumRecords(), actual.getStats().get().getNumRecords());
            }
        }
    }

    @Test
    public void testAddNewTransactionLogs()
            throws Exception
    {
        String tableName = "person";
        File tempDir = Files.createTempDir();
        File tableDir = new File(tempDir, tableName);
        File transactionLogDir = new File(tableDir, TRANSACTION_LOG_DIRECTORY);
        transactionLogDir.mkdirs();

        Path tableLocation = new Path(tableDir.toURI());
        SchemaTableName schemaTableName = new SchemaTableName("schema", tableName);

        File resourceDir = new File(getClass().getClassLoader().getResource("databricks/person/_delta_log").toURI());
        copyTransactionLogEntry(0, 1, resourceDir, transactionLogDir);

        setupTransactionLogAccess(tableName, tableLocation);
        assertEquals(tableSnapshot.getVersion(), 0L);

        copyTransactionLogEntry(1, 2, resourceDir, transactionLogDir);
        TableSnapshot firstUpdate = transactionLogAccess.loadSnapshot(schemaTableName, tableLocation, SESSION);
        assertEquals(firstUpdate.getVersion(), 1L);

        copyTransactionLogEntry(2, 3, resourceDir, transactionLogDir);
        TableSnapshot secondUpdate = transactionLogAccess.loadSnapshot(schemaTableName, tableLocation, SESSION);
        assertEquals(secondUpdate.getVersion(), 2L);
    }

    @Test
    public void testParquetStructStatistics()
            throws Exception
    {
        // See README.md for table contents
        String tableName = "parquet_struct_statistics";
        setupTransactionLogAccess(tableName, new Path(getClass().getClassLoader().getResource("databricks/pruning/" + tableName).toURI()));

        List<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(tableSnapshot, SESSION);

        AddFileEntry addFileEntry = addFileEntries.stream()
                .filter(entry -> entry.getPath().equalsIgnoreCase("part-00000-0e22455f-5650-442f-a094-e1a8b7ed2271-c000.snappy.parquet"))
                .findFirst()
                .get();

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
            assertEquals(
                    fileStats.getMinColumnValue(new DeltaLakeColumnHandle(columnName, IntegerType.INTEGER, REGULAR)),
                    Optional.of(statsValues.get(columnName)));

            assertEquals(
                    fileStats.getMaxColumnValue(new DeltaLakeColumnHandle(columnName, IntegerType.INTEGER, REGULAR)),
                    Optional.of(statsValues.get(columnName)));
        }
    }

    private void copyTransactionLogEntry(int startVersion, int endVersion, File sourceDir, File targetDir)
            throws IOException
    {
        for (int i = startVersion; i < endVersion; i++) {
            if (i % 10 == 0 && i != 0) {
                String checkpointFileName = format("%020d.checkpoint.parquet", i);
                Files.copy(new File(sourceDir, checkpointFileName), new File(targetDir, checkpointFileName));
            }
            String lastTransactionName = format("%020d.json", i);
            Files.copy(new File(sourceDir, lastTransactionName), new File(targetDir, lastTransactionName));
        }
    }
}
