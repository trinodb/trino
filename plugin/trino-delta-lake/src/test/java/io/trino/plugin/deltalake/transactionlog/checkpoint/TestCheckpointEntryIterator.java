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
package io.trino.plugin.deltalake.transactionlog.checkpoint;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.ADD;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.COMMIT;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.METADATA;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.PROTOCOL;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.REMOVE;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.TRANSACTION;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCheckpointEntryIterator
{
    private static final String TEST_CHECKPOINT = "databricks73/person/_delta_log/00000000000000000010.checkpoint.parquet";

    private CheckpointSchemaManager checkpointSchemaManager;

    @BeforeClass
    public void setUp()
    {
        checkpointSchemaManager = new CheckpointSchemaManager(TESTING_TYPE_MANAGER);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        checkpointSchemaManager = null;
    }

    @Test
    public void testReadMetadataEntry()
            throws Exception
    {
        URI checkpointUri = getResource(TEST_CHECKPOINT).toURI();
        assertThat(readMetadataEntry(checkpointUri))
                .isEqualTo(
                        new MetadataEntry(
                                "b6aeffad-da73-4dde-b68e-937e468b1fde",
                                null,
                                null,
                                new MetadataEntry.Format("parquet", Map.of()),
                                "{\"type\":\"struct\",\"fields\":[" +
                                        "{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                                        "{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}," +
                                        "{\"name\":\"married\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}," +

                                        "{\"name\":\"phones\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[" +
                                        "{\"name\":\"number\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                                        "{\"name\":\"label\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}," +
                                        "\"containsNull\":true},\"nullable\":true,\"metadata\":{}}," +

                                        "{\"name\":\"address\",\"type\":{\"type\":\"struct\",\"fields\":[" +
                                        "{\"name\":\"street\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                                        "{\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                                        "{\"name\":\"state\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}," +
                                        "{\"name\":\"zip\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}," +

                                        "{\"name\":\"income\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]}",
                                List.of("age"),
                                Map.of(),
                                1579190100722L));
    }

    @Test
    public void testReadProtocolEntries()
            throws Exception
    {
        URI checkpointUri = getResource(TEST_CHECKPOINT).toURI();
        CheckpointEntryIterator checkpointEntryIterator = createCheckpointEntryIterator(checkpointUri, ImmutableSet.of(PROTOCOL), Optional.empty());
        List<DeltaLakeTransactionLogEntry> entries = ImmutableList.copyOf(checkpointEntryIterator);

        assertThat(entries).hasSize(1);

        assertThat(entries).element(0).extracting(DeltaLakeTransactionLogEntry::getProtocol).isEqualTo(
                new ProtocolEntry(
                        1,
                        2,
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testReadAddEntries()
            throws Exception
    {
        URI checkpointUri = getResource(TEST_CHECKPOINT).toURI();
        CheckpointEntryIterator checkpointEntryIterator = createCheckpointEntryIterator(checkpointUri, ImmutableSet.of(ADD), Optional.of(readMetadataEntry(checkpointUri)));
        List<DeltaLakeTransactionLogEntry> entries = ImmutableList.copyOf(checkpointEntryIterator);

        assertThat(entries).hasSize(9);

        assertThat(entries).element(3).extracting(DeltaLakeTransactionLogEntry::getAdd).isEqualTo(
                new AddFileEntry(
                        "age=42/part-00003-0f53cae3-3e34-4876-b651-e1db9584dbc3.c000.snappy.parquet",
                        Map.of("age", "42"),
                        2634,
                        1579190165000L,
                        false,
                        Optional.of("{" +
                                "\"numRecords\":1," +
                                "\"minValues\":{\"name\":\"Alice\",\"address\":{\"street\":\"100 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":111000.0}," +
                                "\"maxValues\":{\"name\":\"Alice\",\"address\":{\"street\":\"100 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":111000.0}," +
                                "\"nullCount\":{\"name\":0,\"married\":0,\"phones\":0,\"address\":{\"street\":0,\"city\":0,\"state\":0,\"zip\":0},\"income\":0}" +
                                "}"),
                        Optional.empty(),
                        null));

        assertThat(entries).element(7).extracting(DeltaLakeTransactionLogEntry::getAdd).isEqualTo(
                new AddFileEntry(
                        "age=30/part-00002-5800be2e-2373-47d8-8b86-776a8ea9d69f.c000.snappy.parquet",
                        Map.of("age", "30"),
                        2688,
                        1579190165000L,
                        false,
                        Optional.of("{" +
                                "\"numRecords\":1," +
                                "\"minValues\":{\"name\":\"Andy\",\"address\":{\"street\":\"101 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":81000.0}," +
                                "\"maxValues\":{\"name\":\"Andy\",\"address\":{\"street\":\"101 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":81000.0}," +
                                "\"nullCount\":{\"name\":0,\"married\":0,\"phones\":0,\"address\":{\"street\":0,\"city\":0,\"state\":0,\"zip\":0},\"income\":0}" +
                                "}"),
                        Optional.empty(),
                        null));
    }

    @Test
    public void testReadAllEntries()
            throws Exception
    {
        URI checkpointUri = getResource(TEST_CHECKPOINT).toURI();
        MetadataEntry metadataEntry = readMetadataEntry(checkpointUri);
        CheckpointEntryIterator checkpointEntryIterator = createCheckpointEntryIterator(
                checkpointUri,
                ImmutableSet.of(METADATA, PROTOCOL, TRANSACTION, ADD, REMOVE, COMMIT),
                Optional.of(readMetadataEntry(checkpointUri)));
        List<DeltaLakeTransactionLogEntry> entries = ImmutableList.copyOf(checkpointEntryIterator);

        assertThat(entries).hasSize(17);

        // MetadataEntry
        assertThat(entries).element(12).extracting(DeltaLakeTransactionLogEntry::getMetaData).isEqualTo(metadataEntry);

        // ProtocolEntry
        assertThat(entries).element(11).extracting(DeltaLakeTransactionLogEntry::getProtocol).isEqualTo(new ProtocolEntry(1, 2, Optional.empty(), Optional.empty()));

        // TransactionEntry
        // not found in the checkpoint, TODO add a test
        assertThat(entries)
                .map(DeltaLakeTransactionLogEntry::getTxn)
                .filteredOn(Objects::nonNull)
                .isEmpty();

        // AddFileEntry
        assertThat(entries).element(8).extracting(DeltaLakeTransactionLogEntry::getAdd).isEqualTo(
                new AddFileEntry(
                        "age=42/part-00003-0f53cae3-3e34-4876-b651-e1db9584dbc3.c000.snappy.parquet",
                        Map.of("age", "42"),
                        2634,
                        1579190165000L,
                        false,
                        Optional.of("{" +
                                "\"numRecords\":1," +
                                "\"minValues\":{\"name\":\"Alice\",\"address\":{\"street\":\"100 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":111000.0}," +
                                "\"maxValues\":{\"name\":\"Alice\",\"address\":{\"street\":\"100 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":111000.0}," +
                                "\"nullCount\":{\"name\":0,\"married\":0,\"phones\":0,\"address\":{\"street\":0,\"city\":0,\"state\":0,\"zip\":0},\"income\":0}" +
                                "}"),
                        Optional.empty(),
                        null));

        // RemoveFileEntry
        assertThat(entries).element(3).extracting(DeltaLakeTransactionLogEntry::getRemove).isEqualTo(
                new RemoveFileEntry(
                        "age=42/part-00000-951068bd-bcf4-4094-bb94-536f3c41d31f.c000.snappy.parquet",
                        1579190155406L,
                        false));

        // CommitInfoEntry
        // not found in the checkpoint, TODO add a test
        assertThat(entries)
                .map(DeltaLakeTransactionLogEntry::getCommitInfo)
                .filteredOn(Objects::nonNull)
                .isEmpty();
    }

    @Test
    public void testSkipRemoveEntries()
            throws IOException
    {
        MetadataEntry metadataEntry = new MetadataEntry(
                "metadataId",
                "metadataName",
                "metadataDescription",
                new MetadataEntry.Format(
                        "metadataFormatProvider",
                        ImmutableMap.of()),
                "{\"type\":\"struct\",\"fields\":" +
                        "[{\"name\":\"ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}]}",
                ImmutableList.of("part_key"),
                ImmutableMap.of(),
                1000);
        ProtocolEntry protocolEntry = new ProtocolEntry(10, 20, Optional.empty(), Optional.empty());
        AddFileEntry addFileEntryJsonStats = new AddFileEntry(
                "addFilePathJson",
                ImmutableMap.of(),
                1000,
                1001,
                true,
                Optional.of("{" +
                        "\"numRecords\":20," +
                        "\"minValues\":{" +
                        "\"ts\":\"2960-10-31T01:00:00.000Z\"" +
                        "}," +
                        "\"maxValues\":{" +
                        "\"ts\":\"2960-10-31T02:00:00.000Z\"" +
                        "}," +
                        "\"nullCount\":{" +
                        "\"ts\":1" +
                        "}}"),
                Optional.empty(),
                ImmutableMap.of());

        int numRemoveEntries = 100;
        Set<RemoveFileEntry> removeEntries = IntStream.range(0, numRemoveEntries).mapToObj(x ->
                        new RemoveFileEntry(
                                UUID.randomUUID().toString(),
                                1000,
                                true))
                .collect(toImmutableSet());

        CheckpointEntries entries = new CheckpointEntries(
                metadataEntry,
                protocolEntry,
                ImmutableSet.of(),
                ImmutableSet.of(addFileEntryJsonStats),
                removeEntries);

        CheckpointWriter writer = new CheckpointWriter(
                TESTING_TYPE_MANAGER,
                checkpointSchemaManager,
                "test",
                ParquetWriterOptions.builder() // approximately 2 rows per row group
                        .setMaxBlockSize(DataSize.ofBytes(64L))
                        .setMaxPageSize(DataSize.ofBytes(64L))
                        .build());

        File targetFile = File.createTempFile("testSkipRemoveEntries-", ".checkpoint.parquet");
        targetFile.deleteOnExit();

        String targetPath = "file://" + targetFile.getAbsolutePath();
        targetFile.delete(); // file must not exist when writer is called
        writer.write(entries, createOutputFile(targetPath));

        CheckpointEntryIterator addEntryIterator = createCheckpointEntryIterator(
                URI.create(targetPath),
                ImmutableSet.of(ADD),
                Optional.of(metadataEntry));
        CheckpointEntryIterator removeEntryIterator =
                createCheckpointEntryIterator(URI.create(targetPath), ImmutableSet.of(REMOVE), Optional.empty());
        CheckpointEntryIterator txnEntryIterator =
                createCheckpointEntryIterator(URI.create(targetPath), ImmutableSet.of(TRANSACTION), Optional.empty());

        assertThat(Iterators.size(addEntryIterator)).isEqualTo(1);
        assertThat(Iterators.size(removeEntryIterator)).isEqualTo(numRemoveEntries);
        assertThat(Iterators.size(txnEntryIterator)).isEqualTo(0);

        assertThat(addEntryIterator.getCompletedPositions().orElseThrow()).isEqualTo(2L);
        assertThat(removeEntryIterator.getCompletedPositions().orElseThrow()).isEqualTo(100L);
        assertThat(txnEntryIterator.getCompletedPositions().orElseThrow()).isEqualTo(0L);
    }

    private MetadataEntry readMetadataEntry(URI checkpointUri)
            throws IOException
    {
        CheckpointEntryIterator checkpointEntryIterator = createCheckpointEntryIterator(checkpointUri, ImmutableSet.of(METADATA), Optional.empty());
        return Iterators.getOnlyElement(checkpointEntryIterator).getMetaData();
    }

    private CheckpointEntryIterator createCheckpointEntryIterator(URI checkpointUri, Set<CheckpointEntryIterator.EntryType> entryTypes, Optional<MetadataEntry> metadataEntry)
            throws IOException
    {
        TrinoFileSystem fileSystem = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS).create(SESSION);
        TrinoInputFile checkpointFile = fileSystem.newInputFile(Location.of(checkpointUri.toString()));

        return new CheckpointEntryIterator(
                checkpointFile,
                SESSION,
                checkpointFile.length(),
                checkpointSchemaManager,
                TESTING_TYPE_MANAGER,
                entryTypes,
                metadataEntry,
                new FileFormatDataSourceStats(),
                new ParquetReaderConfig().toParquetReaderOptions(),
                true,
                new DeltaLakeConfig().getDomainCompactionThreshold());
    }

    private static TrinoOutputFile createOutputFile(String path)
    {
        return new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS).create(SESSION).newOutputFile(Location.of(path));
    }
}
