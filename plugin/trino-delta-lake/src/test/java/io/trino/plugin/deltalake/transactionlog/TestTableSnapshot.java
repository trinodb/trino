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
package io.trino.plugin.deltalake.transactionlog;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import io.trino.filesystem.TrackingFileSystemFactory;
import io.trino.filesystem.TrackingFileSystemFactory.OperationType;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.SchemaTableName;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_NEW_STREAM;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.ADD;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.PROTOCOL;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.hive.util.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true) // e.g. TrackingFileSystemFactory is shared mutable state
public class TestTableSnapshot
{
    private final ParquetReaderOptions parquetReaderOptions = new ParquetReaderConfig().toParquetReaderOptions();
    private final int domainCompactionThreshold = 32;

    private CheckpointSchemaManager checkpointSchemaManager;
    private TrackingFileSystemFactory trackingFileSystemFactory;
    private TrinoFileSystem trackingFileSystem;
    private String tableLocation;

    @BeforeMethod
    public void setUp()
            throws URISyntaxException
    {
        checkpointSchemaManager = new CheckpointSchemaManager(TESTING_TYPE_MANAGER);
        tableLocation = getClass().getClassLoader().getResource("databricks/person").toURI().toString();

        trackingFileSystemFactory = new TrackingFileSystemFactory(new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS));
        trackingFileSystem = trackingFileSystemFactory.create(SESSION);
    }

    @Test
    public void testOnlyReadsTrailingJsonFiles()
            throws Exception
    {
        AtomicReference<TableSnapshot> tableSnapshot = new AtomicReference<>();
        assertFileSystemAccesses(
                () -> {
                    tableSnapshot.set(TableSnapshot.load(
                            new SchemaTableName("schema", "person"), trackingFileSystem, tableLocation, parquetReaderOptions, true, domainCompactionThreshold));
                },
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation("_last_checkpoint", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation("00000000000000000011.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation("00000000000000000012.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation("00000000000000000013.json", INPUT_FILE_NEW_STREAM), 1)
                        .addCopies(new FileOperation("00000000000000000014.json", INPUT_FILE_NEW_STREAM), 1)
                        .build());

        assertFileSystemAccesses(
                () -> {
                    tableSnapshot.get().getJsonTransactionLogEntries().forEach(entry -> {});
                },
                ImmutableMultiset.of());
    }

    // TODO: Can't test the FileSystem access here because the DeltaLakePageSourceProvider doesn't use the FileSystem passed into the TableSnapshot. (https://github.com/trinodb/trino/issues/12040)
    @Test
    public void readsCheckpointFile()
            throws IOException
    {
        TableSnapshot tableSnapshot = TableSnapshot.load(
                new SchemaTableName("schema", "person"), trackingFileSystem, tableLocation, parquetReaderOptions, true, domainCompactionThreshold);
        tableSnapshot.setCachedMetadata(Optional.of(new MetadataEntry("id", "name", "description", null, "schema", ImmutableList.of(), ImmutableMap.of(), 0)));
        try (Stream<DeltaLakeTransactionLogEntry> stream = tableSnapshot.getCheckpointTransactionLogEntries(
                SESSION, ImmutableSet.of(ADD), checkpointSchemaManager, TESTING_TYPE_MANAGER, trackingFileSystem, new FileFormatDataSourceStats())) {
            List<DeltaLakeTransactionLogEntry> entries = stream.collect(toImmutableList());

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

        // lets read two entry types in one call; add and protocol
        try (Stream<DeltaLakeTransactionLogEntry> stream = tableSnapshot.getCheckpointTransactionLogEntries(
                SESSION, ImmutableSet.of(ADD, PROTOCOL), checkpointSchemaManager, TESTING_TYPE_MANAGER, trackingFileSystem, new FileFormatDataSourceStats())) {
            List<DeltaLakeTransactionLogEntry> entries = stream.collect(toImmutableList());

            assertThat(entries).hasSize(10);

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

            assertThat(entries).element(6).extracting(DeltaLakeTransactionLogEntry::getProtocol).isEqualTo(new ProtocolEntry(1, 2, Optional.empty(), Optional.empty()));

            assertThat(entries).element(8).extracting(DeltaLakeTransactionLogEntry::getAdd).isEqualTo(
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
    }

    @Test
    public void testMaxTransactionId()
            throws IOException
    {
        TableSnapshot tableSnapshot = TableSnapshot.load(
                new SchemaTableName("schema", "person"), trackingFileSystem, tableLocation, parquetReaderOptions, true, domainCompactionThreshold);
        assertEquals(tableSnapshot.getVersion(), 13L);
    }

    private void assertFileSystemAccesses(ThrowingRunnable callback, Multiset<FileOperation> expectedAccesses)
            throws Exception
    {
        trackingFileSystemFactory.reset();
        callback.run();
        assertMultisetsEqual(getOperations(), expectedAccesses);
    }

    private Multiset<FileOperation> getOperations()
    {
        return trackingFileSystemFactory.getOperationCounts()
                .entrySet().stream()
                .flatMap(entry -> nCopies(entry.getValue(), new FileOperation(
                        entry.getKey().getLocation().toString().replaceFirst(".*/_delta_log/", ""),
                        entry.getKey().getOperationType())).stream())
                .collect(toCollection(HashMultiset::create));
    }

    private record FileOperation(String path, OperationType operationType)
    {
        FileOperation
        {
            requireNonNull(path, "path is null");
            requireNonNull(operationType, "operationType is null");
        }
    }

    private interface ThrowingRunnable
    {
        void run()
                throws Exception;
    }
}
