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
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import io.airlift.units.DataSize;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.filesystem.ForwardingTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.deltalake.DefaultDeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.NoOpTableCredentialsProvider;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot.MetadataAndProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.deltalake.transactionlog.checkpoint.LastCheckpoint;
import io.trino.plugin.deltalake.transactionlog.reader.FileSystemTransactionLogReader;
import io.trino.plugin.deltalake.transactionlog.reader.FileSystemTransactionLogReaderFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import io.trino.testing.TestingConnectorContext;
import io.trino.testing.TestingTelemetry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.deltalake.DeltaLakeConfig.DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE;
import static io.trino.plugin.deltalake.transactionlog.TableSnapshot.load;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.readLastCheckpoint;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.ADD;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.PROTOCOL;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTableSnapshot
{
    private final ParquetReaderOptions parquetReaderOptions = ParquetReaderOptions.defaultOptions();
    private final int domainCompactionThreshold = 32;

    private CheckpointSchemaManager checkpointSchemaManager;
    private DefaultDeltaLakeFileSystemFactory tracingFileSystemFactory;
    private TestingTelemetry testingTelemetry = TestingTelemetry.create("test-table-snapshot");
    private TrinoFileSystem trackingFileSystem;
    private String tableLocation;

    @BeforeEach
    public void setUp()
            throws URISyntaxException
    {
        checkpointSchemaManager = new CheckpointSchemaManager(TESTING_TYPE_MANAGER);
        tableLocation = getClass().getClassLoader().getResource("databricks73/person").toURI().toString();

        tracingFileSystemFactory = new DefaultDeltaLakeFileSystemFactory(new TracingFileSystemFactory(testingTelemetry.getTracer(), HDFS_FILE_SYSTEM_FACTORY), new NoOpTableCredentialsProvider());
        trackingFileSystem = tracingFileSystemFactory.create(SESSION, Optional.empty());
    }

    @Test
    public void testOnlyReadsTrailingJsonFiles()
            throws Exception
    {
        AtomicReference<TableSnapshot> tableSnapshot = new AtomicReference<>();
        assertFileSystemAccesses(
                () -> {
                    Optional<LastCheckpoint> lastCheckpoint = readLastCheckpoint(trackingFileSystem, tableLocation);
                    tableSnapshot.set(load(
                            SESSION,
                            new FileSystemTransactionLogReader(tableLocation, Optional.empty(), tracingFileSystemFactory),
                            new SchemaTableName("schema", "person"),
                            lastCheckpoint,
                            tableLocation,
                            parquetReaderOptions,
                            true,
                            domainCompactionThreshold,
                            DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE,
                            Optional.empty()));
                },
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000011.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000012.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000013.json", "InputFile.newStream"))
                        .add(new FileOperation("00000000000000000011.json", "InputFile.length"))
                        .add(new FileOperation("00000000000000000012.json", "InputFile.length"))
                        .add(new FileOperation("00000000000000000013.json", "InputFile.length"))
                        .add(new FileOperation("00000000000000000014.json", "InputFile.length"))
                        .build());

        assertFileSystemAccesses(
                () -> {
                    tableSnapshot.get().getJsonTransactionLogEntries(trackingFileSystem).forEach(_ -> {});
                },
                ImmutableMultiset.of());
    }

    // TODO: Can't test the FileSystem access here because the DeltaLakePageSourceProvider doesn't use the FileSystem passed into the TableSnapshot. (https://github.com/trinodb/trino/issues/12040)
    @Test
    public void readsCheckpointFile()
            throws IOException
    {
        ExecutorService executorService = newDirectExecutorService();
        Optional<LastCheckpoint> lastCheckpoint = readLastCheckpoint(trackingFileSystem, tableLocation);
        TableSnapshot tableSnapshot = load(
                SESSION,
                new FileSystemTransactionLogReader(tableLocation, Optional.empty(), tracingFileSystemFactory),
                new SchemaTableName("schema", "person"),
                lastCheckpoint,
                tableLocation,
                parquetReaderOptions,
                true,
                domainCompactionThreshold,
                DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE,
                Optional.empty());
        TestingConnectorContext context = new TestingConnectorContext();
        TypeManager typeManager = context.getTypeManager();
        TransactionLogAccess transactionLogAccess = new TransactionLogAccess(
                typeManager,
                new CheckpointSchemaManager(typeManager),
                new DeltaLakeConfig(),
                new FileFormatDataSourceStats(),
                tracingFileSystemFactory,
                new ParquetReaderConfig(),
                executorService,
                new FileSystemTransactionLogReaderFactory(tracingFileSystemFactory));
        TrinoFileSystem fileSystem = tracingFileSystemFactory.create(SESSION, tableLocation);
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, fileSystem, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(SESSION, fileSystem, tableSnapshot);
        tableSnapshot.setCachedMetadata(Optional.of(metadataEntry));
        try (Stream<DeltaLakeTransactionLogEntry> stream = tableSnapshot.getCheckpointTransactionLogEntries(
                SESSION,
                ImmutableSet.of(ADD),
                checkpointSchemaManager,
                TESTING_TYPE_MANAGER,
                trackingFileSystem,
                new FileFormatDataSourceStats(),
                Optional.of(new MetadataAndProtocolEntry(metadataEntry, protocolEntry)),
                TupleDomain.all(),
                Optional.of(alwaysTrue()),
                executorService)) {
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
                            null,
                            Optional.empty()));

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
                            null,
                            Optional.empty()));
        }

        // lets read two entry types in one call; add and protocol
        try (Stream<DeltaLakeTransactionLogEntry> stream = tableSnapshot.getCheckpointTransactionLogEntries(
                SESSION,
                ImmutableSet.of(ADD, PROTOCOL),
                checkpointSchemaManager,
                TESTING_TYPE_MANAGER,
                trackingFileSystem,
                new FileFormatDataSourceStats(),
                Optional.of(new MetadataAndProtocolEntry(metadataEntry, protocolEntry)),
                TupleDomain.all(),
                Optional.of(alwaysTrue()),
                executorService)) {
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
                            null,
                            Optional.empty()));

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
                            null,
                            Optional.empty()));
        }
    }

    @Test
    public void testAbandonedV2CheckpointReadClosesSidecarInputs()
            throws Exception
    {
        // The checkpoint has four sidecar files, so the sidecars after the first one are opened but never reached
        String v2TableLocation = getClass().getClassLoader().getResource("deltalake/multipart_v2_checkpoint").toURI().toString();
        AtomicInteger openCounter = new AtomicInteger();
        TrinoFileSystem countingFileSystem = new CountingFileSystem(HDFS_FILE_SYSTEM_FACTORY.create(SESSION), openCounter);
        Optional<LastCheckpoint> lastCheckpoint = readLastCheckpoint(countingFileSystem, v2TableLocation);
        // Files above the small file threshold keep their input open until the page source is closed
        ParquetReaderOptions parquetReaderOptions = ParquetReaderOptions.builder()
                .withSmallFileThreshold(DataSize.ofBytes(0))
                .build();
        TableSnapshot tableSnapshot = load(
                SESSION,
                new FileSystemTransactionLogReader(v2TableLocation, Optional.empty(), tracingFileSystemFactory),
                new SchemaTableName("schema", "table"),
                lastCheckpoint,
                v2TableLocation,
                parquetReaderOptions,
                true,
                domainCompactionThreshold,
                DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE,
                Optional.empty());
        TransactionLogAccess transactionLogAccess = new TransactionLogAccess(
                TESTING_TYPE_MANAGER,
                checkpointSchemaManager,
                new DeltaLakeConfig(),
                new FileFormatDataSourceStats(),
                tracingFileSystemFactory,
                new ParquetReaderConfig(),
                newDirectExecutorService(),
                new FileSystemTransactionLogReaderFactory(tracingFileSystemFactory));
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(SESSION, trackingFileSystem, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(SESSION, trackingFileSystem, tableSnapshot);

        try (Stream<DeltaLakeTransactionLogEntry> stream = tableSnapshot.getCheckpointTransactionLogEntries(
                SESSION,
                ImmutableSet.of(ADD),
                checkpointSchemaManager,
                TESTING_TYPE_MANAGER,
                countingFileSystem,
                new FileFormatDataSourceStats(),
                Optional.of(new MetadataAndProtocolEntry(metadataEntry, protocolEntry)),
                TupleDomain.all(),
                Optional.of(alwaysTrue()),
                newDirectExecutorService())) {
            // Fails while the first sidecar is being read, leaving the other sidecars unreached
            assertThatThrownBy(() -> stream.forEach(entry -> {
                if (entry.getAdd() != null) {
                    throw new IllegalStateException("first add entry read");
                }
            }))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("first add entry read");
        }
        assertThat(openCounter.get()).isEqualTo(0);
    }

    @Test
    public void testMaxTransactionId()
            throws IOException
    {
        Optional<LastCheckpoint> lastCheckpoint = readLastCheckpoint(trackingFileSystem, tableLocation);
        TableSnapshot tableSnapshot = load(
                SESSION,
                new FileSystemTransactionLogReader(tableLocation, Optional.empty(), tracingFileSystemFactory),
                new SchemaTableName("schema", "person"),
                lastCheckpoint,
                tableLocation,
                parquetReaderOptions,
                true,
                domainCompactionThreshold,
                DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE,
                Optional.empty());
        assertThat(tableSnapshot.getVersion()).isEqualTo(13L);
    }

    private void assertFileSystemAccesses(TestingTelemetry.CheckedRunnable<?> callback, Multiset<FileOperation> expectedAccesses)
            throws Exception
    {
        assertMultisetsEqual(getOperations(testingTelemetry.captureSpans(callback::run)), expectedAccesses);
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

    private static class CountingFileSystem
            extends ForwardingTrinoFileSystem
    {
        private final AtomicInteger openCounter;

        CountingFileSystem(TrinoFileSystem delegate, AtomicInteger openCounter)
        {
            super(delegate);
            this.openCounter = requireNonNull(openCounter, "openCounter is null");
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            return new CountingInputFile(super.newInputFile(location), openCounter);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            return new CountingInputFile(super.newInputFile(location, length), openCounter);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            return new CountingInputFile(super.newInputFile(location, length, lastModified), openCounter);
        }
    }

    private record CountingInputFile(TrinoInputFile delegate, AtomicInteger openCounter)
            implements TrinoInputFile
    {
        @Override
        public TrinoInput newInput()
                throws IOException
        {
            TrinoInput input = delegate.newInput();
            openCounter.incrementAndGet();
            return new CountingInput(input, openCounter);
        }

        @Override
        public TrinoInputStream newStream()
                throws IOException
        {
            return delegate.newStream();
        }

        @Override
        public long length()
                throws IOException
        {
            return delegate.length();
        }

        @Override
        public Instant lastModified()
                throws IOException
        {
            return delegate.lastModified();
        }

        @Override
        public boolean exists()
                throws IOException
        {
            return delegate.exists();
        }

        @Override
        public Location location()
        {
            return delegate.location();
        }
    }

    private static class CountingInput
            implements TrinoInput
    {
        private final TrinoInput delegate;
        private final AtomicInteger openCounter;
        private boolean closed;

        CountingInput(TrinoInput delegate, AtomicInteger openCounter)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.openCounter = requireNonNull(openCounter, "openCounter is null");
        }

        @Override
        public void readFully(long position, byte[] buffer, int bufferOffset, int length)
                throws IOException
        {
            delegate.readFully(position, buffer, bufferOffset, length);
        }

        @Override
        public int readTail(byte[] buffer, int bufferOffset, int maxLength)
                throws IOException
        {
            return delegate.readTail(buffer, bufferOffset, maxLength);
        }

        @Override
        public void close()
                throws IOException
        {
            if (!closed) {
                closed = true;
                openCounter.decrementAndGet();
            }
            delegate.close();
        }
    }
}
