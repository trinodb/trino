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
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.plugin.deltalake.DefaultDeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.DeltaLakeSessionProperties;
import io.trino.plugin.deltalake.NoOpTableCredentialsProvider;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.MissingTransactionLogException;
import io.trino.plugin.deltalake.transactionlog.Transaction;
import io.trino.plugin.deltalake.transactionlog.reader.FileSystemTransactionLogReader;
import io.trino.plugin.deltalake.transactionlog.reader.FileSystemTransactionLogReaderFactory;
import io.trino.plugin.deltalake.transactionlog.reader.TransactionLogReader;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.deltalake.DeltaLakeConfig.DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogJsonEntryPath;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTransactionLogTail
{
    private static final String PARALLEL_READ_SESSION_PROPERTY = "transaction_log_parallel_read_enabled";

    @Test
    public void testTail()
            throws Exception
    {
        testTail("databricks73");
        testTail("deltalake");
    }

    private void testTail(String dataSource)
            throws Exception
    {
        String tableLocation = getClass().getClassLoader().getResource(format("%s/person", dataSource)).toURI().toString();
        assertThat(readJsonTransactionLogTails(tableLocation)).hasSize(7);
        assertThat(updateJsonTransactionLogTails(tableLocation)).hasSize(7);
    }

    private List<DeltaLakeTransactionLogEntry> updateJsonTransactionLogTails(String tableLocation)
            throws Exception
    {
        DefaultDeltaLakeFileSystemFactory fileSystemFactory = new DefaultDeltaLakeFileSystemFactory(HDFS_FILE_SYSTEM_FACTORY, new NoOpTableCredentialsProvider());
        TransactionLogReader transactionLogReader = new FileSystemTransactionLogReader(tableLocation, Optional.empty(), fileSystemFactory);
        TransactionLogTail transactionLogTail = transactionLogReader.loadNewTail(SESSION, Optional.of(10L), Optional.of(12L), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE);
        Optional<TransactionLogTail> updatedLogTail = transactionLogReader.getUpdatedTail(SESSION, transactionLogTail, Optional.empty(), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE);
        assertThat(updatedLogTail).isPresent();
        return updatedLogTail.get().getFileEntries(fileSystemFactory.create(SESSION, Optional.empty()));
    }

    private List<DeltaLakeTransactionLogEntry> readJsonTransactionLogTails(String tableLocation)
            throws Exception
    {
        DefaultDeltaLakeFileSystemFactory fileSystemFactory = new DefaultDeltaLakeFileSystemFactory(HDFS_FILE_SYSTEM_FACTORY, new NoOpTableCredentialsProvider());
        TransactionLogReader transactionLogReader = new FileSystemTransactionLogReader(tableLocation, Optional.empty(), fileSystemFactory);
        TransactionLogTail transactionLogTail = transactionLogReader.loadNewTail(SESSION, Optional.of(10L), Optional.empty(), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE);
        return transactionLogTail.getFileEntries(fileSystemFactory.create(SESSION, Optional.empty()));
    }

    @Test
    public void testTailWithListingMatchesTailWithoutListing()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        String tableLocation = "memory:///tail_with_listing";
        writeCommits(fileSystem, tableLocation, 0, 50);

        ExecutorService executor = newFixedThreadPool(8, daemonThreadsNamed("test-transaction-log-tail-%s"));
        try {
            assertTailWithListingMatchesTailWithoutListing(fileSystem, tableLocation, executor, Optional.of(9L), Optional.empty(), 10, 50);
            assertTailWithListingMatchesTailWithoutListing(fileSystem, tableLocation, executor, Optional.empty(), Optional.empty(), 0, 50);
            assertTailWithListingMatchesTailWithoutListing(fileSystem, tableLocation, executor, Optional.of(9L), Optional.of(30L), 10, 30);
            assertTailWithListingMatchesTailWithoutListing(fileSystem, tableLocation, executor, Optional.empty(), Optional.of(30L), 0, 30);
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testTailWithListingFailure()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        String tableLocation = "memory:///tail_with_listing_failure";
        writeCommits(fileSystem, tableLocation, 0, 14);
        writeFile(fileSystem, getTransactionLogJsonEntryPath(tableLocation + "/_delta_log", 15), "not json");
        writeCommits(fileSystem, tableLocation, 16, 20);

        ExecutorService executor = newFixedThreadPool(8, daemonThreadsNamed("test-transaction-log-tail-%s"));
        try {
            assertThatThrownBy(() -> TransactionLogTail.loadNewTailWithListing(fileSystem, tableLocation, Optional.of(9L), Optional.empty(), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE, executor))
                    .hasMessageContaining("Unrecognized token 'not'");
            assertThatThrownBy(() -> TransactionLogTail.loadNewTailWithListing(fileSystem, tableLocation, Optional.of(9L), Optional.of(20L), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE, executor))
                    .hasMessageContaining("Unrecognized token 'not'");
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testTailWithListingOfTableWithoutCommits()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        TransactionLogTail tail = TransactionLogTail.loadNewTailWithListing(fileSystem, "memory:///table_without_commits", Optional.empty(), Optional.empty(), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE, directExecutor());
        assertThat(tail.getTransactions()).isEmpty();
        assertThat(tail.getVersion()).isEqualTo(0);
    }

    @Test
    public void testBackwardTailWithListingWithUnboundedVersionRange()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        String tableLocation = "memory:///backward_tail_unbounded_range";
        writeCommits(fileSystem, tableLocation, 0, 5);

        assertTail(TransactionLogTail.loadNewTailWithListing(fileSystem, tableLocation, Optional.of(-1_000_000_000L), Optional.of(5L), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE, directExecutor()), 0, 5);
        assertThatThrownBy(() -> TransactionLogTail.loadNewTailWithListing(fileSystem, tableLocation, Optional.of(2L), Optional.of(Long.MAX_VALUE), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE, directExecutor()))
                .isInstanceOf(MissingTransactionLogException.class);
    }

    @Test
    public void testTailWithListingEndsAtFirstMissingVersion()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        String tableLocation = "memory:///tail_with_gap";
        writeCommits(fileSystem, tableLocation, 0, 5);
        writeCommits(fileSystem, tableLocation, 7, 8);

        assertTail(TransactionLogTail.loadNewTailWithListing(fileSystem, tableLocation, Optional.of(1L), Optional.empty(), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE, directExecutor()), 2, 5);
    }

    @Test
    public void testTailWithListingWithoutNewCommits()
            throws Exception
    {
        CountingFileSystem fileSystem = new CountingFileSystem();
        String tableLocation = "memory:///tail_without_new_commits";
        writeCommits(fileSystem, tableLocation, 0, 3);

        TransactionLogTail tail = TransactionLogTail.loadNewTailWithListing(fileSystem, tableLocation, Optional.of(3L), Optional.empty(), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE, directExecutor());
        assertThat(tail.getTransactions()).isEmpty();
        assertThat(tail.getVersion()).isEqualTo(3);
        assertThat(fileSystem.listings()).hasValue(0);
        assertThat(fileSystem.lengthProbes()).containsExactly(getTransactionLogJsonEntryPath(tableLocation + "/_delta_log", 4));
    }

    @Test
    public void testTailWithListingMissingFirstVersion()
            throws Exception
    {
        String tableLocation = "memory:///tail_with_listing_missing_first_version";
        Location firstNewCommit = getTransactionLogJsonEntryPath(tableLocation + "/_delta_log", 4);
        TrinoFileSystem fileSystem = new MemoryFileSystem()
        {
            @Override
            public FileIterator listFilesStartingFrom(Location location, String startingFrom)
                    throws IOException
            {
                FileIterator files = super.listFilesStartingFrom(location, startingFrom);
                return new FileIterator()
                {
                    private FileEntry next;

                    @Override
                    public boolean hasNext()
                            throws IOException
                    {
                        while (next == null && files.hasNext()) {
                            FileEntry file = files.next();
                            if (!file.location().equals(firstNewCommit)) {
                                next = file;
                            }
                        }
                        return next != null;
                    }

                    @Override
                    public FileEntry next()
                            throws IOException
                    {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        FileEntry file = next;
                        next = null;
                        return file;
                    }
                };
            }
        };
        writeCommits(fileSystem, tableLocation, 0, 6);

        assertThatThrownBy(() -> TransactionLogTail.loadNewTailWithListing(fileSystem, tableLocation, Optional.of(3L), Optional.empty(), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE, directExecutor()))
                .isInstanceOf(MissingTransactionLogException.class)
                .hasMessageContaining("00000000000000000004.json");
    }

    @Test
    public void testTailWithListingIgnoresFilesInLogSubdirectories()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        String tableLocation = "memory:///tail_with_subdirectories";
        writeCommits(fileSystem, tableLocation, 0, 2);
        writeFile(fileSystem, Location.of(tableLocation + "/_delta_log/_sidecars/00000000000000000003.json"), "{}");

        assertTail(TransactionLogTail.loadNewTailWithListing(fileSystem, tableLocation, Optional.of(0L), Optional.empty(), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE, directExecutor()), 1, 2);
    }

    @Test
    public void testBackwardTailWithListingWithMissingVersion()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        String tableLocation = "memory:///backward_tail_with_gap";
        writeCommits(fileSystem, tableLocation, 0, 3);
        writeCommits(fileSystem, tableLocation, 5, 8);

        assertThatThrownBy(() -> TransactionLogTail.loadNewTailWithListing(fileSystem, tableLocation, Optional.of(1L), Optional.of(8L), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE, directExecutor()))
                .isInstanceOf(MissingTransactionLogException.class)
                .hasMessageContaining("00000000000000000004.json");
        assertTail(TransactionLogTail.loadNewTailWithListing(fileSystem, tableLocation, Optional.empty(), Optional.of(8L), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE, directExecutor()), 5, 8);
    }

    @Test
    public void testBackwardTailStopsListingAtEndVersion()
            throws Exception
    {
        CountingFileSystem fileSystem = new CountingFileSystem();
        String tableLocation = "memory:///backward_tail_bounded_listing";
        writeCommits(fileSystem, tableLocation, 0, 100);

        assertTail(TransactionLogTail.loadNewTailWithListing(fileSystem, tableLocation, Optional.of(10L), Optional.of(12L), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE, directExecutor()), 11, 12);
        assertThat(fileSystem.listedFiles()).hasValue(2);
    }

    @Test
    public void testReaderFromFactoryReadsCommitsInParallel()
            throws Exception
    {
        int parallelism = 4;
        CyclicBarrier concurrentReads = new CyclicBarrier(parallelism);
        AtomicInteger knownLengthOpens = new AtomicInteger();
        TrinoFileSystem fileSystem = new MemoryFileSystem()
        {
            @Override
            public TrinoInputFile newInputFile(Location location, long length)
            {
                knownLengthOpens.incrementAndGet();
                try {
                    concurrentReads.await(10, SECONDS);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return super.newInputFile(location, length);
            }
        };
        String tableLocation = "memory:///parallel_reader_from_factory";
        writeCommits(fileSystem, tableLocation, 0, 19);

        DeltaLakeConfig config = new DeltaLakeConfig()
                .setTransactionLogParallelReadEnabled(true)
                .setMetadataParallelism(parallelism);
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-transaction-log-factory-%s"));
        try {
            TransactionLogReader reader = createReader(fileSystem, tableLocation, executor, config);
            assertTail(reader.loadNewTail(session(config, ImmutableMap.of()), Optional.empty(), Optional.empty(), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE), 0, 19);
            assertThat(knownLengthOpens).hasValue(20);
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testReadersFromFactoryShareCatalogLimit()
            throws Exception
    {
        int perTableLimit = 2;
        int catalogLimit = 3;
        String firstTable = "memory:///first_table";
        String secondTable = "memory:///second_table";
        AtomicInteger firstTableReads = new AtomicInteger();
        AtomicInteger secondTableReads = new AtomicInteger();
        AtomicInteger catalogReads = new AtomicInteger();
        AtomicInteger firstTablePeak = new AtomicInteger();
        AtomicInteger secondTablePeak = new AtomicInteger();
        AtomicInteger catalogPeak = new AtomicInteger();
        CountDownLatch catalogLimitReached = new CountDownLatch(1);
        TrinoFileSystem fileSystem = new MemoryFileSystem()
        {
            @Override
            public TrinoInputFile newInputFile(Location location, long length)
            {
                boolean first = location.toString().startsWith(firstTable);
                AtomicInteger tableReads = first ? firstTableReads : secondTableReads;
                (first ? firstTablePeak : secondTablePeak).accumulateAndGet(tableReads.incrementAndGet(), Math::max);
                int reads = catalogReads.incrementAndGet();
                catalogPeak.accumulateAndGet(reads, Math::max);
                try {
                    // Hold every read until the catalog limit is reached, then give reads beyond the limit time to start
                    if (reads == catalogLimit && catalogLimitReached.getCount() > 0) {
                        Thread.sleep(200);
                        catalogLimitReached.countDown();
                    }
                    assertThat(catalogLimitReached.await(10, SECONDS)).isTrue();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                finally {
                    tableReads.decrementAndGet();
                    catalogReads.decrementAndGet();
                }
                return super.newInputFile(location, length);
            }
        };
        writeCommits(fileSystem, firstTable, 0, 9);
        writeCommits(fileSystem, secondTable, 0, 9);

        DeltaLakeConfig config = new DeltaLakeConfig()
                .setTransactionLogParallelReadEnabled(true)
                .setMetadataParallelism(perTableLimit)
                .setTransactionLogMaxConcurrentReads(catalogLimit);
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-transaction-log-factory-%s"));
        try {
            FileSystemTransactionLogReaderFactory factory = createReaderFactory(fileSystem, executor, config);
            ConnectorSession session = session(config, ImmutableMap.of());
            List<Future<TransactionLogTail>> tails = executor.invokeAll(ImmutableList.of(
                    () -> factory.createReader(firstTable, Optional.empty()).loadNewTail(session, Optional.empty(), Optional.empty(), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE),
                    () -> factory.createReader(secondTable, Optional.empty()).loadNewTail(session, Optional.empty(), Optional.empty(), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE)));
            for (Future<TransactionLogTail> tail : tails) {
                assertTail(tail.get(), 0, 9);
            }
            assertThat(firstTablePeak.get()).isBetween(1, perTableLimit);
            assertThat(secondTablePeak.get()).isBetween(1, perTableLimit);
            assertThat(catalogPeak.get()).isEqualTo(catalogLimit);
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testReaderFromFactoryWithParallelReadDisabledInCatalog()
            throws Exception
    {
        CountingFileSystem fileSystem = new CountingFileSystem();
        String tableLocation = "memory:///reader_parallel_read_disabled_in_catalog";
        writeCommits(fileSystem, tableLocation, 0, 19);

        DeltaLakeConfig config = new DeltaLakeConfig();
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-transaction-log-factory-%s"));
        try {
            // The catalog property gates the feature, so the session property cannot enable it
            ConnectorSession session = session(config, ImmutableMap.of(PARALLEL_READ_SESSION_PROPERTY, true));
            assertTail(createReader(fileSystem, tableLocation, executor, config).loadNewTail(session, Optional.empty(), Optional.empty(), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE), 0, 19);
            assertThat(fileSystem.listings()).hasValue(0);
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testReaderFromFactoryWithParallelReadDisabledInSession()
            throws Exception
    {
        CountingFileSystem fileSystem = new CountingFileSystem();
        String tableLocation = "memory:///reader_parallel_read_disabled_in_session";
        writeCommits(fileSystem, tableLocation, 0, 19);

        DeltaLakeConfig config = new DeltaLakeConfig().setTransactionLogParallelReadEnabled(true);
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-transaction-log-factory-%s"));
        try {
            TransactionLogReader reader = createReader(fileSystem, tableLocation, executor, config);
            assertTail(reader.loadNewTail(session(config, ImmutableMap.of(PARALLEL_READ_SESSION_PROPERTY, false)), Optional.empty(), Optional.empty(), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE), 0, 19);
            assertThat(fileSystem.listings()).hasValue(0);

            assertTail(reader.loadNewTail(session(config, ImmutableMap.of()), Optional.empty(), Optional.empty(), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE), 0, 19);
            assertThat(fileSystem.listings()).hasValue(1);
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static TransactionLogReader createReader(TrinoFileSystem fileSystem, String tableLocation, ExecutorService executor, DeltaLakeConfig config)
    {
        return createReaderFactory(fileSystem, executor, config).createReader(tableLocation, Optional.empty());
    }

    private static FileSystemTransactionLogReaderFactory createReaderFactory(TrinoFileSystem fileSystem, ExecutorService executor, DeltaLakeConfig config)
    {
        return new FileSystemTransactionLogReaderFactory(new DefaultDeltaLakeFileSystemFactory(_ -> fileSystem, new NoOpTableCredentialsProvider()), executor, config);
    }

    private static ConnectorSession session(DeltaLakeConfig config, Map<String, Object> properties)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new DeltaLakeSessionProperties(config, new ParquetReaderConfig(), new ParquetWriterConfig()).getSessionProperties())
                .setPropertyValues(properties)
                .build();
    }

    private static void assertTailWithListingMatchesTailWithoutListing(
            TrinoFileSystem fileSystem,
            String tableLocation,
            ExecutorService executor,
            Optional<Long> startVersion,
            Optional<Long> endVersion,
            long firstVersion,
            long lastVersion)
            throws IOException
    {
        TransactionLogTail tailWithListing = TransactionLogTail.loadNewTailWithListing(fileSystem, tableLocation, startVersion, endVersion, DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE, executor);
        TransactionLogTail tailWithoutListing = TransactionLogTail.loadNewTail(fileSystem, tableLocation, startVersion, endVersion, DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE);
        assertTail(tailWithListing, firstVersion, lastVersion);
        assertThat(tailWithListing.getFileEntries(fileSystem)).isEqualTo(tailWithoutListing.getFileEntries(fileSystem));
        assertThat(tailWithListing.getRetainedSizeInBytes()).isEqualTo(tailWithoutListing.getRetainedSizeInBytes());
    }

    private static void assertTail(TransactionLogTail tail, long firstVersion, long lastVersion)
    {
        assertThat(tail.getTransactions().stream().map(Transaction::transactionId).collect(toImmutableList()))
                .containsExactlyElementsOf(LongStream.rangeClosed(firstVersion, lastVersion).boxed().collect(toImmutableList()));
        assertThat(tail.getVersion()).isEqualTo(lastVersion);
    }

    private static void writeCommits(TrinoFileSystem fileSystem, String tableLocation, long firstVersion, long lastVersion)
            throws IOException
    {
        String transactionLogDir = tableLocation + "/_delta_log";
        for (long version = firstVersion; version <= lastVersion; version++) {
            writeFile(fileSystem, getTransactionLogJsonEntryPath(transactionLogDir, version), "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}");
        }
    }

    private static void writeFile(TrinoFileSystem fileSystem, Location location, String content)
            throws IOException
    {
        TrinoOutputFile outputFile = fileSystem.newOutputFile(location);
        try (OutputStream outputStream = outputFile.create()) {
            outputStream.write(content.getBytes(UTF_8));
        }
    }

    private static class CountingFileSystem
            extends MemoryFileSystem
    {
        private final AtomicInteger listings = new AtomicInteger();
        private final AtomicInteger listedFiles = new AtomicInteger();
        private final List<Location> lengthProbes = new CopyOnWriteArrayList<>();

        public AtomicInteger listings()
        {
            return listings;
        }

        public AtomicInteger listedFiles()
        {
            return listedFiles;
        }

        public List<Location> lengthProbes()
        {
            return lengthProbes;
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            TrinoInputFile inputFile = super.newInputFile(location);
            return new TrinoInputFile()
            {
                @Override
                public TrinoInput newInput()
                        throws IOException
                {
                    return inputFile.newInput();
                }

                @Override
                public TrinoInputStream newStream()
                        throws IOException
                {
                    return inputFile.newStream();
                }

                @Override
                public long length()
                        throws IOException
                {
                    lengthProbes.add(location);
                    return inputFile.length();
                }

                @Override
                public Instant lastModified()
                        throws IOException
                {
                    return inputFile.lastModified();
                }

                @Override
                public boolean exists()
                        throws IOException
                {
                    return inputFile.exists();
                }

                @Override
                public Location location()
                {
                    return inputFile.location();
                }
            };
        }

        @Override
        public FileIterator listFilesStartingFrom(Location location, String startingFrom)
                throws IOException
        {
            listings.incrementAndGet();
            FileIterator files = super.listFilesStartingFrom(location, startingFrom);
            return new FileIterator()
            {
                @Override
                public boolean hasNext()
                        throws IOException
                {
                    return files.hasNext();
                }

                @Override
                public FileEntry next()
                        throws IOException
                {
                    listedFiles.incrementAndGet();
                    return files.next();
                }
            };
        }
    }
}
