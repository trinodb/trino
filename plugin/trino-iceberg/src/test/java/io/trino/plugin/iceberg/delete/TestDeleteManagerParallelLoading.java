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
package io.trino.plugin.iceberg.delete;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.cache.ParquetFooterCache;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.DefaultIcebergFileSystemFactory;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergPageSourceProvider;
import io.trino.plugin.iceberg.IcebergSessionProperties;
import io.trino.plugin.iceberg.encryption.IcebergEncryptionConfig;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.Duration.succinctNanos;
import static io.trino.hdfs.HdfsTestUtils.HDFS_ENVIRONMENT;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.iceberg.IcebergTestUtils.BLOCKS_HASH_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.ENCRYPTION_MANAGER_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.iceberg.FileContent.EQUALITY_DELETES;
import static org.apache.iceberg.FileContent.POSITION_DELETES;
import static org.apache.iceberg.MetadataColumns.ROW_POSITION;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Stresses loading many delete files for a single data file in parallel using the delete loading threadpool.
 * The measured times are logged rather than asserted, this is purely to benchmark performance.
 * For this test delete files live on local disk, where IO is minimal compared to object storage,
 * so the pool cannot do much better than the core count.
 */
@Disabled("Measures wall time for benchmarking, do not enable in CI")
// Ensure tests run in isolation and in single thread to get more reliable numbers
@Isolated
@Execution(SAME_THREAD)
class TestDeleteManagerParallelLoading
{
    private static final Logger log = Logger.get(TestDeleteManagerParallelLoading.class);

    private static final int KEY_FIELD_ID = 1;
    private static final Schema SCHEMA = new Schema(optional(KEY_FIELD_ID, "key", Types.LongType.get()));
    private static final IcebergColumnHandle KEY_COLUMN = getColumnHandle(SCHEMA.findField(KEY_FIELD_ID), TESTING_TYPE_MANAGER);
    private static final IcebergColumnHandle ROW_POSITION_COLUMN = getColumnHandle(ROW_POSITION, TESTING_TYPE_MANAGER);

    private static final OrcReaderConfig ORC_READER_CONFIG = new OrcReaderConfig();
    private static final ParquetReaderConfig PARQUET_READER_CONFIG = new ParquetReaderConfig();
    private static final TrinoFileSystemFactory FILE_SYSTEM_FACTORY = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS);
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new IcebergSessionProperties(
                    new IcebergConfig(),
                    new IcebergEncryptionConfig(),
                    ORC_READER_CONFIG,
                    new OrcWriterConfig(),
                    PARQUET_READER_CONFIG,
                    new ParquetWriterConfig()).getSessionProperties())
            .build();

    /**
     * The data file is never opened, only named: the position delete files record it as the file their positions
     * belong to, and loading them filters on it.
     */
    private static final String DATA_FILE_PATH = "file:///data-file.parquet";
    private static final long SPLIT_SEQUENCE_NUMBER = 5;
    private static final long DELETE_SEQUENCE_NUMBER = 10;

    private static final int LOADING_THREADS = 16;
    private static final int DELETE_FILE_COUNT = 64;

    // delete file i covers rows [i * ROWS_PER_DELETE_FILE, (i + 1) * ROWS_PER_DELETE_FILE) and deletes the first half of them
    private static final int ROWS_PER_DELETE_FILE = 4000;
    private static final int DELETED_ROWS_PER_DELETE_FILE = 2000;
    private static final int DATA_FILE_ROW_COUNT = DELETE_FILE_COUNT * ROWS_PER_DELETE_FILE;

    private static final Duration LOAD_TIMEOUT = new Duration(60, SECONDS);

    @TempDir
    private static Path deleteFileDirectory;

    private static List<DeleteFile> positionDeleteFiles;
    private static List<DeleteFile> equalityDeleteFiles;

    private ExecutorService executor;

    @BeforeAll
    static void writeDeleteFiles()
            throws IOException
    {
        positionDeleteFiles = writeDeleteFiles(POSITION_DELETES);
        equalityDeleteFiles = writeDeleteFiles(EQUALITY_DELETES);
    }

    @BeforeEach
    void setUp()
    {
        executor = newFixedThreadPool(LOADING_THREADS, daemonThreadsNamed("test-delete-loading-%s"));
    }

    @AfterEach
    void tearDown()
    {
        executor.shutdownNow();
    }

    @ParameterizedTest
    @EnumSource(DeleteKind.class)
    void testLoadingManyDeleteFilesPerDataFile(DeleteKind deleteKind)
            throws Exception
    {
        List<DeleteFile> deleteFiles = deleteFiles(deleteKind);
        List<IcebergColumnHandle> readColumns = readColumns(deleteKind);

        // warmup before the measurement
        measureLoad(newDirectExecutorService(), deleteFiles, readColumns);
        measureLoad(executor, deleteFiles, readColumns);

        // measurement
        long onSplitThread = measureLoad(newDirectExecutorService(), deleteFiles, readColumns);
        long onThreadPool = measureLoad(executor, deleteFiles, readColumns);

        log.info(
                "Loading %s %s delete files of one data file, %s deleted rows each, took %s on the split thread and %s on %s threads (%.1fx)",
                deleteFiles.size(),
                deleteKind,
                DELETED_ROWS_PER_DELETE_FILE,
                succinctNanos(onSplitThread),
                succinctNanos(onThreadPool),
                LOADING_THREADS,
                (double) onSplitThread / onThreadPool);
    }

    /**
     * Loads the delete files and returns how long that load took.
     */
    private long measureLoad(ExecutorService loadingExecutor, List<DeleteFile> deleteFiles, List<IcebergColumnHandle> readColumns)
            throws Exception
    {
        // a manager per measurement, so that the equality delete loads of an earlier measurement are not cached
        DeleteManager deleteManager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, loadingExecutor);

        long start = System.nanoTime();
        Optional<PageFilter> pageFilter = deleteManager.createDeletePageFilter(
                        DATA_FILE_PATH,
                        OptionalLong.of(SPLIT_SEQUENCE_NUMBER),
                        deleteFiles,
                        readColumns,
                        SCHEMA,
                        OptionalLong.empty(),
                        OptionalLong.empty(),
                        _ -> { throw new UnsupportedOperationException("unexpected deletion vector read"); },
                        deleteReads(),
                        deleteReads(),
                        MemoryContext.NO_LIMIT)
                .get(LOAD_TIMEOUT.toMillis(), MILLISECONDS);
        long elapsedNanos = System.nanoTime() - start;

        assertDeletesApplied(pageFilter, readColumns);
        return elapsedNanos;
    }

    private static List<DeleteFile> deleteFiles(DeleteKind deleteKind)
    {
        int filesPerKind = DELETE_FILE_COUNT / 2;
        return switch (deleteKind) {
            case POSITION -> positionDeleteFiles;
            case EQUALITY -> equalityDeleteFiles;
            case MIXED -> ImmutableList.<DeleteFile>builder()
                    .addAll(positionDeleteFiles.subList(0, filesPerKind))
                    .addAll(equalityDeleteFiles.subList(filesPerKind, DELETE_FILE_COUNT))
                    .build();
        };
    }

    private static List<IcebergColumnHandle> readColumns(DeleteKind deleteKind)
    {
        return switch (deleteKind) {
            case POSITION -> ImmutableList.of(ROW_POSITION_COLUMN);
            case EQUALITY -> ImmutableList.of(KEY_COLUMN);
            // the key column comes first because equality deletes are matched against the non-metadata columns of the split
            case MIXED -> ImmutableList.of(KEY_COLUMN, ROW_POSITION_COLUMN);
        };
    }

    /**
     * Reads the delete files through the same page source the connector uses to read them.
     */
    private static DeletePageSourceProvider deleteReads()
    {
        IcebergPageSourceProvider pageSourceProvider = new IcebergPageSourceProvider(
                new DefaultIcebergFileSystemFactory(FILE_SYSTEM_FACTORY),
                FILE_IO_FACTORY,
                new FileFormatDataSourceStats(),
                ORC_READER_CONFIG.toOrcReaderOptions(),
                PARQUET_READER_CONFIG.toParquetReaderOptions(),
                TESTING_TYPE_MANAGER,
                // no footer caching, so that a measurement does not get the footers a previous one cached
                ParquetFooterCache.noop(),
                BLOCKS_HASH_FACTORY,
                ENCRYPTION_MANAGER_FACTORY,
                MemoryContext.NO_LIMIT,
                new IcebergConfig().getDomainCompactionThreshold(),
                newDirectExecutorService());

        TrinoFileSystem fileSystem = FILE_SYSTEM_FACTORY.create(SESSION);
        AggregatedMemoryContext splitMemoryContext = newSimpleAggregatedMemoryContext();
        return (deleteFile, deleteColumns, tupleDomain) -> pageSourceProvider.openDeleteFile(
                SESSION,
                fileSystem,
                deleteFile,
                deleteColumns,
                tupleDomain,
                splitMemoryContext.newAggregatedMemoryContext());
    }

    /**
     * Asserts that every delete file was applied, that is that the filter retains exactly the rows of the data file
     * that no delete file covers. A delete file whose load went missing leaves its own stripe of rows behind.
     */
    private static void assertDeletesApplied(Optional<PageFilter> pageFilter, List<IcebergColumnHandle> readColumns)
    {
        assertThat(pageFilter).isPresent();

        // the key of a row is its position in the data file, so the same block serves as every read column
        Block rowIndexes = new LongArrayBlock(DATA_FILE_ROW_COUNT, Optional.empty(), LongStream.range(0, DATA_FILE_ROW_COUNT).toArray());
        Block[] blocks = new Block[readColumns.size()];
        Arrays.fill(blocks, rowIndexes);
        SourcePage page = SourcePage.create(new Page(blocks));

        pageFilter.orElseThrow().applyFilter(page);

        long[] retainedRows = new long[page.getPositionCount()];
        Block retained = page.getBlock(0);
        for (int position = 0; position < retainedRows.length; position++) {
            retainedRows[position] = BIGINT.getLong(retained, position);
        }

        // the rows are compared by mismatch index rather than with isEqualTo, to keep a failure readable
        long[] expectedRetainedRows = expectedRetainedRows();
        assertThat(retainedRows.length).isEqualTo(expectedRetainedRows.length);
        assertThat(Arrays.mismatch(retainedRows, expectedRetainedRows))
                .describedAs("index of the first row that was not filtered as expected")
                .isEqualTo(-1);
    }

    private static long[] expectedRetainedRows()
    {
        return LongStream.range(0, DATA_FILE_ROW_COUNT)
                .filter(row -> row % ROWS_PER_DELETE_FILE >= DELETED_ROWS_PER_DELETE_FILE)
                .toArray();
    }

    private static long[] deletedRows(int fileIndex)
    {
        long firstRow = (long) fileIndex * ROWS_PER_DELETE_FILE;
        return LongStream.range(firstRow, firstRow + DELETED_ROWS_PER_DELETE_FILE).toArray();
    }

    private static List<DeleteFile> writeDeleteFiles(FileContent content)
            throws IOException
    {
        ImmutableList.Builder<DeleteFile> deleteFiles = ImmutableList.builder();
        for (int fileIndex = 0; fileIndex < DELETE_FILE_COUNT; fileIndex++) {
            long[] rows = deletedRows(fileIndex);
            File file = deleteFileDirectory.resolve("%s-%s.parquet".formatted(content.name(), fileIndex)).toFile();
            switch (content) {
                case POSITION_DELETES -> writePositionDeleteFile(file, rows);
                case EQUALITY_DELETES -> writeEqualityDeleteFile(file, rows);
                case DATA, DATA_MANIFEST, DELETE_MANIFEST -> throw new IllegalArgumentException("Not a delete file content: " + content);
            }

            deleteFiles.add(new DeleteFile(
                    content,
                    new LocalInputFile(file).location().toString(),
                    FileFormat.PARQUET,
                    rows.length,
                    file.length(),
                    equalityFieldIds(content),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    DELETE_SEQUENCE_NUMBER,
                    OptionalLong.empty(),
                    Optional.empty(),
                    Optional.empty()));
        }
        return deleteFiles.build();
    }

    private static void writePositionDeleteFile(File file, long[] positions)
            throws IOException
    {
        try (PositionDeleteWriter<Record> writer = Parquet.writeDeletes(Files.localOutput(file))
                .createWriterFunc(GenericParquetWriter::create)
                .withSpec(PartitionSpec.unpartitioned())
                .rowSchema(null)
                .overwrite()
                .buildPositionWriter()) {
            PositionDelete<Record> positionDelete = PositionDelete.create();
            for (long position : positions) {
                writer.write(positionDelete.set(DATA_FILE_PATH, position));
            }
        }
    }

    private static void writeEqualityDeleteFile(File file, long[] keys)
            throws IOException
    {
        try (EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(Files.localOutput(file))
                .createWriterFunc(GenericParquetWriter::create)
                .withSpec(PartitionSpec.unpartitioned())
                .rowSchema(SCHEMA)
                .equalityFieldIds(KEY_FIELD_ID)
                .overwrite()
                .buildEqualityWriter()) {
            Record delete = GenericRecord.create(SCHEMA);
            for (long key : keys) {
                delete.setField("key", key);
                writer.write(delete);
            }
        }
    }

    private static List<Integer> equalityFieldIds(FileContent content)
    {
        return switch (content) {
            case EQUALITY_DELETES -> ImmutableList.of(KEY_FIELD_ID);
            case POSITION_DELETES -> ImmutableList.of();
            case DATA, DATA_MANIFEST, DELETE_MANIFEST -> throw new IllegalArgumentException("Not a delete file content: " + content);
        };
    }

    enum DeleteKind
    {
        POSITION,
        EQUALITY,
        MIXED,
    }
}
