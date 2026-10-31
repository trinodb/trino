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
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.airlift.concurrent.Threads.virtualThreadsNamed;
import static io.trino.hdfs.HdfsTestUtils.HDFS_ENVIRONMENT;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.iceberg.IcebergTestUtils.BLOCKS_HASH_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.ENCRYPTION_MANAGER_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.file.Files.createTempDirectory;
import static java.util.concurrent.Executors.newThreadPerTaskExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.iceberg.FileContent.EQUALITY_DELETES;
import static org.apache.iceberg.FileContent.POSITION_DELETES;
import static org.apache.iceberg.MetadataColumns.ROW_POSITION;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Loads many delete files of a single data file, either opening them on the split thread (loadingThreads=0) or on the
 * delete loading pool, to measure what opening them in parallel gains. Decoding always runs on the thread of the
 * benchmark, as it does on the thread reading the split. Delete files live on local disk, and readLatencyMillis
 * delays every read request to model the round trip of an object store.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
public class BenchmarkDeleteManagerParallelLoading
{
    private static final int KEY_FIELD_ID = 1;
    private static final Schema SCHEMA = new Schema(optional(KEY_FIELD_ID, "key", Types.LongType.get()));
    private static final IcebergColumnHandle KEY_COLUMN = getColumnHandle(SCHEMA.findField(KEY_FIELD_ID), TESTING_TYPE_MANAGER);
    private static final IcebergColumnHandle ROW_POSITION_COLUMN = getColumnHandle(ROW_POSITION, TESTING_TYPE_MANAGER);

    // a non-key column the delete reader never projects, written to make an equality delete file large without changing what is decoded
    private static final int PADDING_FIELD_ID = 2;
    private static final Schema EQUALITY_DELETE_ROW_SCHEMA = new Schema(
            optional(KEY_FIELD_ID, "key", Types.LongType.get()),
            optional(PADDING_FIELD_ID, "padding", Types.BinaryType.get()));

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

    private static final int DELETE_FILE_COUNT = 64;

    // delete file i covers rows [i * ROWS_PER_DELETE_FILE, (i + 1) * ROWS_PER_DELETE_FILE) and deletes the first half of them
    private static final int ROWS_PER_DELETE_FILE = 4000;
    private static final int DELETED_ROWS_PER_DELETE_FILE = 2000;
    private static final int DATA_FILE_ROW_COUNT = DELETE_FILE_COUNT * ROWS_PER_DELETE_FILE;

    private static final long LOAD_TIMEOUT_SECONDS = 60;

    // for large position delete files, the other data files whose deletes they hold, and the row group size they are written with
    private static final int OTHER_DATA_FILE_COUNT = 200;
    private static final int OTHER_DATA_FILE_ROW_GROUP_SIZE_BYTES = 128 * 1024;
    private static final String PARQUET_ROW_GROUP_SIZE_BYTES = "write.parquet.row-group-size-bytes";

    @Param
    public DeleteKind deleteKind;

    // 0 loads on the split thread
    @Param({"0", "16"})
    public int loadingThreads;

    // latency of each read request, 0 for local disk or about 50 for an object store
    @Param({"0", "50"})
    public int readLatencyMillis;

    @Param
    public DeleteFileSize deleteFileSize;

    private Path deleteFileDirectory;
    private ExecutorService executorService;
    private Executor executor;
    private List<DeleteFile> deleteFiles;
    private List<IcebergColumnHandle> readColumns;
    private DeletePageSourceProvider deleteReads;

    @Setup
    public void setup()
            throws Exception
    {
        deleteFileDirectory = createTempDirectory("delete-manager-parallel-loading");
        // the same executor as IcebergExecutorModule and IcebergPageSourceProviderFactory
        if (loadingThreads == 0) {
            executorService = newDirectExecutorService();
            executor = executorService;
        }
        else {
            executorService = newThreadPerTaskExecutor(virtualThreadsNamed("benchmark-delete-loading#v%s"));
            executor = new BoundedExecutor(executorService, loadingThreads);
        }
        deleteReads = deleteReads(readLatencyMillis);
        deleteFiles = deleteFiles(deleteKind);
        readColumns = readColumns(deleteKind);

        // verify that the files are on the intended side of the small file threshold, under which the reader reads them whole when opened
        long smallFileThreshold = PARQUET_READER_CONFIG.getSmallFileThreshold().toBytes();
        for (DeleteFile deleteFile : deleteFiles) {
            switch (deleteFileSize) {
                case SMALL -> assertThat(deleteFile.fileSizeInBytes()).isLessThanOrEqualTo(smallFileThreshold);
                case LARGE -> assertThat(deleteFile.fileSizeInBytes()).isGreaterThan(smallFileThreshold);
            }
        }

        // verify once that the benchmark loads what it is meant to, so that it does not time a broken load
        assertDeletesApplied(loadDeletes(), readColumns);
    }

    @TearDown
    public void tearDown()
            throws IOException
    {
        executorService.shutdownNow();
        MoreFiles.deleteRecursively(deleteFileDirectory, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    @Benchmark
    public Optional<PageFilter> loadDeletes()
            throws Exception
    {
        // a manager per load, so that the equality delete loads of an earlier load are not cached
        DeleteManager deleteManager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);
        // decode on this thread, as the page source decodes on the thread reading the split
        ThreadlessExecutor split = new ThreadlessExecutor();
        ListenableFuture<Optional<PageFilter>> pageFilter = deleteManager.createDeletePageFilter(
                DATA_FILE_PATH,
                OptionalLong.of(SPLIT_SEQUENCE_NUMBER),
                deleteFiles,
                readColumns,
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> { throw new UnsupportedOperationException("unexpected deletion vector read"); },
                deleteReads,
                deleteReads,
                split,
                MemoryContext.NO_LIMIT);
        while (!pageFilter.isDone()) {
            whenAnyComplete(ImmutableList.of(split.taskAvailable(), pageFilter)).get(LOAD_TIMEOUT_SECONDS, SECONDS);
            split.runPending();
        }
        return pageFilter.get();
    }

    private List<DeleteFile> deleteFiles(DeleteKind deleteKind)
            throws IOException
    {
        int filesPerKind = DELETE_FILE_COUNT / 2;
        return switch (deleteKind) {
            case POSITION -> writeDeleteFiles(POSITION_DELETES, 0, DELETE_FILE_COUNT);
            case EQUALITY -> writeDeleteFiles(EQUALITY_DELETES, 0, DELETE_FILE_COUNT);
            case MIXED -> ImmutableList.<DeleteFile>builder()
                    .addAll(writeDeleteFiles(POSITION_DELETES, 0, filesPerKind))
                    .addAll(writeDeleteFiles(EQUALITY_DELETES, filesPerKind, DELETE_FILE_COUNT))
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
    private static DeletePageSourceProvider deleteReads(int readLatencyMillis)
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
        if (readLatencyMillis > 0) {
            fileSystem = new LatencyInjectingFileSystem(fileSystem, new Duration(readLatencyMillis, MILLISECONDS));
        }
        TrinoFileSystem deleteFileSystem = fileSystem;
        AggregatedMemoryContext splitMemoryContext = newSimpleAggregatedMemoryContext();
        return (deleteFile, deleteColumns, tupleDomain) -> pageSourceProvider.openDeleteFile(
                SESSION,
                deleteFileSystem,
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

    private List<DeleteFile> writeDeleteFiles(FileContent content, int fromFileIndex, int toFileIndex)
            throws IOException
    {
        // random, so that the padding does not compress
        Random random = new Random(fromFileIndex);
        ImmutableList.Builder<DeleteFile> deleteFiles = ImmutableList.builder();
        for (int fileIndex = fromFileIndex; fileIndex < toFileIndex; fileIndex++) {
            long[] rows = deletedRows(fileIndex);
            File file = deleteFileDirectory.resolve("%s-%s.parquet".formatted(content.name(), fileIndex)).toFile();
            switch (content) {
                case POSITION_DELETES -> writePositionDeleteFile(file, rows, deleteFileSize.otherDataFileRows(), random);
                case EQUALITY_DELETES -> writeEqualityDeleteFile(file, rows, deleteFileSize.paddingBytesPerRow(), random);
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

    /**
     * Writes the positions deleted from the data file. With {@code otherDataFileRows}, the file also deletes that many
     * random positions from each of the other data files around it, as a position delete file shared by many data files
     * does. Delete files are sorted by path, and the reader prunes the row groups of the other data files by path, so the
     * file is large but the reader decodes about the same rows.
     */
    private static void writePositionDeleteFile(File file, long[] positions, int otherDataFileRows, Random random)
            throws IOException
    {
        try (PositionDeleteWriter<Record> writer = Parquet.writeDeletes(Files.localOutput(file))
                .createWriterFunc(GenericParquetWriter::create)
                .withSpec(PartitionSpec.unpartitioned())
                .rowSchema(null)
                // small row groups, so that most of them hold only the other data files and are pruned
                .set(PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(OTHER_DATA_FILE_ROW_GROUP_SIZE_BYTES))
                .overwrite()
                .buildPositionWriter()) {
            PositionDelete<Record> positionDelete = PositionDelete.create();
            // paths are written sorted, those of the other data files sorting before and after the data file
            for (int otherFile = 0; otherDataFileRows > 0 && otherFile < OTHER_DATA_FILE_COUNT / 2; otherFile++) {
                writeOtherDataFileDeletes(writer, positionDelete, "file:///a-%04d.parquet".formatted(otherFile), otherDataFileRows, random);
            }
            for (long position : positions) {
                writer.write(positionDelete.set(DATA_FILE_PATH, position));
            }
            for (int otherFile = 0; otherDataFileRows > 0 && otherFile < OTHER_DATA_FILE_COUNT / 2; otherFile++) {
                writeOtherDataFileDeletes(writer, positionDelete, "file:///z-%04d.parquet".formatted(otherFile), otherDataFileRows, random);
            }
        }
    }

    private static void writeOtherDataFileDeletes(PositionDeleteWriter<Record> writer, PositionDelete<Record> positionDelete, String path, int rows, Random random)
    {
        // random gaps, so that the positions do not compress to nothing
        long position = 0;
        for (int row = 0; row < rows; row++) {
            position += 1 + random.nextInt(1_000_000);
            writer.write(positionDelete.set(path, position));
        }
    }

    private static void writeEqualityDeleteFile(File file, long[] keys, int paddingBytesPerRow, Random random)
            throws IOException
    {
        Schema rowSchema = paddingBytesPerRow == 0 ? SCHEMA : EQUALITY_DELETE_ROW_SCHEMA;
        try (EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(Files.localOutput(file))
                .createWriterFunc(GenericParquetWriter::create)
                .withSpec(PartitionSpec.unpartitioned())
                .rowSchema(rowSchema)
                .equalityFieldIds(KEY_FIELD_ID)
                .overwrite()
                .buildEqualityWriter()) {
            Record delete = GenericRecord.create(rowSchema);
            for (long key : keys) {
                delete.setField("key", key);
                if (paddingBytesPerRow > 0) {
                    delete.setField("padding", padding(paddingBytesPerRow, random));
                }
                writer.write(delete);
            }
        }
    }

    private static ByteBuffer padding(int length, Random random)
    {
        byte[] padding = new byte[length];
        random.nextBytes(padding);
        return ByteBuffer.wrap(padding);
    }

    private static List<Integer> equalityFieldIds(FileContent content)
    {
        return switch (content) {
            case EQUALITY_DELETES -> ImmutableList.of(KEY_FIELD_ID);
            case POSITION_DELETES -> ImmutableList.of();
            case DATA, DATA_MANIFEST, DELETE_MANIFEST -> throw new IllegalArgumentException("Not a delete file content: " + content);
        };
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkDeleteManagerParallelLoading.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("--add-modules=jdk.incubator.vector"))
                .run();
    }

    public enum DeleteKind
    {
        POSITION,
        EQUALITY,
        MIXED,
    }

    /**
     * Whether the delete files are under parquet_small_file_threshold, so the reader reads them whole when they are opened,
     * or over it, so the reader reads the footer when they are opened and the row groups it does not prune while they are decoded.
     * Large files add data the reader does not decode: the deletes of other data files for position deletes, and a
     * column that is not a key for equality deletes.
     */
    public enum DeleteFileSize
    {
        // about 20KB
        SMALL(0, 0),
        // over 3MB
        LARGE(2048, 4096);

        private final int paddingBytesPerRow;
        private final int otherDataFileRows;

        DeleteFileSize(int paddingBytesPerRow, int otherDataFileRows)
        {
            this.paddingBytesPerRow = paddingBytesPerRow;
            this.otherDataFileRows = otherDataFileRows;
        }

        public int paddingBytesPerRow()
        {
            return paddingBytesPerRow;
        }

        public int otherDataFileRows()
        {
            return otherDataFileRows;
        }
    }
}
