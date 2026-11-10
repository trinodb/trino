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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.local.LocalInputFile;
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
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.parquet.Parquet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.hdfs.HdfsTestUtils.HDFS_ENVIRONMENT;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.iceberg.IcebergTestUtils.BLOCKS_HASH_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.ENCRYPTION_MANAGER_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.file.Files.createTempDirectory;
import static org.apache.iceberg.FileContent.POSITION_DELETES;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reads one position delete file that holds the deletes of many data files, as a partition scoped delete file does.
 * The paths are sorted, as in any position delete file, and dictionary encoded.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
public class BenchmarkPositionDeleteReader
{
    private static final int DATA_FILE_COUNT = 1000;
    private static final int DELETES_PER_DATA_FILE = 1000;
    private static final String DATA_FILE_PATH_FORMAT = "s3://warehouse-bucket/analytics/events/data/event_date=2026-11-10/00000-%04d-2f6d4c1e-8b9a-4f3e-9c7d-1a2b3c4d5e6f-00001.parquet";
    private static final String TARGET_DATA_FILE_PATH = DATA_FILE_PATH_FORMAT.formatted(DATA_FILE_COUNT / 2);

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

    private Path deleteFileDirectory;
    private DeleteFile deleteFile;
    private DeletePageSourceProvider deleteReads;
    private List<IcebergColumnHandle> deleteColumns;

    @Setup
    public void setup()
            throws IOException
    {
        deleteFileDirectory = createTempDirectory("position-delete-reader");
        File file = deleteFileDirectory.resolve("position-deletes.parquet").toFile();
        try (PositionDeleteWriter<Record> writer = Parquet.writeDeletes(Files.localOutput(file))
                .createWriterFunc(GenericParquetWriter::create)
                .withSpec(PartitionSpec.unpartitioned())
                .rowSchema(null)
                .overwrite()
                .buildPositionWriter()) {
            PositionDelete<Record> positionDelete = PositionDelete.create();
            for (int dataFile = 0; dataFile < DATA_FILE_COUNT; dataFile++) {
                String path = DATA_FILE_PATH_FORMAT.formatted(dataFile);
                for (int row = 0; row < DELETES_PER_DATA_FILE; row++) {
                    writer.write(positionDelete.set(path, row * 7L));
                }
            }
        }
        deleteFile = new DeleteFile(
                POSITION_DELETES,
                new LocalInputFile(file).location().toString(),
                FileFormat.PARQUET,
                (long) DATA_FILE_COUNT * DELETES_PER_DATA_FILE,
                file.length(),
                ImmutableList.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                1,
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty());

        IcebergPageSourceProvider pageSourceProvider = new IcebergPageSourceProvider(
                new DefaultIcebergFileSystemFactory(FILE_SYSTEM_FACTORY),
                FILE_IO_FACTORY,
                new FileFormatDataSourceStats(),
                ORC_READER_CONFIG.toOrcReaderOptions(),
                PARQUET_READER_CONFIG.toParquetReaderOptions(),
                TESTING_TYPE_MANAGER,
                ParquetFooterCache.noop(),
                BLOCKS_HASH_FACTORY,
                ENCRYPTION_MANAGER_FACTORY,
                MemoryContext.NO_LIMIT,
                new IcebergConfig().getDomainCompactionThreshold(),
                directExecutor());
        TrinoFileSystem fileSystem = FILE_SYSTEM_FACTORY.create(SESSION);
        deleteReads = (delete, columns, tupleDomain) -> pageSourceProvider.openDeleteFile(
                SESSION,
                fileSystem,
                delete,
                columns,
                tupleDomain,
                newSimpleAggregatedMemoryContext());
        deleteColumns = ImmutableList.of(getColumnHandle(DELETE_FILE_PATH, TESTING_TYPE_MANAGER), getColumnHandle(DELETE_FILE_POS, TESTING_TYPE_MANAGER));

        // verify that both benchmarks read the expected deletes
        assertThat(readPositionDeletes().orElseThrow().cardinality()).isEqualTo(DELETES_PER_DATA_FILE);
        assertThat(readAllPositionDeletes()).hasSize(DATA_FILE_COUNT);
    }

    @TearDown
    public void tearDown()
            throws IOException
    {
        MoreFiles.deleteRecursively(deleteFileDirectory, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    /**
     * Reads the deletes of one data file, as a split of that data file does.
     */
    @Benchmark
    public Optional<DeletionVector> readPositionDeletes()
    {
        return getFutureValue(PositionDeleteReader.readPositionDeletes(
                TARGET_DATA_FILE_PATH,
                ImmutableList.of(deleteFile),
                OptionalLong.empty(),
                OptionalLong.empty(),
                deleteReads,
                TESTING_TYPE_MANAGER,
                directExecutor(),
                directExecutor(),
                MemoryContext.NO_LIMIT));
    }

    /**
     * Reads the deletes of every data file, as merging them into deletion vectors does.
     */
    @Benchmark
    public Map<String, DeletionVector.Builder> readAllPositionDeletes()
            throws IOException
    {
        Map<String, DeletionVector.Builder> deletionVectors = new HashMap<>();
        try (ConnectorPageSource pageSource = deleteReads.openDeletes(deleteFile, deleteColumns, TupleDomain.all())) {
            PositionDeleteReader.readMultiFilePositionDeletes(pageSource, (path, position) ->
                    deletionVectors.computeIfAbsent(path, _ -> DeletionVector.builder()).add(position));
        }
        return deletionVectors;
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkPositionDeleteReader.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("--add-modules=jdk.incubator.vector"))
                .run();
    }
}
