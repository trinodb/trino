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
import io.airlift.units.DataSize;
import io.trino.filesystem.ForwardingTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.TrinoOutputStream;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.deltalake.DefaultDeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.DeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.NoOpTableCredentialsProvider;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.reader.FileSystemTransactionLogReader;
import io.trino.plugin.deltalake.transactionlog.reader.FileSystemTransactionLogReaderFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.hash.Hashing.sha256;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.deltalake.DeltaLakeConfig.DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.readLastCheckpoint;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCheckpointWriterManager
{
    private static final String SCHEMA_STRING = "{\"type\":\"struct\",\"fields\":[{\"name\":\"col\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}";
    private static final MetadataEntry METADATA_ENTRY = new MetadataEntry(
            "id",
            "name",
            "description",
            new MetadataEntry.Format("parquet", ImmutableMap.of()),
            SCHEMA_STRING,
            ImmutableList.of(),
            ImmutableMap.of(),
            1000);
    private static final ProtocolEntry PROTOCOL_ENTRY = new ProtocolEntry(1, 2, Optional.empty(), Optional.empty());
    private static final String TAIL_ADD_JSON = "{\"add\":{\"path\":\"tail.parquet\",\"partitionValues\":{},\"size\":1000,\"modificationTime\":1001,\"dataChange\":true}}\n";
    private static final LastCheckpoint PREVIOUS_LAST_CHECKPOINT = new LastCheckpoint(0, 3, Optional.empty(), Optional.empty());

    private final CheckpointSchemaManager checkpointSchemaManager = new CheckpointSchemaManager(TESTING_TYPE_MANAGER);
    private final TrinoFileSystem fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(SESSION);

    @Test
    public void testPreviousCheckpointEntriesAreCopiedIncrementally(@TempDir Path tableDirectory)
            throws Exception
    {
        String tableLocation = tableDirectory.toUri().toString();
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        OptionalDouble[] bytesReadBeforeFirstWrite = new OptionalDouble[] {OptionalDouble.empty()};
        DeltaLakeFileSystemFactory fileSystemFactory = interceptingOutputStreams((outputFile, memoryContext) -> new WriteListeningOutputStream(outputFile.create(memoryContext), () -> {
            if (bytesReadBeforeFirstWrite[0].isEmpty()) {
                bytesReadBeforeFirstWrite[0] = OptionalDouble.of(stats.getReadBytes().getAllTime().getTotal());
            }
        }));
        Location transactionLogDirectory = Location.of(tableLocation).appendPath("_delta_log");

        // Long incompressible paths keep the row group data far larger than the file footer and fill the writer page buffer early
        Set<AddFileEntry> previousCheckpointAdds = IntStream.range(0, 2000)
                .mapToObj(index -> addFileEntry(IntStream.range(0, 32)
                        .mapToObj(part -> sha256().hashLong(index * 32L + part).toString())
                        .collect(joining("", "", ".parquet"))))
                .collect(toImmutableSet());
        ParquetWriterOptions smallRowGroups = ParquetWriterOptions.builder().setMaxRowGroupRowCount(200).build();
        new CheckpointWriter(TESTING_TYPE_MANAGER, checkpointSchemaManager, "test", smallRowGroups).write(
                new CheckpointEntries(METADATA_ENTRY, PROTOCOL_ENTRY, ImmutableSet.of(), previousCheckpointAdds, ImmutableSet.of()),
                fileSystem.newOutputFile(transactionLogDirectory.appendPath("00000000000000000000.checkpoint.parquet")));
        long previousCheckpointLength = fileSystem.newInputFile(transactionLogDirectory.appendPath("00000000000000000000.checkpoint.parquet")).length();
        fileSystem.newOutputFile(transactionLogDirectory.appendPath("_last_checkpoint"))
                .createOrOverwrite(jsonCodec(LastCheckpoint.class).toJsonBytes(new LastCheckpoint(0, previousCheckpointAdds.size() + 2, Optional.empty(), Optional.empty())));
        Files.writeString(tableDirectory.resolve("_delta_log/00000000000000000001.json"), TAIL_ADD_JSON);

        // No small file buffering or range merging, so each row group is read only when its entries are requested
        ParquetReaderOptions parquetReaderOptions = ParquetReaderOptions.builder()
                .withSmallFileThreshold(DataSize.ofBytes(0))
                .withMaxMergeDistance(DataSize.ofBytes(0))
                .withMaxBufferSize(DataSize.of(4, KILOBYTE))
                .build();
        TableSnapshot snapshot = loadSnapshot(tableLocation, fileSystemFactory, parquetReaderOptions);
        CheckpointWriterManager checkpointWriterManager = checkpointWriterManager(fileSystemFactory, stats, smallRowGroups);

        checkpointWriterManager.writeCheckpoint(SESSION, snapshot, Optional.empty());

        assertThat(bytesReadBeforeFirstWrite[0]).isPresent();
        assertThat(bytesReadBeforeFirstWrite[0].orElseThrow()).isLessThan(previousCheckpointLength / 2.0);
        assertThat(readLastCheckpoint(fileSystem, tableLocation))
                .contains(new LastCheckpoint(1, previousCheckpointAdds.size() + 3, Optional.empty(), Optional.empty()));
        assertThat(fileSystem.newInputFile(transactionLogDirectory.appendPath("00000000000000000001.checkpoint.parquet")).exists()).isTrue();
    }

    @Test
    public void testFailedWriteCreatesNoCheckpoint(@TempDir Path tableDirectory)
            throws Exception
    {
        String tableLocation = tableDirectory.toUri().toString();
        Location transactionLogDirectory = Location.of(tableLocation).appendPath("_delta_log");
        DeltaLakeFileSystemFactory fileSystemFactory = writeTableRemovingCheckpointOnWrite(tableDirectory, TrinoOutputFile::create);
        TableSnapshot snapshot = loadSnapshot(tableLocation, fileSystemFactory, ParquetReaderOptions.builder().build());
        CheckpointWriterManager checkpointWriterManager = checkpointWriterManager(fileSystemFactory, new FileFormatDataSourceStats(), ParquetWriterOptions.builder().build());

        assertThatThrownBy(() -> checkpointWriterManager.writeCheckpoint(SESSION, snapshot, Optional.empty()))
                .hasMessageContaining("mentions a non-existent checkpoint file");

        assertThat(fileSystem.newInputFile(transactionLogDirectory.appendPath("00000000000000000001.checkpoint.parquet")).exists()).isFalse();
        assertThat(readLastCheckpoint(fileSystem, tableLocation)).contains(PREVIOUS_LAST_CHECKPOINT);
    }

    @Test
    public void testFailedWriteKeepsCheckpointOfOtherWriter(@TempDir Path tableDirectory)
            throws Exception
    {
        String tableLocation = tableDirectory.toUri().toString();
        Location transactionLogDirectory = Location.of(tableLocation).appendPath("_delta_log");
        DeltaLakeFileSystemFactory fileSystemFactory = writeTableRemovingCheckpointOnWrite(tableDirectory, (outputFile, _) -> new OverwriteOnCloseOutputStream(outputFile));
        TableSnapshot snapshot = loadSnapshot(tableLocation, fileSystemFactory, ParquetReaderOptions.builder().build());
        // Another writer publishes the checkpoint for version 1 after the snapshot is loaded
        Files.copy(tableDirectory.resolve("_delta_log/00000000000000000000.checkpoint.parquet"), tableDirectory.resolve("_delta_log/00000000000000000001.checkpoint.parquet"));
        byte[] otherWriterCheckpoint = Files.readAllBytes(tableDirectory.resolve("_delta_log/00000000000000000001.checkpoint.parquet"));
        LastCheckpoint otherWriterLastCheckpoint = new LastCheckpoint(1, 3, Optional.empty(), Optional.empty());
        fileSystem.newOutputFile(transactionLogDirectory.appendPath("_last_checkpoint"))
                .createOrOverwrite(jsonCodec(LastCheckpoint.class).toJsonBytes(otherWriterLastCheckpoint));
        CheckpointWriterManager checkpointWriterManager = checkpointWriterManager(fileSystemFactory, new FileFormatDataSourceStats(), ParquetWriterOptions.builder().build());

        assertThatThrownBy(() -> checkpointWriterManager.writeCheckpoint(SESSION, snapshot, Optional.empty()))
                .hasMessageContaining("mentions a non-existent checkpoint file");

        assertThat(tableDirectory.resolve("_delta_log/00000000000000000001.checkpoint.parquet")).hasBinaryContent(otherWriterCheckpoint);
        assertThat(readLastCheckpoint(fileSystem, tableLocation)).contains(otherWriterLastCheckpoint);
    }

    /**
     * Writes a checkpoint for version 0 and a commit for version 1. The returned factory deletes that checkpoint when the checkpoint for version 1 is opened,
     * so copying its entries fails.
     */
    private DeltaLakeFileSystemFactory writeTableRemovingCheckpointOnWrite(Path tableDirectory, OutputStreamFactory outputStreamFactory)
            throws IOException
    {
        Location transactionLogDirectory = Location.of(tableDirectory.toUri().toString()).appendPath("_delta_log");
        Location previousCheckpoint = transactionLogDirectory.appendPath("00000000000000000000.checkpoint.parquet");
        new CheckpointWriter(TESTING_TYPE_MANAGER, checkpointSchemaManager, "test", ParquetWriterOptions.builder().build()).write(
                new CheckpointEntries(METADATA_ENTRY, PROTOCOL_ENTRY, ImmutableSet.of(), ImmutableSet.of(addFileEntry("previous.parquet")), ImmutableSet.of()),
                fileSystem.newOutputFile(previousCheckpoint));
        fileSystem.newOutputFile(transactionLogDirectory.appendPath("_last_checkpoint"))
                .createOrOverwrite(jsonCodec(LastCheckpoint.class).toJsonBytes(PREVIOUS_LAST_CHECKPOINT));
        Files.writeString(tableDirectory.resolve("_delta_log/00000000000000000001.json"), TAIL_ADD_JSON);
        return interceptingOutputStreams((outputFile, memoryContext) -> {
            fileSystem.deleteFile(previousCheckpoint);
            return outputStreamFactory.create(outputFile, memoryContext);
        });
    }

    private TableSnapshot loadSnapshot(String tableLocation, DeltaLakeFileSystemFactory fileSystemFactory, ParquetReaderOptions parquetReaderOptions)
            throws IOException
    {
        return TableSnapshot.load(
                SESSION,
                new FileSystemTransactionLogReader(tableLocation, Optional.empty(), fileSystemFactory),
                new SchemaTableName("schema", "table"),
                readLastCheckpoint(fileSystem, tableLocation),
                tableLocation,
                parquetReaderOptions,
                true,
                new DeltaLakeConfig().getDomainCompactionThreshold(),
                DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE,
                Optional.empty());
    }

    private CheckpointWriterManager checkpointWriterManager(DeltaLakeFileSystemFactory fileSystemFactory, FileFormatDataSourceStats stats, ParquetWriterOptions parquetWriterOptions)
    {
        TransactionLogAccess transactionLogAccess = new TransactionLogAccess(
                TESTING_TYPE_MANAGER,
                checkpointSchemaManager,
                new DeltaLakeConfig(),
                stats,
                fileSystemFactory,
                new ParquetReaderConfig(),
                newDirectExecutorService(),
                new FileSystemTransactionLogReaderFactory(fileSystemFactory));
        return new CheckpointWriterManager(
                TESTING_TYPE_MANAGER,
                checkpointSchemaManager,
                fileSystemFactory,
                new CheckpointWriter(TESTING_TYPE_MANAGER, checkpointSchemaManager, "test", parquetWriterOptions),
                transactionLogAccess,
                stats,
                jsonCodec(LastCheckpoint.class),
                new DeltaLakeConfig(),
                newDirectExecutorService());
    }

    private static DeltaLakeFileSystemFactory interceptingOutputStreams(OutputStreamFactory outputStreamFactory)
    {
        TrinoFileSystem interceptingFileSystem = new InterceptingFileSystem(HDFS_FILE_SYSTEM_FACTORY.create(SESSION), outputStreamFactory);
        return new DefaultDeltaLakeFileSystemFactory(_ -> interceptingFileSystem, new NoOpTableCredentialsProvider());
    }

    private static AddFileEntry addFileEntry(String path)
    {
        return new AddFileEntry(
                path,
                ImmutableMap.of(),
                1000,
                1001,
                true,
                Optional.of("{\"numRecords\":1,\"minValues\":{\"col\":1},\"maxValues\":{\"col\":1},\"nullCount\":{\"col\":0}}"),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());
    }

    private interface OutputStreamFactory
    {
        TrinoOutputStream create(TrinoOutputFile outputFile, AggregatedMemoryContext memoryContext)
                throws IOException;
    }

    private static class InterceptingFileSystem
            extends ForwardingTrinoFileSystem
    {
        private final OutputStreamFactory outputStreamFactory;

        InterceptingFileSystem(TrinoFileSystem delegate, OutputStreamFactory outputStreamFactory)
        {
            super(delegate);
            this.outputStreamFactory = requireNonNull(outputStreamFactory, "outputStreamFactory is null");
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            return new InterceptingOutputFile(super.newOutputFile(location), outputStreamFactory);
        }
    }

    private record InterceptingOutputFile(TrinoOutputFile delegate, OutputStreamFactory outputStreamFactory)
            implements TrinoOutputFile
    {
        @Override
        public TrinoOutputStream create(AggregatedMemoryContext memoryContext)
                throws IOException
        {
            return outputStreamFactory.create(delegate, memoryContext);
        }

        @Override
        public void createOrOverwrite(byte[] data)
                throws IOException
        {
            delegate.createOrOverwrite(data);
        }

        @Override
        public Location location()
        {
            return delegate.location();
        }
    }

    private static class WriteListeningOutputStream
            extends TrinoOutputStream
    {
        private final TrinoOutputStream delegate;
        private final Runnable writeListener;

        WriteListeningOutputStream(TrinoOutputStream delegate, Runnable writeListener)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.writeListener = requireNonNull(writeListener, "writeListener is null");
        }

        @Override
        public void write(int value)
                throws IOException
        {
            writeListener.run();
            delegate.write(value);
        }

        @Override
        public void write(byte[] buffer, int offset, int length)
                throws IOException
        {
            writeListener.run();
            delegate.write(buffer, offset, length);
        }

        @Override
        public void flush()
                throws IOException
        {
            delegate.flush();
        }

        @Override
        public void close()
                throws IOException
        {
            delegate.close();
        }

        @Override
        public void abort()
                throws IOException
        {
            delegate.abort();
        }
    }

    // Buffers the data and creates or overwrites the file when closed, like an object store upload
    private static class OverwriteOnCloseOutputStream
            extends TrinoOutputStream
    {
        private final TrinoOutputFile outputFile;
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private boolean closed;

        OverwriteOnCloseOutputStream(TrinoOutputFile outputFile)
        {
            this.outputFile = requireNonNull(outputFile, "outputFile is null");
        }

        @Override
        public void write(int value)
        {
            buffer.write(value);
        }

        @Override
        public void write(byte[] bytes, int offset, int length)
        {
            buffer.write(bytes, offset, length);
        }

        @Override
        public void close()
                throws IOException
        {
            if (!closed) {
                closed = true;
                outputFile.createOrOverwrite(buffer.toByteArray());
            }
        }

        @Override
        public void abort()
        {
            closed = true;
        }
    }
}
