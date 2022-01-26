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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.deltalake.transactionlog.checkpoint.LastCheckpoint;
import io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_BAD_DATA;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.readLastCheckpoint;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.ADD;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.METADATA;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * The current state of a Delta table.  It's defined by its latest checkpoint and the subsequent transactions
 * not included in the checkpoint.
 */
public class TableSnapshot
{
    private final Optional<LastCheckpoint> lastCheckpoint;
    private final SchemaTableName table;
    private final TransactionLogTail logTail;
    private final Path tableLocation;
    private final ParquetReaderOptions parquetReaderOptions;
    private final boolean checkpointRowStatisticsWritingEnabled;

    private Optional<MetadataEntry> cachedMetadata = Optional.empty();

    private TableSnapshot(
            SchemaTableName table,
            Optional<LastCheckpoint> lastCheckpoint,
            TransactionLogTail logTail,
            Path tableLocation,
            ParquetReaderOptions parquetReaderOptions,
            boolean checkpointRowStatisticsWritingEnabled)
    {
        this.table = requireNonNull(table, "table is null");
        this.lastCheckpoint = requireNonNull(lastCheckpoint, "lastCheckpoint is null");
        this.logTail = requireNonNull(logTail, "logTail is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.parquetReaderOptions = requireNonNull(parquetReaderOptions, "parquetReaderOptions is null");
        this.checkpointRowStatisticsWritingEnabled = checkpointRowStatisticsWritingEnabled;
    }

    public static TableSnapshot load(
            SchemaTableName table,
            FileSystem fileSystem,
            Path tableLocation,
            ParquetReaderOptions parquetReaderOptions,
            boolean checkpointRowStatisticsWritingEnabled)
            throws IOException
    {
        Optional<LastCheckpoint> lastCheckpoint = readLastCheckpoint(fileSystem, tableLocation);
        Optional<Long> lastCheckpointVersion = lastCheckpoint.map(LastCheckpoint::getVersion);
        TransactionLogTail transactionLogTail = TransactionLogTail.loadNewTail(fileSystem, tableLocation, lastCheckpointVersion);

        return new TableSnapshot(
                table,
                lastCheckpoint,
                transactionLogTail,
                tableLocation,
                parquetReaderOptions,
                checkpointRowStatisticsWritingEnabled);
    }

    public Optional<TableSnapshot> getUpdatedSnapshot(FileSystem fileSystem)
            throws IOException
    {
        Optional<LastCheckpoint> lastCheckpoint = readLastCheckpoint(fileSystem, tableLocation);
        long lastCheckpointVersion = lastCheckpoint.map(LastCheckpoint::getVersion).orElse(0L);
        long cachedLastCheckpointVersion = getLastCheckpointVersion().orElse(0L);

        Optional<TransactionLogTail> updatedLogTail;
        if (cachedLastCheckpointVersion == lastCheckpointVersion) {
            updatedLogTail = logTail.getUpdatedTail(fileSystem, tableLocation);
        }
        else {
            updatedLogTail = Optional.of(TransactionLogTail.loadNewTail(fileSystem, tableLocation, Optional.of(lastCheckpointVersion)));
        }

        return updatedLogTail.map(transactionLogTail -> new TableSnapshot(
                table,
                lastCheckpoint,
                transactionLogTail,
                tableLocation,
                parquetReaderOptions,
                checkpointRowStatisticsWritingEnabled));
    }

    public long getVersion()
    {
        return logTail.getVersion();
    }

    public SchemaTableName getTable()
    {
        return table;
    }

    public Optional<MetadataEntry> getCachedMetadata()
    {
        return cachedMetadata;
    }

    public Path getTableLocation()
    {
        return tableLocation;
    }

    public void setCachedMetadata(Optional<MetadataEntry> cachedMetadata)
    {
        this.cachedMetadata = cachedMetadata;
    }

    public List<DeltaLakeTransactionLogEntry> getJsonTransactionLogEntries()
    {
        return logTail.getFileEntries();
    }

    public Stream<DeltaLakeTransactionLogEntry> getCheckpointTransactionLogEntries(
            ConnectorSession session,
            Set<CheckpointEntryIterator.EntryType> entryTypes,
            CheckpointSchemaManager checkpointSchemaManager,
            TypeManager typeManager,
            FileSystem fileSystem,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats)
            throws IOException
    {
        if (lastCheckpoint.isEmpty()) {
            return Stream.empty();
        }

        LastCheckpoint checkpoint = lastCheckpoint.get();
        // Add entries contain statistics. When struct statistics are used the format of the Parquet file depends on the schema. It is important to use the schema at the time
        // of the Checkpoint creation, in case the schema has evolved since it was written.
        Optional<MetadataEntry> metadataEntry = entryTypes.contains(ADD) ?
                Optional.of(getCheckpointMetadataEntry(
                        session,
                        checkpointSchemaManager,
                        typeManager,
                        fileSystem,
                        hdfsEnvironment,
                        stats,
                        checkpoint)) :
                Optional.empty();

        Stream<DeltaLakeTransactionLogEntry> resultStream = Stream.empty();
        for (Path checkpointPath : getCheckpointPartPaths(checkpoint)) {
            resultStream = Stream.concat(
                    resultStream,
                    getCheckpointTransactionLogEntries(
                            session,
                            entryTypes,
                            metadataEntry,
                            checkpointSchemaManager,
                            typeManager,
                            fileSystem,
                            hdfsEnvironment,
                            stats,
                            checkpoint,
                            checkpointPath));
        }
        return resultStream;
    }

    public Optional<Long> getLastCheckpointVersion()
    {
        return lastCheckpoint.map(LastCheckpoint::getVersion);
    }

    private Stream<DeltaLakeTransactionLogEntry> getCheckpointTransactionLogEntries(
            ConnectorSession session,
            Set<CheckpointEntryIterator.EntryType> entryTypes,
            Optional<MetadataEntry> metadataEntry,
            CheckpointSchemaManager checkpointSchemaManager,
            TypeManager typeManager,
            FileSystem fileSystem,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            LastCheckpoint checkpoint,
            Path checkpointPath)
            throws IOException
    {
        FileStatus fileStatus;
        try {
            fileStatus = fileSystem.getFileStatus(checkpointPath);
        }
        catch (FileNotFoundException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("%s mentions a non-existent checkpoint file for table: %s", checkpoint, table));
        }
        Iterator<DeltaLakeTransactionLogEntry> checkpointEntryIterator = new CheckpointEntryIterator(
                checkpointPath,
                session,
                fileStatus.getLen(),
                checkpointSchemaManager,
                typeManager,
                entryTypes,
                metadataEntry,
                hdfsEnvironment,
                stats,
                parquetReaderOptions,
                checkpointRowStatisticsWritingEnabled);
        return stream(checkpointEntryIterator);
    }

    private MetadataEntry getCheckpointMetadataEntry(
            ConnectorSession session,
            CheckpointSchemaManager checkpointSchemaManager,
            TypeManager typeManager,
            FileSystem fileSystem,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            LastCheckpoint checkpoint)
            throws IOException
    {
        for (Path checkpointPath : getCheckpointPartPaths(checkpoint)) {
            Stream<DeltaLakeTransactionLogEntry> metadataEntries = getCheckpointTransactionLogEntries(
                    session,
                    ImmutableSet.of(METADATA),
                    Optional.empty(),
                    checkpointSchemaManager,
                    typeManager,
                    fileSystem,
                    hdfsEnvironment,
                    stats,
                    checkpoint,
                    checkpointPath);
            Optional<DeltaLakeTransactionLogEntry> metadataEntry = metadataEntries.findFirst();
            if (metadataEntry.isPresent()) {
                return metadataEntry.get().getMetaData();
            }
        }
        throw new TrinoException(DELTA_LAKE_BAD_DATA, "Checkpoint found without metadata entry: " + checkpoint);
    }

    private List<Path> getCheckpointPartPaths(LastCheckpoint checkpoint)
    {
        Path transactionLogDir = getTransactionLogDir(tableLocation);
        ImmutableList.Builder<Path> paths = ImmutableList.builder();
        if (checkpoint.getParts().isEmpty()) {
            paths.add(new Path(transactionLogDir, format("%020d.checkpoint.parquet", checkpoint.getVersion())));
        }
        else {
            int partsCount = checkpoint.getParts().get();
            for (int i = 1; i <= partsCount; i++) {
                paths.add(new Path(transactionLogDir, format("%020d.checkpoint.%010d.%010d.parquet", checkpoint.getVersion(), i, partsCount)));
            }
        }
        return paths.build();
    }
}
