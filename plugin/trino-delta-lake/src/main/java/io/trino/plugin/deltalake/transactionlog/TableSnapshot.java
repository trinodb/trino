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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.deltalake.transactionlog.checkpoint.LastCheckpoint;
import io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;

import java.io.FileNotFoundException;
import java.io.IOException;
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
    private final String tableLocation;
    private final ParquetReaderOptions parquetReaderOptions;
    private final boolean checkpointRowStatisticsWritingEnabled;
    private final int domainCompactionThreshold;

    private Optional<MetadataEntry> cachedMetadata = Optional.empty();

    private TableSnapshot(
            SchemaTableName table,
            Optional<LastCheckpoint> lastCheckpoint,
            TransactionLogTail logTail,
            String tableLocation,
            ParquetReaderOptions parquetReaderOptions,
            boolean checkpointRowStatisticsWritingEnabled,
            int domainCompactionThreshold)
    {
        this.table = requireNonNull(table, "table is null");
        this.lastCheckpoint = requireNonNull(lastCheckpoint, "lastCheckpoint is null");
        this.logTail = requireNonNull(logTail, "logTail is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.parquetReaderOptions = requireNonNull(parquetReaderOptions, "parquetReaderOptions is null");
        this.checkpointRowStatisticsWritingEnabled = checkpointRowStatisticsWritingEnabled;
        this.domainCompactionThreshold = domainCompactionThreshold;
    }

    public static TableSnapshot load(
            SchemaTableName table,
            TrinoFileSystem fileSystem,
            String tableLocation,
            ParquetReaderOptions parquetReaderOptions,
            boolean checkpointRowStatisticsWritingEnabled,
            int domainCompactionThreshold)
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
                checkpointRowStatisticsWritingEnabled,
                domainCompactionThreshold);
    }

    public Optional<TableSnapshot> getUpdatedSnapshot(TrinoFileSystem fileSystem)
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
                checkpointRowStatisticsWritingEnabled,
                domainCompactionThreshold));
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

    public String getTableLocation()
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
            TrinoFileSystem fileSystem,
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
                        stats,
                        checkpoint)) :
                Optional.empty();

        Stream<DeltaLakeTransactionLogEntry> resultStream = Stream.empty();
        for (Location checkpointPath : getCheckpointPartPaths(checkpoint)) {
            TrinoInputFile checkpointFile = fileSystem.newInputFile(checkpointPath);
            resultStream = Stream.concat(
                    resultStream,
                    getCheckpointTransactionLogEntries(
                            session,
                            entryTypes,
                            metadataEntry,
                            checkpointSchemaManager,
                            typeManager,
                            stats,
                            checkpoint,
                            checkpointFile));
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
            FileFormatDataSourceStats stats,
            LastCheckpoint checkpoint,
            TrinoInputFile checkpointFile)
            throws IOException
    {
        long fileSize;
        try {
            fileSize = checkpointFile.length();
        }
        catch (FileNotFoundException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("%s mentions a non-existent checkpoint file for table: %s", checkpoint, table));
        }
        return stream(new CheckpointEntryIterator(
                checkpointFile,
                session,
                fileSize,
                checkpointSchemaManager,
                typeManager,
                entryTypes,
                metadataEntry,
                stats,
                parquetReaderOptions,
                checkpointRowStatisticsWritingEnabled,
                domainCompactionThreshold));
    }

    private MetadataEntry getCheckpointMetadataEntry(
            ConnectorSession session,
            CheckpointSchemaManager checkpointSchemaManager,
            TypeManager typeManager,
            TrinoFileSystem fileSystem,
            FileFormatDataSourceStats stats,
            LastCheckpoint checkpoint)
            throws IOException
    {
        for (Location checkpointPath : getCheckpointPartPaths(checkpoint)) {
            TrinoInputFile checkpointFile = fileSystem.newInputFile(checkpointPath);
            Stream<DeltaLakeTransactionLogEntry> metadataEntries = getCheckpointTransactionLogEntries(
                    session,
                    ImmutableSet.of(METADATA),
                    Optional.empty(),
                    checkpointSchemaManager,
                    typeManager,
                    stats,
                    checkpoint,
                    checkpointFile);
            Optional<DeltaLakeTransactionLogEntry> metadataEntry = metadataEntries.findFirst();
            if (metadataEntry.isPresent()) {
                return metadataEntry.get().getMetaData();
            }
        }
        throw new TrinoException(DELTA_LAKE_BAD_DATA, "Checkpoint found without metadata entry: " + checkpoint);
    }

    private List<Location> getCheckpointPartPaths(LastCheckpoint checkpoint)
    {
        Location transactionLogDir = Location.of(getTransactionLogDir(tableLocation));
        ImmutableList.Builder<Location> paths = ImmutableList.builder();
        if (checkpoint.getParts().isEmpty()) {
            paths.add(transactionLogDir.appendPath("%020d.checkpoint.parquet".formatted(checkpoint.getVersion())));
        }
        else {
            int partsCount = checkpoint.getParts().get();
            for (int i = 1; i <= partsCount; i++) {
                paths.add(transactionLogDir.appendPath("%020d.checkpoint.%010d.%010d.parquet".formatted(checkpoint.getVersion(), i, partsCount)));
            }
        }
        return paths.build();
    }
}
