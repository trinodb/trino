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
import com.google.common.collect.Sets;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.deltalake.transactionlog.checkpoint.LastCheckpoint;
import io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.readLastCheckpoint;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.ADD;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.REMOVE;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.SIDECAR;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
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
            Optional<LastCheckpoint> lastCheckpoint,
            TrinoFileSystem fileSystem,
            String tableLocation,
            ParquetReaderOptions parquetReaderOptions,
            boolean checkpointRowStatisticsWritingEnabled,
            int domainCompactionThreshold,
            Optional<Long> endVersion)
            throws IOException
    {
        Optional<Long> lastCheckpointVersion = lastCheckpoint.map(LastCheckpoint::version);
        TransactionLogTail transactionLogTail = TransactionLogTail.loadNewTail(fileSystem, tableLocation, lastCheckpointVersion, endVersion);

        return new TableSnapshot(
                table,
                lastCheckpoint,
                transactionLogTail,
                tableLocation,
                parquetReaderOptions,
                checkpointRowStatisticsWritingEnabled,
                domainCompactionThreshold);
    }

    public Optional<TableSnapshot> getUpdatedSnapshot(TrinoFileSystem fileSystem, Optional<Long> toVersion)
            throws IOException
    {
        if (toVersion.isEmpty()) {
            // Load any newer table snapshot

            Optional<LastCheckpoint> lastCheckpoint = readLastCheckpoint(fileSystem, tableLocation);
            if (lastCheckpoint.isPresent()) {
                long ourCheckpointVersion = getLastCheckpointVersion().orElse(0L);
                if (ourCheckpointVersion != lastCheckpoint.get().version()) {
                    // There is a new checkpoint in the table, load anew
                    return Optional.of(TableSnapshot.load(
                            table,
                            lastCheckpoint,
                            fileSystem,
                            tableLocation,
                            parquetReaderOptions,
                            checkpointRowStatisticsWritingEnabled,
                            domainCompactionThreshold,
                            Optional.empty()));
                }
            }
        }

        Optional<TransactionLogTail> updatedLogTail = logTail.getUpdatedTail(fileSystem, tableLocation, toVersion);
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

    public List<Transaction> getTransactions()
    {
        return logTail.getTransactions();
    }

    public Stream<DeltaLakeTransactionLogEntry> getCheckpointTransactionLogEntries(
            ConnectorSession session,
            Set<CheckpointEntryIterator.EntryType> entryTypes,
            CheckpointSchemaManager checkpointSchemaManager,
            TypeManager typeManager,
            TrinoFileSystem fileSystem,
            FileFormatDataSourceStats stats,
            Optional<MetadataAndProtocolEntry> metadataAndProtocol,
            TupleDomain<DeltaLakeColumnHandle> partitionConstraint,
            Optional<Predicate<String>> addStatsMinMaxColumnFilter)
            throws IOException
    {
        if (lastCheckpoint.isEmpty()) {
            return Stream.empty();
        }

        LastCheckpoint checkpoint = lastCheckpoint.get();
        // Add entries contain statistics. When struct statistics are used the format of the Parquet file depends on the schema. It is important to use the schema at the time
        // of the Checkpoint creation, in case the schema has evolved since it was written.
        if (entryTypes.contains(ADD)) {
            checkState(metadataAndProtocol.isPresent(), "metadata and protocol information is needed to process the add log entries");
        }

        return getCheckpointPartPaths(checkpoint).stream()
                .map(fileSystem::newInputFile)
                .flatMap(checkpointFile -> getCheckpointTransactionLogEntries(
                        session,
                        fileSystem,
                        entryTypes,
                        metadataAndProtocol.map(MetadataAndProtocolEntry::metadataEntry),
                        metadataAndProtocol.map(MetadataAndProtocolEntry::protocolEntry),
                        checkpointSchemaManager,
                        typeManager,
                        stats,
                        checkpoint,
                        checkpointFile,
                        partitionConstraint,
                        addStatsMinMaxColumnFilter));
    }

    public Optional<Long> getLastCheckpointVersion()
    {
        return lastCheckpoint.map(LastCheckpoint::version);
    }

    private Stream<DeltaLakeTransactionLogEntry> getCheckpointTransactionLogEntries(
            ConnectorSession session,
            TrinoFileSystem fileSystem,
            Set<CheckpointEntryIterator.EntryType> entryTypes,
            Optional<MetadataEntry> metadataEntry,
            Optional<ProtocolEntry> protocolEntry,
            CheckpointSchemaManager checkpointSchemaManager,
            TypeManager typeManager,
            FileFormatDataSourceStats stats,
            LastCheckpoint checkpoint,
            TrinoInputFile checkpointFile,
            TupleDomain<DeltaLakeColumnHandle> partitionConstraint,
            Optional<Predicate<String>> addStatsMinMaxColumnFilter)
    {
        long fileSize;
        try {
            fileSize = checkpointFile.length();
        }
        catch (FileNotFoundException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("%s mentions a non-existent checkpoint file for table: %s", checkpoint, table));
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, format("Unexpected IO exception occurred while retrieving the length of the file: %s for the table %s", checkpoint, table), e);
        }
        if (checkpoint.v2Checkpoint().isPresent()) {
            return getV2CheckpointTransactionLogEntriesFrom(
                    session,
                    entryTypes,
                    metadataEntry,
                    protocolEntry,
                    checkpointSchemaManager,
                    typeManager,
                    stats,
                    checkpoint,
                    checkpointFile,
                    partitionConstraint,
                    addStatsMinMaxColumnFilter,
                    fileSystem,
                    fileSize);
        }
        CheckpointEntryIterator checkpointEntryIterator = new CheckpointEntryIterator(
                checkpointFile,
                session,
                fileSize,
                checkpointSchemaManager,
                typeManager,
                entryTypes,
                metadataEntry,
                protocolEntry,
                stats,
                parquetReaderOptions,
                checkpointRowStatisticsWritingEnabled,
                domainCompactionThreshold,
                partitionConstraint,
                addStatsMinMaxColumnFilter);
        return stream(checkpointEntryIterator).onClose(checkpointEntryIterator::close);
    }

    private Stream<DeltaLakeTransactionLogEntry> getV2CheckpointTransactionLogEntriesFrom(
            ConnectorSession session,
            Set<CheckpointEntryIterator.EntryType> entryTypes,
            Optional<MetadataEntry> metadataEntry,
            Optional<ProtocolEntry> protocolEntry,
            CheckpointSchemaManager checkpointSchemaManager,
            TypeManager typeManager,
            FileFormatDataSourceStats stats,
            LastCheckpoint checkpoint,
            TrinoInputFile checkpointFile,
            TupleDomain<DeltaLakeColumnHandle> partitionConstraint,
            Optional<Predicate<String>> addStatsMinMaxColumnFilter,
            TrinoFileSystem fileSystem,
            long fileSize)
    {
        return getV2CheckpointEntries(session, entryTypes, metadataEntry, protocolEntry, checkpointSchemaManager, typeManager, stats, checkpoint, checkpointFile, partitionConstraint, addStatsMinMaxColumnFilter, fileSystem, fileSize)
                .mapMulti((entry, builder) -> {
                    // Sidecar files contain only ADD and REMOVE entry types. https://github.com/delta-io/delta/blob/master/PROTOCOL.md#v2-spec
                    Set<CheckpointEntryIterator.EntryType> dataEntryTypes = Sets.intersection(entryTypes, Set.of(ADD, REMOVE));
                    if (entry.getSidecar() == null || dataEntryTypes.isEmpty()) {
                        builder.accept(entry);
                        return;
                    }
                    Location sidecar = checkpointFile.location().sibling("_sidecars").appendPath(entry.getSidecar().path());
                    CheckpointEntryIterator iterator = new CheckpointEntryIterator(
                            fileSystem.newInputFile(sidecar),
                            session,
                            fileSize,
                            checkpointSchemaManager,
                            typeManager,
                            dataEntryTypes,
                            metadataEntry,
                            protocolEntry,
                            stats,
                            parquetReaderOptions,
                            checkpointRowStatisticsWritingEnabled,
                            domainCompactionThreshold,
                            partitionConstraint,
                            addStatsMinMaxColumnFilter);
                    stream(iterator).onClose(iterator::close).forEach(builder);
                });
    }

    private Stream<DeltaLakeTransactionLogEntry> getV2CheckpointEntries(
            ConnectorSession session,
            Set<CheckpointEntryIterator.EntryType> entryTypes,
            Optional<MetadataEntry> metadataEntry,
            Optional<ProtocolEntry> protocolEntry,
            CheckpointSchemaManager checkpointSchemaManager,
            TypeManager typeManager,
            FileFormatDataSourceStats stats,
            LastCheckpoint checkpoint,
            TrinoInputFile checkpointFile,
            TupleDomain<DeltaLakeColumnHandle> partitionConstraint,
            Optional<Predicate<String>> addStatsMinMaxColumnFilter,
            TrinoFileSystem fileSystem,
            long fileSize)
    {
        if (checkpointFile.location().fileName().endsWith(".json")) {
            try {
                return getEntriesFromJson(checkpoint.version(), checkpointFile).stream().flatMap(List::stream);
            }
            catch (IOException e) {
                throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, format("Unexpected IO exception occurred while reading the entries of the file: %s for the table %s", checkpoint, table), e);
            }
        }
        if (checkpointFile.location().fileName().endsWith(".parquet")) {
            CheckpointEntryIterator checkpointEntryIterator = new CheckpointEntryIterator(
                    fileSystem.newInputFile(checkpointFile.location()),
                    session,
                    fileSize,
                    checkpointSchemaManager,
                    typeManager,
                    ImmutableSet.<CheckpointEntryIterator.EntryType>builder()
                            .addAll(entryTypes)
                            .add(SIDECAR)
                            .build(),
                    metadataEntry,
                    protocolEntry,
                    stats,
                    parquetReaderOptions,
                    checkpointRowStatisticsWritingEnabled,
                    domainCompactionThreshold,
                    partitionConstraint,
                    addStatsMinMaxColumnFilter);
            return stream(checkpointEntryIterator)
                    .onClose(checkpointEntryIterator::close);
        }
        throw new IllegalArgumentException("Unsupported v2 checkpoint file format: " + checkpointFile.location());
    }

    public record MetadataAndProtocolEntry(MetadataEntry metadataEntry, ProtocolEntry protocolEntry)
    {
        public MetadataAndProtocolEntry
        {
            requireNonNull(metadataEntry, "metadataEntry is null");
            requireNonNull(protocolEntry, "protocolEntry is null");
        }
    }

    private List<Location> getCheckpointPartPaths(LastCheckpoint checkpoint)
    {
        Location transactionLogDir = Location.of(getTransactionLogDir(tableLocation));
        ImmutableList.Builder<Location> paths = ImmutableList.builder();
        if (checkpoint.v2Checkpoint().isPresent()) {
            verify(checkpoint.parts().isEmpty(), "v2 checkpoint should not have multi-part checkpoints");
            paths.add(transactionLogDir.appendPath(checkpoint.v2Checkpoint().get().path()));
        }
        else if (checkpoint.parts().isEmpty()) {
            paths.add(transactionLogDir.appendPath("%020d.checkpoint.parquet".formatted(checkpoint.version())));
        }
        else {
            int partsCount = checkpoint.parts().get();
            for (int i = 1; i <= partsCount; i++) {
                paths.add(transactionLogDir.appendPath("%020d.checkpoint.%010d.%010d.parquet".formatted(checkpoint.version(), i, partsCount)));
            }
        }
        return paths.build();
    }
}
