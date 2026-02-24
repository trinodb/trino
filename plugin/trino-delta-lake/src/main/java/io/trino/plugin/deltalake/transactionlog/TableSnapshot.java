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
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.deltalake.transactionlog.checkpoint.LastCheckpoint;
import io.trino.plugin.deltalake.transactionlog.checkpoint.ParallelUnorderedIterator;
import io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail;
import io.trino.plugin.deltalake.transactionlog.reader.TransactionLogReader;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
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
import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 * The current state of a Delta table.  It's defined by its latest checkpoint and the subsequent transactions
 * not included in the checkpoint.
 */
public class TableSnapshot
{
    private static final int INSTANCE_SIZE = instanceSize(TableSnapshot.class);
    private static final int CHECKPOINT_FILE_PROCESSING_QUEUE_SIZE = 32_678;

    private final Optional<LastCheckpoint> lastCheckpoint;
    private final SchemaTableName table;
    private final TransactionLogTail logTail;
    private final String tableLocation;
    private final ParquetReaderOptions parquetReaderOptions;
    private final DataSize transactionLogMaxCachedFileSize;
    private final boolean checkpointRowStatisticsWritingEnabled;
    private final int domainCompactionThreshold;

    private Optional<MetadataEntry> cachedMetadata = Optional.empty();
    private Optional<ProtocolEntry> cachedProtocol = Optional.empty();

    private TableSnapshot(
            SchemaTableName table,
            Optional<LastCheckpoint> lastCheckpoint,
            TransactionLogTail logTail,
            String tableLocation,
            ParquetReaderOptions parquetReaderOptions,
            boolean checkpointRowStatisticsWritingEnabled,
            int domainCompactionThreshold,
            DataSize transactionLogMaxCachedFileSize)
    {
        this.table = requireNonNull(table, "table is null");
        this.lastCheckpoint = requireNonNull(lastCheckpoint, "lastCheckpoint is null");
        this.logTail = requireNonNull(logTail, "logTail is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.parquetReaderOptions = requireNonNull(parquetReaderOptions, "parquetReaderOptions is null");
        this.checkpointRowStatisticsWritingEnabled = checkpointRowStatisticsWritingEnabled;
        this.domainCompactionThreshold = domainCompactionThreshold;
        this.transactionLogMaxCachedFileSize = requireNonNull(transactionLogMaxCachedFileSize, "transactionLogMaxCachedFileSize is null");
    }

    public static TableSnapshot load(
            ConnectorSession session,
            TransactionLogReader transactionLogReader,
            SchemaTableName table,
            Optional<LastCheckpoint> lastCheckpoint,
            String tableLocation,
            ParquetReaderOptions parquetReaderOptions,
            boolean checkpointRowStatisticsWritingEnabled,
            int domainCompactionThreshold,
            DataSize transactionLogMaxCachedFileSize,
            Optional<Long> endVersion)
            throws IOException
    {
        Optional<Long> lastCheckpointVersion = lastCheckpoint.map(LastCheckpoint::version);
        TransactionLogTail transactionLogTail = transactionLogReader.loadNewTail(session, lastCheckpointVersion, endVersion, transactionLogMaxCachedFileSize);

        return new TableSnapshot(
                table,
                lastCheckpoint,
                transactionLogTail,
                tableLocation,
                parquetReaderOptions,
                checkpointRowStatisticsWritingEnabled,
                domainCompactionThreshold,
                transactionLogMaxCachedFileSize);
    }

    public Optional<TableSnapshot> getUpdatedSnapshot(ConnectorSession session, TransactionLogReader transactionLogReader, TrinoFileSystem fileSystem, Optional<Long> toVersion)
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
                            session,
                            transactionLogReader,
                            table,
                            lastCheckpoint,
                            tableLocation,
                            parquetReaderOptions,
                            checkpointRowStatisticsWritingEnabled,
                            domainCompactionThreshold,
                            transactionLogMaxCachedFileSize,
                            Optional.empty()));
                }
            }
        }

        Optional<TransactionLogTail> updatedLogTail = transactionLogReader.getUpdatedTail(session, logTail, toVersion, transactionLogMaxCachedFileSize);
        return updatedLogTail.map(transactionLogTail -> new TableSnapshot(
                table,
                lastCheckpoint,
                transactionLogTail,
                tableLocation,
                parquetReaderOptions,
                checkpointRowStatisticsWritingEnabled,
                domainCompactionThreshold,
                transactionLogMaxCachedFileSize));
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

    public Optional<ProtocolEntry> getCachedProtocol()
    {
        return cachedProtocol;
    }

    public String getTableLocation()
    {
        return tableLocation;
    }

    public void setCachedMetadata(Optional<MetadataEntry> cachedMetadata)
    {
        this.cachedMetadata = cachedMetadata;
    }

    public void setCachedProtocol(Optional<ProtocolEntry> cachedProtocol)
    {
        this.cachedProtocol = cachedProtocol;
    }

    public List<DeltaLakeTransactionLogEntry> getJsonTransactionLogEntries(TrinoFileSystem fileSystem)
    {
        return logTail.getFileEntries(fileSystem);
    }

    public List<Transaction> getTransactions()
    {
        return logTail.getTransactions();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + sizeOf(lastCheckpoint, LastCheckpoint::getRetainedSizeInBytes)
                + table.getRetainedSizeInBytes()
                + logTail.getRetainedSizeInBytes()
                + estimatedSizeOf(tableLocation)
                + sizeOf(cachedMetadata, MetadataEntry::getRetainedSizeInBytes)
                + sizeOf(cachedProtocol, ProtocolEntry::getRetainedSizeInBytes);
    }

    private record CheckpointFileSplit(
            TrinoInputFile checkpointFile,
            long start,
            long length,
            long fileSize)
    {
        private CheckpointFileSplit
        {
            requireNonNull(checkpointFile, "checkpointFile is null");
            checkArgument(start >= 0, "start must be non-negative");
            checkArgument(length >= 0, "length must be non-negative");
            checkArgument(fileSize >= 0, "fileSize must be non-negative");
            checkArgument(start <= fileSize, "start (%s) must be less than or equal to fileSize (%s)", start, fileSize);
            checkArgument(length <= fileSize - start, "length (%s) with start (%s) must fit within fileSize (%s)", length, start, fileSize);
        }
    }

    private Stream<CheckpointFileSplit> computeSplits(TrinoInputFile checkpointFile, long splitSize)
    {
        checkArgument(splitSize > 0, "splitSize must be positive");
        return computeSplits(checkpointFile, getCheckpointFileSize(checkpointFile), splitSize);
    }

    private Stream<CheckpointFileSplit> computeSplits(TrinoInputFile checkpointFile, long fileSize, long splitSize)
    {
        checkArgument(splitSize > 0, "splitSize must be positive");

        long splitCount;
        if (fileSize == 0) {
            splitCount = 1;
        }
        else {
            splitCount = (fileSize + splitSize - 1) / splitSize;
        }

        return LongStream.range(0, splitCount)
                .mapToObj(i -> {
                    long start = i * splitSize;
                    long length = Math.min(splitSize, fileSize - start);
                    return new CheckpointFileSplit(checkpointFile, start, length, fileSize);
                });
    }

    private CheckpointFileSplit computeSingletonSplit(TrinoInputFile checkpointFile)
    {
        long fileSize = getCheckpointFileSize(checkpointFile);
        return new CheckpointFileSplit(checkpointFile, 0, fileSize, fileSize);
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
            Optional<Predicate<String>> addStatsMinMaxColumnFilter,
            Executor checkpointProcessingExecutor,
            boolean checkpointV1ParallelProcessingEnabled,
            boolean checkpointV2ParallelProcessingEnabled,
            boolean checkpointIntraFileParallelProcessingEnabled,
            long checkpointIntraFileParallelProcessingSplitSize)
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

        List<TrinoInputFile> checkpointFiles = getCheckpointPartPaths(checkpoint).stream()
                .map(fileSystem::newInputFile)
                .collect(toImmutableList());

        Optional<MetadataEntry> metadataEntry = metadataAndProtocol.map(MetadataAndProtocolEntry::metadataEntry);
        Optional<ProtocolEntry> protocolEntry = metadataAndProtocol.map(MetadataAndProtocolEntry::protocolEntry);

        if (checkpoint.v2Checkpoint().isEmpty()) {
            return getV1CheckpointTransactionLogEntries(
                    session,
                    entryTypes,
                    checkpointSchemaManager,
                    typeManager,
                    stats,
                    metadataEntry,
                    protocolEntry,
                    partitionConstraint,
                    addStatsMinMaxColumnFilter,
                    checkpointFiles,
                    checkpointProcessingExecutor,
                    checkpointV1ParallelProcessingEnabled,
                    checkpointIntraFileParallelProcessingEnabled,
                    checkpointIntraFileParallelProcessingSplitSize);
        }

        return checkpointFiles
                .stream()
                .flatMap(checkpointFile -> getV2CheckpointTransactionLogEntriesFrom(
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
                        checkpointProcessingExecutor,
                        checkpointV2ParallelProcessingEnabled,
                        checkpointIntraFileParallelProcessingEnabled,
                        checkpointIntraFileParallelProcessingSplitSize));
    }

    private Stream<DeltaLakeTransactionLogEntry> getV1CheckpointTransactionLogEntries(
            ConnectorSession session,
            Set<CheckpointEntryIterator.EntryType> entryTypes,
            CheckpointSchemaManager checkpointSchemaManager,
            TypeManager typeManager,
            FileFormatDataSourceStats stats,
            Optional<MetadataEntry> metadataEntry,
            Optional<ProtocolEntry> protocolEntry,
            TupleDomain<DeltaLakeColumnHandle> partitionConstraint,
            Optional<Predicate<String>> addStatsMinMaxColumnFilter,
            List<TrinoInputFile> checkpointFiles,
            Executor checkpointProcessingExecutor,
            boolean checkpointV1ParallelProcessingEnabled,
            boolean checkpointIntraFileParallelProcessingEnabled,
            long checkpointIntraFileParallelProcessingSplitSize)
    {
        Function<CheckpointFileSplit, Supplier<Stream<DeltaLakeTransactionLogEntry>>> checkpointFileSplitStreamBuilder =
                checkpointFileSplit -> {
                    return () -> buildCheckpointFileSplitStream(
                            checkpointFileSplit,
                            session,
                            checkpointSchemaManager,
                            typeManager,
                            entryTypes,
                            metadataEntry,
                            protocolEntry,
                            stats,
                            partitionConstraint,
                            addStatsMinMaxColumnFilter);
                };

        boolean parallelizable = checkpointV1ParallelProcessingEnabled && (checkpointFiles.size() > 1 || checkpointIntraFileParallelProcessingEnabled);

        if (parallelizable) {
            Function<TrinoInputFile, Supplier<Stream<CheckpointFileSplit>>> checkpointFileSplitSupplierBuilder =
                    checkpointIntraFileParallelProcessingEnabled
                            ? checkpointFile -> () -> computeSplits(
                                    checkpointFile,
                                    checkpointIntraFileParallelProcessingSplitSize)
                            : checkpointFile -> () -> Stream.of(computeSingletonSplit(checkpointFile));

            // Note: we plan the splits first before reading from any of the files. In principle
            // we should be able to interleave planning and reading, but this would require a
            // more sophisticated iterator. Moreover, split planning is still much cheaper than
            // the actual checkpoint scan and decode, so we don't lose much by doing it first
            List<CheckpointFileSplit> checkpointFileSplits = ParallelUnorderedIterator.stream(
                    checkpointFiles.stream().map(checkpointFileSplitSupplierBuilder).collect(toImmutableList()),
                    checkpointProcessingExecutor).collect(toImmutableList());

            checkState(!checkpointFileSplits.isEmpty(), "Checkpoint file splits unexpectedly empty");

            return ParallelUnorderedIterator.stream(
                    checkpointFileSplits.stream().map(checkpointFileSplitStreamBuilder).collect(toImmutableList()),
                    checkpointProcessingExecutor,
                    CHECKPOINT_FILE_PROCESSING_QUEUE_SIZE);
        }

        Stream<CheckpointFileSplit> checkpointFileSplits =
                checkpointFiles.stream().map(this::computeSingletonSplit);

        return checkpointFileSplits
                .flatMap(checkpointFileSplit -> checkpointFileSplitStreamBuilder.apply(checkpointFileSplit).get());
    }

    public Optional<Long> getLastCheckpointVersion()
    {
        return lastCheckpoint.map(LastCheckpoint::version);
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
            Executor checkpointProcessingExecutor,
            boolean checkpointV2ParallelProcessingEnabled,
            boolean checkpointIntraFileParallelProcessingEnabled,
            long checkpointIntraFileParallelProcessingSplitSize)
    {
        // Sidecar files contain only ADD and REMOVE entry types. https://github.com/delta-io/delta/blob/master/PROTOCOL.md#v2-spec
        Set<CheckpointEntryIterator.EntryType> dataEntryTypes = Sets.intersection(entryTypes, Set.of(ADD, REMOVE));
        Stream<DeltaLakeTransactionLogEntry> v2CheckpointEntries = getV2CheckpointEntries(
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
                checkpointProcessingExecutor,
                checkpointV2ParallelProcessingEnabled,
                checkpointIntraFileParallelProcessingEnabled,
                checkpointIntraFileParallelProcessingSplitSize);
        if (dataEntryTypes.isEmpty()) {
            return v2CheckpointEntries;
        }

        Location sidecarDirectoryPath = checkpointFile.location().sibling("_sidecars");

        if (checkpointV2ParallelProcessingEnabled) {
            return getV2CheckpointTransactionLogEntriesFromSidecarsInParallel(
                    v2CheckpointEntries,
                    sidecarDirectoryPath,
                    session,
                    entryTypes,
                    metadataEntry,
                    protocolEntry,
                    checkpointSchemaManager,
                    typeManager,
                    stats,
                    partitionConstraint,
                    addStatsMinMaxColumnFilter,
                    fileSystem,
                    checkpointProcessingExecutor,
                    checkpointIntraFileParallelProcessingEnabled,
                    checkpointIntraFileParallelProcessingSplitSize);
        }

        List<CompletableFuture<Stream<DeltaLakeTransactionLogEntry>>> logEntryStreamFutures = v2CheckpointEntries
                .map(v2checkpointEntry -> {
                    if (v2checkpointEntry.getSidecar() == null) {
                        return CompletableFuture.completedFuture(Stream.of(v2checkpointEntry));
                    }
                    // Sidecar files are retrieved in parallel using a bounded executor
                    return supplyAsync(() -> {
                        Location sidecar = sidecarDirectoryPath.appendPath(v2checkpointEntry.getSidecar().path());
                        CheckpointEntryIterator iterator = new CheckpointEntryIterator(
                                fileSystem.newInputFile(sidecar),
                                session,
                                0,
                                v2checkpointEntry.getSidecar().sizeInBytes(),
                                v2checkpointEntry.getSidecar().sizeInBytes(),
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
                        return stream(iterator).onClose(iterator::close);
                    }, checkpointProcessingExecutor);
                })
                .collect(toImmutableList());
        // Return the stream to retrieve the values of the futures lazily and allow streamlined split generation
        return logEntryStreamFutures.stream()
                .mapMulti((logEntryStream, builder)
                        -> getFutureValue(logEntryStream, TrinoException.class).forEach(builder));
    }

    private Stream<DeltaLakeTransactionLogEntry> getV2CheckpointTransactionLogEntriesFromSidecarsInParallel(
            Stream<DeltaLakeTransactionLogEntry> v2CheckpointEntries,
            Location sidecarDirectoryPath,
            ConnectorSession session,
            Set<CheckpointEntryIterator.EntryType> entryTypes,
            Optional<MetadataEntry> metadataEntry,
            Optional<ProtocolEntry> protocolEntry,
            CheckpointSchemaManager checkpointSchemaManager,
            TypeManager typeManager,
            FileFormatDataSourceStats stats,
            TupleDomain<DeltaLakeColumnHandle> partitionConstraint,
            Optional<Predicate<String>> addStatsMinMaxColumnFilter,
            TrinoFileSystem fileSystem,
            Executor checkpointProcessingExecutor,
            boolean checkpointIntraFileParallelProcessingEnabled,
            long checkpointIntraFileParallelProcessingSplitSize)
    {
        Function<CheckpointFileSplit, Supplier<Stream<DeltaLakeTransactionLogEntry>>> sidecarFileSplitStreamBuilder =
                checkpointFileSplit -> {
                    return () -> buildCheckpointFileSplitStream(
                            checkpointFileSplit,
                            session,
                            checkpointSchemaManager,
                            typeManager,
                            entryTypes,
                            metadataEntry,
                            protocolEntry,
                            stats,
                            partitionConstraint,
                            addStatsMinMaxColumnFilter);
                };

        Map<Boolean, List<DeltaLakeTransactionLogEntry>> partitionedV2CheckpointEntries =
                v2CheckpointEntries.collect(Collectors.partitioningBy(v2checkpointEntry -> v2checkpointEntry.getSidecar() != null));

        List<DeltaLakeTransactionLogEntry> sidecarEntries = partitionedV2CheckpointEntries.get(true);
        List<DeltaLakeTransactionLogEntry> nonSidecarEntries = partitionedV2CheckpointEntries.get(false);

        List<TrinoInputFile> sidecarFiles = sidecarEntries.stream()
                .map(entry -> {
                    SidecarEntry sidecarEntry = entry.getSidecar();
                    Location sidecarPath = sidecarDirectoryPath.appendPath(sidecarEntry.path());
                    return fileSystem.newInputFile(sidecarPath);
                })
                .collect(toImmutableList());

        // Note: we could use sidecarEntry.sizeInBytes() to determine the sidecar file length, rather than checking the
        // length directly from the file in computeSplits. This has the advantage of eliminating the need for I/O to check
        // the length while planning. However, at this time, Trino will always fetch the length anyway in TrinoParquetDataSource,
        // and the length is cached once fetched -- so, using sizeInBytes wouldn't save us any calls overall. Moreover,
        // fetching the length at planning time allows us to write more precise and deterministic file system access tests
        Function<TrinoInputFile, Supplier<Stream<CheckpointFileSplit>>> sidecarFileSplitSupplierBuilder =
                checkpointIntraFileParallelProcessingEnabled
                        ? sidecarFile -> () -> computeSplits(sidecarFile, checkpointIntraFileParallelProcessingSplitSize)
                        : sidecarFile -> () -> Stream.of(computeSingletonSplit(sidecarFile));

        List<CheckpointFileSplit> sidecarFileSplits = ParallelUnorderedIterator.stream(
                    sidecarFiles.stream().map(sidecarFileSplitSupplierBuilder).collect(toImmutableList()),
                    checkpointProcessingExecutor).collect(toImmutableList());

        Stream<DeltaLakeTransactionLogEntry> sidecarFileEntries = ParallelUnorderedIterator.stream(
                sidecarFileSplits.stream().map(sidecarFileSplitStreamBuilder).collect(toImmutableList()),
                checkpointProcessingExecutor,
                CHECKPOINT_FILE_PROCESSING_QUEUE_SIZE);

        return Stream.concat(nonSidecarEntries.stream(), sidecarFileEntries);
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
            Executor checkpointProcessingExecutor,
            boolean checkpointV2ParallelProcessingEnabled,
            boolean checkpointIntraFileParallelProcessingEnabled,
            long checkpointIntraFileParallelProcessingSplitSize)
    {
        if (checkpointFile.location().fileName().endsWith(".json")) {
            try {
                return getEntriesFromJson(checkpoint.version(), checkpointFile, transactionLogMaxCachedFileSize)
                        .stream()
                        .flatMap(logEntries -> logEntries.getEntries(fileSystem));
            }
            catch (IOException e) {
                throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, format("Unexpected IO exception occurred while reading the entries of the file: %s for the table %s", checkpoint, table), e);
            }
        }
        if (checkpointFile.location().fileName().endsWith(".parquet")) {
            ImmutableSet<CheckpointEntryIterator.EntryType> entryTypesWithSidecar =
                    ImmutableSet.<CheckpointEntryIterator.EntryType>builder()
                            .addAll(entryTypes)
                            .add(SIDECAR)
                            .build();

            Function<CheckpointFileSplit, Supplier<Stream<DeltaLakeTransactionLogEntry>>> checkpointFileSplitStreamBuilder =
                    checkpointFileSplit -> {
                        return () -> buildCheckpointFileSplitStream(
                                checkpointFileSplit,
                                session,
                                checkpointSchemaManager,
                                typeManager,
                                entryTypesWithSidecar,
                                metadataEntry,
                                protocolEntry,
                                stats,
                                partitionConstraint,
                                addStatsMinMaxColumnFilter);
                    };

            List<CheckpointFileSplit> checkpointFileSplits;
            if (checkpointV2ParallelProcessingEnabled && checkpointIntraFileParallelProcessingEnabled) {
                checkpointFileSplits = computeSplits(checkpointFile, checkpointIntraFileParallelProcessingSplitSize).collect(toImmutableList());
            }
            else {
                checkpointFileSplits = ImmutableList.of(computeSingletonSplit(checkpointFile));
            }

            return ParallelUnorderedIterator.stream(
                    checkpointFileSplits.stream().map(checkpointFileSplitStreamBuilder).collect(toImmutableList()),
                    checkpointProcessingExecutor,
                    CHECKPOINT_FILE_PROCESSING_QUEUE_SIZE);
        }
        throw new IllegalArgumentException("Unsupported v2 checkpoint file format: " + checkpointFile.location());
    }

    private Stream<DeltaLakeTransactionLogEntry> buildCheckpointFileSplitStream(
            CheckpointFileSplit checkpointFileSplit,
            ConnectorSession session,
            CheckpointSchemaManager checkpointSchemaManager,
            TypeManager typeManager,
            Set<CheckpointEntryIterator.EntryType> entryTypes,
            Optional<MetadataEntry> metadataEntry,
            Optional<ProtocolEntry> protocolEntry,
            FileFormatDataSourceStats stats,
            TupleDomain<DeltaLakeColumnHandle> partitionConstraint,
            Optional<Predicate<String>> addStatsMinMaxColumnFilter)
    {
        CheckpointEntryIterator checkpointEntryIterator = new CheckpointEntryIterator(
                checkpointFileSplit.checkpointFile(),
                session,
                checkpointFileSplit.start(),
                checkpointFileSplit.length(),
                checkpointFileSplit.fileSize(),
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

        return stream(checkpointEntryIterator)
                .onClose(checkpointEntryIterator::close);
    }

    private long getCheckpointFileSize(TrinoInputFile checkpointFile)
    {
        try {
            return checkpointFile.length();
        }
        catch (FileNotFoundException e) {
            String lastCheckpointMemo = lastCheckpoint.map(LastCheckpoint::toString).orElse("<unknown>");
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("%s mentions a non-existent checkpoint file for table: %s", lastCheckpointMemo, table), e);
        }
        catch (IOException e) {
            String lastCheckpointMemo = lastCheckpoint.map(LastCheckpoint::toString).orElse("<unknown>");
            throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, format("Unexpected IO exception occurred while retrieving the length of the file: %s for the table %s", lastCheckpointMemo, table), e);
        }
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
