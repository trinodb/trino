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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.DeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeTableCredentials;
import io.trino.plugin.deltalake.ForDeltaLakeMetadata;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.FileEntryKey;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot.MetadataAndProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointWriter.CheckpointFileWriter;
import io.trino.spi.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.LAST_CHECKPOINT_FILENAME;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.ADD;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.METADATA;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.PROTOCOL;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.REMOVE;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.TRANSACTION;
import static java.util.Objects.requireNonNull;

/**
 * Writes a checkpoint from the last checkpoint and the commits after it.
 * Only metadata, protocol, transaction and log tail entries are held in memory.
 * Add and remove entries of the last checkpoint are copied to the new file as they are read,
 * skipping files that a later commit touched.
 */
public class CheckpointWriterManager
{
    private final TypeManager typeManager;
    private final CheckpointSchemaManager checkpointSchemaManager;
    private final DeltaLakeFileSystemFactory fileSystemFactory;
    private final CheckpointWriter checkpointWriter;
    private final TransactionLogAccess transactionLogAccess;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final JsonCodec<LastCheckpoint> lastCheckpointCodec;
    private final Executor executorService;
    private final int checkpointProcessingParallelism;

    @Inject
    public CheckpointWriterManager(
            TypeManager typeManager,
            CheckpointSchemaManager checkpointSchemaManager,
            DeltaLakeFileSystemFactory fileSystemFactory,
            NodeVersion nodeVersion,
            TransactionLogAccess transactionLogAccess,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            JsonCodec<LastCheckpoint> lastCheckpointCodec,
            DeltaLakeConfig deltaLakeConfig,
            @ForDeltaLakeMetadata ExecutorService executorService)
    {
        this(typeManager,
                checkpointSchemaManager,
                fileSystemFactory,
                new CheckpointWriter(typeManager, checkpointSchemaManager, nodeVersion.toString()),
                transactionLogAccess,
                fileFormatDataSourceStats,
                lastCheckpointCodec,
                deltaLakeConfig,
                executorService);
    }

    @VisibleForTesting
    CheckpointWriterManager(
            TypeManager typeManager,
            CheckpointSchemaManager checkpointSchemaManager,
            DeltaLakeFileSystemFactory fileSystemFactory,
            CheckpointWriter checkpointWriter,
            TransactionLogAccess transactionLogAccess,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            JsonCodec<LastCheckpoint> lastCheckpointCodec,
            DeltaLakeConfig deltaLakeConfig,
            ExecutorService executorService)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.checkpointSchemaManager = requireNonNull(checkpointSchemaManager, "checkpointSchemaManager is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.checkpointWriter = requireNonNull(checkpointWriter, "checkpointWriter is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.lastCheckpointCodec = requireNonNull(lastCheckpointCodec, "lastCheckpointCodec is null");
        this.executorService = requireNonNull(executorService, "ExecutorService is null");
        this.checkpointProcessingParallelism = deltaLakeConfig.getCheckpointProcessingParallelism();
    }

    public void writeCheckpoint(ConnectorSession session, TableSnapshot snapshot, Optional<DeltaLakeTableCredentials> tableCredentials)
    {
        try {
            SchemaTableName table = snapshot.getTable();
            long newCheckpointVersion = snapshot.getVersion();
            snapshot.getLastCheckpointVersion().ifPresent(
                    lastCheckpoint -> checkArgument(
                            newCheckpointVersion > lastCheckpoint,
                            "written checkpoint %s for table %s must be greater than last checkpoint version %s",
                            newCheckpointVersion,
                            table,
                            lastCheckpoint));

            TrinoFileSystem fileSystem = fileSystemFactory.create(session, tableCredentials);
            Executor checkpointReadExecutor = new BoundedExecutor(executorService, checkpointProcessingParallelism);

            // Holds the metadata, protocol, transaction and log tail entries of the new checkpoint
            CheckpointBuilder checkpointBuilder = new CheckpointBuilder();
            List<DeltaLakeTransactionLogEntry> checkpointLogEntries;
            try (Stream<DeltaLakeTransactionLogEntry> checkpointLogEntriesStream = snapshot.getCheckpointTransactionLogEntries(
                    session,
                    ImmutableSet.of(METADATA, PROTOCOL, TRANSACTION),
                    checkpointSchemaManager,
                    typeManager,
                    fileSystem,
                    fileFormatDataSourceStats,
                    Optional.empty(),
                    TupleDomain.all(),
                    Optional.empty(),
                    checkpointReadExecutor)) {
                // V2 checkpoints in JSON format return every entry type, so only the requested types are kept
                checkpointLogEntries = checkpointLogEntriesStream
                        .filter(entry -> entry.getMetaData() != null || entry.getProtocol() != null || entry.getTxn() != null)
                        .collect(toImmutableList());
            }

            Optional<MetadataAndProtocolEntry> lastCheckpointMetadataAndProtocol = Optional.empty();
            if (!checkpointLogEntries.isEmpty()) {
                // TODO HACK: this call is required only to ensure that cachedMetadataEntry is set in snapshot (https://github.com/trinodb/trino/issues/12032),
                // so we can read add entries below this should be reworked so we pass metadata entry explicitly to getCheckpointTransactionLogEntries,
                // and we should get rid of `setCachedMetadata` in TableSnapshot to make it immutable.
                // Also more proper would be to use metadata entry obtained above in snapshot.getCheckpointTransactionLogEntries to read other checkpoint entries, but using newer one should not do harm.
                transactionLogAccess.getMetadataEntry(session, fileSystem, snapshot);

                MetadataEntry lastCheckpointMetadata = checkpointLogEntries.stream()
                        .map(DeltaLakeTransactionLogEntry::getMetaData)
                        .filter(Objects::nonNull)
                        .findFirst()
                        .orElseThrow(() -> new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Metadata not found in transaction log for " + snapshot.getTable()));
                ProtocolEntry lastCheckpointProtocol = checkpointLogEntries.stream()
                        .map(DeltaLakeTransactionLogEntry::getProtocol)
                        .filter(Objects::nonNull)
                        .findFirst()
                        .orElseThrow(() -> new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Protocol not found in transaction log for " + snapshot.getTable()));
                lastCheckpointMetadataAndProtocol = Optional.of(new MetadataAndProtocolEntry(lastCheckpointMetadata, lastCheckpointProtocol));

                checkpointLogEntries.forEach(checkpointBuilder::addLogEntry);
            }

            snapshot.getJsonTransactionLogEntries(fileSystem)
                    .forEach(checkpointBuilder::addLogEntry);
            CheckpointEntries logTailEntries = checkpointBuilder.build();

            Location transactionLogDir = Location.of(getTransactionLogDir(snapshot.getTableLocation()));
            Location targetFile = transactionLogDir.appendPath("%020d.checkpoint.parquet".formatted(newCheckpointVersion));
            TrinoOutputFile checkpointFile = fileSystem.newOutputFile(targetFile);
            long checkpointEntryCount;
            try (CheckpointFileWriter checkpointFileWriter = checkpointWriter.createWriter(logTailEntries.metadataEntry(), logTailEntries.protocolEntry(), checkpointFile)) {
                for (TransactionEntry transactionEntry : logTailEntries.transactionEntries()) {
                    checkpointFileWriter.writeTransaction(transactionEntry);
                }

                if (lastCheckpointMetadataAndProtocol.isPresent()) {
                    // Files touched by commits after the last checkpoint supersede the entries the last checkpoint holds for them
                    Set<FileEntryKey> logTailFileKeys = Stream.concat(
                                    logTailEntries.addFileEntries().stream().map(FileEntryKey::of),
                                    logTailEntries.removeFileEntries().stream().map(FileEntryKey::of))
                            .collect(toImmutableSet());
                    try (Stream<DeltaLakeTransactionLogEntry> checkpointLogEntriesStream = snapshot.getCheckpointTransactionLogEntries(
                            session,
                            ImmutableSet.of(ADD, REMOVE),
                            checkpointSchemaManager,
                            typeManager,
                            fileSystem,
                            fileFormatDataSourceStats,
                            lastCheckpointMetadataAndProtocol,
                            TupleDomain.all(),
                            Optional.of(alwaysTrue()),
                            checkpointReadExecutor)) {
                        // forEach receives entries one at a time, iterator() on this stream buffers the whole checkpoint
                        checkpointLogEntriesStream.forEach(logEntry -> {
                            try {
                                AddFileEntry addFileEntry = logEntry.getAdd();
                                if (addFileEntry != null && !logTailFileKeys.contains(FileEntryKey.of(addFileEntry))) {
                                    checkpointFileWriter.writeAddFile(addFileEntry);
                                }
                                RemoveFileEntry removeFileEntry = logEntry.getRemove();
                                if (removeFileEntry != null && !logTailFileKeys.contains(FileEntryKey.of(removeFileEntry))) {
                                    checkpointFileWriter.writeRemoveFile(removeFileEntry);
                                }
                            }
                            catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        });
                    }
                }

                for (AddFileEntry addFileEntry : logTailEntries.addFileEntries()) {
                    checkpointFileWriter.writeAddFile(addFileEntry);
                }
                for (RemoveFileEntry removeFileEntry : logTailEntries.removeFileEntries()) {
                    checkpointFileWriter.writeRemoveFile(removeFileEntry);
                }
                checkpointFileWriter.finish();
                checkpointEntryCount = checkpointFileWriter.getEntryCount();
            }

            // update last checkpoint file
            LastCheckpoint newLastCheckpoint = new LastCheckpoint(newCheckpointVersion, checkpointEntryCount, Optional.empty(), Optional.empty());
            Location checkpointPath = transactionLogDir.appendPath(LAST_CHECKPOINT_FILENAME);
            TrinoOutputFile outputFile = fileSystem.newOutputFile(checkpointPath);
            outputFile.createOrOverwrite(lastCheckpointCodec.toJsonBytes(newLastCheckpoint));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
