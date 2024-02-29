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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot.MetadataAndProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.LAST_CHECKPOINT_FILENAME;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.ADD;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.COMMIT;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.METADATA;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.PROTOCOL;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.REMOVE;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.TRANSACTION;
import static java.util.Objects.requireNonNull;

public class CheckpointWriterManager
{
    private final TypeManager typeManager;
    private final CheckpointSchemaManager checkpointSchemaManager;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final String trinoVersion;
    private final TransactionLogAccess transactionLogAccess;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final JsonCodec<LastCheckpoint> lastCheckpointCodec;

    @Inject
    public CheckpointWriterManager(
            TypeManager typeManager,
            CheckpointSchemaManager checkpointSchemaManager,
            TrinoFileSystemFactory fileSystemFactory,
            NodeVersion nodeVersion,
            TransactionLogAccess transactionLogAccess,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            JsonCodec<LastCheckpoint> lastCheckpointCodec)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.checkpointSchemaManager = requireNonNull(checkpointSchemaManager, "checkpointSchemaManager is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.trinoVersion = nodeVersion.toString();
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.lastCheckpointCodec = requireNonNull(lastCheckpointCodec, "lastCheckpointCodec is null");
    }

    public void writeCheckpoint(ConnectorSession session, TableSnapshot snapshot)
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

            CheckpointBuilder checkpointBuilder = new CheckpointBuilder();

            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            List<DeltaLakeTransactionLogEntry> checkpointLogEntries;
            try (Stream<DeltaLakeTransactionLogEntry> checkpointLogEntriesStream = snapshot.getCheckpointTransactionLogEntries(
                    session,
                    ImmutableSet.of(METADATA, PROTOCOL),
                    checkpointSchemaManager,
                    typeManager,
                    fileSystem,
                    fileFormatDataSourceStats,
                    Optional.empty(),
                    TupleDomain.all(),
                    Optional.empty())) {
                checkpointLogEntries = checkpointLogEntriesStream.filter(entry -> entry.getMetaData() != null || entry.getProtocol() != null)
                        .collect(toImmutableList());
            }

            if (!checkpointLogEntries.isEmpty()) {
                // TODO HACK: this call is required only to ensure that cachedMetadataEntry is set in snapshot (https://github.com/trinodb/trino/issues/12032),
                // so we can read add entries below this should be reworked so we pass metadata entry explicitly to getCheckpointTransactionLogEntries,
                // and we should get rid of `setCachedMetadata` in TableSnapshot to make it immutable.
                // Also more proper would be to use metadata entry obtained above in snapshot.getCheckpointTransactionLogEntries to read other checkpoint entries, but using newer one should not do harm.
                transactionLogAccess.getMetadataEntry(session, snapshot);

                // register metadata entry in writer
                DeltaLakeTransactionLogEntry metadataLogEntry = checkpointLogEntries.stream()
                        .filter(logEntry -> logEntry.getMetaData() != null)
                        .findFirst()
                        .orElseThrow(() -> new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Metadata not found in transaction log for " + snapshot.getTable()));
                DeltaLakeTransactionLogEntry protocolLogEntry = checkpointLogEntries.stream()
                        .filter(logEntry -> logEntry.getProtocol() != null)
                        .findFirst()
                        .orElseThrow(() -> new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Protocol not found in transaction log for " + snapshot.getTable()));

                checkpointBuilder.addLogEntry(metadataLogEntry);
                checkpointBuilder.addLogEntry(protocolLogEntry);

                // read remaining entries from checkpoint register them in writer
                try (Stream<DeltaLakeTransactionLogEntry> checkpointLogEntriesStream = snapshot.getCheckpointTransactionLogEntries(
                        session,
                        ImmutableSet.of(TRANSACTION, ADD, REMOVE, COMMIT),
                        checkpointSchemaManager,
                        typeManager,
                        fileSystem,
                        fileFormatDataSourceStats,
                        Optional.of(new MetadataAndProtocolEntry(metadataLogEntry.getMetaData(), protocolLogEntry.getProtocol())),
                        TupleDomain.all(),
                        Optional.of(alwaysTrue()))) {
                    checkpointLogEntriesStream.forEach(checkpointBuilder::addLogEntry);
                }
            }

            snapshot.getJsonTransactionLogEntries()
                    .forEach(checkpointBuilder::addLogEntry);

            Location transactionLogDir = Location.of(getTransactionLogDir(snapshot.getTableLocation()));
            Location targetFile = transactionLogDir.appendPath("%020d.checkpoint.parquet".formatted(newCheckpointVersion));
            CheckpointWriter checkpointWriter = new CheckpointWriter(typeManager, checkpointSchemaManager, trinoVersion);
            CheckpointEntries checkpointEntries = checkpointBuilder.build();
            TrinoOutputFile checkpointFile = fileSystemFactory.create(session).newOutputFile(targetFile);
            checkpointWriter.write(checkpointEntries, checkpointFile);

            // update last checkpoint file
            LastCheckpoint newLastCheckpoint = new LastCheckpoint(newCheckpointVersion, checkpointEntries.size(), Optional.empty(), Optional.empty());
            Location checkpointPath = transactionLogDir.appendPath(LAST_CHECKPOINT_FILENAME);
            TrinoOutputFile outputFile = fileSystem.newOutputFile(checkpointPath);
            outputFile.createOrOverwrite(lastCheckpointCodec.toJsonBytes(newLastCheckpoint));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
