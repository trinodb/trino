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
import io.airlift.json.JsonCodec;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.MoreCollectors.toOptional;
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
    private final HdfsEnvironment hdfsEnvironment;
    private final TransactionLogAccess transactionLogAccess;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final JsonCodec<LastCheckpoint> lastCheckpointCodec;

    @Inject
    public CheckpointWriterManager(
            TypeManager typeManager,
            CheckpointSchemaManager checkpointSchemaManager,
            HdfsEnvironment hdfsEnvironment,
            TransactionLogAccess transactionLogAccess,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            JsonCodec<LastCheckpoint> lastCheckpointCodec)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.checkpointSchemaManager = requireNonNull(checkpointSchemaManager, "checkpointSchemaManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
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

            FileSystem fileSystem = hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session), snapshot.getTableLocation());
            Optional<DeltaLakeTransactionLogEntry> checkpointMetadataLogEntry = snapshot.getCheckpointTransactionLogEntries(
                    session,
                    ImmutableSet.of(METADATA),
                    checkpointSchemaManager,
                    typeManager,
                    fileSystem,
                    hdfsEnvironment, fileFormatDataSourceStats)
                    .collect(toOptional());
            if (checkpointMetadataLogEntry.isPresent()) {
                // todo HACK: this call is required only to ensure that cachedMetadataEntry is set in snapshot, so we can read add entries below
                // this should be reworked so we pass metadata entry explicitly to getCheckpointTransactionLogEntries, and we should get rid of `setCachedMetadata` in TableSnapshot to make it immutable.
                // Also more proper would be to use metadata entry obtained above in snapshot.getCheckpointTransactionLogEntries to read other checkpoint entries, but using newer one should not do harm.
                checkState(transactionLogAccess.getMetadataEntry(snapshot, session).isPresent(), "metadata entry in snapshot null");

                // register metadata entry in writer
                checkState(checkpointMetadataLogEntry.get().getMetaData() != null, "metaData not present in log entry");
                checkpointBuilder.addLogEntry(checkpointMetadataLogEntry.get());

                // read remaining entries from checkpoint register them in writer
                snapshot.getCheckpointTransactionLogEntries(
                        session,
                        ImmutableSet.of(PROTOCOL, TRANSACTION, ADD, REMOVE, COMMIT),
                        checkpointSchemaManager,
                        typeManager,
                        fileSystem,
                        hdfsEnvironment,
                        fileFormatDataSourceStats)
                        .forEach(checkpointBuilder::addLogEntry);
            }

            snapshot.getJsonTransactionLogEntries()
                    .forEach(checkpointBuilder::addLogEntry);

            Path transactionLogDirectory = getTransactionLogDir(snapshot.getTableLocation());
            Path targetFile = new Path(transactionLogDirectory, String.format("%020d.checkpoint.parquet", newCheckpointVersion));
            CheckpointWriter checkpointWriter = new CheckpointWriter(typeManager, checkpointSchemaManager, hdfsEnvironment);
            CheckpointEntries checkpointEntries = checkpointBuilder.build();
            checkpointWriter.write(session, checkpointEntries, targetFile);

            // update last checkpoint file
            LastCheckpoint newLastCheckpoint = new LastCheckpoint(newCheckpointVersion, checkpointEntries.size(), Optional.empty());
            try (OutputStream outputStream = fileSystem.create(new Path(transactionLogDirectory, LAST_CHECKPOINT_FILENAME), true)) {
                outputStream.write(lastCheckpointCodec.toJsonBytes(newLastCheckpoint));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
