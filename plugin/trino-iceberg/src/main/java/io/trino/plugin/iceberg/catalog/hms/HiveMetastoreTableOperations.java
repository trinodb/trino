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
package io.trino.plugin.iceberg.catalog.hms;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.annotation.NotThreadSafe;
import io.trino.hive.thrift.metastore.MetaException;
import io.trino.metastore.AcidTransactionOwner;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.Table;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.encryption.EncryptionManagerFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreApiTable;
import static io.trino.plugin.iceberg.IcebergTableName.tableNameFrom;
import static io.trino.plugin.iceberg.IcebergUtil.fixBrokenMetadataLocation;
import static java.lang.Boolean.parseBoolean;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.TableProperties.CURRENT_SNAPSHOT_ID;
import static org.apache.iceberg.TableProperties.CURRENT_SNAPSHOT_TIMESTAMP;
import static org.apache.iceberg.TableProperties.HIVE_LOCK_ENABLED;

@NotThreadSafe
public class HiveMetastoreTableOperations
        extends AbstractMetastoreTableOperations
{
    private static final Logger log = Logger.get(HiveMetastoreTableOperations.class);
    // Emitted by Hive's HiveAlterHandler when the expected 'metadata_location' does not match (HIVE-26882) or the conditional UPDATE affects no row (HIVE-28121)
    private static final String CONCURRENT_MODIFICATION_MESSAGE_PREFIX = "The table has been modified. The parameter value for key '" + METADATA_LOCATION_PROP + "' is";

    private final ThriftMetastore thriftMetastore;
    private final boolean lockingEnabled;

    public HiveMetastoreTableOperations(
            FileIO fileIo,
            CachingHiveMetastore metastore,
            ThriftMetastore thriftMetastore,
            boolean lockingEnabled,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location,
            EncryptionManagerFactory encryptionManagerFactory)
    {
        super(fileIo, metastore, session, database, table, owner, location, encryptionManagerFactory);
        this.thriftMetastore = requireNonNull(thriftMetastore, "thriftMetastore is null");
        this.lockingEnabled = lockingEnabled;
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        Table currentTable = getTable();
        commitTableUpdate(currentTable, metadata, (table, newMetadataLocation) -> Table.builder(table)
                .apply(builder -> updateMetastoreTable(builder, metadata, newMetadataLocation, Optional.of(currentMetadataLocation)))
                .build());
    }

    @Override
    protected final void commitMaterializedView(TableMetadata base, TableMetadata metadata)
    {
        Table materializedView = getTable(database, tableNameFrom(tableName));
        commitTableUpdate(materializedView, metadata, (table, newMetadataLocation) -> {
            if (materializedViewCommitData.isPresent()) {
                table = Table.builder(table)
                        .setParameters(materializedViewCommitData.get().parameters())
                        .setViewOriginalText(Optional.of(materializedViewCommitData.get().viewOriginalText()))
                        .build();
            }
            return Table.builder(table)
                    .apply(builder -> builder
                            .setParameter(METADATA_LOCATION_PROP, newMetadataLocation)
                            .setParameter(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation)
                            .setParameter(CURRENT_SNAPSHOT_ID, String.valueOf(metadata.currentSnapshot().snapshotId()))
                            .setParameter(CURRENT_SNAPSHOT_TIMESTAMP, String.valueOf(metadata.currentSnapshot().timestampMillis())))
                    .build();
        });
    }

    private void commitTableUpdate(Table table, TableMetadata metadata, BiFunction<Table, String, Table> tableUpdateFunction)
    {
        String newMetadataLocation = writeNewMetadata(metadata, version.orElseThrow() + 1);

        boolean lockingEnabled = parseBoolean(table.getParameters().getOrDefault(HIVE_LOCK_ENABLED, Boolean.toString(this.lockingEnabled)));
        HiveLock hiveLock = lockingEnabled ? new ThriftMetastoreLock(table) : new NoLock();
        hiveLock.acquire();

        try {
            Table currentTable = fromMetastoreApiTable(thriftMetastore.getTable(database, table.getTableName())
                    .orElseThrow(() -> new TableNotFoundException(getSchemaTableName())));

            checkState(currentMetadataLocation != null, "No current metadata location for existing table");
            String metadataLocation = fixBrokenMetadataLocation(currentTable.getParameters().get(METADATA_LOCATION_PROP));
            if (!currentMetadataLocation.equals(metadataLocation)) {
                throw new CommitFailedException(
                        "Metadata location [%s] is not same as table metadata location [%s] for %s",
                        currentMetadataLocation,
                        metadataLocation,
                        getSchemaTableName());
            }

            Table updatedTable = tableUpdateFunction.apply(table, newMetadataLocation);

            // Passing environment context causes redundant operations if Hive locking is enabled
            Map<String, String> environmentContext = lockingEnabled ? ImmutableMap.of() : environmentContext(metadataLocation);

            // todo privileges should not be replaced for an alter
            PrincipalPrivileges privileges = table.getOwner().map(MetastoreUtil::buildInitialPrivilegeSet).orElse(NO_PRIVILEGES);
            try {
                metastore.replaceTable(table.getDatabaseName(), table.getTableName(), updatedTable, privileges, environmentContext);
            }
            catch (RuntimeException e) {
                // The exception alone does not tell whether the update was applied: the response may have been lost on a
                // timeout, or a retried request may have been rejected because the first attempt succeeded.
                switch (checkExistingTableCommitStatus(table, metadataLocation, newMetadataLocation, e)) {
                    // The table points at the metadata we wrote, or descends from it: the commit succeeded despite the exception.
                    case SUCCESS -> log.warn(e, "Received an error from metastore while committing to table %s, but the commit was actually applied; treating the commit as successful", getSchemaTableName());
                    // Cannot determine whether the update was applied. CommitStateUnknownException stops the Iceberg
                    // transaction layer from cleaning up the new files.
                    case UNKNOWN -> throw new CommitStateUnknownException(e);
                    // The metastore rejected the conditional update and the table state proves the commit was not
                    // applied, so Iceberg can retry it on the current state. Like the stale-location check above, the
                    // new metadata file is left behind for remove_orphan_files.
                    case FAILURE -> throw new CommitFailedException(e, "Failed to commit to table %s due to a concurrent update", getSchemaTableName());
                }
            }
        }
        finally {
            hiveLock.release();
        }

        shouldRefresh = true;
    }

    private CommitStatus checkExistingTableCommitStatus(Table table, String expectedMetadataLocation, String newMetadataLocation, RuntimeException commitException)
    {
        // re-read the table the update was sent to: for a materialized view that is the view itself, not the storage table
        return checkCommitStatus(
                new SchemaTableName(table.getDatabaseName(), table.getTableName()),
                expectedMetadataLocation,
                newMetadataLocation,
                commitException,
                () -> thriftMetastore.getTable(table.getDatabaseName(), table.getTableName())
                        .map(current -> current.getParameters().get(METADATA_LOCATION_PROP))
                        .map(IcebergUtil::fixBrokenMetadataLocation),
                location -> TableMetadataParser.read(io(), location));
    }

    /**
     * Determines whether a commit whose metastore call failed was actually applied. Like
     * {@link AbstractMetastoreTableOperations#commitNewTable}, the check is biased towards {@link CommitStatus#UNKNOWN}.
     * The commit is known to be applied when the metastore points at the new metadata location, or when that location
     * is in the current metadata log. {@link CommitStatus#FAILURE} is only returned when the metastore reported that it
     * rejected the conditional update (HIVE-26882, HIVE-28121) and the location this commit expected is still in the
     * current metadata log: the rejection means the request was processed without being applied, and every attempt of
     * the same request carries the same expected value, so it cannot be applied later either. Once
     * {@code write.metadata.previous-versions-max} has pushed the expected location out of the log, the outcome stays
     * unknown.
     */
    @VisibleForTesting
    static CommitStatus checkCommitStatus(
            SchemaTableName schemaTableName,
            String expectedMetadataLocation,
            String newMetadataLocation,
            RuntimeException commitException,
            Supplier<Optional<String>> committedMetadataLocation,
            Function<String, TableMetadata> metadataLoader)
    {
        Optional<String> committedLocation;
        try {
            committedLocation = committedMetadataLocation.get();
        }
        catch (RuntimeException e) {
            log.error(e, "Could not determine commit status for table %s; treating commit state as unknown", schemaTableName);
            return CommitStatus.UNKNOWN;
        }
        if (committedLocation.isEmpty()) {
            return CommitStatus.UNKNOWN;
        }
        if (newMetadataLocation.equals(committedLocation.get())) {
            return CommitStatus.SUCCESS;
        }
        TableMetadata committedMetadata;
        try {
            committedMetadata = metadataLoader.apply(committedLocation.get());
        }
        catch (RuntimeException e) {
            log.error(e, "Could not read current metadata %s of table %s to determine commit status; treating commit state as unknown", committedLocation.get(), schemaTableName);
            return CommitStatus.UNKNOWN;
        }
        // a later commit by another writer built on the metadata we wrote and lists its location in the metadata log
        if (isInMetadataLog(committedMetadata, newMetadataLocation)) {
            return CommitStatus.SUCCESS;
        }
        // the metadata log is a contiguous suffix of the table history, so if the location this commit expected is still
        // in it, an applied commit would be in it too; a rejection on top of that is a definite concurrent-update failure
        if (isConcurrentModificationRejection(commitException) && isInMetadataLog(committedMetadata, expectedMetadataLocation)) {
            return CommitStatus.FAILURE;
        }
        return CommitStatus.UNKNOWN;
    }

    private static boolean isInMetadataLog(TableMetadata metadata, String metadataLocation)
    {
        return metadata.previousFiles().stream().anyMatch(entry -> metadataLocation.equals(entry.file()));
    }

    @VisibleForTesting
    static boolean isConcurrentModificationRejection(Throwable throwable)
    {
        return Throwables.getCausalChain(throwable).stream()
                .anyMatch(cause -> cause instanceof MetaException && cause.getMessage() != null && cause.getMessage().contains(CONCURRENT_MODIFICATION_MESSAGE_PREFIX));
    }

    private static Map<String, String> environmentContext(String metadataLocation)
    {
        if (metadataLocation == null) {
            return ImmutableMap.of();
        }
        return ImmutableMap.<String, String>builder()
                .put("expected_parameter_key", "metadata_location")
                .put("expected_parameter_value", metadataLocation)
                .buildOrThrow();
    }

    private class ThriftMetastoreLock
            implements HiveLock
    {
        private long lockId;

        private final Table table;

        public ThriftMetastoreLock(Table table)
        {
            this.table = requireNonNull(table, "table is null");
        }

        @Override
        public void acquire()
        {
            lockId = thriftMetastore.acquireTableExclusiveLock(
                    new AcidTransactionOwner(session.getUser()),
                    session.getQueryId(),
                    table.getDatabaseName(),
                    table.getTableName());
        }

        @Override
        public void release()
        {
            try {
                thriftMetastore.releaseTableLock(lockId);
            }
            catch (RuntimeException e) {
                // Release lock step has failed. Not throwing this exception, after commit has already succeeded.
                // So, that underlying iceberg API will not do the metadata cleanup, otherwise table will be in unusable state.
                // If configured and supported, the unreleased lock will be automatically released by the metastore after not hearing a heartbeat for a while,
                // or otherwise it might need to be manually deleted from the metastore backend storage.
                log.error(e, "Failed to release lock %s when committing to table %s", lockId, table.getTableName());
            }
        }
    }

    // HIVE-26882 requires HMS client 2 or later. Our HMS client is based on version 3.
    private static class NoLock
            implements HiveLock
    {
        @Override
        public void acquire() {}

        @Override
        public void release() {}
    }

    private interface HiveLock
    {
        void acquire();

        void release();
    }
}
