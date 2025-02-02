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
package io.trino.plugin.deltalake.procedure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.base.util.UncheckedCloseable;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import io.trino.plugin.deltalake.DeltaLakeMetadataFactory;
import io.trino.plugin.deltalake.DeltaLakeSessionProperties;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriter;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriterFactory;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.procedure.Procedure.Argument;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.alwaysFalse;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.filesystem.Locations.areDirectoryLocationsEquivalent;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.MAX_WRITER_VERSION;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.checkUnsupportedUniversalFormat;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.checkValidTableHandle;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getVacuumMinRetention;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isVacuumLoggingEnabled;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.IsolationLevel;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.DELETION_VECTORS_FEATURE_NAME;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.unsupportedWriterFeatures;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.TRANSACTION_LOG_DIRECTORY;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Comparator.naturalOrder;
import static java.util.Objects.requireNonNull;

public class VacuumProcedure
        implements Provider<Procedure>
{
    private static final Logger log = Logger.get(VacuumProcedure.class);
    private static final int DELETE_BATCH_SIZE = 1000;

    private static final MethodHandle VACUUM;

    static {
        try {
            VACUUM = lookup().unreflect(VacuumProcedure.class.getMethod("vacuum", ConnectorSession.class, ConnectorAccessControl.class, String.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final CatalogName catalogName;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final DeltaLakeMetadataFactory metadataFactory;
    private final TransactionLogAccess transactionLogAccess;
    private final TransactionLogWriterFactory transactionLogWriterFactory;
    private final String nodeVersion;
    private final String nodeId;

    @Inject
    public VacuumProcedure(
            CatalogName catalogName,
            TrinoFileSystemFactory fileSystemFactory,
            DeltaLakeMetadataFactory metadataFactory,
            TransactionLogAccess transactionLogAccess,
            TransactionLogWriterFactory transactionLogWriterFactory,
            NodeManager nodeManager)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.transactionLogWriterFactory = requireNonNull(transactionLogWriterFactory, "transactionLogWriterFactory is null");
        this.nodeVersion = nodeManager.getCurrentNode().getVersion();
        this.nodeId = nodeManager.getCurrentNode().getNodeIdentifier();
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "vacuum",
                ImmutableList.of(
                        new Argument("SCHEMA_NAME", VARCHAR),
                        new Argument("TABLE_NAME", VARCHAR),
                        // Databricks's VACUUM has 7d default. We decided not to pick the default here.
                        new Argument("RETENTION", VARCHAR)),
                VACUUM.bindTo(this));
    }

    public void vacuum(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            String schema,
            String table,
            String retention)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doVacuum(session, accessControl, schema, table, retention);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, format("Failure when vacuuming %s.%s with retention %s: %s", schema, table, retention, e), e);
        }
        catch (RuntimeException e) {
            // This is not categorized as TrinoException. All possible external failures should be handled explicitly.
            throw new RuntimeException(format("Failure when vacuuming %s.%s with retention %s: %s", schema, table, retention, e), e);
        }
    }

    private void doVacuum(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            String schema,
            String table,
            String retention)
            throws IOException
    {
        checkProcedureArgument(schema != null, "schema_name cannot be null");
        checkProcedureArgument(!schema.isEmpty(), "schema_name cannot be empty");
        checkProcedureArgument(table != null, "table_name cannot be null");
        checkProcedureArgument(!table.isEmpty(), "table_name cannot be empty");
        checkProcedureArgument(retention != null, "retention cannot be null");

        Duration retentionDuration = Duration.valueOf(retention);
        Duration minRetention = getVacuumMinRetention(session);
        boolean isVacuumLoggingEnabled = isVacuumLoggingEnabled(session);
        checkProcedureArgument(
                retentionDuration.compareTo(minRetention) >= 0,
                "Retention specified (%s) is shorter than the minimum retention configured in the system (%s). " +
                        "Minimum retention can be changed with %s configuration property or %s.%s session property",
                retentionDuration,
                minRetention,
                DeltaLakeConfig.VACUUM_MIN_RETENTION,
                catalogName,
                DeltaLakeSessionProperties.VACUUM_MIN_RETENTION);

        Instant threshold = Instant.now().minusMillis(retentionDuration.toMillis());

        DeltaLakeMetadata metadata = metadataFactory.create(session.getIdentity());
        metadata.beginQuery(session);
        try (UncheckedCloseable ignore = () -> metadata.cleanupQuery(session)) {
            SchemaTableName tableName = new SchemaTableName(schema, table);
            ConnectorTableHandle connectorTableHandle = metadata.getTableHandle(session, tableName, Optional.empty(), Optional.empty());
            checkProcedureArgument(connectorTableHandle != null, "Table '%s' does not exist", tableName);
            DeltaLakeTableHandle handle = checkValidTableHandle(connectorTableHandle);

            accessControl.checkCanInsertIntoTable(null, tableName);
            accessControl.checkCanDeleteFromTable(null, tableName);

            checkUnsupportedUniversalFormat(handle.getMetadataEntry());

            ProtocolEntry protocolEntry = handle.getProtocolEntry();
            if (protocolEntry.minWriterVersion() > MAX_WRITER_VERSION) {
                throw new TrinoException(NOT_SUPPORTED, "Cannot execute vacuum procedure with %d writer version".formatted(protocolEntry.minWriterVersion()));
            }
            Set<String> writerFeatures = protocolEntry.writerFeatures().orElse(ImmutableSet.of());
            Set<String> unsupportedWriterFeatures = unsupportedWriterFeatures(protocolEntry.writerFeatures().orElse(ImmutableSet.of()));
            if (!unsupportedWriterFeatures.isEmpty()) {
                throw new TrinoException(NOT_SUPPORTED, "Cannot execute vacuum procedure with %s writer features".formatted(unsupportedWriterFeatures));
            }
            if (writerFeatures.contains(DELETION_VECTORS_FEATURE_NAME)) {
                // TODO https://github.com/trinodb/trino/issues/22809 Add support for vacuuming tables with deletion vectors
                throw new TrinoException(NOT_SUPPORTED, "Cannot execute vacuum procedure with %s writer features".formatted(DELETION_VECTORS_FEATURE_NAME));
            }

            TableSnapshot tableSnapshot = metadata.getSnapshot(session, tableName, handle.getLocation(), Optional.of(handle.getReadVersion()));
            String tableLocationString = tableSnapshot.getTableLocation();
            String transactionLogDir = getTransactionLogDir(tableLocationString);
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            String commonPathPrefix = tableLocationString.endsWith("/") ? tableLocationString : tableLocationString + "/";
            String queryId = session.getQueryId();

            // Retain all active files and every file removed by a "recent" transaction (except for the oldest "recent").
            // Any remaining file are not live, and not needed to read any "recent" snapshot.
            List<Long> recentVersions = transactionLogAccess.getPastTableVersions(fileSystem, transactionLogDir, threshold, tableSnapshot.getVersion());
            Set<String> retainedPaths;
            try (Stream<AddFileEntry> activeAddEntries = transactionLogAccess.getActiveFiles(
                    session,
                    tableSnapshot,
                    handle.getMetadataEntry(),
                    handle.getProtocolEntry(),
                    TupleDomain.all(),
                    alwaysFalse())) {
                try (Stream<String> pathEntries = Stream.concat(
                        activeAddEntries
                                // paths can be absolute as well in case of shallow-cloned tables, and they shouldn't be deleted as part of vacuum because according to
                                // delta-protocol absolute paths are inherited from base table and the vacuum procedure should only list and delete local file references
                                .map(AddFileEntry::getPath),
                        transactionLogAccess.getJsonEntries(
                                        fileSystem,
                                        transactionLogDir,
                                        // discard oldest "recent" snapshot, since we take RemoveFileEntry only, to identify files that are no longer
                                        // active files, but still needed to read a "recent" snapshot
                                        recentVersions.stream().sorted(naturalOrder())
                                                .skip(1)
                                                .collect(toImmutableList()))
                                .map(DeltaLakeTransactionLogEntry::getRemove)
                                .filter(Objects::nonNull)
                                .map(RemoveFileEntry::path))) {
                    retainedPaths = pathEntries
                            .peek(path -> checkState(!path.startsWith(tableLocationString), "Unexpected absolute path in transaction log: %s", path))
                            .collect(toImmutableSet());
                }
            }

            log.debug(
                    "[%s] attempting to vacuum table %s [%s] with %s retention (expiry threshold %s). %s data file paths marked for retention",
                    queryId,
                    tableName,
                    tableLocationString,
                    retention,
                    threshold,
                    retainedPaths.size());

            Location tableLocation = Location.of(tableLocationString);
            long allPathsChecked = 0;
            long transactionLogFiles = 0;
            long retainedKnownFiles = 0;
            long retainedUnknownFiles = 0;
            List<TrinoInputFile> filesToDelete = new ArrayList<>();
            Set<String> vacuumedDirectories = new HashSet<>();
            vacuumedDirectories.add(tableLocation.path());
            long filesToDeleteSize = 0;

            FileIterator listing = fileSystem.listFiles(tableLocation);
            while (listing.hasNext()) {
                FileEntry entry = listing.next();

                String location = entry.location().toString();
                checkState(
                        location.startsWith(commonPathPrefix),
                        "Unexpected path [%s] returned when listing files under [%s]",
                        location,
                        tableLocationString);
                String relativePath = location.substring(commonPathPrefix.length());
                if (relativePath.isEmpty()) {
                    // A file returned for "tableLocationString/", might be possible on S3.
                    continue;
                }
                allPathsChecked++;
                Location parentDirectory = entry.location().parentDirectory();
                while (!areDirectoryLocationsEquivalent(parentDirectory, tableLocation)) {
                    vacuumedDirectories.add(parentDirectory.path());
                    parentDirectory = parentDirectory.parentDirectory();
                }

                // ignore tableLocationString/_delta_log/**
                if (relativePath.equals(TRANSACTION_LOG_DIRECTORY) || relativePath.startsWith(TRANSACTION_LOG_DIRECTORY + "/")) {
                    log.debug("[%s] skipping a file inside transaction log dir: %s", queryId, location);
                    transactionLogFiles++;
                    continue;
                }

                // skip retained files
                if (retainedPaths.contains(relativePath)) {
                    log.debug("[%s] retaining a known file: %s", queryId, location);
                    retainedKnownFiles++;
                    continue;
                }

                // ignore recently created files
                Instant modificationTime = entry.lastModified();
                if (!modificationTime.isBefore(threshold)) {
                    log.debug("[%s] retaining an unknown file %s with modification time %s", queryId, location, modificationTime);
                    retainedUnknownFiles++;
                    continue;
                }
                log.debug("[%s] deleting file [%s] with modification time %s", queryId, location, modificationTime);

                Location fileLocation = Location.of(location);
                TrinoInputFile inputFile = fileSystem.newInputFile(fileLocation);
                filesToDelete.add(inputFile);
                filesToDeleteSize += inputFile.length();
            }
            long readVersion = handle.getReadVersion();
            if (isVacuumLoggingEnabled) {
                logVacuumStart(session, handle.location(), readVersion, filesToDelete.size(), filesToDeleteSize);
            }
            int totalFilesToDelete = filesToDelete.size();
            int batchCount = (int) Math.ceil((double) totalFilesToDelete / DELETE_BATCH_SIZE);
            try {
                for (int batchNumber = 0; batchNumber < batchCount; batchNumber++) {
                    int start = batchNumber * DELETE_BATCH_SIZE;
                    int end = Math.min(start + DELETE_BATCH_SIZE, totalFilesToDelete);

                    List<TrinoInputFile> batch = filesToDelete.subList(start, end);
                    fileSystem.deleteFiles(batch.stream().map(TrinoInputFile::location).collect(toImmutableList()));
                }
            }
            catch (IOException e) {
                if (isVacuumLoggingEnabled) {
                    // This mimics Delta behaviour where it sets metrics to 0 in case of a failure
                    logVacuumEnd(session, handle.location(), readVersion, 0, 0, "FAILED");
                }
                throw e;
            }
            if (isVacuumLoggingEnabled) {
                logVacuumEnd(session, handle.location(), readVersion, filesToDelete.size(), vacuumedDirectories.size(), "COMPLETED");
            }
            log.info(
                    "[%s] finished vacuuming table %s [%s]: files checked: %s; metadata files: %s; retained known files: %s; retained unknown files: %s; removed files: %s",
                    queryId,
                    tableName,
                    tableLocationString,
                    allPathsChecked,
                    transactionLogFiles,
                    retainedKnownFiles,
                    retainedUnknownFiles,
                    totalFilesToDelete);
        }
    }

    private void logVacuumStart(ConnectorSession session, String location, long readVersion, long numFilesToDelete, long filesToDeleteSize)
            throws IOException
    {
        long createdTime = System.currentTimeMillis();
        long commitVersion = readVersion + 1;

        TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriterWithoutTransactionIsolation(session, location);
        transactionLogWriter.appendCommitInfoEntry(getCommitInfoEntry(
                session,
                commitVersion,
                createdTime,
                "VACUUM START",
                ImmutableMap.of("queryId", session.getQueryId()),
                ImmutableMap.of("numFilesToDelete", String.valueOf(numFilesToDelete), "sizeOfDataToDelete", String.valueOf(filesToDeleteSize)),
                readVersion));
        transactionLogWriter.flush();
    }

    private void logVacuumEnd(ConnectorSession session, String location, long readVersion, int numDeletedFiles, int numVacuumedDirectories, String status)
            throws IOException
    {
        long createdTime = System.currentTimeMillis();
        long commitVersion = readVersion + 2;

        TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriterWithoutTransactionIsolation(session, location);
        transactionLogWriter.appendCommitInfoEntry(getCommitInfoEntry(
                session,
                commitVersion,
                createdTime,
                "VACUUM END",
                ImmutableMap.of("queryId", session.getQueryId(), "status", status),
                ImmutableMap.of("numDeletedFiles", String.valueOf(numDeletedFiles), "numVacuumedDirectories", String.valueOf(numVacuumedDirectories)),
                readVersion));
        transactionLogWriter.flush();
    }

    private CommitInfoEntry getCommitInfoEntry(
            ConnectorSession session,
            long commitVersion,
            long createdTime,
            String operation, Map<String,
            String> operationParameters,
            Map<String, String> operationMetrics,
            long readVersion)
    {
        return new CommitInfoEntry(
                commitVersion,
                createdTime,
                session.getUser(),
                session.getUser(),
                operation,
                operationParameters,
                null,
                null,
                "trino-" + nodeVersion + "-" + nodeId,
                readVersion,
                IsolationLevel.WRITESERIALIZABLE.getValue(),
                Optional.of(true),
                operationMetrics);
    }
}
