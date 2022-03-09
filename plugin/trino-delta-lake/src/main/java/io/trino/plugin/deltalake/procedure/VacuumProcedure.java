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
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import io.trino.plugin.deltalake.DeltaLakeMetadataFactory;
import io.trino.plugin.deltalake.DeltaLakeSessionProperties;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.procedure.Procedure.Argument;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getVacuumMinRetention;
import static io.trino.plugin.deltalake.procedure.Procedures.checkProcedureArgument;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.TRANSACTION_LOG_DIRECTORY;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.block.MethodHandleUtil.methodHandle;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Comparator.naturalOrder;
import static java.util.Objects.requireNonNull;

public class VacuumProcedure
        implements Provider<Procedure>
{
    private static final Logger log = Logger.get(VacuumProcedure.class);

    private static final MethodHandle VACUUM = methodHandle(
            VacuumProcedure.class,
            "vacuum",
            ConnectorSession.class,
            String.class,
            String.class,
            String.class);

    private final CatalogName catalogName;
    private final HdfsEnvironment hdfsEnvironment;
    private final DeltaLakeMetadataFactory metadataFactory;
    private final TransactionLogAccess transactionLogAccess;

    @Inject
    public VacuumProcedure(
            CatalogName catalogName,
            HdfsEnvironment hdfsEnvironment,
            DeltaLakeMetadataFactory metadataFactory,
            TransactionLogAccess transactionLogAccess)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
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
            String schema,
            String table,
            String retention)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doVacuum(session, schema, table, retention);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            // This is not categorized as TrinoException. All possible external failures should be handled explicitly.
            throw new RuntimeException(format("Failure when vacuuming %s.%s with retention %s", schema, table, retention), e);
        }
    }

    private void doVacuum(
            ConnectorSession session,
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
        SchemaTableName tableName = new SchemaTableName(schema, table);
        DeltaLakeTableHandle handle = metadata.getTableHandle(session, tableName);
        checkProcedureArgument(handle != null, "Table '%s' does not exist", tableName);

        TableSnapshot tableSnapshot = transactionLogAccess.loadSnapshot(tableName, new Path(handle.getLocation()), session);
        Path tableLocation = tableSnapshot.getTableLocation();
        Path transactionLogDir = getTransactionLogDir(tableLocation);
        FileSystem fileSystem = hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session), tableLocation);
        String commonPathPrefix = tableLocation + "/";
        String queryId = session.getQueryId();

        // Retain all active files and every file removed by a "recent" transaction (except for the oldest "recent").
        // Any remaining file are not live, and not needed to read any "recent" snapshot.
        List<Long> recentVersions = transactionLogAccess.getPastTableVersions(fileSystem, transactionLogDir, threshold, tableSnapshot.getVersion());
        Set<String> retainedPaths = Stream.concat(
                        transactionLogAccess.getActiveFiles(tableSnapshot, session).stream()
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
                                .map(RemoveFileEntry::getPath))
                .peek(path -> checkState(!path.startsWith(tableLocation.toString()), "Unexpected absolute path in transaction log: %s", path))
                .collect(toImmutableSet());

        log.debug(
                "[%s] attempting to vacuum table %s [%s] with %s retention (expiry threshold %s). %s data file paths marked for retention",
                queryId,
                tableName,
                tableLocation,
                retention,
                threshold,
                retainedPaths.size());

        long nonFiles = 0;
        long allPathsChecked = 0;
        long transactionLogFiles = 0;
        long retainedKnownFiles = 0;
        long retainedUnknownFiles = 0;
        long removedFiles = 0;

        RemoteIterator<LocatedFileStatus> listing = fileSystem.listFiles(tableLocation, true);
        while (listing.hasNext()) {
            LocatedFileStatus fileStatus = listing.next();
            Path path = fileStatus.getPath();
            checkState(
                    path.toString().startsWith(commonPathPrefix),
                    "Unexpected path [%s] returned when listing files under [%s]",
                    path,
                    tableLocation);
            String relativePath = path.toString().substring(commonPathPrefix.length());
            if (relativePath.isEmpty()) {
                // A file returned for "tableLocation/", might be possible on S3.
                continue;
            }
            allPathsChecked++;

            // Ignore directories (and symlinks, etc.). If a partition directory is old and empty (or made empty by us), removing it could break concurrent writes
            // on file systems with directories (e.g. HDFS).
            // TODO Note: Databricks can delete directories during vacuum on s3. This might need to be revisited.
            if (!fileStatus.isFile()) {
                nonFiles++;
                continue;
            }

            // ignore tableLocation/_delta_log/**
            if (relativePath.equals(TRANSACTION_LOG_DIRECTORY) || relativePath.startsWith(TRANSACTION_LOG_DIRECTORY + "/")) {
                log.debug("[%s] skipping a file inside transaction log dir: %s", queryId, path);
                transactionLogFiles++;
                continue;
            }

            // skip retained files
            if (retainedPaths.contains(relativePath)) {
                log.debug("[%s] retaining a known file: %s", queryId, path);
                retainedKnownFiles++;
                continue;
            }

            // ignore recently created files
            long modificationTime = fileStatus.getModificationTime();
            Instant modificationInstant = Instant.ofEpochMilli(modificationTime);
            if (!modificationInstant.isBefore(threshold)) {
                log.debug("[%s] retaining an unknown file %s with modification time %s (%s)", queryId, path, modificationTime, modificationInstant);
                retainedUnknownFiles++;
                continue;
            }

            log.debug(
                    "[%s] deleting file [%s] with modification time %s (%s)",
                    queryId,
                    path,
                    modificationTime,
                    modificationInstant);
            if (!fileSystem.delete(path, false)) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to delete file: " + path);
            }
            removedFiles++;
        }

        log.info(
                "[%s] finished vacuuming table %s [%s]: files checked: %s; non-files: %s; metadata files: %s; retained known files: %s; retained unknown files: %s; removed files: %s",
                queryId,
                tableName,
                tableLocation,
                allPathsChecked,
                nonFiles,
                transactionLogFiles,
                retainedKnownFiles,
                retainedUnknownFiles,
                removedFiles);
    }
}
