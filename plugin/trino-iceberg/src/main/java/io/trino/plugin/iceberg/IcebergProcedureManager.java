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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.procedure.IcebergExpireSnapshotsHandle;
import io.trino.plugin.iceberg.procedure.IcebergRemoveOrphanFilesHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableExecuteHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.union;
import static com.google.common.collect.Streams.concat;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getExpireSnapshotMinRetention;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getRemoveOrphanFilesMinRetention;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.EXPIRE_SNAPSHOTS;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.REMOVE_ORPHAN_FILES;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.ReachableFileUtil.metadataFileLocations;
import static org.apache.iceberg.ReachableFileUtil.versionHintLocation;
import static org.apache.iceberg.TableProperties.WRITE_LOCATION_PROVIDER_IMPL;

public class IcebergProcedureManager
{
    private static final Logger log = Logger.get(IcebergProcedureManager.class);

    private static final int CLEANING_UP_PROCEDURES_MAX_SUPPORTED_TABLE_VERSION = 2;

    private final TrinoCatalog catalog;
    private final HdfsEnvironment hdfsEnvironment;

    public IcebergProcedureManager(TrinoCatalog catalog, HdfsEnvironment hdfsEnvironment)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    public void executeExpireSnapshots(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        IcebergExpireSnapshotsHandle expireSnapshotsHandle = (IcebergExpireSnapshotsHandle) executeHandle.getProcedureHandle();

        Table table = catalog.loadTable(session, executeHandle.getSchemaTableName());
        Duration retention = requireNonNull(expireSnapshotsHandle.getRetentionThreshold(), "retention is null");
        validateTableExecuteParameters(
                table,
                executeHandle.getSchemaTableName(),
                EXPIRE_SNAPSHOTS.name(),
                retention,
                getExpireSnapshotMinRetention(session),
                IcebergConfig.EXPIRE_SNAPSHOTS_MIN_RETENTION,
                IcebergSessionProperties.EXPIRE_SNAPSHOTS_MIN_RETENTION);

        long expireTimestampMillis = session.getStart().toEpochMilli() - retention.toMillis();
        expireSnapshots(table, expireTimestampMillis, session, executeHandle.getSchemaTableName());
    }

    private void expireSnapshots(Table table, long expireTimestamp, ConnectorSession session, SchemaTableName schemaTableName)
    {
        Set<String> originalFiles = buildSetOfValidFiles(table);
        table.expireSnapshots().expireOlderThan(expireTimestamp).cleanExpiredFiles(false).commit();
        Set<String> validFiles = buildSetOfValidFiles(table);
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session), new Path(table.location()));
            Sets.SetView<String> filesToDelete = difference(originalFiles, validFiles);
            for (String filePath : filesToDelete) {
                log.debug("Deleting file %s while expiring snapshots %s", filePath, schemaTableName.getTableName());
                fileSystem.delete(new Path(filePath), false);
            }
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed accessing data for table: " + schemaTableName, e);
        }
    }

    public void executeRemoveOrphanFiles(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        IcebergRemoveOrphanFilesHandle removeOrphanFilesHandle = (IcebergRemoveOrphanFilesHandle) executeHandle.getProcedureHandle();

        Table table = catalog.loadTable(session, executeHandle.getSchemaTableName());
        Duration retention = requireNonNull(removeOrphanFilesHandle.getRetentionThreshold(), "retention is null");
        validateTableExecuteParameters(
                table,
                executeHandle.getSchemaTableName(),
                REMOVE_ORPHAN_FILES.name(),
                retention,
                getRemoveOrphanFilesMinRetention(session),
                IcebergConfig.REMOVE_ORPHAN_FILES_MIN_RETENTION,
                IcebergSessionProperties.REMOVE_ORPHAN_FILES_MIN_RETENTION);

        long expireTimestampMillis = session.getStart().toEpochMilli() - retention.toMillis();
        removeOrphanFiles(table, session, executeHandle.getSchemaTableName(), expireTimestampMillis);
        removeOrphanMetadataFiles(table, session, executeHandle.getSchemaTableName(), expireTimestampMillis);
    }

    private void removeOrphanFiles(Table table, ConnectorSession session, SchemaTableName schemaTableName, long expireTimestamp)
    {
        Set<String> validDataFilePaths = stream(table.snapshots())
                .map(Snapshot::snapshotId)
                .flatMap(snapshotId -> stream(table.newScan().useSnapshot(snapshotId).planFiles()))
                // compare only paths not to delete too many files, see https://github.com/apache/iceberg/pull/2890
                .map(fileScanTask -> URI.create(fileScanTask.file().path().toString()).getPath())
                .collect(toImmutableSet());
        Set<String> validDeleteFilePaths = stream(table.snapshots())
                .map(Snapshot::snapshotId)
                .flatMap(snapshotId -> stream(table.newScan().useSnapshot(snapshotId).planFiles()))
                .flatMap(fileScanTask -> fileScanTask.deletes().stream().map(deleteFile -> URI.create(deleteFile.path().toString()).getPath()))
                .collect(Collectors.toUnmodifiableSet());
        scanAndDeleteInvalidFiles(table, session, schemaTableName, expireTimestamp, union(validDataFilePaths, validDeleteFilePaths), "/data");
    }

    private void removeOrphanMetadataFiles(Table table, ConnectorSession session, SchemaTableName schemaTableName, long expireTimestamp)
    {
        ImmutableSet<String> manifests = stream(table.snapshots())
                .flatMap(snapshot -> snapshot.allManifests().stream())
                .map(ManifestFile::path)
                .collect(toImmutableSet());
        List<String> manifestLists = ReachableFileUtil.manifestListLocations(table);
        List<String> otherMetadataFiles = concat(
                metadataFileLocations(table, false).stream(),
                Stream.of(versionHintLocation(table)))
                .collect(toImmutableList());
        Set<String> validMetadataFiles = concat(manifests.stream(), manifestLists.stream(), otherMetadataFiles.stream())
                .map(path -> URI.create(path).getPath())
                .collect(toImmutableSet());
        scanAndDeleteInvalidFiles(table, session, schemaTableName, expireTimestamp, validMetadataFiles, "/metadata");
    }

    private void scanAndDeleteInvalidFiles(Table table, ConnectorSession session, SchemaTableName schemaTableName, long expireTimestamp, Set<String> validFiles, String subfolder)
    {
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session), new Path(table.location()));
            RemoteIterator<LocatedFileStatus> allFiles = fileSystem.listFiles(new Path(table.location() + subfolder), true);
            while (allFiles.hasNext()) {
                LocatedFileStatus file = allFiles.next();
                if (file.isFile()) {
                    String normalizedPath = file.getPath().toUri().getPath();
                    if (file.getModificationTime() < expireTimestamp && !validFiles.contains(normalizedPath)) {
                        log.debug("Deleting %s file while removing orphan files %s", file.getPath().toString(), schemaTableName.getTableName());
                        fileSystem.delete(file.getPath(), false);
                    }
                    else {
                        log.debug("%s file retained while removing orphan files %s", file.getPath().toString(), schemaTableName.getTableName());
                    }
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed accessing data for table: " + schemaTableName, e);
        }
    }

    private static void validateTableExecuteParameters(
            Table table,
            SchemaTableName schemaTableName,
            String procedureName,
            Duration retentionThreshold,
            Duration minRetention,
            String minRetentionParameterName,
            String sessionMinRetentionParameterName)
    {
        int tableFormatVersion = ((BaseTable) table).operations().current().formatVersion();
        if (tableFormatVersion > CLEANING_UP_PROCEDURES_MAX_SUPPORTED_TABLE_VERSION) {
            // It is not known if future version won't bring any new kind of metadata or data files
            // because of the way procedures are implemented it is safer to fail here than to potentially remove
            // files that should stay there
            throw new TrinoException(NOT_SUPPORTED, format("%s is not supported for Iceberg table format version > %d. " +
                            "Table %s format version is %s.",
                    procedureName,
                    CLEANING_UP_PROCEDURES_MAX_SUPPORTED_TABLE_VERSION,
                    schemaTableName,
                    tableFormatVersion));
        }
        Map<String, String> properties = table.properties();
        if (properties.containsKey(WRITE_LOCATION_PROVIDER_IMPL)) {
            throw new TrinoException(NOT_SUPPORTED, "Table " + schemaTableName + " specifies " + properties.get(WRITE_LOCATION_PROVIDER_IMPL) +
                    " as a location provider. Writing to Iceberg tables with custom location provider is not supported.");
        }

        Duration retention = requireNonNull(retentionThreshold, "retention is null");
        checkProcedureArgument(retention.compareTo(minRetention) >= 0,
                "Retention specified (%s) is shorter than the minimum retention configured in the system (%s). " +
                        "Minimum retention can be changed with %s configuration property or iceberg.%s session property",
                retention,
                minRetention,
                minRetentionParameterName,
                sessionMinRetentionParameterName);
    }

    private static Set<String> buildSetOfValidFiles(Table table)
    {
        List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
        Stream<String> dataFiles = snapshots.stream()
                .map(Snapshot::snapshotId)
                .flatMap(snapshotId -> stream(table.newScan().useSnapshot(snapshotId).planFiles()))
                .map(fileScanTask -> fileScanTask.file().path().toString());
        Stream<String> manifests = snapshots.stream()
                .flatMap(snapshot -> snapshot.allManifests().stream())
                .map(ManifestFile::path);
        Stream<String> manifestLists = snapshots.stream()
                .map(Snapshot::manifestListLocation);
        Stream<String> otherMetadataFiles = concat(
                metadataFileLocations(table, false).stream(),
                Stream.of(versionHintLocation(table)));
        return concat(dataFiles, manifests, manifestLists, otherMetadataFiles)
                .map(file -> URI.create(file).getPath())
                .collect(toImmutableSet());
    }
}
