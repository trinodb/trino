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
package io.trino.plugin.hive.procedure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.util.UncheckedCloseable;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.TransactionalMetadata;
import io.trino.plugin.hive.TransactionalMetadataFactory;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.procedure.Procedure.Argument;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Sets.difference;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Boolean.TRUE;
import static java.lang.String.join;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SyncPartitionMetadataProcedure
        implements Provider<Procedure>
{
    public enum SyncMode
    {
        ADD, DROP, FULL
    }

    private static final MethodHandle SYNC_PARTITION_METADATA;

    static {
        try {
            SYNC_PARTITION_METADATA = lookup().unreflect(SyncPartitionMetadataProcedure.class.getMethod("syncPartitionMetadata", ConnectorSession.class, ConnectorAccessControl.class, String.class, String.class, String.class, boolean.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final TransactionalMetadataFactory hiveMetadataFactory;
    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public SyncPartitionMetadataProcedure(
            TransactionalMetadataFactory hiveMetadataFactory,
            TrinoFileSystemFactory fileSystemFactory)
    {
        this.hiveMetadataFactory = requireNonNull(hiveMetadataFactory, "hiveMetadataFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "sync_partition_metadata",
                ImmutableList.of(
                        new Argument("SCHEMA_NAME", VARCHAR),
                        new Argument("TABLE_NAME", VARCHAR),
                        new Argument("MODE", VARCHAR),
                        new Argument("CASE_SENSITIVE", BOOLEAN, false, TRUE)),
                SYNC_PARTITION_METADATA.bindTo(this));
    }

    public void syncPartitionMetadata(ConnectorSession session, ConnectorAccessControl accessControl, String schemaName, String tableName, String mode, boolean caseSensitive)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doSyncPartitionMetadata(session, accessControl, schemaName, tableName, mode, caseSensitive);
        }
    }

    private void doSyncPartitionMetadata(ConnectorSession session, ConnectorAccessControl accessControl, String schemaName, String tableName, String mode, boolean caseSensitive)
    {
        checkProcedureArgument(schemaName != null, "schema_name cannot be null");
        checkProcedureArgument(tableName != null, "table_name cannot be null");
        checkProcedureArgument(mode != null, "mode cannot be null");

        SyncMode syncMode = toSyncMode(mode);
        TransactionalMetadata hiveMetadata = hiveMetadataFactory.create(session.getIdentity(), true);
        hiveMetadata.beginQuery(session);
        try (UncheckedCloseable ignore = () -> hiveMetadata.cleanupQuery(session)) {
            SemiTransactionalHiveMetastore metastore = hiveMetadata.getMetastore();
            SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);

            Table table = metastore.getTable(schemaName, tableName)
                    .orElseThrow(() -> new TableNotFoundException(schemaTableName));
            if (table.getPartitionColumns().isEmpty()) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Table is not partitioned: " + schemaTableName);
            }

            if (syncMode == SyncMode.ADD || syncMode == SyncMode.FULL) {
                accessControl.checkCanInsertIntoTable(null, new SchemaTableName(schemaName, tableName));
            }
            if (syncMode == SyncMode.DROP || syncMode == SyncMode.FULL) {
                accessControl.checkCanDeleteFromTable(null, new SchemaTableName(schemaName, tableName));
            }

            Location tableLocation = Location.of(table.getStorage().getLocation());

            Set<String> partitionsInMetastore = metastore.getPartitionNames(schemaName, tableName)
                    .map(ImmutableSet::copyOf)
                    .orElseThrow(() -> new TableNotFoundException(schemaTableName));
            Set<String> partitionsInFileSystem = listPartitions(fileSystemFactory.create(session), tableLocation, table.getPartitionColumns(), caseSensitive);

            // partitions in file system but not in metastore
            Set<String> partitionsToAdd = difference(partitionsInFileSystem, partitionsInMetastore);

            // partitions in metastore but not in file system
            Set<String> partitionsToDrop = difference(partitionsInMetastore, partitionsInFileSystem);

            syncPartitions(partitionsToAdd, partitionsToDrop, syncMode, metastore, session, table);
        }
    }

    private static Set<String> listPartitions(TrinoFileSystem fileSystem, Location directory, List<Column> partitionColumns, boolean caseSensitive)
    {
        return doListPartitions(fileSystem, directory, partitionColumns, partitionColumns.size(), caseSensitive, ImmutableList.of());
    }

    private static Set<String> doListPartitions(TrinoFileSystem fileSystem, Location directory, List<Column> partitionColumns, int depth, boolean caseSensitive, List<String> partitions)
    {
        if (depth == 0) {
            return ImmutableSet.of(join("/", partitions));
        }

        ImmutableSet.Builder<String> result = ImmutableSet.builder();
        for (Location location : listDirectories(fileSystem, directory)) {
            String path = listedDirectoryName(directory, location);
            Column column = partitionColumns.get(partitionColumns.size() - depth);
            if (!isValidPartitionPath(path, column, caseSensitive)) {
                continue;
            }
            List<String> current = ImmutableList.<String>builder().addAll(partitions).add(path).build();
            result.addAll(doListPartitions(fileSystem, location, partitionColumns, depth - 1, caseSensitive, current));
        }
        return result.build();
    }

    private static Set<Location> listDirectories(TrinoFileSystem fileSystem, Location directory)
    {
        try {
            return fileSystem.listDirectories(directory);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, e);
        }
    }

    private static String listedDirectoryName(Location directory, Location location)
    {
        String prefix = directory.path();
        if (!prefix.endsWith("/")) {
            prefix += "/";
        }
        String path = location.path();
        verify(path.endsWith("/"), "path does not end with slash: %s", location);
        verify(path.startsWith(prefix), "path [%s] is not a child of directory [%s]", location, directory);
        return path.substring(prefix.length(), path.length() - 1);
    }

    private static boolean isValidPartitionPath(String path, Column column, boolean caseSensitive)
    {
        if (!caseSensitive) {
            path = path.toLowerCase(ENGLISH);
        }
        return path.startsWith(column.getName() + '=');
    }

    private static void syncPartitions(
            Set<String> partitionsToAdd,
            Set<String> partitionsToDrop,
            SyncMode syncMode,
            SemiTransactionalHiveMetastore metastore,
            ConnectorSession session,
            Table table)
    {
        if (syncMode == SyncMode.ADD || syncMode == SyncMode.FULL) {
            addPartitions(metastore, session, table, partitionsToAdd);
        }
        if (syncMode == SyncMode.DROP || syncMode == SyncMode.FULL) {
            dropPartitions(metastore, session, table, partitionsToDrop);
        }
        metastore.commit();
    }

    private static void addPartitions(
            SemiTransactionalHiveMetastore metastore,
            ConnectorSession session,
            Table table,
            Set<String> partitions)
    {
        for (String name : partitions) {
            metastore.addPartition(
                    session,
                    table.getDatabaseName(),
                    table.getTableName(),
                    buildPartitionObject(session, table, name),
                    Location.of(table.getStorage().getLocation()).appendPath(name),
                    Optional.empty(), // no need for failed attempts cleanup
                    PartitionStatistics.empty(),
                    false);
        }
    }

    private static void dropPartitions(
            SemiTransactionalHiveMetastore metastore,
            ConnectorSession session,
            Table table,
            Set<String> partitions)
    {
        for (String name : partitions) {
            metastore.dropPartition(
                    session,
                    table.getDatabaseName(),
                    table.getTableName(),
                    extractPartitionValues(name),
                    false);
        }
    }

    private static Partition buildPartitionObject(ConnectorSession session, Table table, String partitionName)
    {
        return Partition.builder()
                .setDatabaseName(table.getDatabaseName())
                .setTableName(table.getTableName())
                .setColumns(table.getDataColumns())
                .setValues(extractPartitionValues(partitionName))
                .setParameters(ImmutableMap.of(TRINO_QUERY_ID_NAME, session.getQueryId()))
                .withStorage(storage -> storage
                        .setStorageFormat(table.getStorage().getStorageFormat())
                        .setLocation(Location.of(table.getStorage().getLocation()).appendPath(partitionName).toString())
                        .setBucketProperty(table.getStorage().getBucketProperty())
                        .setSerdeParameters(table.getStorage().getSerdeParameters()))
                .build();
    }

    private static SyncMode toSyncMode(String mode)
    {
        try {
            return SyncMode.valueOf(mode.toUpperCase(ENGLISH));
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Invalid partition metadata sync mode: " + mode);
        }
    }
}
