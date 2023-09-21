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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.filesystem.Location;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.PartitionStatistics;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Boolean.TRUE;
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

    private static final int BATCH_GET_PARTITIONS_BY_NAMES_MAX_PAGE_SIZE = 1000;

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
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public SyncPartitionMetadataProcedure(
            TransactionalMetadataFactory hiveMetadataFactory,
            HdfsEnvironment hdfsEnvironment)
    {
        this.hiveMetadataFactory = requireNonNull(hiveMetadataFactory, "hiveMetadataFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
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
        HdfsContext hdfsContext = new HdfsContext(session);
        SemiTransactionalHiveMetastore metastore = hiveMetadataFactory.create(session.getIdentity(), true).getMetastore();
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

        Path tableLocation = new Path(table.getStorage().getLocation());

        Set<String> partitionsToAdd;
        Set<String> partitionsToDrop;

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(hdfsContext, tableLocation);
            List<String> partitionsNamesInMetastore = metastore.getPartitionNames(schemaName, tableName)
                    .orElseThrow(() -> new TableNotFoundException(schemaTableName));
            List<String> partitionsInMetastore = getPartitionsInMetastore(schemaTableName, tableLocation, partitionsNamesInMetastore, metastore);
            List<String> partitionsInFileSystem = listDirectory(fileSystem, fileSystem.getFileStatus(tableLocation), table.getPartitionColumns(), table.getPartitionColumns().size(), caseSensitive).stream()
                    .map(fileStatus -> fileStatus.getPath().toUri())
                    .map(uri -> tableLocation.toUri().relativize(uri).getPath())
                    .collect(toImmutableList());

            // partitions in file system but not in metastore
            partitionsToAdd = difference(partitionsInFileSystem, partitionsInMetastore);
            // partitions in metastore but not in file system
            partitionsToDrop = difference(partitionsInMetastore, partitionsInFileSystem);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, e);
        }

        syncPartitions(partitionsToAdd, partitionsToDrop, syncMode, metastore, session, table);
    }

    private List<String> getPartitionsInMetastore(SchemaTableName schemaTableName, Path tableLocation, List<String> partitionsNames, SemiTransactionalHiveMetastore metastore)
    {
        ImmutableList.Builder<String> partitionsInMetastoreBuilder = ImmutableList.builderWithExpectedSize(partitionsNames.size());
        for (List<String> partitionsNamesBatch : Lists.partition(partitionsNames, BATCH_GET_PARTITIONS_BY_NAMES_MAX_PAGE_SIZE)) {
            metastore.getPartitionsByNames(schemaTableName.getSchemaName(), schemaTableName.getTableName(), partitionsNamesBatch).values().stream()
                    .filter(Optional::isPresent).map(Optional::get)
                    .map(partition -> new Path(partition.getStorage().getLocation()).toUri())
                    .map(uri -> tableLocation.toUri().relativize(uri).getPath())
                    .forEach(partitionsInMetastoreBuilder::add);
        }
        return partitionsInMetastoreBuilder.build();
    }

    private static List<FileStatus> listDirectory(FileSystem fileSystem, FileStatus current, List<Column> partitionColumns, int depth, boolean caseSensitive)
    {
        if (depth == 0) {
            return ImmutableList.of(current);
        }

        try {
            return Stream.of(fileSystem.listStatus(current.getPath()))
                    .filter(fileStatus -> isValidPartitionPath(fileStatus, partitionColumns.get(partitionColumns.size() - depth), caseSensitive))
                    .flatMap(directory -> listDirectory(fileSystem, directory, partitionColumns, depth - 1, caseSensitive).stream())
                    .collect(toImmutableList());
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, e);
        }
    }

    private static boolean isValidPartitionPath(FileStatus file, Column column, boolean caseSensitive)
    {
        String path = file.getPath().getName();
        if (!caseSensitive) {
            path = path.toLowerCase(ENGLISH);
        }
        String prefix = column.getName() + '=';
        return file.isDirectory() && path.startsWith(prefix);
    }

    // calculate relative complement of set b with respect to set a
    private static Set<String> difference(List<String> a, List<String> b)
    {
        return Sets.difference(new HashSet<>(a), new HashSet<>(b));
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
                .setParameters(ImmutableMap.of(PRESTO_QUERY_ID_NAME, session.getQueryId()))
                .withStorage(storage -> storage
                        .setStorageFormat(table.getStorage().getStorageFormat())
                        .setLocation(new Path(table.getStorage().getLocation(), partitionName).toString())
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
