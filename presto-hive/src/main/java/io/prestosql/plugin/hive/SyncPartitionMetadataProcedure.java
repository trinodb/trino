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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.procedure.Procedure.Argument;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.prestosql.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.prestosql.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.prestosql.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.prestosql.spi.block.MethodHandleUtil.methodHandle;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SyncPartitionMetadataProcedure
        implements Provider<Procedure>
{
    public enum SyncMode
    {
        ADD, DROP, FULL
    }

    private static final MethodHandle SYNC_PARTITION_METADATA = methodHandle(
            SyncPartitionMetadataProcedure.class,
            "syncPartitionMetadata",
            ConnectorSession.class,
            String.class,
            String.class,
            String.class);

    private final Supplier<TransactionalMetadata> hiveMetadataFactory;
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public SyncPartitionMetadataProcedure(
            Supplier<TransactionalMetadata> hiveMetadataFactory,
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
                        new Argument("schema_name", VARCHAR),
                        new Argument("table_name", VARCHAR),
                        new Argument("mode", VARCHAR)),
                SYNC_PARTITION_METADATA.bindTo(this));
    }

    public void syncPartitionMetadata(ConnectorSession session, String schemaName, String tableName, String mode)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doSyncPartitionMetadata(session, schemaName, tableName, mode);
        }
    }

    private void doSyncPartitionMetadata(ConnectorSession session, String schemaName, String tableName, String mode)
    {
        SyncMode syncMode = toSyncMode(mode);
        HdfsContext hdfsContext = new HdfsContext(session, schemaName, tableName);
        HiveIdentity identity = new HiveIdentity(session);
        SemiTransactionalHiveMetastore metastore = ((HiveMetadata) hiveMetadataFactory.get()).getMetastore();
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);

        Table table = metastore.getTable(identity, schemaName, tableName)
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));
        if (table.getPartitionColumns().isEmpty()) {
            throw new PrestoException(INVALID_PROCEDURE_ARGUMENT, "Table is not partitioned: " + schemaTableName);
        }
        Path tableLocation = new Path(table.getStorage().getLocation());

        Set<String> partitionsToAdd;
        Set<String> partitionsToDrop;

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(hdfsContext, tableLocation);
            List<String> partitionsInMetastore = metastore.getPartitionNames(identity, schemaName, tableName)
                    .orElseThrow(() -> new TableNotFoundException(schemaTableName));
            List<String> partitionsInFileSystem = listDirectory(fileSystem, fileSystem.getFileStatus(tableLocation), table.getPartitionColumns(), table.getPartitionColumns().size()).stream()
                    .map(fileStatus -> fileStatus.getPath().toUri())
                    .map(uri -> tableLocation.toUri().relativize(uri).getPath())
                    .collect(toImmutableList());

            // partitions in file system but not in metastore
            partitionsToAdd = difference(partitionsInFileSystem, partitionsInMetastore);
            // partitions in metastore but not in file system
            partitionsToDrop = difference(partitionsInMetastore, partitionsInFileSystem);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, e);
        }

        syncPartitions(partitionsToAdd, partitionsToDrop, syncMode, metastore, session, table);
    }

    private static List<FileStatus> listDirectory(FileSystem fileSystem, FileStatus current, List<Column> partitionColumns, int depth)
    {
        if (depth == 0) {
            return ImmutableList.of(current);
        }

        try {
            return Stream.of(fileSystem.listStatus(current.getPath()))
                    .filter(fileStatus -> isValidPartitionPath(fileStatus, partitionColumns.get(partitionColumns.size() - depth)))
                    .flatMap(directory -> listDirectory(fileSystem, directory, partitionColumns, depth - 1).stream())
                    .collect(toImmutableList());
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, e);
        }
    }

    private static boolean isValidPartitionPath(FileStatus file, Column column)
    {
        Path path = file.getPath();
        String prefix = column.getName() + '=';
        return file.isDirectory() && path.getName().startsWith(prefix);
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
                    new Path(table.getStorage().getLocation(), name),
                    PartitionStatistics.empty());
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
                    extractPartitionValues(name));
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
            throw new PrestoException(INVALID_PROCEDURE_ARGUMENT, "Invalid partition metadata sync mode: " + mode);
        }
    }
}
