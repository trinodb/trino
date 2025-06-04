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
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.Table;
import io.trino.plugin.base.util.UncheckedCloseable;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.TransactionalMetadata;
import io.trino.plugin.hive.TransactionalMetadataFactory;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.ArrayType;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static io.trino.metastore.Partitions.makePartName;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.hive.procedure.Procedures.checkIsPartitionedTable;
import static io.trino.plugin.hive.procedure.Procedures.checkPartitionColumns;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class RegisterPartitionProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle REGISTER_PARTITION;

    static {
        try {
            REGISTER_PARTITION = lookup().unreflect(RegisterPartitionProcedure.class.getMethod("registerPartition", ConnectorSession.class, ConnectorAccessControl.class, String.class, String.class, List.class, List.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final boolean allowRegisterPartition;
    private final TransactionalMetadataFactory hiveMetadataFactory;
    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public RegisterPartitionProcedure(HiveConfig hiveConfig, TransactionalMetadataFactory hiveMetadataFactory, TrinoFileSystemFactory fileSystemFactory)
    {
        this.allowRegisterPartition = hiveConfig.isAllowRegisterPartition();
        this.hiveMetadataFactory = requireNonNull(hiveMetadataFactory, "hiveMetadataFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "register_partition",
                ImmutableList.of(
                        new Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        new Procedure.Argument("TABLE_NAME", VARCHAR),
                        new Procedure.Argument("PARTITION_COLUMNS", new ArrayType(VARCHAR)),
                        new Procedure.Argument("PARTITION_VALUES", new ArrayType(VARCHAR)),
                        new Procedure.Argument("LOCATION", VARCHAR, false, null)),
                REGISTER_PARTITION.bindTo(this));
    }

    public void registerPartition(ConnectorSession session, ConnectorAccessControl accessControl, String schemaName, String tableName, List<String> partitionColumns, List<String> partitionValues, String location)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doRegisterPartition(session, accessControl, schemaName, tableName, partitionColumns, partitionValues, location);
        }
    }

    private void doRegisterPartition(ConnectorSession session, ConnectorAccessControl accessControl, String schemaName, String tableName, List<String> partitionColumns, List<String> partitionValues, String location)
    {
        checkProcedureArgument(schemaName != null, "schema_name cannot be null");
        checkProcedureArgument(tableName != null, "table_name cannot be null");
        checkProcedureArgument(partitionColumns != null, "partition_columns cannot be null");
        checkProcedureArgument(partitionValues != null, "partition_values cannot be null");

        if (!allowRegisterPartition) {
            throw new TrinoException(PERMISSION_DENIED, "register_partition procedure is disabled");
        }

        TransactionalMetadata hiveMetadata = hiveMetadataFactory.create(session.getIdentity(), true);
        hiveMetadata.beginQuery(session);
        try (UncheckedCloseable ignore = () -> hiveMetadata.cleanupQuery(session)) {
            SemiTransactionalHiveMetastore metastore = hiveMetadata.getMetastore();

            SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);

            Table table = metastore.getTable(schemaName, tableName)
                    .orElseThrow(() -> new TableNotFoundException(schemaTableName));

            accessControl.checkCanInsertIntoTable(null, schemaTableName);

            checkIsPartitionedTable(table);
            checkPartitionColumns(table, partitionColumns);

            Optional<Partition> partition = metastore.unsafeGetRawHiveMetastore().getPartition(table, partitionValues);
            if (partition.isPresent()) {
                String partitionName = makePartName(partitionColumns, partitionValues);
                throw new TrinoException(ALREADY_EXISTS, format("Partition [%s] is already registered with location %s", partitionName, partition.get().getStorage().getLocation()));
            }

            Location partitionLocation = Optional.ofNullable(location)
                    .map(Location::of)
                    .orElseGet(() -> Location.of(table.getStorage().getLocation()).appendPath(makePartName(partitionColumns, partitionValues)));

            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            try {
                if (!fileSystem.directoryExists(partitionLocation).orElse(true)) {
                    throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Partition location does not exist: " + partitionLocation);
                }
            }
            catch (IOException e) {
                throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed checking partition location: " + partitionLocation, e);
            }

            metastore.addPartition(
                    session,
                    table.getDatabaseName(),
                    table.getTableName(),
                    buildPartitionObject(session, table, partitionValues, partitionLocation),
                    partitionLocation,
                    Optional.empty(), // no need for failed attempts cleanup
                    PartitionStatistics.empty(),
                    false);

            metastore.commit();
        }
    }

    private static Partition buildPartitionObject(ConnectorSession session, Table table, List<String> partitionValues, Location location)
    {
        return Partition.builder()
                .setDatabaseName(table.getDatabaseName())
                .setTableName(table.getTableName())
                .setColumns(table.getDataColumns())
                .setValues(partitionValues)
                .setParameters(ImmutableMap.of(TRINO_QUERY_ID_NAME, session.getQueryId()))
                .withStorage(storage -> storage
                        .setStorageFormat(table.getStorage().getStorageFormat())
                        .setLocation(location.toString())
                        .setBucketProperty(table.getStorage().getBucketProperty())
                        .setSerdeParameters(table.getStorage().getSerdeParameters()))
                .build();
    }
}
