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
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.base.util.UncheckedCloseable;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveInsertTableHandle;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.LocationService;
import io.trino.plugin.hive.LocationService.WriteInfo;
import io.trino.plugin.hive.PartitionUpdate;
import io.trino.plugin.hive.PartitionUpdate.UpdateMode;
import io.trino.plugin.hive.TransactionalMetadata;
import io.trino.plugin.hive.TransactionalMetadataFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.procedure.Procedure.Argument;
import io.trino.spi.type.ArrayType;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metastore.Partitions.makePartName;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class CreateEmptyPartitionProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle CREATE_EMPTY_PARTITION;

    static {
        try {
            CREATE_EMPTY_PARTITION = lookup().unreflect(CreateEmptyPartitionProcedure.class.getMethod("createEmptyPartition", ConnectorSession.class, ConnectorAccessControl.class, String.class, String.class, List.class, List.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final TransactionalMetadataFactory hiveMetadataFactory;
    private final LocationService locationService;
    private final JsonCodec<PartitionUpdate> partitionUpdateJsonCodec;

    @Inject
    public CreateEmptyPartitionProcedure(TransactionalMetadataFactory hiveMetadataFactory, LocationService locationService, JsonCodec<PartitionUpdate> partitionUpdateCodec)
    {
        this.hiveMetadataFactory = requireNonNull(hiveMetadataFactory, "hiveMetadataFactory is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.partitionUpdateJsonCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "create_empty_partition",
                ImmutableList.of(
                        new Argument("SCHEMA_NAME", VARCHAR),
                        new Argument("TABLE_NAME", VARCHAR),
                        new Argument("PARTITION_COLUMNS", new ArrayType(VARCHAR)),
                        new Argument("PARTITION_VALUES", new ArrayType(VARCHAR))),
                CREATE_EMPTY_PARTITION.bindTo(this));
    }

    public void createEmptyPartition(ConnectorSession session, ConnectorAccessControl accessControl, String schema, String table, List<String> partitionColumnNames, List<String> partitionValues)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doCreateEmptyPartition(session, accessControl, schema, table, partitionColumnNames, partitionValues);
        }
    }

    private void doCreateEmptyPartition(ConnectorSession session, ConnectorAccessControl accessControl, String schemaName, String tableName, List<String> partitionColumnNames, List<String> partitionValues)
    {
        checkProcedureArgument(schemaName != null, "schema_name cannot be null");
        checkProcedureArgument(tableName != null, "table_name cannot be null");
        checkProcedureArgument(partitionColumnNames != null, "partition_columns cannot be null");
        checkProcedureArgument(partitionValues != null, "partition_values cannot be null");

        TransactionalMetadata hiveMetadata = hiveMetadataFactory.create(session.getIdentity(), true);
        hiveMetadata.beginQuery(session);
        try (UncheckedCloseable ignore = () -> hiveMetadata.cleanupQuery(session)) {
            HiveTableHandle tableHandle = (HiveTableHandle) hiveMetadata.getTableHandle(session, new SchemaTableName(schemaName, tableName), Optional.empty(), Optional.empty());
            if (tableHandle == null) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, format("Table '%s' does not exist", new SchemaTableName(schemaName, tableName)));
            }

            accessControl.checkCanInsertIntoTable(null, new SchemaTableName(schemaName, tableName));

            List<String> actualPartitionColumnNames = tableHandle.getPartitionColumns().stream()
                    .map(HiveColumnHandle::getName)
                    .collect(toImmutableList());

            if (!Objects.equals(partitionColumnNames, actualPartitionColumnNames)) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Provided partition column names do not match actual partition column names: " + actualPartitionColumnNames);
            }

            HiveMetastore metastore = hiveMetadata.getMetastore().unsafeGetRawHiveMetastore();
            if (metastore.getTable(schemaName, tableName).flatMap(table -> metastore.getPartition(table, partitionValues)).isPresent()) {
                throw new TrinoException(ALREADY_EXISTS, "Partition already exists");
            }
            HiveInsertTableHandle hiveInsertTableHandle = (HiveInsertTableHandle) hiveMetadata.beginInsert(session, tableHandle, ImmutableList.of(), NO_RETRIES);
            String partitionName = makePartName(actualPartitionColumnNames, partitionValues);

            WriteInfo writeInfo = locationService.getPartitionWriteInfo(hiveInsertTableHandle.getLocationHandle(), Optional.empty(), partitionName);
            Slice serializedPartitionUpdate = Slices.wrappedBuffer(
                    partitionUpdateJsonCodec.toJsonBytes(
                            new PartitionUpdate(
                                    partitionName,
                                    UpdateMode.NEW,
                                    writeInfo.writePath().toString(),
                                    writeInfo.targetPath().toString(),
                                    ImmutableList.of(),
                                    0,
                                    0,
                                    0)));

            hiveMetadata.finishInsert(
                    session,
                    hiveInsertTableHandle,
                    ImmutableList.of(),
                    ImmutableList.of(serializedPartitionUpdate),
                    ImmutableList.of());
            hiveMetadata.commit();
        }
    }
}
