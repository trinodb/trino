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
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveInsertTableHandle;
import io.trino.plugin.hive.HiveMetastoreClosure;
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
import org.apache.hadoop.hive.common.FileUtils;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.block.MethodHandleUtil.methodHandle;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CreateEmptyPartitionProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle CREATE_EMPTY_PARTITION = methodHandle(
            CreateEmptyPartitionProcedure.class,
            "createEmptyPartition",
            ConnectorSession.class,
            ConnectorAccessControl.class,
            String.class,
            String.class,
            List.class,
            List.class);

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
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doCreateEmptyPartition(session, accessControl, schema, table, partitionColumnNames, partitionValues);
        }
    }

    private void doCreateEmptyPartition(ConnectorSession session, ConnectorAccessControl accessControl, String schemaName, String tableName, List<String> partitionColumnNames, List<String> partitionValues)
    {
        TransactionalMetadata hiveMetadata = hiveMetadataFactory.create(session.getIdentity(), true);
        HiveTableHandle tableHandle = (HiveTableHandle) hiveMetadata.getTableHandle(session, new SchemaTableName(schemaName, tableName));
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

        HiveMetastoreClosure metastore = hiveMetadata.getMetastore().unsafeGetRawHiveMetastoreClosure();
        if (metastore.getPartition(schemaName, tableName, partitionValues).isPresent()) {
            throw new TrinoException(ALREADY_EXISTS, "Partition already exists");
        }
        HiveInsertTableHandle hiveInsertTableHandle = (HiveInsertTableHandle) hiveMetadata.beginInsert(session, tableHandle, ImmutableList.of(), NO_RETRIES);
        String partitionName = FileUtils.makePartName(actualPartitionColumnNames, partitionValues);

        WriteInfo writeInfo = locationService.getPartitionWriteInfo(hiveInsertTableHandle.getLocationHandle(), Optional.empty(), partitionName);
        Slice serializedPartitionUpdate = Slices.wrappedBuffer(
                partitionUpdateJsonCodec.toJsonBytes(
                        new PartitionUpdate(
                                partitionName,
                                UpdateMode.NEW,
                                writeInfo.getWritePath(),
                                writeInfo.getTargetPath(),
                                ImmutableList.of(),
                                0,
                                0,
                                0)));

        hiveMetadata.finishInsert(
                session,
                hiveInsertTableHandle,
                ImmutableList.of(serializedPartitionUpdate),
                ImmutableList.of());
        hiveMetadata.commit();
    }
}
