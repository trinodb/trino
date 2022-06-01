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
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.TransactionalMetadataFactory;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveWriteUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.ArrayType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.trino.plugin.hive.procedure.Procedures.checkIsPartitionedTable;
import static io.trino.plugin.hive.procedure.Procedures.checkPartitionColumns;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.block.MethodHandleUtil.methodHandle;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RegisterPartitionProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle REGISTER_PARTITION = methodHandle(
            RegisterPartitionProcedure.class,
            "registerPartition",
            ConnectorSession.class,
            ConnectorAccessControl.class,
            String.class,
            String.class,
            List.class,
            List.class,
            String.class);

    private final boolean allowRegisterPartition;
    private final TransactionalMetadataFactory hiveMetadataFactory;
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public RegisterPartitionProcedure(HiveConfig hiveConfig, TransactionalMetadataFactory hiveMetadataFactory, HdfsEnvironment hdfsEnvironment)
    {
        this.allowRegisterPartition = requireNonNull(hiveConfig, "hiveConfig is null").isAllowRegisterPartition();
        this.hiveMetadataFactory = requireNonNull(hiveMetadataFactory, "hiveMetadataFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
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

    public void registerPartition(ConnectorSession session, ConnectorAccessControl accessControl, String schemaName, String tableName, List<String> partitionColumn, List<String> partitionValues, String location)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doRegisterPartition(session, accessControl, schemaName, tableName, partitionColumn, partitionValues, location);
        }
    }

    private void doRegisterPartition(ConnectorSession session, ConnectorAccessControl accessControl, String schemaName, String tableName, List<String> partitionColumn, List<String> partitionValues, String location)
    {
        if (!allowRegisterPartition) {
            throw new TrinoException(PERMISSION_DENIED, "register_partition procedure is disabled");
        }

        SemiTransactionalHiveMetastore metastore = hiveMetadataFactory.create(session.getIdentity(), true).getMetastore();

        HdfsContext hdfsContext = new HdfsContext(session);
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);

        Table table = metastore.getTable(schemaName, tableName)
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));

        accessControl.checkCanInsertIntoTable(null, schemaTableName);

        checkIsPartitionedTable(table);
        checkPartitionColumns(table, partitionColumn);

        Optional<Partition> partition = metastore.unsafeGetRawHiveMetastoreClosure().getPartition(schemaName, tableName, partitionValues);
        if (partition.isPresent()) {
            String partitionName = FileUtils.makePartName(partitionColumn, partitionValues);
            throw new TrinoException(ALREADY_EXISTS, format("Partition [%s] is already registered with location %s", partitionName, partition.get().getStorage().getLocation()));
        }

        Path partitionLocation;

        if (location == null) {
            partitionLocation = new Path(table.getStorage().getLocation(), FileUtils.makePartName(partitionColumn, partitionValues));
        }
        else {
            partitionLocation = new Path(location);
        }

        if (!HiveWriteUtils.pathExists(hdfsContext, hdfsEnvironment, partitionLocation)) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Partition location does not exist: " + partitionLocation);
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

    private static Partition buildPartitionObject(ConnectorSession session, Table table, List<String> partitionValues, Path location)
    {
        return Partition.builder()
                .setDatabaseName(table.getDatabaseName())
                .setTableName(table.getTableName())
                .setColumns(table.getDataColumns())
                .setValues(partitionValues)
                .setParameters(ImmutableMap.of(PRESTO_QUERY_ID_NAME, session.getQueryId()))
                .withStorage(storage -> storage
                        .setStorageFormat(table.getStorage().getStorageFormat())
                        .setLocation(location.toString())
                        .setBucketProperty(table.getStorage().getBucketProperty())
                        .setSerdeParameters(table.getStorage().getSerdeParameters()))
                .build();
    }
}
