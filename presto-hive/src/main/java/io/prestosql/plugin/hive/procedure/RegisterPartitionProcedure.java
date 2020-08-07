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
package io.prestosql.plugin.hive.procedure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveMetadata;
import io.prestosql.plugin.hive.HiveMetastoreClosure;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.TransactionalMetadataFactory;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.util.HiveWriteUtils;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.type.ArrayType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static io.prestosql.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.prestosql.plugin.hive.procedure.Procedures.checkIsPartitionedTable;
import static io.prestosql.plugin.hive.procedure.Procedures.checkPartitionColumns;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.prestosql.spi.block.MethodHandleUtil.methodHandle;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RegisterPartitionProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle REGISTER_PARTITION = methodHandle(
            RegisterPartitionProcedure.class,
            "registerPartition",
            ConnectorSession.class,
            String.class,
            String.class,
            List.class,
            List.class,
            String.class);

    private final boolean allowRegisterPartition;
    private final TransactionalMetadataFactory hiveMetadataFactory;
    private final HdfsEnvironment hdfsEnvironment;
    private final HiveMetastoreClosure metastore;

    @Inject
    public RegisterPartitionProcedure(HiveConfig hiveConfig, TransactionalMetadataFactory hiveMetadataFactory, HiveMetastore metastore, HdfsEnvironment hdfsEnvironment)
    {
        this.allowRegisterPartition = requireNonNull(hiveConfig, "hiveConfig is null").isAllowRegisterPartition();
        this.hiveMetadataFactory = requireNonNull(hiveMetadataFactory, "hiveMetadataFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.metastore = new HiveMetastoreClosure(requireNonNull(metastore, "metastore is null"));
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "register_partition",
                ImmutableList.of(
                        new Procedure.Argument("schema_name", VARCHAR),
                        new Procedure.Argument("table_name", VARCHAR),
                        new Procedure.Argument("partition_columns", new ArrayType(VARCHAR)),
                        new Procedure.Argument("partition_values", new ArrayType(VARCHAR)),
                        new Procedure.Argument("location", VARCHAR, false, null)),
                REGISTER_PARTITION.bindTo(this));
    }

    public void registerPartition(ConnectorSession session, String schemaName, String tableName, List<String> partitionColumn, List<String> partitionValues, String location)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doRegisterPartition(session, schemaName, tableName, partitionColumn, partitionValues, location);
        }
    }

    private void doRegisterPartition(ConnectorSession session, String schemaName, String tableName, List<String> partitionColumn, List<String> partitionValues, String location)
    {
        if (!allowRegisterPartition) {
            throw new PrestoException(PERMISSION_DENIED, "register_partition procedure is disabled");
        }

        HiveIdentity identity = new HiveIdentity(session);
        HdfsContext hdfsContext = new HdfsContext(session, schemaName, tableName);
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);

        Table table = metastore.getTable(identity, schemaName, tableName)
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));

        checkIsPartitionedTable(table);
        checkPartitionColumns(table, partitionColumn);

        Optional<Partition> partition = metastore.getPartition(new HiveIdentity(session), schemaName, tableName, partitionValues);
        if (partition.isPresent()) {
            String partitionName = FileUtils.makePartName(partitionColumn, partitionValues);
            throw new PrestoException(ALREADY_EXISTS, format("Partition [%s] is already registered with location %s", partitionName, partition.get().getStorage().getLocation()));
        }

        Path partitionLocation;

        if (location == null) {
            partitionLocation = new Path(table.getStorage().getLocation(), FileUtils.makePartName(partitionColumn, partitionValues));
        }
        else {
            partitionLocation = new Path(location);
        }

        if (!HiveWriteUtils.pathExists(hdfsContext, hdfsEnvironment, partitionLocation)) {
            throw new PrestoException(INVALID_PROCEDURE_ARGUMENT, "Partition location does not exist: " + partitionLocation);
        }

        SemiTransactionalHiveMetastore metastore = ((HiveMetadata) hiveMetadataFactory.create()).getMetastore();

        metastore.addPartition(
                session,
                table.getDatabaseName(),
                table.getTableName(),
                buildPartitionObject(session, table, partitionValues, partitionLocation),
                partitionLocation,
                PartitionStatistics.empty());

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
