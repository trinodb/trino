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
import io.prestosql.plugin.hive.HiveMetadata;
import io.prestosql.plugin.hive.HiveMetastoreClosure;
import io.prestosql.plugin.hive.TransactionalMetadataFactory;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.type.ArrayType;
import org.apache.hadoop.hive.common.FileUtils;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static io.prestosql.plugin.hive.procedure.Procedures.checkIsPartitionedTable;
import static io.prestosql.plugin.hive.procedure.Procedures.checkPartitionColumns;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.block.MethodHandleUtil.methodHandle;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class UnregisterPartitionProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle UNREGISTER_PARTITION = methodHandle(
            UnregisterPartitionProcedure.class,
            "unregisterPartition",
            ConnectorSession.class,
            String.class,
            String.class,
            List.class,
            List.class);

    private final TransactionalMetadataFactory hiveMetadataFactory;
    private final HiveMetastoreClosure metastore;

    @Inject
    public UnregisterPartitionProcedure(TransactionalMetadataFactory hiveMetadataFactory, HiveMetastore metastore)
    {
        this.hiveMetadataFactory = requireNonNull(hiveMetadataFactory, "hiveMetadataFactory is null");
        this.metastore = new HiveMetastoreClosure(requireNonNull(metastore, "metastore is null"));
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "unregister_partition",
                ImmutableList.of(
                        new Procedure.Argument("schema_name", VARCHAR),
                        new Procedure.Argument("table_name", VARCHAR),
                        new Procedure.Argument("partition_columns", new ArrayType(VARCHAR)),
                        new Procedure.Argument("partition_values", new ArrayType(VARCHAR))),
                UNREGISTER_PARTITION.bindTo(this));
    }

    public void unregisterPartition(ConnectorSession session, String schemaName, String tableName, List<String> partitionColumn, List<String> partitionValues)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doUnregisterPartition(session, schemaName, tableName, partitionColumn, partitionValues);
        }
    }

    private void doUnregisterPartition(ConnectorSession session, String schemaName, String tableName, List<String> partitionColumn, List<String> partitionValues)
    {
        HiveIdentity identity = new HiveIdentity(session);
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);

        Table table = metastore.getTable(identity, schemaName, tableName)
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));

        checkIsPartitionedTable(table);
        checkPartitionColumns(table, partitionColumn);

        String partitionName = FileUtils.makePartName(partitionColumn, partitionValues);

        Partition partition = metastore.getPartition(new HiveIdentity(session), schemaName, tableName, partitionValues)
                .orElseThrow(() -> new PrestoException(NOT_FOUND, format("Partition '%s' does not exist", partitionName)));

        SemiTransactionalHiveMetastore metastore = ((HiveMetadata) hiveMetadataFactory.create()).getMetastore();

        metastore.dropPartition(
                session,
                table.getDatabaseName(),
                table.getTableName(),
                partition.getValues(),
                false);

        metastore.commit();
    }
}
