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
import io.trino.metastore.Partition;
import io.trino.metastore.Table;
import io.trino.plugin.base.util.UncheckedCloseable;
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

import java.lang.invoke.MethodHandle;
import java.util.List;

import static io.trino.metastore.Partitions.makePartName;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.hive.procedure.Procedures.checkIsPartitionedTable;
import static io.trino.plugin.hive.procedure.Procedures.checkPartitionColumns;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class UnregisterPartitionProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle UNREGISTER_PARTITION;

    static {
        try {
            UNREGISTER_PARTITION = lookup().unreflect(UnregisterPartitionProcedure.class.getMethod("unregisterPartition", ConnectorSession.class, ConnectorAccessControl.class, String.class, String.class, List.class, List.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final TransactionalMetadataFactory hiveMetadataFactory;

    @Inject
    public UnregisterPartitionProcedure(TransactionalMetadataFactory hiveMetadataFactory)
    {
        this.hiveMetadataFactory = requireNonNull(hiveMetadataFactory, "hiveMetadataFactory is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "unregister_partition",
                ImmutableList.of(
                        new Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        new Procedure.Argument("TABLE_NAME", VARCHAR),
                        new Procedure.Argument("PARTITION_COLUMNS", new ArrayType(VARCHAR)),
                        new Procedure.Argument("PARTITION_VALUES", new ArrayType(VARCHAR))),
                UNREGISTER_PARTITION.bindTo(this));
    }

    public void unregisterPartition(ConnectorSession session, ConnectorAccessControl accessControl, String schemaName, String tableName, List<String> partitionColumns, List<String> partitionValues)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doUnregisterPartition(session, accessControl, schemaName, tableName, partitionColumns, partitionValues);
        }
    }

    private void doUnregisterPartition(ConnectorSession session, ConnectorAccessControl accessControl, String schemaName, String tableName, List<String> partitionColumns, List<String> partitionValues)
    {
        checkProcedureArgument(schemaName != null, "schema_name cannot be null");
        checkProcedureArgument(tableName != null, "table_name cannot be null");
        checkProcedureArgument(partitionColumns != null, "partition_columns cannot be null");
        checkProcedureArgument(partitionValues != null, "partition_values cannot be null");

        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);

        TransactionalMetadata hiveMetadata = hiveMetadataFactory.create(session.getIdentity(), true);
        hiveMetadata.beginQuery(session);
        try (UncheckedCloseable ignore = () -> hiveMetadata.cleanupQuery(session)) {
            SemiTransactionalHiveMetastore metastore = hiveMetadata.getMetastore();

            Table table = metastore.getTable(schemaName, tableName)
                    .orElseThrow(() -> new TableNotFoundException(schemaTableName));

            accessControl.checkCanDeleteFromTable(null, schemaTableName);

            checkIsPartitionedTable(table);
            checkPartitionColumns(table, partitionColumns);

            String partitionName = makePartName(partitionColumns, partitionValues);

            Partition partition = metastore.unsafeGetRawHiveMetastore().getPartition(table, partitionValues)
                    .orElseThrow(() -> new TrinoException(NOT_FOUND, format("Partition '%s' does not exist", partitionName)));

            metastore.dropPartition(
                    session,
                    table.getDatabaseName(),
                    table.getTableName(),
                    partition.getValues(),
                    false);

            metastore.commit();
        }
    }
}
