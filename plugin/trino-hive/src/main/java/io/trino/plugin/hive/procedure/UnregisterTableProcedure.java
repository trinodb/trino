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
import io.trino.plugin.hive.TableType;
import io.trino.plugin.hive.TransactionalMetadata;
import io.trino.plugin.hive.TransactionalMetadataFactory;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.procedure.Procedure;

import java.lang.invoke.MethodHandle;

import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class UnregisterTableProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle UNREGISTER_TABLE;
    private static final String SYSTEM_SCHEMA = "system";
    private static final String PROCEDURE_NAME = "unregister_table";
    private static final String SCHEMA_NAME = "SCHEMA_NAME";
    private static final String TABLE_NAME = "TABLE_NAME";

    static {
        try {
            UNREGISTER_TABLE = lookup().unreflect(UnregisterTableProcedure.class.getMethod("unregisterTable", ConnectorAccessControl.class, ConnectorSession.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final TransactionalMetadataFactory hiveMetadataFactory;

    @Inject
    public UnregisterTableProcedure(TransactionalMetadataFactory hiveMetadataFactory)
    {
        this.hiveMetadataFactory = requireNonNull(hiveMetadataFactory, "hiveMetadataFactory is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                SYSTEM_SCHEMA,
                PROCEDURE_NAME,
                ImmutableList.of(
                        new Procedure.Argument(SCHEMA_NAME, VARCHAR),
                        new Procedure.Argument(TABLE_NAME, VARCHAR)),
                UNREGISTER_TABLE.bindTo(this));
    }

    public void unregisterTable(ConnectorAccessControl accessControl, ConnectorSession session, String schemaName, String tableName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doUnregisterTable(accessControl, session, schemaName, tableName);
        }
    }

    private void doUnregisterTable(ConnectorAccessControl accessControl, ConnectorSession session, String schemaName, String tableName)
    {
        checkProcedureArgument(schemaName != null, "schema_name cannot be null");
        checkProcedureArgument(tableName != null, "table_name cannot be null");
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);

        accessControl.checkCanDropTable(null, schemaTableName);
        TransactionalMetadata hiveMetadata = hiveMetadataFactory.create(session.getIdentity(), true);
        SemiTransactionalHiveMetastore metastore = hiveMetadata.getMetastore();

        if (metastore.getDatabase(schemaName).isEmpty()) {
            throw new SchemaNotFoundException(schemaName);
        }

        ConnectorTableHandle tableHandle = hiveMetadata.getTableHandle(session, new SchemaTableName(schemaName, tableName));
        if (tableHandle == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        Table table = metastore.getTable(schemaName, tableName).orElseThrow(() -> new TableNotFoundException(schemaTableName));
        if (!isTable(table.getTableType())) {
            throw new TrinoException(UNSUPPORTED_TABLE_TYPE, format("Not a Hive table '%s'", tableName));
        }

        metastore.dropTable(session, schemaName, tableName, false);
        metastore.commit();
    }

    private boolean isTable(String tableType)
    {
        return TableType.MANAGED_TABLE.name().equals(tableType) || TableType.EXTERNAL_TABLE.name().equals(tableType);
    }
}
