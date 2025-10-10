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
package io.trino.plugin.lakehouse.procedures;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.deltalake.procedure.UnregisterTableProcedure;
import io.trino.plugin.lakehouse.TableType;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

/**
 * A procedure that unregisters a table from the metastore.
 * <p>
 * It is delegated to the appropriate underlying procedure based on the table type.
 * Currently, it supports Delta Lake and Iceberg table types.
 */
public class LakehouseUnregisterTableProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle UNREGISTER_TABLE;

    private static final String SYSTEM_SCHEMA = "system";
    private static final String PROCEDURE_NAME = "unregister_table";

    private static final String TABLE_TYPE = "TABLE_TYPE";
    private static final String SCHEMA_NAME = "SCHEMA_NAME";
    private static final String TABLE_NAME = "TABLE_NAME";

    static {
        try {
            UNREGISTER_TABLE = lookup().unreflect(LakehouseUnregisterTableProcedure.class.getMethod(
                    "unregisterTable", ConnectorAccessControl.class, ConnectorSession.class, String.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final UnregisterTableProcedure deltaLakeUnregisterTableProcedure;
    private final io.trino.plugin.iceberg.procedure.UnregisterTableProcedure icebergUnregisterTableProcedure;

    @Inject
    public LakehouseUnregisterTableProcedure(
            UnregisterTableProcedure deltaLakeUnregisterTableProcedure,
            io.trino.plugin.iceberg.procedure.UnregisterTableProcedure icebergUnregisterTableProcedure)
    {
        this.deltaLakeUnregisterTableProcedure = requireNonNull(deltaLakeUnregisterTableProcedure, "deltaLakeUnregisterTableProcedure is null");
        this.icebergUnregisterTableProcedure = requireNonNull(icebergUnregisterTableProcedure, "icebergUnregisterTableProcedure is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                SYSTEM_SCHEMA,
                PROCEDURE_NAME,
                ImmutableList.of(
                        new Procedure.Argument(TABLE_TYPE, VARCHAR),
                        new Procedure.Argument(SCHEMA_NAME, VARCHAR),
                        new Procedure.Argument(TABLE_NAME, VARCHAR)),
                UNREGISTER_TABLE.bindTo(this));
    }

    public void unregisterTable(ConnectorAccessControl accessControl, ConnectorSession session, String tableType, String schema, String table)
    {
        if (TableType.DELTA.name().equals(tableType)) {
            deltaLakeUnregisterTableProcedure.unregisterTable(accessControl, session, schema, table);
        }
        else if (TableType.ICEBERG.name().equals(tableType)) {
            icebergUnregisterTableProcedure.unregisterTable(accessControl, session, schema, table);
        }
        else {
            throw new IllegalArgumentException("Unsupported table type: " + tableType);
        }
    }
}
