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
import io.trino.plugin.deltalake.procedure.DropExtendedStatsProcedure;
import io.trino.plugin.hive.procedure.DropStatsProcedure;
import io.trino.plugin.lakehouse.TableType;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.ArrayType;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

/**
 * A procedure that drops statistics.
 * <p>
 * It is delegated to the appropriate underlying procedure based on the table type.
 * Currently, it supports Delta Lake and Hive table types.
 */
public class LakehouseDropStatsProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle DROP_STATS;

    private static final String SYSTEM_SCHEMA = "system";
    private static final String PROCEDURE_NAME = "drop_stats";

    private static final String TABLE_TYPE = "TABLE_TYPE";
    private static final String SCHEMA_NAME = "SCHEMA_NAME";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String PARTITION_VALUES = "PARTITION_VALUES";

    static {
        try {
            DROP_STATS = lookup().unreflect(LakehouseDropStatsProcedure.class.getMethod(
                    "dropStats", ConnectorSession.class, ConnectorAccessControl.class, String.class, String.class, String.class, List.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final DropExtendedStatsProcedure deltaLakeDropStatsProcedure;
    private final DropStatsProcedure hiveDropStatsProcedure;

    @Inject
    public LakehouseDropStatsProcedure(
            DropExtendedStatsProcedure deltaLakeDropStatsProcedure,
            DropStatsProcedure hiveDropStatsProcedure)
    {
        this.deltaLakeDropStatsProcedure = requireNonNull(deltaLakeDropStatsProcedure, "deltaLakeDropStatsProcedure is null");
        this.hiveDropStatsProcedure = requireNonNull(hiveDropStatsProcedure, "hiveDropStatsProcedure is null");
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
                        new Procedure.Argument(TABLE_NAME, VARCHAR),
                        new Procedure.Argument(PARTITION_VALUES, new ArrayType(new ArrayType(VARCHAR)), false, null)),
                DROP_STATS.bindTo(this));
    }

    public void dropStats(ConnectorSession session, ConnectorAccessControl accessControl, String tableType, String schema, String table, List<?> partitionValues)
    {
        if (TableType.DELTA.name().equals(tableType)) {
            if (partitionValues != null) {
                throw new IllegalArgumentException("Partition values are not supported for Delta Lake procedure");
            }
            deltaLakeDropStatsProcedure.dropStats(session, accessControl, schema, table);
        }
        else if (TableType.HIVE.name().equals(tableType)) {
            hiveDropStatsProcedure.dropStats(session, accessControl, schema, table, partitionValues);
        }
        else {
            throw new IllegalArgumentException("Unsupported table type: " + tableType);
        }
    }
}
