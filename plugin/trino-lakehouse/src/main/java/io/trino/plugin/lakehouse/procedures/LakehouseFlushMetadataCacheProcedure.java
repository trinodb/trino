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
import io.trino.plugin.deltalake.procedure.FlushMetadataCacheProcedure;
import io.trino.plugin.lakehouse.TableType;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.ArrayType;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

/**
 * A procedure that flushes the metadata cache for a table or a specific partition of a table.
 * <p>
 * It is delegated to the appropriate underlying procedure based on the table type.
 * Currently, it supports Delta Lake and Hive table types.
 */
public class LakehouseFlushMetadataCacheProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle FLUSH_METADATA_CACHE;

    private static final String SYSTEM_SCHEMA = "system";
    private static final String PROCEDURE_NAME = "flush_metadata_cache";

    private static final String TABLE_TYPE = "TABLE_TYPE";
    private static final String SCHEMA_NAME = "SCHEMA_NAME";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String PARAM_PARTITION_COLUMNS = "PARTITION_COLUMNS";
    private static final String PARAM_PARTITION_VALUES = "PARTITION_VALUES";

    static {
        try {
            FLUSH_METADATA_CACHE = lookup().unreflect(LakehouseFlushMetadataCacheProcedure.class.getMethod(
                    "flushMetadataCache", ConnectorSession.class, String.class, String.class, String.class, List.class, List.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final FlushMetadataCacheProcedure deltaLakeFlushMetadataCacheProcedure;
    private final io.trino.plugin.hive.procedure.FlushMetadataCacheProcedure hiveFlushMetadataCacheProcedure;

    @Inject
    public LakehouseFlushMetadataCacheProcedure(
            FlushMetadataCacheProcedure deltaLakeFlushMetadataCacheProcedure,
            io.trino.plugin.hive.procedure.FlushMetadataCacheProcedure hiveFlushMetadataCacheProcedure)
    {
        this.deltaLakeFlushMetadataCacheProcedure = requireNonNull(deltaLakeFlushMetadataCacheProcedure, "deltaLakeFlushMetadataCacheProcedure is null");
        this.hiveFlushMetadataCacheProcedure = requireNonNull(hiveFlushMetadataCacheProcedure, "hiveFlushMetadataCacheProcedure is null");
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
                        new Procedure.Argument(PARAM_PARTITION_COLUMNS, new ArrayType(VARCHAR), false, null),
                        new Procedure.Argument(PARAM_PARTITION_VALUES, new ArrayType(VARCHAR), false, null)),
                FLUSH_METADATA_CACHE.bindTo(this));
    }

    public void flushMetadataCache(ConnectorSession session, String tableType, String schema, String table, List<String> partitionColumns, List<String> partitionValues)
    {
        if (TableType.DELTA.name().equals(tableType)) {
            if (partitionColumns != null && !partitionColumns.isEmpty()) {
                throw new IllegalArgumentException("Partition columns are not supported for Delta Lake tables");
            }
            if (partitionValues != null && !partitionValues.isEmpty()) {
                throw new IllegalArgumentException("Partition values are not supported for Delta Lake tables");
            }
            deltaLakeFlushMetadataCacheProcedure.flushMetadataCache(schema, table);
        }
        else if (TableType.HIVE.name().equals(tableType)) {
            hiveFlushMetadataCacheProcedure.flushMetadataCache(session, schema, table, partitionColumns, partitionValues);
        }
        else {
            throw new IllegalArgumentException("Unsupported table type: " + tableType);
        }
    }
}
