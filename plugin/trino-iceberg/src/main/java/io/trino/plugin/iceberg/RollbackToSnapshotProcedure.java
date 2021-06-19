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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;
import org.apache.iceberg.Table;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

import static io.trino.plugin.iceberg.IcebergUtil.loadIcebergTable;
import static io.trino.spi.block.MethodHandleUtil.methodHandle;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class RollbackToSnapshotProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle ROLLBACK_TO_SNAPSHOT = methodHandle(
            RollbackToSnapshotProcedure.class,
            "rollbackToSnapshot",
            ConnectorSession.class,
            String.class,
            String.class,
            Long.class);

    private final HiveTableOperationsProvider tableOperationsProvider;

    @Inject
    public RollbackToSnapshotProcedure(HiveTableOperationsProvider tableOperationsProvider)
    {
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "rollback_to_snapshot",
                ImmutableList.of(
                        new Procedure.Argument("schema", VARCHAR),
                        new Procedure.Argument("table", VARCHAR),
                        new Procedure.Argument("snapshot_id", BIGINT)),
                ROLLBACK_TO_SNAPSHOT.bindTo(this));
    }

    public void rollbackToSnapshot(ConnectorSession clientSession, String schema, String table, Long snapshotId)
    {
        SchemaTableName schemaTableName = new SchemaTableName(schema, table);
        Table icebergTable = loadIcebergTable(tableOperationsProvider, clientSession, schemaTableName);
        icebergTable.rollback().toSnapshotId(snapshotId).commit();
    }
}
