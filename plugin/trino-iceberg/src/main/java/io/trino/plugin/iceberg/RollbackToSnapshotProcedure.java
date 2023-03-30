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
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;
import org.apache.iceberg.Table;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class RollbackToSnapshotProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle ROLLBACK_TO_SNAPSHOT;

    static {
        try {
            ROLLBACK_TO_SNAPSHOT = lookup().unreflect(RollbackToSnapshotProcedure.class.getMethod("rollbackToSnapshot", ConnectorSession.class, String.class, String.class, Long.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final TrinoCatalogFactory catalogFactory;

    @Inject
    public RollbackToSnapshotProcedure(TrinoCatalogFactory catalogFactory)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "rollback_to_snapshot",
                ImmutableList.of(
                        new Procedure.Argument("SCHEMA", VARCHAR),
                        new Procedure.Argument("TABLE", VARCHAR),
                        new Procedure.Argument("SNAPSHOT_ID", BIGINT)),
                ROLLBACK_TO_SNAPSHOT.bindTo(this));
    }

    public void rollbackToSnapshot(ConnectorSession clientSession, String schema, String table, Long snapshotId)
    {
        checkProcedureArgument(schema != null, "schema cannot be null");
        checkProcedureArgument(table != null, "table cannot be null");
        checkProcedureArgument(snapshotId != null, "snapshot_id cannot be null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            SchemaTableName schemaTableName = new SchemaTableName(schema, table);
            Table icebergTable = catalogFactory.create(clientSession.getIdentity()).loadTable(clientSession, schemaTableName);
            icebergTable.manageSnapshots().setCurrentSnapshot(snapshotId).commit();
        }
    }
}
