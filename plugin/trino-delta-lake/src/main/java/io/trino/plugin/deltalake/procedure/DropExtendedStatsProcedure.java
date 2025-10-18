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
package io.trino.plugin.deltalake.procedure;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.base.util.UncheckedCloseable;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import io.trino.plugin.deltalake.DeltaLakeMetadataFactory;
import io.trino.plugin.deltalake.LocatedTableHandle;
import io.trino.plugin.deltalake.statistics.ExtendedStatisticsAccess;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.procedure.Procedure.Argument;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class DropExtendedStatsProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle PROCEDURE_METHOD;

    static {
        try {
            PROCEDURE_METHOD = lookup().unreflect(DropExtendedStatsProcedure.class.getMethod("dropStats", ConnectorSession.class, ConnectorAccessControl.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final DeltaLakeMetadataFactory metadataFactory;
    private final ExtendedStatisticsAccess statsAccess;

    @Inject
    public DropExtendedStatsProcedure(DeltaLakeMetadataFactory metadataFactory, ExtendedStatisticsAccess statsAccess)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory");
        this.statsAccess = requireNonNull(statsAccess, "statsAccess");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "drop_extended_stats",
                List.of(
                        new Argument("SCHEMA_NAME", VARCHAR),
                        new Argument("TABLE_NAME", VARCHAR)),
                PROCEDURE_METHOD.bindTo(this));
    }

    public void dropStats(ConnectorSession session, ConnectorAccessControl accessControl, String schema, String table)
    {
        checkProcedureArgument(schema != null, "schema_name cannot be null");
        checkProcedureArgument(table != null, "table_name cannot be null");

        SchemaTableName name = new SchemaTableName(schema, table);
        DeltaLakeMetadata metadata = metadataFactory.create(session.getIdentity());
        metadata.beginQuery(session);
        try (UncheckedCloseable ignore = () -> metadata.cleanupQuery(session)) {
            LocatedTableHandle tableHandle = metadata.getTableHandle(session, name, Optional.empty(), Optional.empty());
            if (tableHandle == null) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, format("Table '%s' does not exist", name));
            }
            accessControl.checkCanInsertIntoTable(null, name);
            statsAccess.deleteExtendedStatistics(session, name, tableHandle.location(), tableHandle.toCredentialsHandle());
        }
    }
}
