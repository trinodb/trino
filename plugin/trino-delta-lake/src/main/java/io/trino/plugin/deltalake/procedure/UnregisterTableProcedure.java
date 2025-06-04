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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.base.util.UncheckedCloseable;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import io.trino.plugin.deltalake.DeltaLakeMetadataFactory;
import io.trino.plugin.deltalake.LocatedTableHandle;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.procedure.Procedure;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class UnregisterTableProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle UNREGISTER_TABLE;

    private static final String PROCEDURE_NAME = "unregister_table";
    private static final String SYSTEM_SCHEMA = "system";

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

    private final DeltaLakeMetadataFactory metadataFactory;
    private final TransactionLogAccess transactionLogAccess;
    private final CachingExtendedStatisticsAccess statisticsAccess;

    @Inject
    public UnregisterTableProcedure(DeltaLakeMetadataFactory metadataFactory, TransactionLogAccess transactionLogAccess, CachingExtendedStatisticsAccess statisticsAccess)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.statisticsAccess = requireNonNull(statisticsAccess, "statisticsAccess is null");
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
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doUnregisterTable(accessControl, session, schemaName, tableName);
        }
    }

    private void doUnregisterTable(ConnectorAccessControl accessControl, ConnectorSession session, String schemaName, String tableName)
    {
        checkProcedureArgument(!isNullOrEmpty(schemaName), "schema_name cannot be null or empty");
        checkProcedureArgument(!isNullOrEmpty(tableName), "table_name cannot be null or empty");
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);

        accessControl.checkCanDropTable(null, schemaTableName);
        DeltaLakeMetadata metadata = metadataFactory.create(session.getIdentity());
        metadata.beginQuery(session);
        try (UncheckedCloseable ignore = () -> metadata.cleanupQuery(session)) {
            LocatedTableHandle tableHandle = metadata.getTableHandle(session, schemaTableName, Optional.empty(), Optional.empty());
            if (tableHandle == null) {
                throw new TableNotFoundException(schemaTableName);
            }
            metadata.getMetastore().dropTable(schemaTableName, tableHandle.location(), false);
            // As a precaution, clear the caches
            statisticsAccess.invalidateCache(schemaTableName, Optional.of(tableHandle.location()));
            transactionLogAccess.invalidateCache(schemaTableName, Optional.of(tableHandle.location()));
        }
    }
}
