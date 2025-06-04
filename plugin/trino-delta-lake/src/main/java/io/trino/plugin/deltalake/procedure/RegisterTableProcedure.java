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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.Table;
import io.trino.plugin.base.util.UncheckedCloseable;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import io.trino.plugin.deltalake.DeltaLakeMetadataFactory;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastore;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_TABLE;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class RegisterTableProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle REGISTER_TABLE;

    private static final String PROCEDURE_NAME = "register_table";
    private static final String SYSTEM_SCHEMA = "system";

    private static final String SCHEMA_NAME = "SCHEMA_NAME";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String TABLE_LOCATION = "TABLE_LOCATION";

    static {
        try {
            REGISTER_TABLE = lookup().unreflect(RegisterTableProcedure.class.getMethod("registerTable", ConnectorAccessControl.class, ConnectorSession.class, String.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final DeltaLakeMetadataFactory metadataFactory;
    private final TransactionLogAccess transactionLogAccess;
    private final CachingExtendedStatisticsAccess statisticsAccess;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final boolean registerTableProcedureEnabled;

    @Inject
    public RegisterTableProcedure(
            DeltaLakeMetadataFactory metadataFactory,
            TransactionLogAccess transactionLogAccess,
            CachingExtendedStatisticsAccess statisticsAccess,
            TrinoFileSystemFactory fileSystemFactory,
            DeltaLakeConfig deltaLakeConfig)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.statisticsAccess = requireNonNull(statisticsAccess, "statisticsAccess is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.registerTableProcedureEnabled = deltaLakeConfig.isRegisterTableProcedureEnabled();
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                SYSTEM_SCHEMA,
                PROCEDURE_NAME,
                ImmutableList.of(
                        new Procedure.Argument(SCHEMA_NAME, VARCHAR),
                        new Procedure.Argument(TABLE_NAME, VARCHAR),
                        new Procedure.Argument(TABLE_LOCATION, VARCHAR)),
                REGISTER_TABLE.bindTo(this));
    }

    public void registerTable(
            ConnectorAccessControl accessControl,
            ConnectorSession clientSession,
            String schemaName,
            String tableName,
            String tableLocation)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doRegisterTable(
                    accessControl,
                    clientSession,
                    schemaName,
                    tableName,
                    tableLocation);
        }
    }

    private void doRegisterTable(
            ConnectorAccessControl accessControl,
            ConnectorSession session,
            String schemaName,
            String tableName,
            String tableLocation)
    {
        if (!registerTableProcedureEnabled) {
            throw new TrinoException(PERMISSION_DENIED, "register_table procedure is disabled");
        }
        checkProcedureArgument(!isNullOrEmpty(schemaName), "schema_name cannot be null or empty");
        checkProcedureArgument(!isNullOrEmpty(tableName), "table_name cannot be null or empty");
        checkProcedureArgument(!isNullOrEmpty(tableLocation), "table_location cannot be null or empty");

        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        accessControl.checkCanCreateTable(null, schemaTableName, ImmutableMap.of());
        DeltaLakeMetadata metadata = metadataFactory.create(session.getIdentity());
        metadata.beginQuery(session);
        try (UncheckedCloseable ignore = () -> metadata.cleanupQuery(session)) {
            DeltaLakeMetastore metastore = metadata.getMetastore();

            if (metastore.getDatabase(schemaName).isEmpty()) {
                throw new SchemaNotFoundException(schemaTableName.getSchemaName());
            }

            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            try {
                Location transactionLogDir = Location.of(getTransactionLogDir(tableLocation));
                if (!fileSystem.listFiles(transactionLogDir).hasNext()) {
                    throw new TrinoException(GENERIC_USER_ERROR, format("No transaction log found in location %s", transactionLogDir));
                }
            }
            catch (IOException e) {
                throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, format("Failed checking table location %s", tableLocation), e);
            }

            statisticsAccess.invalidateCache(schemaTableName, Optional.of(tableLocation));
            transactionLogAccess.invalidateCache(schemaTableName, Optional.of(tableLocation));
            // Verify we're registering a location with a valid table
            TableSnapshot tableSnapshot;
            MetadataEntry metadataEntry;
            try {
                tableSnapshot = transactionLogAccess.loadSnapshot(session, schemaTableName, tableLocation, Optional.empty());
                metadataEntry = transactionLogAccess.getMetadataEntry(session, tableSnapshot);
            }
            catch (TrinoException e) {
                throw e;
            }
            catch (IOException | RuntimeException e) {
                throw new TrinoException(DELTA_LAKE_INVALID_TABLE, "Failed to access table location: " + tableLocation, e);
            }

            Table table = metadata.buildTable(
                    session,
                    schemaTableName,
                    tableLocation,
                    true,
                    Optional.ofNullable(metadataEntry.getDescription()),
                    tableSnapshot.getVersion(),
                    metadataEntry.getSchemaString());
            PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(table.getOwner().orElseThrow());
            metastore.createTable(table, principalPrivileges);
        }
    }
}
