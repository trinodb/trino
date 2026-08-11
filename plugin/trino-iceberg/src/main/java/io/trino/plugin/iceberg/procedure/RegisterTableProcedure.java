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
package io.trino.plugin.iceberg.procedure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isUseFileSizeFromMetadata;
import static io.trino.plugin.iceberg.procedure.RegisterProcedureUtils.getMetadataLocation;
import static io.trino.plugin.iceberg.procedure.RegisterProcedureUtils.locationEquivalent;
import static io.trino.plugin.iceberg.procedure.RegisterProcedureUtils.validateMetadataLocation;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
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
    private static final String METADATA_FILE_NAME = "METADATA_FILE_NAME";

    static {
        try {
            REGISTER_TABLE = lookup().unreflect(RegisterTableProcedure.class.getMethod("registerTable", ConnectorAccessControl.class, ConnectorSession.class, String.class, String.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final TrinoCatalogFactory catalogFactory;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ForwardingFileIoFactory fileIoFactory;
    private final boolean registerTableProcedureEnabled;

    @Inject
    public RegisterTableProcedure(
            TrinoCatalogFactory catalogFactory,
            TrinoFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            IcebergConfig icebergConfig)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
        this.registerTableProcedureEnabled = requireNonNull(icebergConfig, "icebergConfig is null").isRegisterTableProcedureEnabled();
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
                        new Procedure.Argument(TABLE_LOCATION, VARCHAR),
                        new Procedure.Argument(METADATA_FILE_NAME, VARCHAR, false, null)),
                REGISTER_TABLE.bindTo(this));
    }

    public void registerTable(
            ConnectorAccessControl accessControl,
            ConnectorSession clientSession,
            String schemaName,
            String tableName,
            String tableLocation,
            String metadataFileName)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doRegisterTable(
                    accessControl,
                    clientSession,
                    schemaName,
                    tableName,
                    tableLocation,
                    Optional.ofNullable(metadataFileName));
        }
    }

    private void doRegisterTable(
            ConnectorAccessControl accessControl,
            ConnectorSession clientSession,
            String schemaName,
            String tableName,
            String tableLocation,
            Optional<String> metadataFileName)
    {
        if (!registerTableProcedureEnabled) {
            throw new TrinoException(PERMISSION_DENIED, "register_table procedure is disabled");
        }
        checkProcedureArgument(schemaName != null && !schemaName.isEmpty(), "schema_name cannot be null or empty");
        checkProcedureArgument(tableName != null && !tableName.isEmpty(), "table_name cannot be null or empty");
        checkProcedureArgument(tableLocation != null && !tableLocation.isEmpty(), "table_location cannot be null or empty");
        metadataFileName.ifPresent(RegisterProcedureUtils::validateMetadataFileName);

        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        accessControl.checkCanCreateTable(null, schemaTableName, ImmutableMap.of());
        TrinoCatalog catalog = catalogFactory.create(clientSession.getIdentity());
        if (!catalog.namespaceExists(clientSession, schemaTableName.getSchemaName())) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaTableName.getSchemaName()));
        }

        TrinoFileSystem fileSystem = fileSystemFactory.create(clientSession);
        String metadataLocation = getMetadataLocation(fileSystem, tableLocation, metadataFileName);
        validateMetadataLocation(fileSystem, Location.of(metadataLocation));
        TableMetadata tableMetadata;
        try {
            // Try to read the metadata file. Invalid metadata file will throw the exception.
            tableMetadata = TableMetadataParser.read(fileIoFactory.create(fileSystem, isUseFileSizeFromMetadata(clientSession)), metadataLocation);
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Invalid metadata file: " + metadataLocation, e);
        }

        if (!locationEquivalent(tableLocation, tableMetadata.location())) {
            throw new TrinoException(
                    ICEBERG_INVALID_METADATA,
                    """
                    Table metadata file [%s] declares table location as [%s] which differs from location provided [%s]. \
                    Iceberg table can only be registered with the same location it was created with.""".formatted(metadataLocation, tableMetadata.location(), tableLocation));
        }

        catalog.registerTable(clientSession, schemaTableName, tableMetadata);
    }
}
