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
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergUtil.METADATA_FOLDER_NAME;
import static io.trino.plugin.iceberg.IcebergUtil.getLatestMetadataLocation;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;

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
            REGISTER_TABLE = lookup().unreflect(RegisterTableProcedure.class.getMethod("registerTable", ConnectorSession.class, String.class, String.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final TrinoCatalogFactory catalogFactory;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final boolean registerTableProcedureEnabled;

    @Inject
    public RegisterTableProcedure(TrinoCatalogFactory catalogFactory, TrinoFileSystemFactory fileSystemFactory, IcebergConfig icebergConfig)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
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
            ConnectorSession clientSession,
            String schemaName,
            String tableName,
            String tableLocation,
            String metadataFileName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doRegisterTable(
                    clientSession,
                    schemaName,
                    tableName,
                    tableLocation,
                    Optional.ofNullable(metadataFileName));
        }
    }

    private void doRegisterTable(
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
        metadataFileName.ifPresent(RegisterTableProcedure::validateMetadataFileName);

        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
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
            tableMetadata = TableMetadataParser.read(new ForwardingFileIo(fileSystem), metadataLocation);
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Invalid metadata file: " + metadataLocation, e);
        }

        if (!locationEquivalent(tableLocation, tableMetadata.location())) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, """
                    Table metadata file [%s] declares table location as [%s] which is differs from location provided [%s]. \
                    Iceberg table can only be registered with the same location it was created with.""".formatted(metadataLocation, tableMetadata.location(), tableLocation));
        }

        catalog.registerTable(clientSession, schemaTableName, tableMetadata);
    }

    private static void validateMetadataFileName(String fileName)
    {
        String metadataFileName = fileName.trim();
        checkProcedureArgument(!metadataFileName.isEmpty(), "metadata_file_name cannot be empty when provided as an argument");
        checkProcedureArgument(!metadataFileName.contains("/"), "%s is not a valid metadata file", metadataFileName);
    }

    /**
     * Get the latest metadata file location present in location if metadataFileName is not provided, otherwise
     * form the metadata file location using location and metadataFileName
     */
    private static String getMetadataLocation(TrinoFileSystem fileSystem, String location, Optional<String> metadataFileName)
    {
        return metadataFileName
                .map(fileName -> format("%s/%s/%s", stripTrailingSlash(location), METADATA_FOLDER_NAME, fileName))
                .orElseGet(() -> getLatestMetadataLocation(fileSystem, location));
    }

    private static void validateMetadataLocation(TrinoFileSystem fileSystem, Location location)
    {
        try {
            if (!fileSystem.newInputFile(location).exists()) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Metadata file does not exist: " + location);
            }
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Invalid metadata file location: " + location, e);
        }
    }

    private static boolean locationEquivalent(String a, String b)
    {
        return normalizeS3Uri(a).equals(normalizeS3Uri(b));
    }

    private static String normalizeS3Uri(String tableLocation)
    {
        // Normalize e.g. s3a to s3, so that table can be registered using s3:// location
        // even if internally it uses s3a:// paths.
        String normalizedSchema = tableLocation.replaceFirst("^s3[an]://", "s3://");
        // Remove trailing slashes so that test_dir is equal to test_dir/
        return stripTrailingSlash(normalizedSchema);
    }
}
