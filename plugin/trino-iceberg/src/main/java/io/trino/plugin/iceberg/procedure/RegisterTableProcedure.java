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
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
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
import org.apache.iceberg.TableMetadataParser;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergUtil.METADATA_FILE_EXTENSION;
import static io.trino.plugin.iceberg.IcebergUtil.METADATA_FOLDER_NAME;
import static io.trino.plugin.iceberg.IcebergUtil.parseVersion;
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
        try {
            // Try to read the metadata file. Invalid metadata file will throw the exception.
            TableMetadataParser.read(new ForwardingFileIo(fileSystem), metadataLocation);
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Invalid metadata file: " + metadataLocation, e);
        }

        catalog.registerTable(clientSession, schemaTableName, tableLocation, metadataLocation);
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

    public static String getLatestMetadataLocation(TrinoFileSystem fileSystem, String location)
    {
        List<String> latestMetadataLocations = new ArrayList<>();
        String metadataDirectoryLocation = format("%s/%s", stripTrailingSlash(location), METADATA_FOLDER_NAME);
        try {
            int latestMetadataVersion = -1;
            FileIterator fileIterator = fileSystem.listFiles(Location.of(metadataDirectoryLocation));
            while (fileIterator.hasNext()) {
                FileEntry fileEntry = fileIterator.next();
                String fileLocation = fileEntry.location().toString();
                if (fileLocation.contains(METADATA_FILE_EXTENSION)) {
                    OptionalInt version = parseVersion(fileLocation);
                    if (version.isPresent()) {
                        int versionNumber = version.getAsInt();
                        if (versionNumber > latestMetadataVersion) {
                            latestMetadataVersion = versionNumber;
                            latestMetadataLocations.clear();
                            latestMetadataLocations.add(fileLocation);
                        }
                        else if (versionNumber == latestMetadataVersion) {
                            latestMetadataLocations.add(fileLocation);
                        }
                    }
                }
            }
            if (latestMetadataLocations.isEmpty()) {
                throw new TrinoException(ICEBERG_INVALID_METADATA, "No versioned metadata file exists at location: " + metadataDirectoryLocation);
            }
            if (latestMetadataLocations.size() > 1) {
                throw new TrinoException(ICEBERG_INVALID_METADATA, format(
                        "More than one latest metadata file found at location: %s, latest metadata files are %s",
                        metadataDirectoryLocation,
                        latestMetadataLocations));
            }
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed checking table location: " + location, e);
        }
        return getOnlyElement(latestMetadataLocations);
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
}
