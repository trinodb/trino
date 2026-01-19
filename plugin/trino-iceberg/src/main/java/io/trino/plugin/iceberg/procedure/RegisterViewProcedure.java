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
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandle;
import java.util.Optional;
import java.util.regex.Pattern;

import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isUseFileSizeFromMetadata;
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

public class RegisterViewProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle REGISTER_VIEW;

    private static final String PROCEDURE_NAME = "register_view";
    private static final String SYSTEM_SCHEMA = "system";

    private static final String SCHEMA_NAME = "SCHEMA_NAME";
    private static final String VIEW_NAME = "VIEW_NAME";
    private static final String VIEW_LOCATION = "VIEW_LOCATION";
    private static final String METADATA_FILE_NAME = "METADATA_FILE_NAME";
    private static final Pattern S3_SCHEMA_PATTERN = Pattern.compile("^s3[an]://");

    static {
        try {
            REGISTER_VIEW = lookup().unreflect(RegisterViewProcedure.class.getMethod("registerView", ConnectorAccessControl.class, ConnectorSession.class, String.class, String.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final TrinoCatalogFactory catalogFactory;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ForwardingFileIoFactory fileIoFactory;
    private final boolean registerViewProcedureEnabled;

    @Inject
    public RegisterViewProcedure(
            TrinoCatalogFactory catalogFactory,
            TrinoFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            IcebergConfig icebergConfig)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
        registerViewProcedureEnabled = icebergConfig.isRegisterViewProcedureEnabled();
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                SYSTEM_SCHEMA,
                PROCEDURE_NAME,
                ImmutableList.of(
                        new Procedure.Argument(SCHEMA_NAME, VARCHAR),
                        new Procedure.Argument(VIEW_NAME, VARCHAR),
                        new Procedure.Argument(VIEW_LOCATION, VARCHAR),
                        new Procedure.Argument(METADATA_FILE_NAME, VARCHAR, false, null)),
                REGISTER_VIEW.bindTo(this));
    }

    public void registerView(
            ConnectorAccessControl accessControl,
            ConnectorSession clientSession,
            String schemaName,
            String viewName,
            String viewLocation,
            String metadataFileName)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doRegisterView(
                    accessControl,
                    clientSession,
                    schemaName,
                    viewName,
                    viewLocation,
                    Optional.ofNullable(metadataFileName));
        }
    }

    private void doRegisterView(
            ConnectorAccessControl accessControl,
            ConnectorSession clientSession,
            String schemaName,
            String viewName,
            String viewLocation,
            Optional<String> metadataFileName)
    {
        if (!registerViewProcedureEnabled) {
            throw new TrinoException(PERMISSION_DENIED, "register_view procedure is disabled");
        }
        checkProcedureArgument(schemaName != null && !schemaName.isEmpty(), "schema_name cannot be null or empty");
        checkProcedureArgument(viewName != null && !viewName.isEmpty(), "view_name cannot be null or empty");
        checkProcedureArgument(viewLocation != null && !viewLocation.isEmpty(), "view_location cannot be null or empty");
        metadataFileName.ifPresent(RegisterViewProcedure::validateMetadataFileName);

        SchemaTableName schemaViewName = new SchemaTableName(schemaName, viewName);
        accessControl.checkCanCreateView(null, schemaViewName);
        TrinoCatalog catalog = catalogFactory.create(clientSession.getIdentity());
        if (!catalog.namespaceExists(clientSession, schemaViewName.getSchemaName())) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaViewName.getSchemaName()));
        }

        TrinoFileSystem fileSystem = fileSystemFactory.create(clientSession);
        String metadataLocation = getMetadataLocation(fileSystem, viewLocation, metadataFileName);
        validateMetadataLocation(fileSystem, Location.of(metadataLocation));
        ViewMetadata viewMetadata;
        try {
            // Try to read the metadata file. Invalid metadata file will throw the exception.
            viewMetadata = ViewMetadataParser.read(fileIoFactory.create(fileSystem, isUseFileSizeFromMetadata(clientSession)), metadataLocation);
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Invalid metadata file: " + metadataLocation, e);
        }

        if (!locationEquivalent(viewLocation, viewMetadata.location())) {
            throw new TrinoException(
                    ICEBERG_INVALID_METADATA,
                    """
                    View metadata file [%s] declares view location as [%s] which is differs from location provided [%s]. \
                    Iceberg view can only be registered with the same location it was created with.""".formatted(metadataLocation, viewMetadata.location(), viewLocation));
        }

        catalog.registerView(clientSession, schemaViewName, viewMetadata);
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
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Invalid metadata file location: " + location, e);
        }
    }

    private static boolean locationEquivalent(String a, String b)
    {
        return normalizeS3Uri(a).equals(normalizeS3Uri(b));
    }

    private static String normalizeS3Uri(String viewLocation)
    {
        // Normalize e.g. s3a to s3, so that view can be registered using s3:// location
        // even if internally it uses s3a:// paths.
        String normalizedSchema = S3_SCHEMA_PATTERN.matcher(viewLocation).replaceFirst("s3://");
        // Remove trailing slashes so that test_dir is equal to test_dir/
        return stripTrailingSlash(normalizedSchema);
    }
}
