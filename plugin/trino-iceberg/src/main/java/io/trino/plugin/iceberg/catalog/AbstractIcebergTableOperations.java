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
package io.trino.plugin.iceberg.catalog;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.trino.annotation.NotThreadSafe;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.Column;
import io.trino.metastore.HiveType;
import io.trino.metastore.StorageFormat;
import io.trino.plugin.iceberg.IcebergExceptions;
import io.trino.plugin.iceberg.util.HiveSchemaUtil;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import jakarta.annotation.Nullable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types.NestedField;

import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hive.formats.HiveClassNames.FILE_INPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.FILE_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.LAZY_SIMPLE_SERDE_CLASS;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergExceptions.translateMetadataException;
import static io.trino.plugin.iceberg.IcebergTableName.isMaterializedViewStorage;
import static io.trino.plugin.iceberg.IcebergUtil.METADATA_FOLDER_NAME;
import static io.trino.plugin.iceberg.IcebergUtil.fixBrokenMetadataLocation;
import static io.trino.plugin.iceberg.IcebergUtil.getLocationProvider;
import static io.trino.plugin.iceberg.IcebergUtil.parseVersion;
import static io.trino.plugin.iceberg.procedure.MigrateProcedure.PROVIDER_PROPERTY_KEY;
import static io.trino.plugin.iceberg.procedure.MigrateProcedure.PROVIDER_PROPERTY_VALUE;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.CatalogUtil.deleteRemovedMetadataFiles;
import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.TableProperties.METADATA_COMPRESSION;
import static org.apache.iceberg.TableProperties.METADATA_COMPRESSION_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_METADATA_LOCATION;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;

@NotThreadSafe
public abstract class AbstractIcebergTableOperations
        implements IcebergTableOperations
{
    public static final StorageFormat ICEBERG_METASTORE_STORAGE_FORMAT = StorageFormat.create(
            LAZY_SIMPLE_SERDE_CLASS,
            FILE_INPUT_FORMAT_CLASS,
            FILE_OUTPUT_FORMAT_CLASS);

    protected final ConnectorSession session;
    protected final String database;
    protected final String tableName;
    protected final Optional<String> owner;
    protected final Optional<String> location;
    protected final FileIO fileIo;

    protected TableMetadata currentMetadata;
    protected String currentMetadataLocation;
    protected boolean shouldRefresh = true;
    protected OptionalInt version = OptionalInt.empty();

    protected AbstractIcebergTableOperations(
            FileIO fileIo,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        this.fileIo = requireNonNull(fileIo, "fileIo is null");
        this.session = requireNonNull(session, "session is null");
        this.database = requireNonNull(database, "database is null");
        this.tableName = requireNonNull(table, "table is null");
        this.owner = requireNonNull(owner, "owner is null");
        this.location = requireNonNull(location, "location is null");
    }

    @Override
    public void initializeFromMetadata(TableMetadata tableMetadata)
    {
        checkState(currentMetadata == null, "already initialized");
        currentMetadata = tableMetadata;
        currentMetadataLocation = tableMetadata.metadataFileLocation();
        shouldRefresh = false;
        version = OptionalInt.of(parseVersion(Location.of(currentMetadataLocation).fileName()));
    }

    @Override
    public TableMetadata current()
    {
        if (shouldRefresh) {
            return refresh(false);
        }
        return currentMetadata;
    }

    @Override
    public TableMetadata refresh()
    {
        return refresh(true);
    }

    public TableMetadata refresh(boolean invalidateCaches)
    {
        if (location.isPresent()) {
            refreshFromMetadataLocation(null);
            return currentMetadata;
        }
        refreshFromMetadataLocation(fixBrokenMetadataLocation(getRefreshedLocation(invalidateCaches)));
        return currentMetadata;
    }

    @Override
    public void commit(@Nullable TableMetadata base, TableMetadata metadata)
    {
        requireNonNull(metadata, "metadata is null");

        // if the metadata is already out of date, reject it
        if (!Objects.equals(base, current())) {
            throw new CommitFailedException("Cannot commit: stale table metadata for %s", getSchemaTableName());
        }

        // if the metadata is not changed, return early
        if (Objects.equals(base, metadata)) {
            return;
        }

        if (isMaterializedViewStorage(tableName)) {
            commitMaterializedViewRefresh(base, metadata);
            return;
        }

        if (base == null) {
            if (PROVIDER_PROPERTY_VALUE.equals(metadata.properties().get(PROVIDER_PROPERTY_KEY))) {
                // Assume this is a table executing migrate procedure
                version = OptionalInt.of(0);
                currentMetadataLocation = metadata.properties().get(METADATA_LOCATION_PROP);
                commitToExistingTable(base, metadata);
            }
            else {
                commitNewTable(metadata);
            }
        }
        else {
            commitToExistingTable(base, metadata);
            deleteRemovedMetadataFiles(fileIo, base, metadata);
        }

        shouldRefresh = true;
    }

    protected abstract String getRefreshedLocation(boolean invalidateCaches);

    protected abstract void commitNewTable(TableMetadata metadata);

    protected abstract void commitToExistingTable(TableMetadata base, TableMetadata metadata);

    protected abstract void commitMaterializedViewRefresh(TableMetadata base, TableMetadata metadata);

    @Override
    public FileIO io()
    {
        return fileIo;
    }

    @Override
    public String metadataFileLocation(String filename)
    {
        TableMetadata metadata = current();
        String location;
        if (metadata != null) {
            String writeLocation = metadata.properties().get(WRITE_METADATA_LOCATION);
            if (writeLocation != null) {
                return format("%s/%s", stripTrailingSlash(writeLocation), filename);
            }
            location = metadata.location();
        }
        else {
            location = this.location.orElseThrow(() -> new IllegalStateException("Location not set"));
        }
        return format("%s/%s/%s", stripTrailingSlash(location), METADATA_FOLDER_NAME, filename);
    }

    @Override
    public LocationProvider locationProvider()
    {
        TableMetadata metadata = current();
        return getLocationProvider(getSchemaTableName(), metadata.location(), metadata.properties());
    }

    protected SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(database, tableName);
    }

    protected String writeNewMetadata(TableMetadata metadata, int newVersion)
    {
        String newTableMetadataFilePath = newTableMetadataFilePath(metadata, newVersion);
        OutputFile newMetadataLocation = fileIo.newOutputFile(newTableMetadataFilePath);

        // write the new metadata
        TableMetadataParser.write(metadata, newMetadataLocation);

        return newTableMetadataFilePath;
    }

    protected void refreshFromMetadataLocation(String newLocation)
    {
        refreshFromMetadataLocation(
                newLocation,
                metadataLocation -> TableMetadataParser.read(fileIo, fileIo.newInputFile(metadataLocation)));
    }

    protected void refreshFromMetadataLocation(String newLocation, Function<String, TableMetadata> metadataLoader)
    {
        // use null-safe equality check because new tables have a null metadata location
        if (Objects.equals(currentMetadataLocation, newLocation)) {
            shouldRefresh = false;
            return;
        }

        // a table that is replaced doesn't need its metadata reloaded
        if (newLocation == null) {
            shouldRefresh = false;
            return;
        }

        TableMetadata newMetadata;
        try {
            newMetadata = Failsafe.with(RetryPolicy.builder()
                            .withMaxRetries(3)
                            .withBackoff(100, 5000, MILLIS, 4.0)
                            .withMaxDuration(Duration.ofMinutes(3))
                            .abortOn(throwable -> TrinoFileSystem.isUnrecoverableException(throwable) || IcebergExceptions.isFatalException(throwable))
                            .build())
                    .get(() -> metadataLoader.apply(newLocation));
        }
        catch (UncheckedIOException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Error accessing metadata file for table %s".formatted(getSchemaTableName().toString()), e);
        }
        catch (Throwable failure) {
            throw translateMetadataException(failure, getSchemaTableName().toString());
        }

        String newUUID = newMetadata.uuid();
        if (currentMetadata != null) {
            checkState(newUUID == null || newUUID.equals(currentMetadata.uuid()),
                    "Table UUID does not match: current=%s != refreshed=%s", currentMetadata.uuid(), newUUID);
        }

        currentMetadata = newMetadata;
        currentMetadataLocation = newLocation;
        version = OptionalInt.of(parseVersion(Location.of(newLocation).fileName()));
        shouldRefresh = false;
    }

    protected static String newTableMetadataFilePath(TableMetadata meta, int newVersion)
    {
        String codec = meta.property(METADATA_COMPRESSION, METADATA_COMPRESSION_DEFAULT);
        return metadataFileLocation(meta, format("%05d-%s%s", newVersion, randomUUID(), getFileExtension(codec)));
    }

    protected static String metadataFileLocation(TableMetadata metadata, String filename)
    {
        String location = metadata.properties().get(WRITE_METADATA_LOCATION);
        if (location != null) {
            return format("%s/%s", stripTrailingSlash(location), filename);
        }
        return format("%s/%s/%s", stripTrailingSlash(metadata.location()), METADATA_FOLDER_NAME, filename);
    }

    public static List<Column> toHiveColumns(List<NestedField> columns)
    {
        return columns.stream()
                .map(column -> new Column(
                        column.name(),
                        HiveType.fromTypeInfo(HiveSchemaUtil.convert(column.type())),
                        Optional.empty(),
                        Map.of()))
                .collect(toImmutableList());
    }
}
