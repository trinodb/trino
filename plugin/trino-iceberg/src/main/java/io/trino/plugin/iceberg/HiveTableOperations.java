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

import io.airlift.log.Logger;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.Tasks;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergUtil.isIcebergTable;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.TableProperties.METADATA_COMPRESSION;
import static org.apache.iceberg.TableProperties.METADATA_COMPRESSION_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_METADATA_LOCATION;

@NotThreadSafe
public class HiveTableOperations
        implements TableOperations
{
    private static final Logger log = Logger.get(HiveTableOperations.class);

    public static final String METADATA_LOCATION = "metadata_location";
    public static final String PREVIOUS_METADATA_LOCATION = "previous_metadata_location";
    private static final String METADATA_FOLDER_NAME = "metadata";

    private static final StorageFormat STORAGE_FORMAT = StorageFormat.create(
            LazySimpleSerDe.class.getName(),
            FileInputFormat.class.getName(),
            FileOutputFormat.class.getName());

    private final HiveMetastore metastore;
    private final HiveIdentity identity;
    private final String database;
    private final String tableName;
    private final Optional<String> owner;
    private final Optional<String> location;
    private final FileIO fileIo;

    private TableMetadata currentMetadata;
    private String currentMetadataLocation;
    private boolean shouldRefresh = true;
    private int version = -1;

    HiveTableOperations(FileIO fileIo, HiveMetastore metastore, HiveIdentity identity, String database, String table, Optional<String> owner, Optional<String> location)
    {
        this.fileIo = requireNonNull(fileIo, "fileIo is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.identity = requireNonNull(identity, "identity is null");
        this.database = requireNonNull(database, "database is null");
        this.tableName = requireNonNull(table, "table is null");
        this.owner = requireNonNull(owner, "owner is null");
        this.location = requireNonNull(location, "location is null");
    }

    public void initializeFromMetadata(TableMetadata tableMetadata)
    {
        checkState(currentMetadata == null, "already initialized");
        currentMetadata = tableMetadata;
        currentMetadataLocation = tableMetadata.metadataFileLocation();
        shouldRefresh = false;
        version = parseVersion(currentMetadataLocation);
    }

    @Override
    public TableMetadata current()
    {
        if (shouldRefresh) {
            return refresh();
        }
        return currentMetadata;
    }

    @Override
    public TableMetadata refresh()
    {
        if (location.isPresent()) {
            refreshFromMetadataLocation(null);
            return currentMetadata;
        }

        Table table = getTable();

        if (!isIcebergTable(table)) {
            throw new UnknownTableTypeException(getSchemaTableName());
        }

        String metadataLocation = table.getParameters().get(METADATA_LOCATION);
        if (metadataLocation == null) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Table is missing [%s] property: %s", METADATA_LOCATION, getSchemaTableName()));
        }

        refreshFromMetadataLocation(metadataLocation);

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

        String newMetadataLocation = writeNewMetadata(metadata, version + 1);

        // TODO: use metastore locking

        Table table;
        try {
            if (base == null) {
                Table.Builder builder = Table.builder()
                        .setDatabaseName(database)
                        .setTableName(tableName)
                        .setOwner(owner.orElseThrow(() -> new IllegalStateException("Owner not set")))
                        .setTableType(TableType.EXTERNAL_TABLE.name())
                        .setDataColumns(toHiveColumns(metadata.schema().columns()))
                        .withStorage(storage -> storage.setLocation(metadata.location()))
                        .withStorage(storage -> storage.setStorageFormat(STORAGE_FORMAT))
                        .setParameter("EXTERNAL", "TRUE")
                        .setParameter(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE)
                        .setParameter(METADATA_LOCATION, newMetadataLocation);
                String tableComment = metadata.properties().get(TABLE_COMMENT);
                if (tableComment != null) {
                    builder.setParameter(TABLE_COMMENT, tableComment);
                }
                table = builder.build();
            }
            else {
                Table currentTable = getTable();

                checkState(currentMetadataLocation != null, "No current metadata location for existing table");
                String metadataLocation = currentTable.getParameters().get(METADATA_LOCATION);
                if (!currentMetadataLocation.equals(metadataLocation)) {
                    throw new CommitFailedException("Metadata location [%s] is not same as table metadata location [%s] for %s",
                            currentMetadataLocation, metadataLocation, getSchemaTableName());
                }

                table = Table.builder(currentTable)
                        .setDataColumns(toHiveColumns(metadata.schema().columns()))
                        .withStorage(storage -> storage.setLocation(metadata.location()))
                        .setParameter(METADATA_LOCATION, newMetadataLocation)
                        .setParameter(PREVIOUS_METADATA_LOCATION, currentMetadataLocation)
                        .build();
            }
        }
        catch (RuntimeException e) {
            try {
                io().deleteFile(newMetadataLocation);
            }
            catch (RuntimeException ex) {
                e.addSuppressed(ex);
            }
            throw e;
        }

        PrincipalPrivileges privileges = buildInitialPrivilegeSet(table.getOwner());
        ConnectorIdentity identity = ConnectorIdentity.ofUser(table.getOwner());
        HiveIdentity context = new HiveIdentity(identity);
        if (base == null) {
            metastore.createTable(context, table, privileges);
        }
        else {
            metastore.replaceTable(context, database, tableName, table, privileges);
        }

        shouldRefresh = true;
    }

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
                return format("%s/%s", writeLocation, filename);
            }
            location = metadata.location();
        }
        else {
            location = this.location.orElseThrow(() -> new IllegalStateException("Location not set"));
        }
        return format("%s/%s/%s", location, METADATA_FOLDER_NAME, filename);
    }

    @Override
    public LocationProvider locationProvider()
    {
        TableMetadata metadata = current();
        return LocationProviders.locationsFor(metadata.location(), metadata.properties());
    }

    private Table getTable()
    {
        return metastore.getTable(identity, database, tableName)
                .orElseThrow(() -> new TableNotFoundException(getSchemaTableName()));
    }

    private SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(database, tableName);
    }

    private String writeNewMetadata(TableMetadata metadata, int newVersion)
    {
        String newTableMetadataFilePath = newTableMetadataFilePath(metadata, newVersion);
        OutputFile newMetadataLocation = fileIo.newOutputFile(newTableMetadataFilePath);

        // write the new metadata
        TableMetadataParser.write(metadata, newMetadataLocation);

        return newTableMetadataFilePath;
    }

    private void refreshFromMetadataLocation(String newLocation)
    {
        // use null-safe equality check because new tables have a null metadata location
        if (Objects.equals(currentMetadataLocation, newLocation)) {
            shouldRefresh = false;
            return;
        }

        AtomicReference<TableMetadata> newMetadata = new AtomicReference<>();
        Tasks.foreach(newLocation)
                .retry(20)
                .exponentialBackoff(100, 5000, 600000, 4.0)
                .suppressFailureWhenFinished()
                .run(metadataLocation -> newMetadata.set(
                        TableMetadataParser.read(this, io().newInputFile(metadataLocation))));

        String newUUID = newMetadata.get().uuid();
        if (currentMetadata != null) {
            checkState(newUUID == null || newUUID.equals(currentMetadata.uuid()),
                    "Table UUID does not match: current=%s != refreshed=%s", currentMetadata.uuid(), newUUID);
        }

        currentMetadata = newMetadata.get();
        currentMetadataLocation = newLocation;
        version = parseVersion(newLocation);
        shouldRefresh = false;
    }

    private static String newTableMetadataFilePath(TableMetadata meta, int newVersion)
    {
        String codec = meta.property(METADATA_COMPRESSION, METADATA_COMPRESSION_DEFAULT);
        return metadataFileLocation(meta, format("%05d-%s%s", newVersion, randomUUID(), getFileExtension(codec)));
    }

    private static String metadataFileLocation(TableMetadata metadata, String filename)
    {
        String location = metadata.properties().get(WRITE_METADATA_LOCATION);
        if (location != null) {
            return format("%s/%s", location, filename);
        }
        return format("%s/%s/%s", metadata.location(), METADATA_FOLDER_NAME, filename);
    }

    private static int parseVersion(String metadataLocation)
    {
        int versionStart = metadataLocation.lastIndexOf('/') + 1; // if '/' isn't found, this will be 0
        int versionEnd = metadataLocation.indexOf('-', versionStart);
        try {
            return parseInt(metadataLocation.substring(versionStart, versionEnd));
        }
        catch (NumberFormatException | IndexOutOfBoundsException e) {
            log.warn(e, "Unable to parse version from metadata location: %s", metadataLocation);
            return -1;
        }
    }

    private static List<Column> toHiveColumns(List<NestedField> columns)
    {
        return columns.stream()
                .map(column -> new Column(
                        column.name(),
                        toHiveType(HiveSchemaUtil.convert(column.type())),
                        Optional.empty()))
                .collect(toImmutableList());
    }
}
