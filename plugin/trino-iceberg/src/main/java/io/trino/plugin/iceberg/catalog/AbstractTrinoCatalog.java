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

import com.google.common.collect.ImmutableMap;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HiveMetadata;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergMaterializedViewDefinition;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.PartitionTransforms.ColumnTransform;
import io.trino.plugin.iceberg.fileio.ForwardingOutputFile;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HiveMetadata.STORAGE_TABLE;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.ViewReaderUtil.ICEBERG_MATERIALIZED_VIEW_COMMENT;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.mappedCopy;
import static io.trino.plugin.hive.util.HiveUtil.escapeTableName;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.decodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergMaterializedViewProperties.STORAGE_SCHEMA;
import static io.trino.plugin.iceberg.IcebergMaterializedViewProperties.getStorageSchema;
import static io.trino.plugin.iceberg.IcebergTableName.tableNameWithType;
import static io.trino.plugin.iceberg.IcebergTableProperties.getPartitioning;
import static io.trino.plugin.iceberg.IcebergTableProperties.getSortOrder;
import static io.trino.plugin.iceberg.IcebergTableProperties.getTableLocation;
import static io.trino.plugin.iceberg.IcebergUtil.METADATA_FOLDER_NAME;
import static io.trino.plugin.iceberg.IcebergUtil.commit;
import static io.trino.plugin.iceberg.IcebergUtil.createTableProperties;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableProperties;
import static io.trino.plugin.iceberg.IcebergUtil.schemaFromMetadata;
import static io.trino.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.trino.plugin.iceberg.PartitionTransforms.getColumnTransform;
import static io.trino.plugin.iceberg.SortFieldUtils.parseSortFields;
import static io.trino.plugin.iceberg.TableType.MATERIALIZED_VIEW_STORAGE;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.TableProperties.METADATA_COMPRESSION_DEFAULT;
import static org.apache.iceberg.Transactions.createOrReplaceTableTransaction;
import static org.apache.iceberg.Transactions.createTableTransaction;

public abstract class AbstractTrinoCatalog
        implements TrinoCatalog
{
    public static final String TRINO_CREATED_BY_VALUE = "Trino Iceberg connector";
    protected static final String TRINO_CREATED_BY = HiveMetadata.TRINO_CREATED_BY;
    protected static final String TRINO_QUERY_ID_NAME = HiveMetadata.TRINO_QUERY_ID_NAME;

    private final CatalogName catalogName;
    private final TypeManager typeManager;
    protected final IcebergTableOperationsProvider tableOperationsProvider;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final boolean useUniqueTableLocation;

    protected AbstractTrinoCatalog(
            CatalogName catalogName,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            TrinoFileSystemFactory fileSystemFactory,
            boolean useUniqueTableLocation)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.useUniqueTableLocation = useUniqueTableLocation;
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        Table icebergTable = loadTable(session, schemaTableName);
        if (comment.isEmpty()) {
            icebergTable.updateProperties().remove(TABLE_COMMENT).commit();
        }
        else {
            icebergTable.updateProperties().set(TABLE_COMMENT, comment.get()).commit();
        }
        invalidateTableCache(schemaTableName);
    }

    @Override
    public void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment)
    {
        Table icebergTable = loadTable(session, schemaTableName);
        icebergTable.updateSchema().updateColumnDoc(columnIdentity.getName(), comment.orElse(null)).commit();
        invalidateTableCache(schemaTableName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        for (SchemaTableName name : listViews(session, namespace)) {
            try {
                getView(session, name).ifPresent(view -> views.put(name, view));
            }
            catch (TrinoException e) {
                if (e.getErrorCode().equals(TABLE_NOT_FOUND.toErrorCode()) || e instanceof TableNotFoundException || e instanceof ViewNotFoundException) {
                    // Ignore view that was dropped during query execution (race condition)
                }
                else {
                    throw e;
                }
            }
        }
        return views.buildOrThrow();
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        try {
            return Failsafe.with(RetryPolicy.builder()
                            .withMaxAttempts(10)
                            .withBackoff(1, 5_000, ChronoUnit.MILLIS, 4)
                            .withMaxDuration(Duration.ofSeconds(30))
                            .abortOn(failure -> !(failure instanceof MaterializedViewMayBeBeingRemovedException))
                            .build())
                    .get(() -> doGetMaterializedView(session, schemaViewName));
        }
        catch (MaterializedViewMayBeBeingRemovedException e) {
            throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    protected abstract Optional<ConnectorMaterializedViewDefinition> doGetMaterializedView(ConnectorSession session, SchemaTableName schemaViewName);

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
    {
        SchemaTableName storageTableName = definition.getStorageTable()
                .orElseThrow(() -> new TrinoException(ICEBERG_INVALID_METADATA, "Materialized view definition is missing a storage table"))
                .getSchemaTableName();

        try {
            Table storageTable = loadTable(session, definition.getStorageTable().orElseThrow().getSchemaTableName());
            return ImmutableMap.<String, Object>builder()
                    .putAll(getIcebergTableProperties(storageTable))
                    .put(STORAGE_SCHEMA, storageTableName.getSchemaName())
                    .buildOrThrow();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Unable to load storage table metadata for materialized view: " + viewName);
        }
    }

    protected Transaction newCreateTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            SortOrder sortOrder,
            String location,
            Map<String, String> properties,
            Optional<String> owner)
    {
        TableMetadata metadata = newTableMetadata(schema, partitionSpec, sortOrder, location, properties);
        TableOperations ops = tableOperationsProvider.createTableOperations(
                this,
                session,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                owner,
                Optional.of(location));
        return createTableTransaction(schemaTableName.toString(), ops, metadata);
    }

    protected Transaction newCreateOrReplaceTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            SortOrder sortOrder,
            String location,
            Map<String, String> properties,
            Optional<String> owner)
    {
        BaseTable table;
        Optional<TableMetadata> metadata = Optional.empty();
        try {
            table = (BaseTable) loadTable(session, new SchemaTableName(schemaTableName.getSchemaName(), schemaTableName.getTableName()));
            metadata = Optional.of(table.operations().current());
        }
        catch (TableNotFoundException ignored) {
            // ignored
        }
        IcebergTableOperations operations = tableOperationsProvider.createTableOperations(
                this,
                session,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                owner,
                Optional.of(location));
        TableMetadata newMetaData;
        if (metadata.isPresent()) {
            operations.initializeFromMetadata(metadata.get());
            newMetaData = operations.current()
                    // don't inherit table properties from earlier snapshots
                    .replaceProperties(properties)
                    .buildReplacement(schema, partitionSpec, sortOrder, location, properties);
        }
        else {
            newMetaData = newTableMetadata(schema, partitionSpec, sortOrder, location, properties);
        }
        return createOrReplaceTableTransaction(schemaTableName.toString(), operations, newMetaData);
    }

    protected String createNewTableName(String baseTableName)
    {
        String tableNameLocationComponent = escapeTableName(baseTableName);
        if (useUniqueTableLocation) {
            tableNameLocationComponent += "-" + randomUUID().toString().replace("-", "");
        }
        return tableNameLocationComponent;
    }

    protected void deleteTableDirectory(TrinoFileSystem fileSystem, SchemaTableName schemaTableName, String tableLocation)
    {
        try {
            fileSystem.deleteDirectory(Location.of(tableLocation));
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, format("Failed to delete directory %s of the table %s", tableLocation, schemaTableName), e);
        }
    }

    protected Location createMaterializedViewStorage(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties)
    {
        if (getStorageSchema(materializedViewProperties).isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Materialized view property '%s' is not supported when hiding materialized view storage tables is enabled".formatted(STORAGE_SCHEMA));
        }
        SchemaTableName storageTableName = new SchemaTableName(viewName.getSchemaName(), tableNameWithType(viewName.getTableName(), MATERIALIZED_VIEW_STORAGE));
        String tableLocation = getTableLocation(materializedViewProperties)
                .orElseGet(() -> defaultTableLocation(session, viewName));
        List<ColumnMetadata> columns = columnsForMaterializedView(definition, materializedViewProperties);

        Schema schema = schemaFromMetadata(columns);
        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(materializedViewProperties));
        SortOrder sortOrder = parseSortFields(schema, getSortOrder(materializedViewProperties));
        Map<String, String> properties = createTableProperties(new ConnectorTableMetadata(storageTableName, columns, materializedViewProperties, Optional.empty()));

        TableMetadata metadata = newTableMetadata(schema, partitionSpec, sortOrder, tableLocation, properties);

        String fileName = format("%05d-%s%s", 0, randomUUID(), getFileExtension(METADATA_COMPRESSION_DEFAULT));
        Location metadataFileLocation = Location.of(tableLocation).appendPath(METADATA_FOLDER_NAME).appendPath(fileName);

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TableMetadataParser.write(metadata, new ForwardingOutputFile(fileSystem, metadataFileLocation));

        return metadataFileLocation;
    }

    protected SchemaTableName createMaterializedViewStorageTable(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties)
    {
        // Generate a storage table name and create a storage table. The properties in the definition are table properties for the
        // storage table as indicated in the materialized view definition.
        String storageTableName = "st_" + randomUUID().toString().replace("-", "");

        String storageSchema = getStorageSchema(materializedViewProperties).orElse(viewName.getSchemaName());
        SchemaTableName storageTable = new SchemaTableName(storageSchema, storageTableName);
        List<ColumnMetadata> columns = columnsForMaterializedView(definition, materializedViewProperties);

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(storageTable, columns, materializedViewProperties, Optional.empty());
        String tableLocation = getTableLocation(tableMetadata.getProperties())
                .orElseGet(() -> defaultTableLocation(session, tableMetadata.getTable()));
        Transaction transaction = IcebergUtil.newCreateTableTransaction(this, tableMetadata, session, false, tableLocation);
        AppendFiles appendFiles = transaction.newAppend();
        commit(appendFiles, session);
        transaction.commitTransaction();
        return storageTable;
    }

    private List<ColumnMetadata> columnsForMaterializedView(ConnectorMaterializedViewDefinition definition, Map<String, Object> materializedViewProperties)
    {
        Schema schemaWithTimestampTzPreserved = schemaFromMetadata(mappedCopy(
                definition.getColumns(),
                column -> {
                    Type type = typeManager.getType(column.getType());
                    if (type instanceof TimestampWithTimeZoneType timestampTzType && timestampTzType.getPrecision() <= 6) {
                        // For now preserve timestamptz columns so that we can parse partitioning
                        type = TIMESTAMP_TZ_MICROS;
                    }
                    else {
                        type = typeForMaterializedViewStorageTable(type);
                    }
                    return new ColumnMetadata(column.getName(), type);
                }));
        PartitionSpec partitionSpec = parsePartitionFields(schemaWithTimestampTzPreserved, getPartitioning(materializedViewProperties));
        Set<String> temporalPartitioningSources = partitionSpec.fields().stream()
                .flatMap(partitionField -> {
                    Types.NestedField sourceField = schemaWithTimestampTzPreserved.findField(partitionField.sourceId());
                    Type sourceType = toTrinoType(sourceField.type(), typeManager);
                    ColumnTransform columnTransform = getColumnTransform(partitionField, sourceType);
                    if (!columnTransform.isTemporal()) {
                        return Stream.of();
                    }
                    return Stream.of(sourceField.name());
                })
                .collect(toImmutableSet());

        return mappedCopy(
                definition.getColumns(),
                column -> {
                    Type type = typeManager.getType(column.getType());
                    if (type instanceof TimestampWithTimeZoneType timestampTzType && timestampTzType.getPrecision() <= 6 && temporalPartitioningSources.contains(column.getName())) {
                        // Apply point-in-time semantics to maintain partitioning capabilities
                        type = TIMESTAMP_TZ_MICROS;
                    }
                    else {
                        type = typeForMaterializedViewStorageTable(type);
                    }
                    return new ColumnMetadata(column.getName(), type);
                });
    }

    /**
     * Substitutes type not supported by Iceberg with a type that is supported.
     * Upon reading from a materialized view, the types will be coerced back to the original ones,
     * stored in the materialized view definition.
     */
    private Type typeForMaterializedViewStorageTable(Type type)
    {
        if (type == TINYINT || type == SMALLINT) {
            return INTEGER;
        }
        if (type instanceof CharType) {
            return VARCHAR;
        }
        if (type instanceof TimeType timeType) {
            // Iceberg supports microsecond precision only
            return timeType.getPrecision() <= 6
                    ? TIME_MICROS
                    : VARCHAR;
        }
        if (type instanceof TimeWithTimeZoneType) {
            return VARCHAR;
        }
        if (type instanceof TimestampType timestampType) {
            // Iceberg supports microsecond precision only
            return timestampType.getPrecision() <= 6
                    ? TIMESTAMP_MICROS
                    : VARCHAR;
        }
        if (type instanceof TimestampWithTimeZoneType) {
            // Iceberg does not store the time zone.
            return VARCHAR;
        }
        if (type instanceof ArrayType arrayType) {
            return new ArrayType(typeForMaterializedViewStorageTable(arrayType.getElementType()));
        }
        if (type instanceof MapType mapType) {
            return new MapType(
                    typeForMaterializedViewStorageTable(mapType.getKeyType()),
                    typeForMaterializedViewStorageTable(mapType.getValueType()),
                    typeManager.getTypeOperators());
        }
        if (type instanceof RowType rowType) {
            return RowType.rowType(
                    rowType.getFields().stream()
                            .map(field -> new RowType.Field(field.getName(), typeForMaterializedViewStorageTable(field.getType())))
                            .toArray(RowType.Field[]::new));
        }

        // Pass through all the types not explicitly handled above. If a type is not accepted by the connector,
        // creation of the storage table will fail anyway.
        return type;
    }

    protected ConnectorMaterializedViewDefinition getMaterializedViewDefinition(
            Optional<String> owner,
            String viewOriginalText,
            SchemaTableName storageTableName)
    {
        IcebergMaterializedViewDefinition definition = decodeMaterializedViewData(viewOriginalText);
        return new ConnectorMaterializedViewDefinition(
                definition.getOriginalSql(),
                Optional.of(new CatalogSchemaTableName(catalogName.toString(), storageTableName)),
                definition.getCatalog(),
                definition.getSchema(),
                toSpiMaterializedViewColumns(definition.getColumns()),
                definition.getGracePeriod(),
                definition.getComment(),
                owner,
                definition.getPath());
    }

    protected List<ConnectorMaterializedViewDefinition.Column> toSpiMaterializedViewColumns(List<IcebergMaterializedViewDefinition.Column> columns)
    {
        return columns.stream()
                .map(column -> new ConnectorMaterializedViewDefinition.Column(column.getName(), column.getType(), column.getComment()))
                .collect(toImmutableList());
    }

    protected Map<String, String> createMaterializedViewProperties(ConnectorSession session, SchemaTableName storageTableName)
    {
        return ImmutableMap.<String, String>builder()
                .put(TRINO_QUERY_ID_NAME, session.getQueryId())
                .put(STORAGE_SCHEMA, storageTableName.getSchemaName())
                .put(STORAGE_TABLE, storageTableName.getTableName())
                .put(PRESTO_VIEW_FLAG, "true")
                .put(TRINO_CREATED_BY, TRINO_CREATED_BY_VALUE)
                .put(TABLE_COMMENT, ICEBERG_MATERIALIZED_VIEW_COMMENT)
                .buildOrThrow();
    }

    protected Map<String, String> createMaterializedViewProperties(ConnectorSession session, Location storageMetadataLocation)
    {
        return ImmutableMap.<String, String>builder()
                .put(TRINO_QUERY_ID_NAME, session.getQueryId())
                .put(METADATA_LOCATION_PROP, storageMetadataLocation.toString())
                .put(PRESTO_VIEW_FLAG, "true")
                .put(TRINO_CREATED_BY, TRINO_CREATED_BY_VALUE)
                .put(TABLE_COMMENT, ICEBERG_MATERIALIZED_VIEW_COMMENT)
                .buildOrThrow();
    }

    protected abstract void invalidateTableCache(SchemaTableName schemaTableName);

    protected static class MaterializedViewMayBeBeingRemovedException
            extends RuntimeException
    {
        public MaterializedViewMayBeBeingRemovedException(Throwable cause)
        {
            super(requireNonNull(cause, "cause is null"));
        }
    }
}
