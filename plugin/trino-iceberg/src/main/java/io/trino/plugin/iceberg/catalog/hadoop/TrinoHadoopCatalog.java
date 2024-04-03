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
package io.trino.plugin.iceberg.catalog.hadoop;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.util.Pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.TrinoMetricsReporter.TRINO_METRICS_REPORTER;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class TrinoHadoopCatalog
        extends AbstractTrinoCatalog
{
    private static final Joiner SLASH = Joiner.on("/");
    private static final Splitter DOT = Splitter.on('.');
    private static final int PER_QUERY_CACHES_SIZE = 1000;
    private final TrinoFileSystem trinoFileSystem;
    private final CatalogName catalogName;
    private final String warehouse;
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final Cache<SchemaTableName, TableMetadata> tableMetadataCache = EvictableCacheBuilder.newBuilder()
            .maximumSize(PER_QUERY_CACHES_SIZE)
            .build();

    public TrinoHadoopCatalog(
            CatalogName catalogName,
            TypeManager typeManager,
            ConnectorIdentity identity,
            IcebergTableOperationsProvider tableOperationsProvider,
            TrinoFileSystemFactory fileSystemFactory,
            boolean useUniqueTableLocation,
            IcebergConfig icebergConfig)
    {
        super(catalogName, typeManager, tableOperationsProvider, fileSystemFactory, useUniqueTableLocation);
        checkArgument(
                !Strings.isNullOrEmpty(icebergConfig.getCatalogWarehouse()),
                "Cannot initialize HadoopCatalog because warehousePath must not be null or empty");
        this.warehouse = icebergConfig.getCatalogWarehouse();
        this.trinoFileSystem = fileSystemFactory.create(identity);
        this.catalogName = catalogName;
        this.tableOperationsProvider = tableOperationsProvider;
    }

    @Override
    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        String ns = splitQualifiedNamespace(namespace);

        if (!ns.equals(ns.toLowerCase(ENGLISH))) {
            // Currently, Trino schemas are always lowercase, so this one cannot exist (https://github.com/trinodb/trino/issues/17)
            return false;
        }
        return listNamespaces(session).stream().filter(ns::equals).findAny().orElse(null) != null;
    }

    private List<String> listNamespaces(ConnectorSession session, Optional<String> namespace)
    {
        if (namespace.isPresent()) {
            return ImmutableList.of(namespace.get());
        }
        return listNamespaces(session);
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        try {
            return trinoFileSystem.listDirectories(Location.of(warehouse)).stream().filter(this::isNamespace).map(this::extractFilename).toList();
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "could not list namespaces", e);
        }
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        Location nsLocation = Location.of(SLASH.join(warehouse, SLASH.join(DOT.split(namespace))));

        if (!isNamespace(nsLocation) || namespace.isEmpty()) {
            throw new TrinoException(SCHEMA_NOT_FOUND, "could not find namespace");
        }

        try {
            if (!trinoFileSystem.listDirectories(nsLocation).isEmpty() || trinoFileSystem.listFiles(nsLocation).hasNext()) {
                throw new TrinoException(SCHEMA_NOT_EMPTY, String.format("Namespace %s is not empty.", namespace));
            }

            trinoFileSystem.deleteDirectory(nsLocation);
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, String.format("Namespace delete failed: %s", namespace), e);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        Location nsLocation = Location.of(SLASH.join(warehouse, SLASH.join(DOT.split(namespace))));

        if (!isNamespace(nsLocation) || namespace.isEmpty()) {
            throw new TrinoException(SCHEMA_NOT_FOUND, "could not find namespace");
        }
        return ImmutableMap.of("location", nsLocation.toString());
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        checkArgument(owner.getType() == PrincipalType.USER, "Owner type must be USER");
        checkArgument(owner.getName().equals(session.getUser().toLowerCase(ENGLISH)), "Explicit schema owner is not supported");
        checkArgument(
                !namespace.isEmpty(), "Cannot create namespace with invalid name: %s", namespace);
        if (!properties.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Cannot create namespace " + namespace + ": metadata is not supported");
        }

        Location nsLocation = Location.of(SLASH.join(warehouse, SLASH.join(DOT.split(namespace))));
        if (isNamespace(nsLocation)) {
            throw new TrinoException(SCHEMA_ALREADY_EXISTS, String.format("Namespace already exists: %s", namespace));
        }

        try {
            trinoFileSystem.createDirectory(nsLocation);
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, String.format("Create namespace failed: %s", namespace), e);
        }
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        throw new TrinoException(NOT_SUPPORTED, "getNamespacePrincipal is not supported by " + catalogName);
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setNamespacePrincipal is not supported by " + catalogName);
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameNamespace is not supported by " + catalogName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
    {
        ImmutableList.Builder<SchemaTableName> schemaTableNames = ImmutableList.builder();
        try {
            for (String ns : listNamespaces(session, namespace)) {
                Location nsLocation = Location.of(SLASH.join(warehouse, ns));
                if (!isDirectory(nsLocation)) {
                    throw new TrinoException(SCHEMA_NOT_FOUND, String.format("could not find namespace %s", ns));
                }
                schemaTableNames.addAll(trinoFileSystem.listDirectories(nsLocation).stream()
                        .filter(this::isTableDir).map(tableLocation -> new SchemaTableName(ns, extractFilename(tableLocation))).toList());
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, String.format("Failed to list tables under: %s", namespace), e);
        }
        return schemaTableNames.build();
    }

    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(ConnectorSession session, Optional<String> namespace)
    {
        // views and materialized views are currently not supported
        verify(listViews(session, namespace).isEmpty(), "Unexpected views support");
        verify(listMaterializedViews(session, namespace).isEmpty(), "Unexpected views support");
        return listTables(session, namespace).stream()
                .collect(toImmutableMap(identity(), ignore -> RelationType.TABLE));
    }

    @Override
    public Optional<Iterator<RelationColumnsMetadata>> streamRelationColumns(ConnectorSession session, Optional<String> namespace, UnaryOperator<Set<SchemaTableName>> relationFilter, Predicate<SchemaTableName> isRedirected)
    {
        return Optional.empty();
    }

    @Override
    public Optional<Iterator<RelationCommentMetadata>> streamRelationComments(ConnectorSession session, Optional<String> namespace, UnaryOperator<Set<SchemaTableName>> relationFilter, Predicate<SchemaTableName> isRedirected)
    {
        return Optional.empty();
    }

    @Override
    public Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder, String location, Map<String, String> properties)
    {
        return newCreateTableTransaction(
                session,
                schemaTableName,
                schema,
                partitionSpec,
                sortOrder,
                location,
                properties,
                Optional.of(session.getUser()));
    }

    @Override
    public Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder, String location, Map<String, String> properties, Optional<String> owner)
    {
        // Location cannot be specified for hadoop tables.
        return builder(schemaTableName, schema)
                .withPartitionSpec(partitionSpec)
                .withProperties(properties)
                .createTransaction(session);
    }

    @Override
    public Transaction newCreateOrReplaceTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder, String location, Map<String, String> properties)
    {
        return newCreateOrReplaceTableTransaction(
                session,
                schemaTableName,
                schema,
                partitionSpec,
                sortOrder,
                location,
                properties,
                Optional.of(session.getUser()));
    }

    @Override
    public void registerTable(ConnectorSession session, SchemaTableName schemaTableName, TableMetadata tableMetadata)
    {
        requireNonNull(schemaTableName);
        requireNonNull(tableMetadata);
        checkArgument(tableMetadata.metadataFileLocation() != null && !tableMetadata.metadataFileLocation().isEmpty(),
                "Cannot register an empty metadata file location as a table");

        // Throw an exception if this table already exists in the catalog.
        if (tableExists(session, Optional.empty(), schemaTableName)) {
            throw new AlreadyExistsException("Table already exists: %s", schemaTableName.getTableName());
        }

        TableOperations ops = newTableOps(session, Optional.empty(), schemaTableName);
        try (FileIO fileIo = new ForwardingFileIo(trinoFileSystem)) {
            InputFile metadataFile = fileIo.newInputFile(tableLocation(schemaTableName));
            TableMetadata metadata = TableMetadataParser.read(ops.io(), metadataFile);
            ops.commit(null, metadata);
        }
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName tableName)
    {
        dropTable(session, tableName);
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        TableOperations ops = newTableOps(session, Optional.empty(), schemaTableName);
        try {
            // Since the data files and the metadata files may store in different locations,
            // so it has to call dropTableData to force delete the data file.
            CatalogUtil.dropTableData(ops.io(), ops.current());
            trinoFileSystem.deleteDirectory(Location.of(tableLocation(schemaTableName)));
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to delete table: " + schemaTableName, e);
        }
//        invalidateTableCache(schemaTableName);
    }

    @Override
    public void dropCorruptedTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        dropTable(session, schemaTableName);
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameTable is not supported by " + catalogName);
    }

//    @Override
//    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
//    {
//        TableMetadata metadata;
//        try {
//            metadata = uncheckedCacheGet(
//                    tableMetadataCache,
//                    schemaTableName,
//                    () -> {
//                        IcebergTableOperations operations = tableOperationsProvider.createTableOperations(
//                                this,
//                                session,
//                                schemaTableName.getSchemaName(),
//                                schemaTableName.getTableName(),
//                                Optional.empty(),
//                                Optional.ofNullable(tableLocation(schemaTableName)));
//                        operations.refresh();
//                        return new BaseTable(operations, quotedTableName(schemaTableName), TRINO_METRICS_REPORTER).operations().current();
//                    });
//        }
//        catch (UncheckedExecutionException e) {
//            throwIfUnchecked(e.getCause());
//            throw new NoSuchTableException("Table does not exist at location: %s", tableLocation(schemaTableName));
//        }
//
//        return getIcebergTableWithMetadata(this, tableOperationsProvider, session, schemaTableName, metadata);
//    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Table result;
        String location = tableLocation(schemaTableName);
        Pair<String, MetadataTableType> parsedMetadataType = parseMetadataType(location);

        if (parsedMetadataType != null) {
            // Load a metadata table
            result = loadMetadataTable(session, Optional.empty(), schemaTableName, parsedMetadataType.first(), parsedMetadataType.second());
        }
        else {
            // Load a normal table
            TableOperations ops = newTableOps(session, Optional.empty(), schemaTableName);
            if (ops.current() != null) {
                result = new BaseTable(ops, location, TRINO_METRICS_REPORTER);
            }
            else {
                throw new TableNotFoundException(schemaTableName, String.format("Table %1s does not exist at location: %2s", schemaTableName.getTableName(), location));
            }
        }
        return result;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> tryGetColumnMetadata(ConnectorSession session, List<SchemaTableName> tables)
    {
        return null;
    }

    public String tableLocation(SchemaTableName schemaTableName)
    {
        return SLASH.join(this.warehouse, schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    @Override
    protected void invalidateTableCache(SchemaTableName schemaTableName)
    {
        tableMetadataCache.invalidate(schemaTableName);
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setTablePrincipal is not supported by " + catalogName);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        throw new TrinoException(NOT_SUPPORTED, "createView is not supported by " + catalogName);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameView is not supported by " + catalogName);
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setViewPrincipal is not supported by " + catalogName);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropView is not supported by " + catalogName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        return ImmutableList.of();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> namespace)
    {
        return ImmutableList.of();
    }

    @Override
    public void updateViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateViewComment is not supported by " + catalogName);
    }

    @Override
    public void updateViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateViewColumnComment is not supported by " + catalogName);
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return tableLocation(schemaTableName);
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties,
            boolean replace,
            boolean ignoreExisting)
    {
        throw new TrinoException(NOT_SUPPORTED, "createMaterializedView is not supported by " + catalogName);
    }

    @Override
    public void updateMaterializedViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateMaterializedViewColumnComment is not supported by " + catalogName);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropMaterializedView is not supported by " + catalogName);
    }

    @Override
    public Optional<BaseTable> getMaterializedViewStorageTable(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameMaterializedView is not supported by " + catalogName);
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName, String catalogName)
    {
        return Optional.empty();
    }

    @Override
    protected Optional<ConnectorMaterializedViewDefinition> doGetMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        return Optional.empty();
    }

    private String extractFilename(Location location)
    {
        String filePath = location.path();
        String trimmedFilePath = "";
        if (filePath != null && filePath.endsWith("/")) {
            trimmedFilePath = CharMatcher.is('/').trimFrom(filePath);
            return trimmedFilePath.substring(trimmedFilePath.lastIndexOf('/') + 1);
        }
        return filePath.substring(filePath.lastIndexOf('/') + 1);
    }

    public CatalogName getCatalogName()
    {
        return catalogName;
    }

    public TrinoFileSystem getTrinoFileSystem()
    {
        return trinoFileSystem;
    }

    private boolean isDirectory(Location location)
    {
        try {
            Optional<Boolean> directoryExists = trinoFileSystem.directoryExists(location);

            if (directoryExists.isPresent()) {
                return directoryExists.get();
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "isDirectory(Location) encountered an error ", e);
        }
        return false;
    }

    private boolean isTableDir(Location location)
    {
        Location metadataLocation = location.appendPath("metadata");
        try {
            Optional<Boolean> directoryExists = trinoFileSystem.directoryExists(metadataLocation);
            if (directoryExists.isEmpty() || !directoryExists.get()) {
                return false;
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "isTableDir(Location) could not determine whether directory exists", e);
        }

        try {
            FileIterator files = trinoFileSystem.listFiles(metadataLocation);
            while (files.hasNext()) {
                if (files.next().location().fileName().endsWith(".metadata.json")) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "isTableDir(Location) could not list files", e);
        }
        return false;
    }

    private boolean isNamespace(Location location)
    {
        return isDirectory(location) && !isTableDir(location);
    }

    protected String splitQualifiedNamespace(String namespace)
    {
        String[] parts = Iterables.toArray(DOT.split(namespace), String.class);
        return parts[parts.length - 1];
    }

    public boolean tableExists(ConnectorSession session, Optional<String> owner, SchemaTableName schemaTableName)
    {
        return newTableOps(session, owner, schemaTableName).current() != null;
    }

    private TableOperations newTableOps(ConnectorSession session, Optional<String> owner, SchemaTableName schemaTableName)
    {
        return tableOperationsProvider.createTableOperations(TrinoHadoopCatalog.this, session,
                schemaTableName.getSchemaName(), schemaTableName.getTableName(), owner, Optional.of(tableLocation(schemaTableName)));
    }

    /**
     * Try to resolve a metadata table, which we encode as URI fragments e.g.
     * hdfs:///warehouse/my_table#snapshots
     *
     * @param location Path to parse
     * @return A base table name and MetadataTableType if a type is found, null if not
     */
    private Pair<String, MetadataTableType> parseMetadataType(String location)
    {
        int hashIndex = location.lastIndexOf('#');
        if (hashIndex != -1 && !location.endsWith("#")) {
            String baseTable = location.substring(0, hashIndex);
            String metaTable = location.substring(hashIndex + 1);
            MetadataTableType type = MetadataTableType.from(metaTable);
            return (type == null) ? null : Pair.of(baseTable, type);
        }
        else {
            return null;
        }
    }

    private Table loadMetadataTable(ConnectorSession session, Optional<String> owner, SchemaTableName schemaTableName, String table, MetadataTableType type)
    {
        TableOperations ops = newTableOps(session, owner, schemaTableName);
        if (ops.current() == null) {
            throw new NoSuchTableException("Table does not exist at location: %s", tableLocation(schemaTableName));
        }
        return MetadataTableUtils.createMetadataTableInstance(ops, tableLocation(schemaTableName), table, type);
    }

    public TrinoHadoopCatalogTableBuilder builder(SchemaTableName schemaTableName, Schema schema)
    {
        return new TrinoHadoopCatalogTableBuilder(schemaTableName, schema);
    }

    protected class TrinoHadoopCatalogTableBuilder
    {
        private final String tableLocation;
        private final SchemaTableName schemaTableName;
        private final ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
        private final Schema schema;
        private PartitionSpec spec = PartitionSpec.unpartitioned();
        private SortOrder sortOrder = SortOrder.unsorted();
        private Optional<String> owner = Optional.empty();

        TrinoHadoopCatalogTableBuilder(SchemaTableName schemaTableName, Schema schema)
        {
            this.schemaTableName = schemaTableName;
            this.tableLocation = tableLocation(schemaTableName);
            this.schema = schema;
        }

        public TrinoHadoopCatalogTableBuilder withPartitionSpec(PartitionSpec newSpec)
        {
            this.spec = newSpec != null ? newSpec : PartitionSpec.unpartitioned();
            return this;
        }

        public TrinoHadoopCatalogTableBuilder withSortOrder(SortOrder newSortOrder)
        {
            this.sortOrder = newSortOrder != null ? newSortOrder : SortOrder.unsorted();
            return this;
        }

        public TrinoHadoopCatalogTableBuilder withLocation(String location)
        {
            checkArgument(
                    location == null || location.equals(tableLocation),
                    "Cannot set a custom location for a path-based table. Expected "
                            + tableLocation
                            + " but got "
                            + location);
            return this;
        }

        public TrinoHadoopCatalogTableBuilder withProperties(Map<String, String> properties)
        {
            if (properties != null) {
                propertiesBuilder.putAll(properties);
            }
            return this;
        }

        public TrinoHadoopCatalogTableBuilder withProperty(String key, String value)
        {
            propertiesBuilder.put(key, value);
            return this;
        }

        public TrinoHadoopCatalogTableBuilder withOwner(String owner)
        {
            this.owner = Optional.of(owner);
            return this;
        }

        public Table create(ConnectorSession session)
        {
            TableOperations ops = tableOperationsProvider.createTableOperations(TrinoHadoopCatalog.this, session,
                    schemaTableName.getSchemaName(), schemaTableName.getTableName(), owner, Optional.of(tableLocation));
            if (ops.current() != null) {
                throw new AlreadyExistsException("Table already exists at location: %s", tableLocation);
            }

            Map<String, String> properties = propertiesBuilder.buildOrThrow();
            TableMetadata metadata = tableMetadata(schema, spec, sortOrder, properties, tableLocation);
            ops.commit(null, metadata);
            return new BaseTable(ops, tableLocation);
        }

        public Transaction createTransaction(ConnectorSession session)
        {
            TableOperations ops = tableOperationsProvider.createTableOperations(TrinoHadoopCatalog.this, session,
                    schemaTableName.getSchemaName(), schemaTableName.getTableName(), owner, Optional.of(tableLocation));
            if (ops.current() != null) {
                throw new AlreadyExistsException("Table already exists: %s", tableLocation);
            }

            Map<String, String> properties = propertiesBuilder.buildOrThrow();
            TableMetadata metadata = tableMetadata(schema, spec, null, properties, tableLocation);
            return Transactions.createTableTransaction(tableLocation, ops, metadata);
        }

        public Transaction replaceTransaction(ConnectorSession session)
        {
            return newReplaceTableTransaction(false, session);
        }

        public Transaction createOrReplaceTransaction(ConnectorSession session)
        {
            return newReplaceTableTransaction(true, session);
        }

        private Transaction newReplaceTableTransaction(boolean orCreate, ConnectorSession session)
        {
            TableOperations ops = newTableOps(session, Optional.empty(), schemaTableName);
            if (!orCreate && ops.current() == null) {
                throw new NoSuchTableException("No such table: %s", tableLocation);
            }

            Map<String, String> properties = propertiesBuilder.buildOrThrow();
            TableMetadata metadata;
            if (ops.current() != null) {
                metadata = ops.current().buildReplacement(schema, spec, sortOrder, tableLocation, properties);
            }
            else {
                metadata = tableMetadata(schema, spec, sortOrder, properties, tableLocation);
            }

            if (orCreate) {
                return Transactions.createOrReplaceTableTransaction(tableLocation, ops, metadata);
            }
            else {
                return Transactions.replaceTableTransaction(tableLocation, ops, metadata);
            }
        }

        private TableMetadata tableMetadata(Schema schema, PartitionSpec spec, SortOrder order, Map<String, String> properties, String location)
        {
            requireNonNull(schema, "A table schema is required");

            Map<String, String> tableProps = properties == null ? ImmutableMap.of() : properties;
            PartitionSpec partitionSpec = spec == null ? PartitionSpec.unpartitioned() : spec;
            SortOrder sortOrder = order == null ? SortOrder.unsorted() : order;
            return TableMetadata.newTableMetadata(schema, partitionSpec, sortOrder, location, tableProps);
        }
    }
}
