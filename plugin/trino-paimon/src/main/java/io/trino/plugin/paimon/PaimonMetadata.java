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
package io.trino.plugin.paimon;

import io.airlift.slice.Slice;
import io.trino.plugin.paimon.catalog.PaimonTrinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.plugin.paimon.PaimonColumnHandle.TRINO_ROW_ID_NAME;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Trino {@link ConnectorMetadata}.
 */
public class PaimonMetadata
        implements ConnectorMetadata
{
    private static final String TAG_PREFIX = "tag-";

    protected final PaimonTrinoCatalog catalog;

    public PaimonMetadata(PaimonTrinoCatalog catalog)
    {
        this.catalog = catalog;
    }

    public PaimonTrinoCatalog catalog()
    {
        return catalog;
    }

    // todo support dynamic bucket table
    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(
            ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Table table = paimonTableHandle.table(session, catalog);
        checkArgument(table instanceof FileStoreTable, "%s is not supported", table.getClass());
        FileStoreTable storeTable = (FileStoreTable) table;
        BucketMode bucketMode = storeTable.bucketMode();
        switch (bucketMode) {
            case HASH_FIXED:
                try {
                    return Optional.of(
                            new ConnectorTableLayout(
                                    new PaimonPartitioningHandle(
                                            InstantiationUtil.serializeObject(storeTable.schema())),
                                    storeTable.schema().bucketKeys(),
                                    false));
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            case BUCKET_UNAWARE:
                return Optional.empty();
            default:
                throw new IllegalArgumentException("Unknown table bucket mode: " + bucketMode);
        }
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(
            ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            Optional<ConnectorTableLayout> layout,
            RetryMode retryMode,
            boolean replace)
    {
        createTable(session, tableMetadata, SaveMode.IGNORE);
        return getTableHandle(session, tableMetadata.getTable(), Collections.emptyMap());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        if (fragments.isEmpty()) {
            return Optional.empty();
        }
        return commit(session, (PaimonTableHandle) tableHandle, fragments);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            RetryMode retryMode)
    {
        return (ConnectorInsertTableHandle) tableHandle;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistic)
    {
        return commit(session, (PaimonTableHandle) insertHandle, fragments);
    }

    private Optional<ConnectorOutputMetadata> commit(
            ConnectorSession session, PaimonTableHandle insertHandle, Collection<Slice> fragments)
    {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        List<CommitMessage> commitMessages =
                fragments.stream()
                        .map(
                                slice -> {
                                    try {
                                        return serializer.deserialize(
                                                serializer.getVersion(), slice.getBytes());
                                    }
                                    catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                        .collect(toList());

        if (commitMessages.isEmpty()) {
            return Optional.empty();
        }

        BatchWriteBuilder batchWriteBuilder =
                insertHandle.tableWithDynamicOptions(catalog, session).newBatchWriteBuilder();
        if (PaimonSessionProperties.enableInsertOverwrite(session)) {
            batchWriteBuilder.withOverwrite();
        }
        try (BatchTableCommit commit = batchWriteBuilder.newCommit()) {
            commit.commit(commitMessages);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to commit", e);
        }
        return Optional.empty();
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(
            ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return DELETE_ROW_AND_INSERT_ROW;
    }

    // todo support dynamic bucket table
    @Override
    public ColumnHandle getMergeRowIdColumnHandle(
            ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Table table = paimonTableHandle.table(session, catalog);
        checkArgument(table instanceof FileStoreTable, "%s is not supported", table.getClass());
        FileStoreTable storeTable = (FileStoreTable) table;
        BucketMode bucketMode = storeTable.bucketMode();
        checkArgument(bucketMode == BucketMode.HASH_FIXED, "Unsupported table bucket mode: %s", bucketMode);
        Set<String> pkSet = new HashSet<>(table.primaryKeys());
        DataField[] row =
                table.rowType().getFields().stream()
                        .filter(dataField -> pkSet.contains(dataField.name()))
                        .toArray(DataField[]::new);
        return PaimonColumnHandle.of(TRINO_ROW_ID_NAME, DataTypes.ROW(row), -1);
    }

    // todo support dynamic bucket table
    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(
            ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Table table = paimonTableHandle.table(session, catalog);
        checkArgument(table instanceof FileStoreTable, "%s is not supported", table.getClass());
        FileStoreTable storeTable = (FileStoreTable) table;
        BucketMode bucketMode = storeTable.bucketMode();
        checkArgument(bucketMode == BucketMode.HASH_FIXED, "Unsupported table bucket mode: %s", bucketMode);
        try {
            return Optional.of(
                    new PaimonPartitioningHandle(
                            InstantiationUtil.serializeObject(storeTable.schema())));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(
            ConnectorSession session, ConnectorTableHandle tableHandle, Map<Integer, Collection<ColumnHandle>> updateCaseColumns, RetryMode retryMode)
    {
        return new PaimonMergeTableHandle((PaimonTableHandle) tableHandle);
    }

    @Override
    public void finishMerge(
            ConnectorSession session,
            ConnectorMergeTableHandle mergeTableHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        commit(session, (PaimonTableHandle) mergeTableHandle.getTableHandle(), fragments);
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        try {
            catalog.getDatabase(session, schemaName);
            return true;
        }
        catch (Catalog.DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return catalog.listDatabases(session);
    }

    @Override
    public void createSchema(
            ConnectorSession session,
            String schemaName,
            Map<String, Object> properties,
            TrinoPrincipal owner)
    {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(schemaName),
                "schemaName cannot be null or empty");

        try {
            catalog.createDatabase(session, schemaName, true);
        }
        catch (Catalog.DatabaseAlreadyExistException e) {
            throw new RuntimeException(format("database already existed: '%s'", schemaName));
        }
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(schemaName),
                "schemaName cannot be null or empty");
        try {
            catalog.dropDatabase(session, schemaName, false, true);
        }
        catch (Catalog.DatabaseNotEmptyException e) {
            throw new RuntimeException(format("database is not empty: '%s'", schemaName));
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new RuntimeException(format("database not exists: '%s'", schemaName));
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent()) {
            throw new TrinoException(
                    NOT_SUPPORTED, "Read paimon table with start version is not supported");
        }

        Map<String, String> dynamicOptions = new HashMap<>();
        if (endVersion.isPresent()) {
            ConnectorTableVersion version = endVersion.get();
            Type versionType = version.getVersionType();
            switch (version.getPointerType()) {
                case TEMPORAL: {
                    if (!(versionType
                            instanceof TimestampWithTimeZoneType timeZonedVersionType)) {
                        throw new TrinoException(
                                NOT_SUPPORTED,
                                "Unsupported type for table version: "
                                        + versionType.getDisplayName());
                    }
                    long epochMillis =
                            timeZonedVersionType.isShort()
                                    ? unpackMillisUtc((long) version.getVersion())
                                    : ((LongTimestampWithTimeZone) version.getVersion())
                                    .getEpochMillis();
                    dynamicOptions.put(
                            CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
                            String.valueOf(epochMillis));
                    break;
                }
                case TARGET_ID: {
                    String tagOrVersion;
                    if (versionType instanceof VarcharType) {
                        tagOrVersion =
                                BinaryString.fromBytes(
                                                ((Slice) version.getVersion()).getBytes())
                                        .toString();
                    }
                    else {
                        tagOrVersion = version.getVersion().toString();
                    }

                    // if value is not number, set tag option
                    boolean isNumber = StringUtils.isNumeric(tagOrVersion);
                    if (!isNumber) {
                        dynamicOptions.put(CoreOptions.SCAN_TAG_NAME.key(), tagOrVersion);
                    }
                    else {
                        try {
                            String path =
                                    catalog.getTable(
                                                    session,
                                                    new Identifier(
                                                            tableName.getSchemaName(),
                                                            tableName.getTableName()))
                                            .options()
                                            .get("path");

                            if (catalog.exists(
                                    session,
                                    new Path(path + "/tag/" + TAG_PREFIX + tagOrVersion))) {
                                dynamicOptions.put(
                                        CoreOptions.SCAN_TAG_NAME.key(), tagOrVersion);
                            }
                            else {
                                dynamicOptions.put(
                                        CoreOptions.SCAN_SNAPSHOT_ID.key(), tagOrVersion);
                            }
                        }
                        catch (IOException | Catalog.TableNotExistException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    break;
                }
            }
        }
        return getTableHandle(session, tableName, dynamicOptions);
    }

    @Override
    public ConnectorTableProperties getTableProperties(
            ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    public PaimonTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Map<String, String> dynamicOptions)
    {
        try {
            catalog.getTable(
                    session,
                    Identifier.create(tableName.getSchemaName(), tableName.getTableName()));
            return new PaimonTableHandle(
                    tableName.getSchemaName(), tableName.getTableName(), dynamicOptions);
        }
        catch (Catalog.TableNotExistException e) {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return ((PaimonTableHandle) tableHandle).tableMetadata(session, catalog);
    }

    @Override
    public void setTableProperties(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Map<String, Optional<Object>> properties)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier =
                new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        List<SchemaChange> changes = new ArrayList<>();
        Map<String, String> options =
                properties.entrySet().stream()
                        .collect(toMap(Map.Entry::getKey, e -> (String) e.getValue().get()));
        options.forEach((key, value) -> changes.add(SchemaChange.setOption(key, value)));
        // TODO: remove options, SET PROPERTIES x = DEFAULT
        try {
            catalog.alterTable(session, identifier, changes, false);
        }
        catch (Exception e) {
            throw new RuntimeException(
                    format("failed to alter table: '%s'", paimonTableHandle.getTableName()), e);
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        List<SchemaTableName> tables = new ArrayList<>();
        schemaName
                .map(Collections::singletonList)
                .orElseGet(() -> catalog.listDatabases(session))
                .forEach(schema -> tables.addAll(listTables(session, schema)));
        return tables;
    }

    private List<SchemaTableName> listTables(ConnectorSession session, String schema)
    {
        try {
            return catalog.listTables(session, schema).stream()
                    .map(table -> new SchemaTableName(schema, table))
                    .collect(toList());
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createTable(
            ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        SchemaTableName table = tableMetadata.getTable();
        Identifier identifier = Identifier.create(table.getSchemaName(), table.getTableName());

        try {
            catalog.createTable(session, identifier, prepareSchema(tableMetadata), false);
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new RuntimeException(format("database not exists: '%s'", table.getSchemaName()));
        }
        catch (Catalog.TableAlreadyExistException e) {
            switch (saveMode) {
                case IGNORE:
                    return;
                case REPLACE:
                    try {
                        catalog.dropTable(session, identifier, false);
                        catalog.createTable(
                                session, identifier, prepareSchema(tableMetadata), true);
                    }
                    catch (Catalog.DatabaseNotExistException ex) {
                        throw new RuntimeException(
                                format("database not existed: '%s'", table.getTableName()));
                    }
                    catch (Catalog.TableAlreadyExistException ex) {
                        throw new RuntimeException(
                                format("table already exists: '%s'", table.getTableName()));
                    }
                    catch (Catalog.TableNotExistException ex) {
                        throw new RuntimeException(
                                format("table not exists: '%s'", table.getTableName()));
                    }
                    break;
                case FAIL:
                    throw new RuntimeException(
                            format("table already existed: '%s'", table.getTableName()));
                default:
                    throw new IllegalArgumentException("Unsupported save mode: " + saveMode);
            }
        }
    }

    private Schema prepareSchema(ConnectorTableMetadata tableMetadata)
    {
        Map<String, Object> properties = new HashMap<>(tableMetadata.getProperties());
        Schema.Builder builder =
                Schema.newBuilder()
                        .primaryKey(PaimonTableOptions.getPrimaryKeys(properties))
                        .partitionKeys(PaimonTableOptions.getPartitionedKeys(properties));

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            builder.column(
                    column.getName(),
                    PaimonTypeUtils.toPaimonType(column.getType()),
                    column.getComment());
        }

        PaimonTableOptionUtils.buildOptions(builder, properties);

        return builder.build();
    }

    @Override
    public void renameTable(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            SchemaTableName newTableName)
    {
        PaimonTableHandle oldTableHandle = (PaimonTableHandle) tableHandle;
        try {
            catalog.renameTable(
                    session,
                    new Identifier(oldTableHandle.getSchemaName(), oldTableHandle.getTableName()),
                    new Identifier(newTableName.getSchemaName(), newTableName.getTableName()),
                    false);
        }
        catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(
                    format("table not exists: '%s'", oldTableHandle.getTableName()));
        }
        catch (Catalog.TableAlreadyExistException e) {
            throw new RuntimeException(
                    format("table already existed: '%s'", newTableName.getTableName()));
        }
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        try {
            catalog.dropTable(
                    session,
                    new Identifier(
                            paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName()),
                    false);
        }
        catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(
                    format("table not exists: '%s'", paimonTableHandle.getTableName()));
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PaimonTableHandle table = (PaimonTableHandle) tableHandle;
        Map<String, ColumnHandle> handleMap = new HashMap<>();
        for (ColumnMetadata column : table.columnMetadatas(session, catalog)) {
            handleMap.put(column.getName(), table.columnHandle(session, catalog, column.getName()));
        }
        return handleMap;
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((PaimonColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        List<SchemaTableName> tableNames;
        if (prefix.getTable().isPresent()) {
            tableNames = Collections.singletonList(prefix.toSchemaTableName());
        }
        else {
            tableNames = listTables(session, prefix.getSchema());
        }

        return tableNames.stream()
                .collect(
                        toMap(
                                Function.identity(),
                                table ->
                                        getTableHandle(session, table, Collections.emptyMap())
                                                .columnMetadatas(session, catalog)));
    }

    @Override
    public void addColumn(
            ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier =
                new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(
                SchemaChange.addColumn(
                        column.getName(), PaimonTypeUtils.toPaimonType(column.getType())));
        try {
            catalog.alterTable(session, identifier, changes, false);
        }
        catch (Exception e) {
            throw new RuntimeException(
                    format("failed to alter table: '%s'", paimonTableHandle.getTableName()), e);
        }
    }

    @Override
    public void renameColumn(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle source,
            String target)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier =
                new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = (PaimonColumnHandle) source;
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.renameColumn(paimonColumnHandle.getColumnName(), target));
        try {
            catalog.alterTable(session, identifier, changes, false);
        }
        catch (Exception e) {
            throw new RuntimeException(
                    format("failed to alter table: '%s'", paimonTableHandle.getTableName()), e);
        }
    }

    @Override
    public void dropColumn(
            ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier =
                new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = (PaimonColumnHandle) column;
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.dropColumn(paimonColumnHandle.getColumnName()));
        try {
            catalog.alterTable(session, identifier, changes, false);
        }
        catch (Exception e) {
            throw new RuntimeException(
                    format("failed to alter table: '%s'", paimonTableHandle.getTableName()), e);
        }
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) handle;
        Optional<PaimonFilterExtractor.TrinoFilter> extract =
                PaimonFilterExtractor.extract(session, catalog, paimonTableHandle, constraint);
        if (extract.isPresent()) {
            PaimonFilterExtractor.TrinoFilter trinoFilter = extract.get();
            return Optional.of(
                    new ConstraintApplicationResult<>(
                            paimonTableHandle.copy(trinoFilter.getFilter()),
                            trinoFilter.getRemainFilter().transformKeys(columnHandle -> columnHandle),
                            constraint.getExpression(),
                            false));
        }
        else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        // TODO: Support row kind projection.
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) handle;
        Set<PaimonColumnHandle> newColumns =
                assignments.values().stream()
                        .map(PaimonColumnHandle.class::cast)
                        .collect(Collectors.toSet());

        if (paimonTableHandle.getProjectedColumns().equals(newColumns)) {
            return Optional.empty();
        }

        List<Assignment> assignmentList = new ArrayList<>();
        assignments.forEach(
                (name, column) ->
                        assignmentList.add(
                                new Assignment(
                                        name,
                                        column,
                                        ((PaimonColumnHandle) column).getTrinoType())));

        return Optional.of(
                new ProjectionApplicationResult<>(
                        paimonTableHandle.copy(newColumns), projections, assignmentList, false));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        PaimonTableHandle table = (PaimonTableHandle) handle;

        if (table.getLimit().isPresent() && table.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        if (!table.getPredicate().isAll()) {
            Table paimonTable = table.table(session, catalog);
            LinkedHashMap<PaimonColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
            LinkedHashMap<PaimonColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();
            new PaimonFilterConverter(paimonTable.rowType())
                    .convert(table.getPredicate(), acceptedDomains, unsupportedDomains);
            Set<String> acceptedFields =
                    acceptedDomains.keySet().stream()
                            .map(PaimonColumnHandle::getColumnName)
                            .collect(Collectors.toSet());
            if (!unsupportedDomains.isEmpty()
                    || !new HashSet<>(paimonTable.partitionKeys()).containsAll(acceptedFields)) {
                return Optional.empty();
            }
        }

        table = table.copy(OptionalLong.of(limit));

        return Optional.of(new LimitApplicationResult<>(table, false, false));
    }

    public void rollback()
    {
        // do nothing
    }
}
