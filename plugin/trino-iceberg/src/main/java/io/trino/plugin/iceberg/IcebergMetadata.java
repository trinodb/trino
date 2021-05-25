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

import com.google.common.base.Splitter;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.base.classloader.ClassLoaderSafeSystemTable;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HiveWrittenPartitions;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNewTableLayout;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DiscretePredicates;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HiveMetadata.STORAGE_TABLE;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.getPartitioning;
import static io.trino.plugin.iceberg.IcebergTableProperties.getTableLocation;
import static io.trino.plugin.iceberg.IcebergUtil.deserializePartitionValue;
import static io.trino.plugin.iceberg.IcebergUtil.getColumns;
import static io.trino.plugin.iceberg.IcebergUtil.getDataPath;
import static io.trino.plugin.iceberg.IcebergUtil.getFileFormat;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionKeys;
import static io.trino.plugin.iceberg.IcebergUtil.getTableComment;
import static io.trino.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.trino.plugin.iceberg.PartitionFields.toPartitionFields;
import static io.trino.plugin.iceberg.TableType.DATA;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergType;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;

public abstract class IcebergMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(IcebergMetadata.class);
    public static final String DEPENDS_ON_TABLES = "dependsOnTables";
    private final HdfsEnvironment hdfsEnvironment;
    protected final TypeManager typeManager;
    private final JsonCodec<CommitTaskData> commitTaskCodec;

    private final Map<String, Optional<Long>> snapshotIds = new ConcurrentHashMap<>();

    private Transaction transaction;

    public IcebergMetadata(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
    }

    public abstract org.apache.iceberg.Table getIcebergTable(ConnectorSession session, SchemaTableName schemaTableName) throws TableNotFoundException, UnknownTableTypeException;

    @Override
    public abstract List<String> listSchemaNames(ConnectorSession session);

    @Override
    public abstract Map<String, Object> getSchemaProperties(ConnectorSession session, CatalogSchemaName schemaName);

    @Override
    public abstract Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, CatalogSchemaName schemaName);

    @Override
    public IcebergTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        IcebergTableName name = IcebergTableName.from(tableName.getTableName());
        verify(name.getTableType() == DATA, "Wrong table type: " + name.getTableType());

        try {
            org.apache.iceberg.Table table = getIcebergTable(session, tableName);
            Optional<Long> snapshotId = getSnapshotId(table, name.getSnapshotId());

            return new IcebergTableHandle(
                    tableName.getSchemaName(),
                    name.getTableName(),
                    name.getTableType(),
                    snapshotId,
                    TupleDomain.all(),
                    TupleDomain.all());
        }
        catch (TableNotFoundException | UnknownTableTypeException e) {
            return null;
        }
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return getRawSystemTable(session, tableName)
                .map(systemTable -> new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
    }

    protected Optional<SystemTable> getRawSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        IcebergTableName name = IcebergTableName.from(tableName.getTableName());
        // The given tableName can container system specific strings like orders$files
        try {
            // The IcebergTableName refer to the actual table "orders", we use it to create a SchemaTableName to lookup the iceberg table
            SchemaTableName realTableName = new SchemaTableName(tableName.getSchemaName(), name.getTableName());
            org.apache.iceberg.Table table = getIcebergTable(session, realTableName);

            SchemaTableName systemTableName = new SchemaTableName(tableName.getSchemaName(), name.getTableNameWithType());
            switch (name.getTableType()) {
                case HISTORY:
                    if (name.getSnapshotId().isPresent()) {
                        throw new TrinoException(NOT_SUPPORTED, "Snapshot ID not supported for history table: " + systemTableName);
                    }
                    return Optional.of(new HistoryTable(systemTableName, table));
                case SNAPSHOTS:
                    if (name.getSnapshotId().isPresent()) {
                        throw new TrinoException(NOT_SUPPORTED, "Snapshot ID not supported for snapshots table: " + systemTableName);
                    }
                    return Optional.of(new SnapshotsTable(systemTableName, typeManager, table));
                case PARTITIONS:
                    return Optional.of(new PartitionTable(systemTableName, typeManager, table, getSnapshotId(table, name.getSnapshotId())));
                case MANIFESTS:
                    return Optional.of(new ManifestsTable(systemTableName, table, getSnapshotId(table, name.getSnapshotId())));
                case FILES:
                    return Optional.of(new FilesTable(systemTableName, typeManager, table, getSnapshotId(table, name.getSnapshotId())));
            }
            return Optional.empty();
        }
        catch (TableNotFoundException | UnknownTableTypeException e) {
            return Optional.empty();
        }
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, table.getSchemaTableName());

        TupleDomain<IcebergColumnHandle> enforcedPredicate = table.getEnforcedPredicate();

        TableScan tableScan = icebergTable.newScan()
                .useSnapshot(table.getSnapshotId().get())
                .filter(toIcebergExpression(enforcedPredicate))
                .includeColumnStats();

        CloseableIterable<FileScanTask> files = tableScan.planFiles();

        // Extract identity partition fields that are present in all partition specs, for creating the discrete predicates.
        Set<Integer> partitionSourceIds = identityPartitionColumnsInAllSpecs(icebergTable);

        DiscretePredicates discretePredicates = null;
        if (!partitionSourceIds.isEmpty()) {
            // Extract identity partition columns
            Map<Integer, IcebergColumnHandle> columns = getColumns(icebergTable.schema(), typeManager).stream()
                    .filter(column -> partitionSourceIds.contains(column.getId()))
                    .collect(toImmutableMap(IcebergColumnHandle::getId, Function.identity()));

            Iterable<TupleDomain<ColumnHandle>> discreteTupleDomain = Iterables.transform(files, fileScan -> {
                // Extract partition values in the data file
                Map<Integer, String> partitionColumnValueStrings = getPartitionKeys(fileScan);
                Map<ColumnHandle, NullableValue> partitionValues = partitionSourceIds.stream()
                        .filter(partitionColumnValueStrings::containsKey)
                        .collect(toImmutableMap(
                                columns::get,
                                columnId -> {
                                    IcebergColumnHandle column = columns.get(columnId);
                                    Object prestoValue = deserializePartitionValue(
                                            column.getType(),
                                            partitionColumnValueStrings.get(columnId),
                                            column.getName(),
                                            session.getTimeZoneKey());

                                    return NullableValue.of(column.getType(), prestoValue);
                                }));

                return TupleDomain.fromFixedValues(partitionValues);
            });

            discretePredicates = new DiscretePredicates(
                    columns.values().stream()
                            .map(ColumnHandle.class::cast)
                            .collect(toImmutableList()),
                    discreteTupleDomain);
        }

        return new ConnectorTableProperties(
                // Using the predicate here directly avoids eagerly loading all partition values. Logically, this
                // still keeps predicate and discretePredicates evaluation the same on every row of the table. This
                // can be further optimized by intersecting with partition values at the cost of iterating
                // over all tableScan.planFiles() and caching partition values in table handle.
                enforcedPredicate.transform(ColumnHandle.class::cast),
                // TODO: implement table partitioning
                Optional.empty(),
                Optional.empty(),
                Optional.ofNullable(discretePredicates),
                ImmutableList.of());
    }

    @Override
    public abstract List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName);

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, table.getSchemaTableName());
        return getColumns(icebergTable.schema(), typeManager).stream()
                .collect(toImmutableMap(IcebergColumnHandle::getName, identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        IcebergColumnHandle column = (IcebergColumnHandle) columnHandle;
        return ColumnMetadata.builder()
                .setName(column.getName())
                .setType(column.getType())
                .setComment(column.getComment())
                .build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tables = prefix.getTable()
                .map(ignored -> singletonList(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName table : tables) {
            try {
                columns.put(table, getIcebergTableMetadata(session, table).getColumns());
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
            catch (UnknownTableTypeException e) {
                // ignore table of unknown type
            }
        }
        return columns.build();
    }

    @Override
    public abstract void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner);

    @Override
    public abstract void dropSchema(ConnectorSession session, String schemaName);

    @Override
    public abstract void renameSchema(ConnectorSession session, String source, String target);

    @Override
    public abstract void setSchemaAuthorization(ConnectorSession session, String source, TrinoPrincipal principal);

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        Optional<ConnectorNewTableLayout> layout = getNewTableLayout(session, tableMetadata);
        finishCreateTable(session, beginCreateTable(session, tableMetadata, layout), ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public abstract void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment);

    protected abstract Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, String targetPath, ImmutableMap<String, String> newTableProperties);

    @Override
    public final ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builderWithExpectedSize(2);
        FileFormat fileFormat = IcebergTableProperties.getFileFormat(tableMetadata.getProperties());
        propertiesBuilder.put(DEFAULT_FILE_FORMAT, fileFormat.toString());
        if (tableMetadata.getComment().isPresent()) {
            propertiesBuilder.put(TABLE_COMMENT, tableMetadata.getComment().get());
        }

        ImmutableMap<String, String> newTableProperties = propertiesBuilder.build();

        String targetPath = getTableLocation(tableMetadata.getProperties());

        Schema schema = toIcebergSchema(tableMetadata.getColumns());
        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));

        transaction = newCreateTableTransaction(session, schemaTableName, schema, partitionSpec, targetPath, newTableProperties);

        return new IcebergWritableTableHandle(
                schemaName,
                tableName,
                SchemaParser.toJson(transaction.table().schema()),
                PartitionSpecParser.toJson(transaction.table().spec()),
                getColumns(transaction.table().schema(), typeManager),
                transaction.table().location(),
                fileFormat);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return finishInsert(session, (IcebergWritableTableHandle) tableHandle, fragments, computedStatistics);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, table.getSchemaTableName());

        transaction = icebergTable.newTransaction();

        return new IcebergWritableTableHandle(
                table.getSchemaName(),
                table.getTableName(),
                SchemaParser.toJson(icebergTable.schema()),
                PartitionSpecParser.toJson(icebergTable.spec()),
                getColumns(icebergTable.schema(), typeManager),
                getDataPath(icebergTable.location()),
                getFileFormat(icebergTable));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        IcebergWritableTableHandle table = (IcebergWritableTableHandle) insertHandle;
        org.apache.iceberg.Table icebergTable = transaction.table();

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                .collect(toImmutableList());

        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        icebergTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);

        AppendFiles appendFiles = transaction.newFastAppend();
        for (CommitTaskData task : commitTasks) {
            HdfsContext context = new HdfsContext(session, table.getSchemaName(), table.getTableName());

            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withInputFile(new HdfsInputFile(new Path(task.getPath()), hdfsEnvironment, context))
                    .withFormat(table.getFileFormat())
                    .withMetrics(task.getMetrics().metrics());

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.getPartitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            appendFiles.appendFile(builder.build());
        }

        appendFiles.commit();
        transaction.commitTransaction();

        return Optional.of(new HiveWrittenPartitions(commitTasks.stream()
                .map(CommitTaskData::getPath)
                .collect(toImmutableList())));
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return new IcebergColumnHandle(0, "$row_id", BIGINT, Optional.empty());
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        return Optional.of(new IcebergInputInfo(table.getSnapshotId()));
    }

    @Override
    public abstract void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle);

    @Override
    public abstract void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable);

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, handle.getSchemaTableName());
        icebergTable.updateSchema().addColumn(column.getName(), toIcebergType(column.getType())).commit();
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        IcebergColumnHandle handle = (IcebergColumnHandle) column;
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, icebergTableHandle.getSchemaTableName());
        icebergTable.updateSchema().deleteColumn(handle.getName()).commit();
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        IcebergColumnHandle columnHandle = (IcebergColumnHandle) source;
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, icebergTableHandle.getSchemaTableName());
        icebergTable.updateSchema().renameColumn(columnHandle.getName(), target).commit();
    }

    private final ConnectorTableMetadata getIcebergTableMetadata(ConnectorSession session, SchemaTableName table) throws TableNotFoundException, UnknownTableTypeException
    {
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, table);

        List<ColumnMetadata> columns = getColumnMetadatas(icebergTable);

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(FILE_FORMAT_PROPERTY, getFileFormat(icebergTable));
        if (!icebergTable.spec().fields().isEmpty()) {
            properties.put(PARTITIONING_PROPERTY, toPartitionFields(icebergTable.spec()));
        }

        return new ConnectorTableMetadata(table, columns, properties.build(), getTableComment(icebergTable));
    }

    @Override
    public final ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return getIcebergTableMetadata(session, ((IcebergTableHandle) table).getSchemaTableName());
    }

    private List<ColumnMetadata> getColumnMetadatas(org.apache.iceberg.Table table)
    {
        return table.schema().columns().stream()
                .map(column -> {
                    return ColumnMetadata.builder()
                            .setName(column.name())
                            .setType(toTrinoType(column.type(), typeManager))
                            .setNullable(column.isOptional())
                            .setComment(Optional.ofNullable(column.doc()))
                            .build();
                })
                .collect(toImmutableList());
    }

    private static Schema toIcebergSchema(List<ColumnMetadata> columns)
    {
        List<NestedField> icebergColumns = new ArrayList<>();
        for (ColumnMetadata column : columns) {
            if (!column.isHidden()) {
                int index = icebergColumns.size();
                Type type = toIcebergType(column.getType());
                NestedField field = column.isNullable()
                        ? NestedField.optional(index, column.getName(), type, column.getComment())
                        : NestedField.required(index, column.getName(), type, column.getComment());
                icebergColumns.add(field);
            }
        }
        Type icebergSchema = Types.StructType.of(icebergColumns);
        AtomicInteger nextFieldId = new AtomicInteger(1);
        icebergSchema = TypeUtil.assignFreshIds(icebergSchema, nextFieldId::getAndIncrement);
        return new Schema(icebergSchema.asStructType().fields());
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return Optional.of(handle);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector only supports delete where one or more partitions are deleted entirely");
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;

        org.apache.iceberg.Table icebergTable = getIcebergTable(session, handle.getSchemaTableName());

        icebergTable.newDelete()
                .deleteFromRowFilter(toIcebergExpression(handle.getEnforcedPredicate()))
                .commit();

        // TODO: it should be possible to return number of deleted records
        return OptionalLong.empty();
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    public void rollback()
    {
        // TODO: cleanup open transaction
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, table.getSchemaTableName());

        Set<Integer> partitionSourceIds = identityPartitionColumnsInAllSpecs(icebergTable);
        BiPredicate<IcebergColumnHandle, Domain> isIdentityPartition = (column, domain) -> partitionSourceIds.contains(column.getId());

        // TODO: Avoid enforcing the constraint when partition filters have large IN expressions, since iceberg cannot
        // support it. Such large expressions cannot be simplified since simplification changes the filtered set.
        TupleDomain<IcebergColumnHandle> newEnforcedConstraint = constraint.getSummary()
                .transform(IcebergColumnHandle.class::cast)
                .filter(isIdentityPartition)
                .intersect(table.getEnforcedPredicate());

        TupleDomain<IcebergColumnHandle> newUnenforcedConstraint = constraint.getSummary()
                .transform(IcebergColumnHandle.class::cast)
                .filter(isIdentityPartition.negate())
                .intersect(table.getUnenforcedPredicate());

        if (newEnforcedConstraint.equals(table.getEnforcedPredicate())
                && newUnenforcedConstraint.equals(table.getUnenforcedPredicate())) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                new IcebergTableHandle(table.getSchemaName(),
                        table.getTableName(),
                        table.getTableType(),
                        table.getSnapshotId(),
                        newUnenforcedConstraint,
                        newEnforcedConstraint),
                newUnenforcedConstraint.transform(ColumnHandle.class::cast)));
    }

    private static Set<Integer> identityPartitionColumnsInAllSpecs(org.apache.iceberg.Table table)
    {
        // Extract identity partition column source ids common to ALL specs
        return table.spec().fields().stream()
                .filter(field -> field.transform().isIdentity())
                .filter(field -> table.specs().values().stream().allMatch(spec -> spec.fields().contains(field)))
                .map(PartitionField::sourceId)
                .collect(toImmutableSet());
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, handle.getSchemaTableName());
        return TableStatisticsMaker.getTableStatistics(typeManager, constraint, handle, icebergTable);
    }

    private Optional<Long> getSnapshotId(org.apache.iceberg.Table table, Optional<Long> snapshotId)
    {
        return snapshotIds.computeIfAbsent(table.toString(), ignored -> snapshotId
                .map(id -> IcebergUtil.resolveSnapshotId(table, id))
                .or(() -> Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId)));
    }

    @Override
    public abstract void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting);

    @Override
    public abstract void dropMaterializedView(ConnectorSession session, SchemaTableName viewName);

    @Override
    public ConnectorInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorTableHandle> sourceTableHandles)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, table.getSchemaTableName());
        transaction = icebergTable.newTransaction();

        return new IcebergWritableTableHandle(
            table.getSchemaName(),
            table.getTableName(),
            SchemaParser.toJson(icebergTable.schema()),
            PartitionSpecParser.toJson(icebergTable.spec()),
            getColumns(icebergTable.schema(), typeManager),
            getDataPath(icebergTable.location()),
            getFileFormat(icebergTable));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<ConnectorTableHandle> sourceTableHandles)
    {
        // delete before insert .. simulating overwrite
        executeDelete(session, tableHandle);

        IcebergWritableTableHandle table = (IcebergWritableTableHandle) insertHandle;

        org.apache.iceberg.Table icebergTable = transaction.table();
        List<CommitTaskData> commitTasks = fragments.stream()
                .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                .collect(toImmutableList());

        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
            .map(field -> field.transform().getResultType(
                icebergTable.schema().findType(field.sourceId())))
            .toArray(Type[]::new);

        AppendFiles appendFiles = transaction.newFastAppend();
        for (CommitTaskData task : commitTasks) {
            HdfsContext context = new HdfsContext(session, table.getSchemaName(), table.getTableName());
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withInputFile(new HdfsInputFile(new Path(task.getPath()), hdfsEnvironment, context))
                    .withFormat(table.getFileFormat())
                    .withMetrics(task.getMetrics().metrics());

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.getPartitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            appendFiles.appendFile(builder.build());
        }

        String dependencies = sourceTableHandles.stream()
                .map(handle -> (IcebergTableHandle) handle)
                .filter(handle -> handle.getSnapshotId().isPresent())
                .map(handle -> handle.getSchemaTableName() + "=" + handle.getSnapshotId().get())
                .collect(joining(","));

        // Update the 'dependsOnTables' property that tracks tables on which the materialized view depends and the corresponding snapshot ids of the tables
        appendFiles.set(DEPENDS_ON_TABLES, dependencies);
        appendFiles.commit();

        transaction.commitTransaction();
        return Optional.of(new HiveWrittenPartitions(commitTasks.stream()
            .map(CommitTaskData::getPath)
            .collect(toImmutableList())));
    }

    protected boolean isMaterializedView(Table table)
    {
        if (table.getTableType().equals(VIRTUAL_VIEW.name()) &&
                "true".equals(table.getParameters().get(PRESTO_VIEW_FLAG)) &&
                table.getParameters().containsKey(STORAGE_TABLE)) {
            return true;
        }
        return false;
    }

    protected abstract boolean isMaterializedView(ConnectorSession session, SchemaTableName schemaTableName);

    @Override
    public abstract Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName);

    public Optional<TableToken> getTableToken(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, table.getSchemaTableName());
        return Optional.ofNullable(icebergTable.currentSnapshot())
            .map(snapshot -> new TableToken(snapshot.snapshotId()));
    }

    public boolean isTableCurrent(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<TableToken> tableToken)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Optional<TableToken> currentToken = getTableToken(session, handle);

        if (tableToken.isEmpty() || currentToken.isEmpty()) {
            return false;
        }

        return tableToken.get().getSnapshotId() == currentToken.get().getSnapshotId();
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName materializedViewName)
    {
        Map<String, Optional<TableToken>> refreshStateMap = getMaterializedViewToken(session, materializedViewName);
        if (refreshStateMap.isEmpty()) {
            return new MaterializedViewFreshness(false);
        }

        for (Map.Entry<String, Optional<TableToken>> entry : refreshStateMap.entrySet()) {
            List<String> strings = Splitter.on(".").splitToList(entry.getKey());
            if (strings.size() == 3) {
                strings = strings.subList(1, 3);
            }
            else if (strings.size() != 2) {
                throw new TrinoException(ICEBERG_INVALID_METADATA, String.format("Invalid table name in '%s' property: %s'", DEPENDS_ON_TABLES, strings));
            }
            String schema = strings.get(0);
            String name = strings.get(1);
            SchemaTableName schemaTableName = new SchemaTableName(schema, name);
            if (!isTableCurrent(session, getTableHandle(session, schemaTableName), entry.getValue())) {
                return new MaterializedViewFreshness(false);
            }
        }
        return new MaterializedViewFreshness(true);
    }

    private Map<String, Optional<TableToken>> getMaterializedViewToken(ConnectorSession session, SchemaTableName name)
    {
        Map<String, Optional<TableToken>> viewToken = new HashMap<>();
        Optional<ConnectorMaterializedViewDefinition> materializedViewDefinition = getMaterializedView(session, name);
        if (materializedViewDefinition.isEmpty()) {
            return viewToken;
        }

        String storageTableName = materializedViewDefinition.get().getProperties().getOrDefault(STORAGE_TABLE, "").toString();
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, new SchemaTableName(name.getSchemaName(), storageTableName));
        String dependsOnTables = icebergTable.currentSnapshot().summary().getOrDefault(DEPENDS_ON_TABLES, "");
        if (!dependsOnTables.isEmpty()) {
            Map<String, String> tableToSnapshotIdMap = Splitter.on(',').withKeyValueSeparator('=').split(dependsOnTables);
            for (Map.Entry<String, String> entry : tableToSnapshotIdMap.entrySet()) {
                viewToken.put(entry.getKey(), Optional.of(new TableToken(Long.parseLong(entry.getValue()))));
            }
        }
        return viewToken;
    }

    private static class TableToken
    {
        // Current Snapshot ID of the table
        private long snapshotId;

        public TableToken(long snapshotId)
        {
            this.snapshotId = snapshotId;
        }

        public long getSnapshotId()
        {
            return this.snapshotId;
        }
    }
}
