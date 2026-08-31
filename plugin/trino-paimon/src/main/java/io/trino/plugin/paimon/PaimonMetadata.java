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
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
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
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.ColumnDirectiveUtils.ConvertedColumn;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.BatchWriteBuilderImpl;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewChange;
import org.apache.paimon.view.ViewImpl;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.plugin.paimon.PaimonColumnHandle.TRINO_ROW_ID_NAME;
import static io.trino.plugin.paimon.PaimonDynamicBucketUtils.dynamicBucketWritePartitionColumns;
import static io.trino.plugin.paimon.PaimonDynamicBucketUtils.keyDynamicAssignerParallelism;
import static io.trino.plugin.paimon.PaimonDynamicBucketUtils.keyDynamicWritePartitionColumns;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_COMMIT_ERROR;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_METADATA_ERROR;
import static io.trino.plugin.paimon.PaimonSchemaProperties.COMMENT_PROPERTY;
import static io.trino.plugin.paimon.PaimonSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.paimon.PaimonSchemaProperties.OWNER_PROPERTY;
import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.paimonTimestampToTrino;
import static io.trino.plugin.paimon.PaimonTrinoTypeConversions.paimonTimestampToTrinoTimestampWithTimeZone;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.COLUMN_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.READ_ONLY_VIOLATION;
import static io.trino.spi.StandardErrorCode.SCHEMA_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.expression.Constant.TRUE;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;
import static org.apache.paimon.schema.ColumnDirectiveUtils.applyAddColumnDirective;
import static org.apache.paimon.utils.Preconditions.checkArgument;

public final class PaimonMetadata
        implements ConnectorMetadata
{
    private final PaimonCatalog catalog;
    private final TypeManager typeManager;
    private final IntSupplier dynamicBucketWorkerCountSupplier;
    private final ConcurrentMap<String, List<BootstrapCleanup>> bootstrapCleanups = new ConcurrentHashMap<>();
    private final PaimonKeyDynamicWriteCoordinator keyDynamicWriteCoordinator = new PaimonKeyDynamicWriteCoordinator();

    private static final int MAX_LIST_PARTITIONS_BY_NAMES_BATCH_SIZE = 1000;
    private static final int MAX_PARTITION_DELETE_SPECS = 1024;
    private static final String MERGE_OPERATION = "MERGE";
    private static final String OVERWRITE_OPERATION = "OVERWRITE";
    private static final String PAIMON_SNAPSHOT_OPERATION_CLASS_NAME = "org.apache.paimon.Snapshot$Operation";
    private static final String TRINO_SCHEMA_OWNER_TYPE_PROPERTY = "trino.owner-type";
    private static final Set<String> PAIMON_OPTION_UPDATES_REQUIRING_EXISTING_OPTIONS = Set.of(
            CoreOptions.BUCKET.key(),
            CoreOptions.DELETION_VECTORS_ENABLED.key(),
            CoreOptions.IGNORE_DELETE.key(),
            CoreOptions.IGNORE_UPDATE_BEFORE.key(),
            CoreOptions.CLUSTERING_COLUMNS.key());
    private static final Set<String> PAIMON_OPTION_REMOVES_REQUIRING_EXISTING_OPTIONS = Set.of(
            CoreOptions.BUCKET.key(),
            CoreOptions.CLUSTERING_COLUMNS.key());

    public PaimonMetadata(PaimonCatalog catalog, TypeManager typeManager, IntSupplier dynamicBucketWorkerCountSupplier)
    {
        requireNonNull(catalog, "catalog is null");
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(dynamicBucketWorkerCountSupplier, "dynamicBucketWorkerCountSupplier is null");
        this.catalog = catalog;
        this.typeManager = typeManager;
        this.dynamicBucketWorkerCountSupplier = dynamicBucketWorkerCountSupplier;
    }

    public PaimonMetadata(PaimonCatalog catalog, TypeManager typeManager)
    {
        this(catalog, typeManager, () -> 1);
    }

    public PaimonCatalog catalog()
    {
        return catalog;
    }

    public TypeManager typeManager()
    {
        return typeManager;
    }

    public IntSupplier dynamicBucketWorkerCountSupplier()
    {
        return dynamicBucketWorkerCountSupplier;
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        requireNonNull(session, "session is null");
        List<BootstrapCleanup> cleanups = bootstrapCleanups.remove(session.getQueryId());
        try {
            if (cleanups != null) {
                cleanups.forEach(BootstrapCleanup::cleanup);
            }
        }
        finally {
            // Query cancellation may arrive while a cleanup is running. Always release the local
            // fallback slot so a failed cleanup cannot block every later write to this table.
            keyDynamicWriteCoordinator.releaseQuery(session.getQueryId());
        }
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableMetadata, "tableMetadata is null");
        rejectSystemTableWrite(tableMetadata.getTable(), "create table");
        TableSchema tableSchema = newTableSchema(tableMetadata);
        return writeLayout(tableSchema, "new table layout", tableMetadata.getTable().toString());
    }

    private TableSchema newTableSchema(ConnectorTableMetadata tableMetadata)
    {
        return TableSchema.create(0, prepareSchema(tableMetadata, true));
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("insert layout", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "insert layout");
        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable storeTable = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, "insert layout");
        Optional<ConnectorTableLayout> layout = writeLayout(
                storeTable.schema(),
                storeTable.bucketMode(),
                "insert layout",
                schemaTableName(paimonTableHandle).toString());
        paimonTableHandle.rememberPlannedInsertDynamicBucketAssignerParallelism(
                dynamicBucketAssignerParallelism(layout));
        return layout;
    }

    private Optional<ConnectorTableLayout> writeLayout(TableSchema tableSchema, String operation, String tableName)
    {
        return writeLayout(tableSchema, bucketMode(tableSchema), operation, tableName);
    }

    private Optional<ConnectorTableLayout> writeLayout(
            TableSchema tableSchema,
            BucketMode bucketMode,
            String operation,
            String tableName)
    {
        requireNonNull(tableSchema, "tableSchema is null");
        requireNonNull(bucketMode, "bucketMode is null");
        requireNonNull(operation, "operation is null");
        requireNonNull(tableName, "tableName is null");
        return switch (bucketMode) {
            case HASH_FIXED -> {
                try {
                    yield Optional.of(new ConnectorTableLayout(
                            new PaimonPartitioningHandle(InstantiationUtil.serializeObject(tableSchema)),
                            fixedBucketWritePartitionColumns(tableSchema),
                            false));
                }
                catch (IOException e) {
                    throw new TrinoException(
                            PAIMON_METADATA_ERROR,
                            format("Failed to prepare Paimon %s for table '%s'", operation, tableName),
                            e);
                }
            }
            case HASH_DYNAMIC -> {
                try {
                    yield Optional.of(new ConnectorTableLayout(
                            new PaimonPartitioningHandle(
                                    InstantiationUtil.serializeObject(tableSchema),
                                    false,
                                    dynamicBucketAssignerParallelism(tableSchema)),
                            dynamicBucketWritePartitionColumns(tableSchema),
                            false));
                }
                catch (IOException e) {
                    throw new TrinoException(
                            PAIMON_METADATA_ERROR,
                            format("Failed to prepare Paimon %s for table '%s'", operation, tableName),
                            e);
                }
            }
            case KEY_DYNAMIC -> {
                try {
                    yield Optional.of(new ConnectorTableLayout(
                            new PaimonPartitioningHandle(
                                    InstantiationUtil.serializeObject(tableSchema),
                                    false,
                                    dynamicBucketAssignerParallelism(tableSchema)),
                            keyDynamicWritePartitionColumns(tableSchema),
                            false));
                }
                catch (IOException e) {
                    throw new TrinoException(
                            PAIMON_METADATA_ERROR,
                            format("Failed to prepare Paimon %s for table '%s'", operation, tableName),
                            e);
                }
            }
            case BUCKET_UNAWARE -> Optional.empty();
            default -> throw PaimonTableSupport.unsupportedBucketMode(operation, bucketMode);
        };
    }

    private static List<String> fixedBucketWritePartitionColumns(TableSchema schema)
    {
        List<String> partitionColumns = new ArrayList<>(schema.partitionKeys());
        partitionColumns.addAll(schema.bucketKeys());
        return List.copyOf(partitionColumns);
    }

    private static BucketMode bucketMode(TableSchema schema)
    {
        requireNonNull(schema, "schema is null");
        int bucket = CoreOptions.fromMap(schema.options()).bucket();
        if (bucket == BucketMode.POSTPONE_BUCKET) {
            return BucketMode.POSTPONE_MODE;
        }
        if (bucket != -1) {
            return BucketMode.HASH_FIXED;
        }
        if (schema.primaryKeys().isEmpty()) {
            return BucketMode.BUCKET_UNAWARE;
        }
        return schema.crossPartitionUpdate() ? BucketMode.KEY_DYNAMIC : BucketMode.HASH_DYNAMIC;
    }

    private OptionalInt dynamicBucketAssignerParallelism(TableSchema tableSchema)
    {
        requireNonNull(tableSchema, "tableSchema is null");
        CoreOptions coreOptions = new CoreOptions(tableSchema.options());
        return switch (bucketMode(tableSchema)) {
            case HASH_DYNAMIC -> OptionalInt.of(PaimonDynamicBucketUtils.dynamicBucketAssignerParallelism(
                    coreOptions,
                    dynamicBucketWorkerCountSupplier.getAsInt()));
            case KEY_DYNAMIC -> OptionalInt.of(keyDynamicAssignerParallelism(
                    coreOptions,
                    dynamicBucketWorkerCountSupplier.getAsInt()));
            default -> OptionalInt.empty();
        };
    }

    private OptionalInt dynamicBucketAssignerParallelism(FileStoreTable storeTable)
    {
        requireNonNull(storeTable, "storeTable is null");
        return switch (storeTable.bucketMode()) {
            case HASH_DYNAMIC, KEY_DYNAMIC -> dynamicBucketAssignerParallelism(storeTable.schema());
            default -> OptionalInt.empty();
        };
    }

    private PaimonTableHandle planKeyDynamicBootstrap(
            FileStoreTable storeTable,
            PaimonTableHandle tableHandle,
            String queryId)
    {
        requireNonNull(storeTable, "storeTable is null");
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(queryId, "queryId is null");
        if (storeTable.bucketMode() != BucketMode.KEY_DYNAMIC) {
            return tableHandle;
        }
        try {
            PaimonKeyDynamicBootstrap.validateAtomicCommitCapability(storeTable);
            PaimonTableHandle planned = tableHandle.withKeyDynamicBootstrapSnapshot(
                    PaimonKeyDynamicBootstrap.latestSnapshot(storeTable));
            PaimonKeyDynamicBootstrap.prepare(
                    storeTable,
                    queryId,
                    PaimonKeyDynamicBootstrap.snapshotFor(planned),
                    planned.getDynamicBucketAssignerParallelism().orElseThrow());
            bootstrapCleanups.computeIfAbsent(queryId, _ -> new CopyOnWriteArrayList<>())
                    .add(new BootstrapCleanup(
                            queryId,
                            storeTable,
                            PaimonKeyDynamicBootstrap.snapshotFor(planned),
                            planned.getDynamicBucketAssignerParallelism().orElseThrow()));
            return planned;
        }
        catch (Exception e) {
            throw PaimonPageSink.wrapWriteException(e);
        }
    }

    private PaimonTableHandle planWriteWithKeyDynamicSlot(
            FileStoreTable storeTable,
            PaimonTableHandle tableHandle,
            String queryId)
    {
        requireNonNull(storeTable, "storeTable is null");
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(queryId, "queryId is null");
        if (storeTable.bucketMode() != BucketMode.KEY_DYNAMIC) {
            return planKeyDynamicBootstrap(storeTable, tableHandle, queryId);
        }
        keyDynamicWriteCoordinator.acquire(queryId, storeTable.name());
        try {
            return planKeyDynamicBootstrap(storeTable, tableHandle, queryId);
        }
        catch (RuntimeException e) {
            keyDynamicWriteCoordinator.releaseQuery(queryId);
            throw e;
        }
    }

    private record BootstrapCleanup(
            String queryId,
            FileStoreTable table,
            PaimonKeyDynamicBootstrap.OptionalSnapshot snapshot,
            int assignerParallelism)
    {
        private void cleanup()
        {
            PaimonKeyDynamicBootstrap.cleanup(table, queryId, snapshot, assignerParallelism);
        }
    }

    private static PaimonTableHandle pinKeyDynamicBootstrapSnapshot(
            Table table,
            PaimonTableHandle tableHandle,
            boolean keyDynamic)
    {
        requireNonNull(table, "table is null");
        requireNonNull(tableHandle, "tableHandle is null");
        if (!keyDynamic || !(table instanceof FileStoreTable storeTable)) {
            return tableHandle;
        }
        if (storeTable.bucketMode() != BucketMode.KEY_DYNAMIC) {
            return tableHandle;
        }

        // CTAS has just created an empty table, so there is no existing routing state to read.
        // Pin the empty snapshot explicitly so the commit path cannot silently skip the
        // KEY_DYNAMIC validator when a storage implementation does not expose a snapshot yet.
        return tableHandle.withKeyDynamicBootstrapSnapshot(OptionalLong.empty());
    }

    private static OptionalInt dynamicBucketAssignerParallelism(Optional<ConnectorTableLayout> layout)
    {
        requireNonNull(layout, "layout is null");
        if (layout.isEmpty()) {
            return OptionalInt.empty();
        }
        Optional<ConnectorPartitioningHandle> partitioning = layout.get().getPartitioning();
        if (partitioning.isEmpty()) {
            return OptionalInt.empty();
        }
        ConnectorPartitioningHandle partitioningHandle = partitioning.get();
        if (!(partitioningHandle instanceof PaimonPartitioningHandle paimonPartitioningHandle)) {
            return OptionalInt.empty();
        }
        return paimonPartitioningHandle.dynamicBucketAssignerParallelism();
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(
            ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            Optional<ConnectorTableLayout> layout,
            RetryMode retryMode,
            boolean replace)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableMetadata, "tableMetadata is null");
        requireNonNull(layout, "layout is null");
        requireNonNull(retryMode, "retryMode is null");
        validateNoQueryRetries(retryMode);
        rejectSystemTableWrite(tableMetadata.getTable(), "create table");
        TableSchema createTableSchema = newTableSchema(tableMetadata);
        writeLayout(createTableSchema, "create table", tableMetadata.getTable().toString());
        boolean keyDynamic = bucketMode(createTableSchema) == BucketMode.KEY_DYNAMIC;
        if (keyDynamic) {
            keyDynamicWriteCoordinator.acquire(session.getQueryId(), tableMetadata.getTable().toString());
        }
        try {
            createTable(
                    session,
                    tableMetadata,
                    replace ? SaveMode.REPLACE : SaveMode.FAIL);
            PaimonTableHandle tableHandle = requireNonNull(getTableHandle(
                    session,
                    tableMetadata.getTable(),
                    Collections.emptyMap()));
            Catalog sessionCatalog = catalog.forSession(session);
            Table table = tableHandle.tableWithWriteDynamicOptions(sessionCatalog);
            if (keyDynamic) {
                if (!(table instanceof FileStoreTable storeTable)) {
                    throw new UnsupportedOperationException(
                            "Paimon KEY_DYNAMIC CTAS requires a FileStoreTable with atomic snapshot validation; got "
                                    + table.getClass().getName());
                }
                // Probe after creation but before returning the output handle. CTAS has no existing
                // snapshot to bootstrap, but unsupported catalog commit paths must still fail before
                // Trino schedules any workers for the write.
                try {
                    PaimonKeyDynamicBootstrap.validateAtomicCommitCapability(storeTable);
                }
                catch (Exception e) {
                    throw PaimonPageSink.wrapWriteException(e);
                }
            }
            Map<String, DataField> createdFieldsByLowerName = createdTableFieldsByLowerName(
                    table.rowType().getFields(),
                    tableMetadata.getTable());
            String createTableOperation = replace
                    ? PaimonTableHandle.CREATE_OR_REPLACE_TABLE_AS_SELECT_OPERATION
                    : PaimonTableHandle.CREATE_TABLE_AS_SELECT_OPERATION;
            PaimonTableHandle outputHandle = tableHandle.withWriteColumns(tableMetadata.getColumns().stream()
                            .map(column -> {
                                DataField field = createdTableField(
                                        createdFieldsByLowerName,
                                        column.getName(),
                                        tableMetadata.getTable());
                                return toPaimonColumnHandle(field);
                            })
                            .collect(toList()))
                    .withCreateTableOperation(createTableOperation)
                    .withDynamicBucketAssignerParallelism(dynamicBucketAssignerParallelism(layout));
            // CTAS starts with an empty table, so workers do not need a bootstrap artifact. The initial
            // snapshot is still pinned so a concurrent write cannot race this query's first commit.
            return pinKeyDynamicBootstrapSnapshot(table, outputHandle, keyDynamic);
        }
        catch (RuntimeException e) {
            keyDynamicWriteCoordinator.releaseQuery(session.getQueryId());
            throw e;
        }
    }

    static Map<String, DataField> createdTableFieldsByLowerName(List<DataField> fields, SchemaTableName tableName)
    {
        requireNonNull(fields, "fields is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, DataField> fieldsByLowerName = new LinkedHashMap<>();
        for (DataField field : fields) {
            DataField createdField = requireNonNull(field, "fields contains null field");
            String lowerFieldName = FieldNameUtils.toLowerCase(createdField.name());
            if (fieldsByLowerName.putIfAbsent(lowerFieldName, createdField) != null) {
                throw new IllegalStateException(
                        "Created Paimon table '%s' schema contains case-insensitive duplicate field name '%s'"
                                .formatted(tableName, lowerFieldName));
            }
        }
        return Collections.unmodifiableMap(fieldsByLowerName);
    }

    private static DataField createdTableField(
            Map<String, DataField> fieldsByLowerName,
            String columnName,
            SchemaTableName tableName)
    {
        requireNonNull(fieldsByLowerName, "fieldsByLowerName is null");
        String lowerColumnName = FieldNameUtils.toLowerCase(columnName);
        DataField field = fieldsByLowerName.get(lowerColumnName);
        if (field != null) {
            return field;
        }
        throw new IllegalStateException(format(
                "Created Paimon table '%s' is missing write column '%s'",
                tableName,
                columnName));
    }

    private static DataField canonicalColumn(
            FileStoreTable table,
            SchemaTableName tableName,
            PaimonColumnHandle columnHandle)
    {
        requireNonNull(table, "table is null");
        requireNonNull(columnHandle, "columnHandle is null");
        String lowerColumnName = FieldNameUtils.toLowerCase(columnHandle.getColumnName());
        DataField match = null;
        for (DataField field : table.rowType().getFields()) {
            if (FieldNameUtils.toLowerCase(field.name()).equals(lowerColumnName)) {
                if (match != null) {
                    throw new TrinoException(NOT_SUPPORTED,
                            "Paimon schema change is ambiguous for case-insensitive column name '"
                                    + lowerColumnName + "'");
                }
                match = field;
            }
        }
        if (match == null) {
            throw new TrinoException(COLUMN_NOT_FOUND,
                    format("Column '%s' does not exist in table '%s'",
                            columnHandle.getColumnName(),
                            tableName));
        }
        return match;
    }

    private static void validateNoCaseInsensitiveDuplicateColumnName(
            FileStoreTable table,
            SchemaTableName tableName,
            String columnName,
            Optional<String> existingColumnName)
    {
        requireNonNull(table, "table is null");
        requireNonNull(tableName, "tableName is null");
        validateNoCaseInsensitiveDuplicateFieldName(
                table.rowType().getFields(),
                tableName.toString(),
                columnName,
                existingColumnName);
    }

    private static void validateNoCaseInsensitiveDuplicateFieldName(
            List<DataField> fields,
            String scopeName,
            String fieldName,
            Optional<String> existingFieldName)
    {
        requireNonNull(fields, "fields is null");
        requireNonNull(scopeName, "scopeName is null");
        validateFieldName("fieldName", fieldName);
        requireNonNull(existingFieldName, "existingFieldName is null");
        String lowerFieldName = FieldNameUtils.toLowerCase(fieldName);
        Optional<String> lowerExistingFieldName = existingFieldName.map(FieldNameUtils::toLowerCase);
        for (DataField field : fields) {
            String lowerExistingName = FieldNameUtils.toLowerCase(field.name());
            if (lowerExistingName.equals(lowerFieldName) && !lowerExistingFieldName.equals(Optional.of(lowerExistingName))) {
                throw new TrinoException(
                        COLUMN_ALREADY_EXISTS,
                        "Column '%s' already exists in Paimon schema scope '%s'".formatted(fieldName, scopeName));
            }
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getOutputTableHandle(tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "finish create table");
        return commit(
                session,
                paimonTableHandle,
                fragments,
                PaimonSessionProperties.InsertExistingPartitionsBehavior.APPEND,
                paimonTableHandle.getCreateTableOperation());
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            RetryMode retryMode)
    {
        requireNonNull(session, "session is null");
        requireNonNull(retryMode, "retryMode is null");
        validateNoQueryRetries(retryMode);
        PaimonTableHandle paimonTableHandle = getTableHandle("begin insert", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "begin insert");
        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable storeTable = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, "begin insert");
        OptionalInt plannedAssignerParallelism = paimonTableHandle.getPlannedInsertDynamicBucketAssignerParallelism();
        PaimonTableHandle insertHandle = paimonTableHandle.withWriteColumns(columns)
                .withDynamicBucketAssignerParallelism(plannedAssignerParallelism.isPresent()
                        ? plannedAssignerParallelism
                        : dynamicBucketAssignerParallelism(storeTable));
        return planWriteWithKeyDynamicSlot(storeTable, insertHandle, session.getQueryId());
    }

    @Override
    public boolean supportsMissingColumnsOnInsert()
    {
        return true;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getInsertTableHandle(insertHandle);
        rejectSystemTableWrite(paimonTableHandle, "finish insert");
        return commit(
                session,
                paimonTableHandle,
                fragments,
                PaimonSessionProperties.getInsertExistingPartitionsBehavior(session));
    }

    private Optional<ConnectorOutputMetadata> commit(
            ConnectorSession session,
            PaimonTableHandle tableHandle,
            Collection<Slice> fragments,
            PaimonSessionProperties.InsertExistingPartitionsBehavior insertBehavior)
    {
        return commit(session, tableHandle, fragments, insertBehavior, Optional.empty());
    }

    private Optional<ConnectorOutputMetadata> commit(
            ConnectorSession session,
            PaimonTableHandle tableHandle,
            Collection<Slice> fragments,
            PaimonSessionProperties.InsertExistingPartitionsBehavior insertBehavior,
            Optional<String> operation)
    {
        requireNonNull(session, "session is null");
        requireNonNull(insertBehavior, "insertBehavior is null");
        requireNonNull(operation, "operation is null");
        List<Slice> fragmentsList = copyFragments(fragments);
        Optional<String> commitOperation = commitOperation(insertBehavior, operation);
        if (fragmentsList.isEmpty()
                && insertBehavior != PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE
                && !tableHandle.isKeyDynamicBootstrapSnapshotPlanned()) {
            return Optional.empty();
        }

        List<CommitMessage> commitMessages = deserializeCommitMessages(fragmentsList);
        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable fileStoreTable = null;
        PaimonKeyDynamicBootstrap.OptionalSnapshot expectedSnapshot = PaimonKeyDynamicBootstrap.snapshotFor(tableHandle);

        try {
            fileStoreTable = latestWriteFileStoreTable(tableHandle, sessionCatalog, "commit writes");
            FileStoreTable commitTable = fileStoreTable;
            if (insertBehavior == PaimonSessionProperties.InsertExistingPartitionsBehavior.ERROR) {
                validateInsertTargetIsNew(sessionCatalog, fileStoreTable, tableHandle, commitMessages);
            }

            BatchWriteBuilder batchWriteBuilder = fileStoreTable.newBatchWriteBuilder();
            enableKeyDynamicConflictCheck(fileStoreTable, batchWriteBuilder);
            if (insertBehavior == PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE) {
                PaimonTableSupport.validateInsertOverwrite(fileStoreTable);
                batchWriteBuilder.withOverwrite();
            }

            try (BatchTableCommit commit = batchWriteBuilder.newCommit()) {
                if (tableHandle.isKeyDynamicBootstrapSnapshotPlanned()) {
                    int assignerParallelism = tableHandle.getDynamicBucketAssignerParallelism()
                            .orElseThrow(() -> new IllegalStateException(
                                    "Paimon KEY_DYNAMIC commit is missing assigner parallelism"));
                    boolean rejectConcurrentSnapshot = insertBehavior == PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE
                            || operation
                            .filter(operationName -> PaimonTableHandle.CREATE_TABLE_AS_SELECT_OPERATION.equals(operationName)
                                    || PaimonTableHandle.CREATE_OR_REPLACE_TABLE_AS_SELECT_OPERATION.equals(operationName))
                            .isPresent();
                    PaimonKeyDynamicBootstrap.validateSnapshotForAtomicCommit(
                            commitTable,
                            session.getQueryId(),
                            expectedSnapshot,
                            assignerParallelism,
                            commitTable.store().snapshotManager().latestSnapshot(),
                            rejectConcurrentSnapshot);
                }
                if (commitOperation.isPresent()) {
                    applyCommitOperationIfSupported(commit, commitOperation.get());
                }
                commit.commit(commitMessages);
            }
        }
        catch (Exception e) {
            Throwable commitFailure = firstRecognizedCommitFailure(e);
            if (commitFailure instanceof TrinoException trinoException) {
                throw trinoException;
            }
            if (commitFailure instanceof UnsupportedOperationException unsupportedOperationException) {
                String detail = unsupportedOperationException.getMessage();
                throw new TrinoException(
                        NOT_SUPPORTED,
                        detail == null || detail.isBlank()
                                ? "Paimon commit uses features which are not supported by the Trino connector"
                                : "Paimon commit uses features which are not supported by the Trino connector: " + detail,
                        unsupportedOperationException);
            }
            if (commitFailure instanceof CommitValidationException validationException) {
                throw new TrinoException(
                        PAIMON_COMMIT_ERROR,
                        validationException.getMessage(),
                        validationException);
            }
            if (e instanceof RuntimeException runtimeException) {
                throw new TrinoException(PAIMON_COMMIT_ERROR, "Failed to commit Paimon write fragments", runtimeException);
            }
            throw new TrinoException(PAIMON_COMMIT_ERROR, "Failed to commit Paimon write fragments", e);
        }
        finally {
            try {
                cleanupKeyDynamicBootstrap(session, tableHandle, fileStoreTable, expectedSnapshot);
            }
            finally {
                keyDynamicWriteCoordinator.releaseQuery(session.getQueryId());
            }
        }
        return Optional.empty();
    }

    private static void enableKeyDynamicConflictCheck(FileStoreTable table, BatchWriteBuilder batchWriteBuilder)
    {
        if (batchWriteBuilder instanceof BatchWriteBuilderImpl builder && table.bucketMode() == BucketMode.KEY_DYNAMIC) {
            builder.appendCommitCheckConflict(true);
        }
    }

    private void cleanupKeyDynamicBootstrap(
            ConnectorSession session,
            PaimonTableHandle tableHandle,
            @Nullable FileStoreTable table,
            PaimonKeyDynamicBootstrap.OptionalSnapshot expectedSnapshot)
    {
        if (!tableHandle.isKeyDynamicBootstrapSnapshotPlanned()) {
            return;
        }
        if (table == null) {
            try {
                table = requireFileStoreTable(tableHandle.table(catalog.forSession(session)), "cleanup bootstrap");
            }
            catch (Exception ignored) {
                return;
            }
        }
        OptionalInt assignerParallelism = tableHandle.getDynamicBucketAssignerParallelism();
        if (assignerParallelism.isPresent()) {
            PaimonKeyDynamicBootstrap.cleanup(
                    table,
                    session.getQueryId(),
                    expectedSnapshot,
                    assignerParallelism.orElseThrow());
        }
    }

    private static Throwable firstRecognizedCommitFailure(Exception exception)
    {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = exception;
        while (current != null && visited.add(current)) {
            if (current instanceof TrinoException
                    || current instanceof UnsupportedOperationException
                    || current instanceof CommitValidationException) {
                return current;
            }
            current = current.getCause();
        }
        return exception;
    }

    private static Optional<String> commitOperation(
            PaimonSessionProperties.InsertExistingPartitionsBehavior insertBehavior,
            Optional<String> explicitOperation)
    {
        if (explicitOperation.isPresent()) {
            return explicitOperation;
        }
        if (insertBehavior == PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE) {
            return Optional.of(OVERWRITE_OPERATION);
        }
        return Optional.empty();
    }

    private static void applyCommitOperationIfSupported(BatchTableCommit commit, String operationName)
            throws ReflectiveOperationException
    {
        requireNonNull(commit, "commit is null");
        requireNonNull(operationName, "operationName is null");

        Class<?> operationClass;
        try {
            operationClass = Class.forName(
                    PAIMON_SNAPSHOT_OPERATION_CLASS_NAME,
                    false,
                    BatchTableCommit.class.getClassLoader());
        }
        catch (ClassNotFoundException e) {
            return;
        }

        Object operation;
        try {
            operation = operationValue(operationClass, operationName);
        }
        catch (IllegalArgumentException e) {
            return;
        }

        Method withOperation;
        try {
            withOperation = BatchTableCommit.class.getMethod("withOperation", operationClass);
        }
        catch (NoSuchMethodException e) {
            return;
        }
        withOperation.invoke(commit, operation);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Object operationValue(Class<?> operationClass, String operationName)
    {
        return Enum.valueOf(operationClass.asSubclass(Enum.class), operationName);
    }

    private static List<CommitMessage> deserializeCommitMessages(List<Slice> fragments)
    {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        return fragments.stream().map(slice -> {
            try {
                return serializer.deserialize(serializer.getVersion(), slice.getBytes());
            }
            catch (IOException | RuntimeException e) {
                throw new TrinoException(PAIMON_COMMIT_ERROR, "Failed to deserialize Paimon commit fragment", e);
            }
        }).collect(toList());
    }

    private static void validateInsertTargetIsNew(
            Catalog catalog,
            FileStoreTable fileStoreTable,
            PaimonTableHandle tableHandle,
            List<CommitMessage> commitMessages)
            throws Catalog.TableNotExistException
    {
        SchemaTableName tableName = schemaTableName(tableHandle);
        if (fileStoreTable.partitionKeys().isEmpty()) {
            if (!fileStoreTable.newSnapshotReader().partitionEntries().isEmpty()) {
                throw new TrinoException(
                        READ_ONLY_VIOLATION,
                        format("Cannot insert into an existing non-partitioned Paimon table: %s", tableName));
            }
            return;
        }

        List<Map<String, String>> writtenPartitions = writtenPartitionSpecs(fileStoreTable, commitMessages);
        for (int start = 0; start < writtenPartitions.size(); start += MAX_LIST_PARTITIONS_BY_NAMES_BATCH_SIZE) {
            int end = Math.min(start + MAX_LIST_PARTITIONS_BY_NAMES_BATCH_SIZE, writtenPartitions.size());
            List<Partition> existingPartitions = catalog.listPartitionsByNames(
                    new Identifier(
                            tableHandle.getSchemaName(),
                            tableHandle.getTableName(),
                            fileStoreTable.coreOptions().branch()),
                    writtenPartitions.subList(start, end));
            if (!existingPartitions.isEmpty()) {
                throw new TrinoException(
                        READ_ONLY_VIOLATION,
                        format("Cannot insert into an existing partition of Paimon table: %s", tableName));
            }
        }
    }

    private static List<Map<String, String>> writtenPartitionSpecs(
            FileStoreTable fileStoreTable,
            List<CommitMessage> commitMessages)
    {
        RowType partitionType = new RowType(fileStoreTable.partitionKeys().stream()
                .map(partitionKey -> fileStoreTable.rowType().getField(partitionKey))
                .collect(toList()));
        InternalRowPartitionComputer partitionComputer = new InternalRowPartitionComputer(
                fileStoreTable.coreOptions().partitionDefaultName(),
                partitionType,
                fileStoreTable.partitionKeys().toArray(new String[0]),
                fileStoreTable.coreOptions().legacyPartitionName());
        Set<Map<String, String>> writtenPartitions = new LinkedHashSet<>();
        for (CommitMessage commitMessage : commitMessages) {
            writtenPartitions.add(partitionComputer.generatePartValues(commitMessage.partition()));
        }
        return List.copyOf(writtenPartitions);
    }

    private static List<Slice> copyFragments(Collection<Slice> fragments)
    {
        requireNonNull(fragments, "fragments is null");
        fragments.forEach(fragment -> requireNonNull(fragment, "fragments contains null fragment"));
        return List.copyOf(fragments);
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("row change paradigm", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "row change paradigm");
        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable storeTable = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, "row-level change");
        try {
            rowLevelChangeFileStoreTable(storeTable, "row-level change");
        }
        catch (TrinoException e) {
            if (canUseMetadataDeleteFallback(storeTable)) {
                return DELETE_ROW_AND_INSERT_ROW;
            }
            throw e;
        }
        return DELETE_ROW_AND_INSERT_ROW;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("merge row id", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "merge row id");
        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable storeTable = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, "merge row id");
        try {
            rowLevelChangeFileStoreTable(storeTable, "merge row id");
        }
        catch (TrinoException e) {
            if (canUseMetadataDeleteFallback(storeTable)) {
                return metadataDeleteRowIdColumnHandle();
            }
            throw e;
        }
        DataField[] row = storeTable.primaryKeys().stream()
                .map(primaryKey -> {
                    if (!storeTable.rowType().containsField(primaryKey)) {
                        throw new IllegalStateException("Paimon primary key '%s' is not present in table schema %s"
                                .formatted(primaryKey, storeTable.rowType().getFieldNames()));
                    }
                    return storeTable.rowType().getField(primaryKey);
                })
                .toArray(DataField[]::new);
        return PaimonColumnHandle.of(TRINO_ROW_ID_NAME, DataTypes.ROW(row), typeManager);
    }

    private static PaimonColumnHandle metadataDeleteRowIdColumnHandle()
    {
        return PaimonColumnHandle.of(TRINO_ROW_ID_NAME, DataTypes.ROW(
                DataTypes.FIELD(0, PaimonMergePageSourceWrapper.METADATA_DELETE_ROW_ID_FIELD, DataTypes.BIGINT())));
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(
            ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("update layout", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "update layout");
        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable storeTable = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, "update layout");
        try {
            rowLevelChangeFileStoreTable(storeTable, "update layout");
        }
        catch (TrinoException e) {
            if (canUseMetadataDeleteFallback(storeTable)) {
                return Optional.empty();
            }
            throw e;
        }
        try {
            OptionalInt dynamicBucketAssignerParallelism = dynamicBucketAssignerParallelism(storeTable);
            paimonTableHandle.rememberPlannedRowLevelDynamicBucketAssignerParallelism(
                    dynamicBucketAssignerParallelism);
            return Optional.of(new PaimonPartitioningHandle(
                    InstantiationUtil.serializeObject(storeTable.schema()),
                    false,
                    dynamicBucketAssignerParallelism));
        }
        catch (IOException e) {
            throw new TrinoException(
                    PAIMON_METADATA_ERROR,
                    format("Failed to prepare Paimon update layout for table '%s'",
                            schemaTableName(paimonTableHandle)),
                    e);
        }
    }

    private static boolean canUseMetadataDeleteFallback(FileStoreTable storeTable)
    {
        requireNonNull(storeTable, "storeTable is null");
        BucketMode bucketMode = storeTable.bucketMode();
        return bucketMode == BucketMode.BUCKET_UNAWARE
                || bucketMode == BucketMode.HASH_FIXED;
    }

    private static FileStoreTable requireFileStoreTable(Table table, String operation)
    {
        return PaimonTableSupport.requireFileStoreTable(table, operation);
    }

    private static FileStoreTable latestFileStoreTable(Table table, String operation)
    {
        return requireFileStoreTable(table, operation).copyWithLatestSchema();
    }

    private static FileStoreTable latestWriteFileStoreTable(
            PaimonTableHandle tableHandle,
            Catalog sessionCatalog,
            String operation)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(sessionCatalog, "sessionCatalog is null");
        try {
            return latestFileStoreTable(tableHandle.tableWithWriteDynamicOptions(sessionCatalog), operation);
        }
        catch (TrinoException e) {
            if (e.getErrorCode().equals(TABLE_NOT_FOUND.toErrorCode())) {
                throw new TrinoException(
                        TABLE_NOT_FOUND,
                        format("Table '%s' does not exist", schemaTableName(tableHandle)),
                        e.getCause() != null ? e.getCause() : e);
            }
            throw e;
        }
    }

    private static Optional<FileStoreTable> tryLatestWriteFileStoreTable(
            PaimonTableHandle tableHandle,
            Catalog sessionCatalog,
            String operation)
    {
        try {
            return Optional.of(latestWriteFileStoreTable(tableHandle, sessionCatalog, operation));
        }
        catch (TrinoException e) {
            if (e.getErrorCode().equals(TABLE_NOT_FOUND.toErrorCode())) {
                return Optional.empty();
            }
            throw e;
        }
    }

    private static FileStoreTable rowLevelChangeFileStoreTable(
            PaimonTableHandle tableHandle,
            Catalog sessionCatalog,
            String operation)
    {
        FileStoreTable storeTable = latestWriteFileStoreTable(tableHandle, sessionCatalog, operation);
        BucketMode bucketMode = storeTable.bucketMode();
        if (bucketMode != BucketMode.HASH_FIXED
                && bucketMode != BucketMode.HASH_DYNAMIC
                && bucketMode != BucketMode.KEY_DYNAMIC) {
            throw PaimonTableSupport.unsupportedBucketMode(operation, bucketMode);
        }
        PaimonTableSupport.validateRowLevelDelete(storeTable, operation);
        return storeTable;
    }

    private static void rowLevelChangeFileStoreTable(FileStoreTable storeTable, String operation)
    {
        requireNonNull(storeTable, "storeTable is null");
        BucketMode bucketMode = storeTable.bucketMode();
        if (bucketMode != BucketMode.HASH_FIXED
                && bucketMode != BucketMode.HASH_DYNAMIC
                && bucketMode != BucketMode.KEY_DYNAMIC) {
            throw PaimonTableSupport.unsupportedBucketMode(operation, bucketMode);
        }
        PaimonTableSupport.validateRowLevelDelete(storeTable, operation);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Map<Integer, Collection<ColumnHandle>> updateCaseColumns,
            RetryMode retryMode)
    {
        requireNonNull(session, "session is null");
        requireNonNull(retryMode, "retryMode is null");
        validateNoQueryRetries(retryMode);
        PaimonTableHandle paimonTableHandle = getTableHandle("begin merge", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "begin merge");
        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable storeTable = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, "merge");
        try {
            rowLevelChangeFileStoreTable(storeTable, "merge");
        }
        catch (TrinoException e) {
            if (canUseMetadataDeleteFallback(storeTable)) {
                if (storeTable.bucketMode() == BucketMode.KEY_DYNAMIC) {
                    keyDynamicWriteCoordinator.acquire(session.getQueryId(), storeTable.name());
                }
                return PaimonMergeTableHandle.forMetadataDeleteFallback(paimonTableHandle);
            }
            throw e;
        }
        List<ColumnHandle> writeColumns = storeTable.rowType().getFields().stream()
                .map(this::toPaimonColumnHandle)
                .collect(toList());
        OptionalInt plannedAssignerParallelism = paimonTableHandle.getPlannedRowLevelDynamicBucketAssignerParallelism();
        PaimonTableHandle mergeHandle = paimonTableHandle.withWriteColumns(writeColumns)
                .withDynamicBucketAssignerParallelism(plannedAssignerParallelism.isPresent()
                        ? plannedAssignerParallelism
                        : dynamicBucketAssignerParallelism(storeTable));
        return new PaimonMergeTableHandle(planWriteWithKeyDynamicSlot(storeTable, mergeHandle, session.getQueryId()));
    }

    private static void validateNoQueryRetries(RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
    }

    @Override
    public void finishMerge(
            ConnectorSession session,
            ConnectorMergeTableHandle mergeTableHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(session, "session is null");
        PaimonMergeTableHandle paimonMergeTableHandle = getPaimonMergeTableHandle(mergeTableHandle);
        PaimonTableHandle paimonTableHandle = paimonMergeTableHandle.paimonTableHandle();
        rejectSystemTableWrite(paimonTableHandle, "finish merge");
        if (paimonMergeTableHandle.isMetadataDeleteFallback()) {
            try {
                finishMetadataDeleteFallbackMerge(session, paimonTableHandle, fragments);
            }
            finally {
                keyDynamicWriteCoordinator.releaseQuery(session.getQueryId());
            }
            return;
        }
        commit(session,
                paimonTableHandle,
                fragments,
                PaimonSessionProperties.InsertExistingPartitionsBehavior.APPEND,
                Optional.of(MERGE_OPERATION));
    }

    private void finishMetadataDeleteFallbackMerge(
            ConnectorSession session,
            PaimonTableHandle paimonTableHandle,
            Collection<Slice> fragments)
    {
        long deletedRowCount = metadataDeleteDeletedRowCount(fragments);
        if (deletedRowCount == 0) {
            return;
        }
        if (paimonTableHandle.getLimit().isPresent() || paimonTableHandle.getDeletePartitionSpecs().isPresent()) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon metadata delete fallback can only delete all rows or complete partitions from an unlimited table handle");
        }
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            FileStoreTable fileStoreTable = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, "delete");
            Optional<List<Map<String, String>>> deletePartitionSpecs = Optional.empty();
            if (!isSafeFullTableDeleteHandle(paimonTableHandle)) {
                deletePartitionSpecs = partitionDeleteSpecs(paimonTableHandle, fileStoreTable);
                if (deletePartitionSpecs.isEmpty()) {
                    throw new TrinoException(
                            NOT_SUPPORTED,
                            "Paimon metadata delete fallback can only delete all rows or complete partitions from an unlimited table handle");
                }
            }
            long currentRowCount = currentVisibleRowCount(fileStoreTable, deletePartitionSpecs);
            if (deletedRowCount != currentRowCount) {
                throw new TrinoException(NOT_SUPPORTED,
                        deletePartitionSpecs
                                .map(_ -> "Paimon metadata delete fallback can only delete complete partitions; query deleted "
                                        + deletedRowCount + " rows but selected partitions currently contain " + currentRowCount + " rows")
                                .orElse("Paimon metadata delete fallback can only delete all rows; query deleted "
                                        + deletedRowCount + " rows but table currently contains " + currentRowCount + " rows"));
            }
            truncatePaimonTable(fileStoreTable, paimonTableHandle, "delete", "delete rows from", deletePartitionSpecs, null);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (UnsupportedOperationException e) {
            String detail = e.getMessage();
            throw new TrinoException(
                    NOT_SUPPORTED,
                    detail == null || detail.isBlank()
                            ? "Paimon delete uses features which are not supported by the Trino connector"
                            : "Paimon delete uses features which are not supported by the Trino connector: " + detail,
                    e);
        }
        catch (Exception e) {
            throw paimonMetadataException(
                    format("Failed to delete rows from Paimon table '%s'", paimonTableHandle.getTableName()),
                    e);
        }
    }

    private static long metadataDeleteDeletedRowCount(Collection<Slice> fragments)
    {
        long deletedRowCount = 0;
        for (Slice fragment : copyFragments(fragments)) {
            try {
                deletedRowCount = Math.addExact(
                        deletedRowCount,
                        PaimonMetadataDeleteMergeSink.decodeDeletedRowCount(fragment));
            }
            catch (IllegalArgumentException | ArithmeticException e) {
                throw new TrinoException(
                        PAIMON_COMMIT_ERROR,
                        "Failed to deserialize Paimon metadata-delete merge fragment",
                        e);
            }
        }
        return deletedRowCount;
    }

    private static long currentVisibleRowCount(
            FileStoreTable fileStoreTable,
            Optional<List<Map<String, String>>> deletePartitionSpecs)
    {
        requireNonNull(fileStoreTable, "fileStoreTable is null");
        requireNonNull(deletePartitionSpecs, "deletePartitionSpecs is null");
        long rowCount = 0;
        ReadBuilder readBuilder = fileStoreTable.newReadBuilder().dropStats();
        if (deletePartitionSpecs.isPresent()) {
            RowType partitionType = partitionType(fileStoreTable);
            PartitionPredicate partitionPredicate = requireNonNull(
                    PartitionPredicate.fromMaps(
                            partitionType,
                            deletePartitionSpecs.get(),
                            fileStoreTable.coreOptions().partitionDefaultName()),
                    "partitionPredicate is null");
            readBuilder.withPartitionFilter(partitionPredicate);
        }
        List<Split> splits = readBuilder.newScan().plan().splits();
        for (Split split : splits) {
            OptionalLong mergedRowCount = split.mergedRowCount();
            if (mergedRowCount.isPresent()) {
                rowCount = addCurrentVisibleRowCount(rowCount, mergedRowCount.orElseThrow());
            }
            else if (fileStoreTable.primaryKeys().isEmpty()) {
                rowCount = addCurrentVisibleRowCount(rowCount, split.rowCount());
            }
            else {
                throw new TrinoException(
                        NOT_SUPPORTED,
                        "Paimon metadata delete fallback cannot determine the current row count for primary-key tables");
            }
        }
        return rowCount;
    }

    private static long addCurrentVisibleRowCount(long currentRowCount, long splitRowCount)
    {
        if (splitRowCount < 0) {
            throw new TrinoException(NOT_SUPPORTED,
                    "Paimon metadata delete fallback cannot determine the current row count because Paimon reported a negative split row count: "
                            + splitRowCount);
        }
        if (Long.MAX_VALUE - currentRowCount < splitRowCount) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon metadata delete fallback cannot determine the current row count because Paimon split row counts exceed the supported range");
        }
        return currentRowCount + splitRowCount;
    }

    static PaimonTableHandle getOutputTableHandle(ConnectorOutputTableHandle tableHandle)
    {
        if (!(requireNonNull(tableHandle, "tableHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon finish create table requires PaimonTableHandle, got: "
                    + tableHandle.getClass().getName());
        }
        return paimonTableHandle;
    }

    static PaimonTableHandle getInsertTableHandle(ConnectorInsertTableHandle insertHandle)
    {
        if (!(requireNonNull(insertHandle, "insertHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon finish insert requires PaimonTableHandle, got: "
                    + insertHandle.getClass().getName());
        }
        return paimonTableHandle;
    }

    static PaimonTableHandle getMergeTableHandle(ConnectorMergeTableHandle mergeTableHandle)
    {
        return getPaimonMergeTableHandle(mergeTableHandle).paimonTableHandle();
    }

    static PaimonMergeTableHandle getPaimonMergeTableHandle(ConnectorMergeTableHandle mergeTableHandle)
    {
        ConnectorTableHandle tableHandle = requireNonNull(mergeTableHandle, "mergeTableHandle is null").getTableHandle();
        if (!(requireNonNull(tableHandle, "mergeTableHandle tableHandle is null") instanceof PaimonTableHandle)) {
            throw new IllegalStateException("Paimon finish merge requires PaimonTableHandle, got: "
                    + tableHandle.getClass().getName());
        }
        if (!(mergeTableHandle instanceof PaimonMergeTableHandle paimonMergeTableHandle)) {
            throw new IllegalStateException("Paimon finish merge requires PaimonMergeTableHandle, got: "
                    + mergeTableHandle.getClass().getName());
        }
        return paimonMergeTableHandle;
    }

    static PaimonTableHandle getTableHandle(String operation, ConnectorTableHandle tableHandle)
    {
        if (!(requireNonNull(tableHandle, "tableHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon " + operation + " requires PaimonTableHandle, got: "
                    + tableHandle.getClass().getName());
        }
        return paimonTableHandle;
    }

    static PaimonColumnHandle getColumnHandle(String operation, ColumnHandle columnHandle)
    {
        if (!(requireNonNull(columnHandle, "columnHandle is null") instanceof PaimonColumnHandle paimonColumnHandle)) {
            throw new IllegalStateException("Paimon " + operation + " requires PaimonColumnHandle, got: "
                    + columnHandle.getClass().getName());
        }
        return paimonColumnHandle;
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        requireNonNull(session, "session is null");
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(schemaName), "schemaName cannot be null or empty");
        if (SYSTEM_DATABASE_NAME.equals(schemaName)) {
            return true;
        }
        Catalog sessionCatalog = catalog.forSession(session);
        try {
            sessionCatalog.getDatabase(schemaName);
            return true;
        }
        catch (Catalog.DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        requireNonNull(session, "session is null");
        Catalog sessionCatalog = catalog.forSession(session);
        List<String> schemaNames = new ArrayList<>(sessionCatalog.listDatabases());
        if (!schemaNames.contains(SYSTEM_DATABASE_NAME)) {
            schemaNames.add(SYSTEM_DATABASE_NAME);
        }
        return schemaNames;
    }

    @Override
    public void createSchema(
            ConnectorSession session,
            String schemaName,
            Map<String, Object> properties,
            TrinoPrincipal owner)
    {
        requireNonNull(session, "session is null");
        requireNonNull(properties, "properties is null");
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(schemaName), "schemaName cannot be null or empty");
        rejectSystemSchemaWrite(schemaName, "create schema");
        Map<String, String> paimonProperties = schemaProperties(properties, owner);

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.createDatabase(schemaName, false, paimonProperties);
        }
        catch (Catalog.DatabaseAlreadyExistException e) {
            throw new TrinoException(SCHEMA_ALREADY_EXISTS, format("Schema '%s' already exists", schemaName), e);
        }
        catch (Exception e) {
            throw paimonMetadataException(format("Failed to create Paimon schema '%s'", schemaName), e);
        }
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        requireNonNull(session, "session is null");
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(schemaName), "schemaName cannot be null or empty");
        if (SYSTEM_DATABASE_NAME.equals(schemaName)) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon schema properties are not supported for the system schema '" + SYSTEM_DATABASE_NAME + "'");
        }
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            return supportedSchemaProperties(sessionCatalog.getDatabase(schemaName).options());
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaName), e);
        }
        catch (Exception e) {
            throw paimonMetadataException(format("Failed to get Paimon schema properties for '%s'", schemaName), e);
        }
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, String schemaName)
    {
        requireNonNull(session, "session is null");
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(schemaName), "schemaName cannot be null or empty");
        if (SYSTEM_DATABASE_NAME.equals(schemaName)) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon schema owner is not supported for the system schema '" + SYSTEM_DATABASE_NAME + "'");
        }
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            Map<String, String> properties = sessionCatalog.getDatabase(schemaName).options();
            String owner = properties.get(OWNER_PROPERTY);
            if (owner == null || owner.isBlank()) {
                return Optional.empty();
            }
            return Optional.of(new TrinoPrincipal(schemaOwnerType(properties), owner));
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaName), e);
        }
        catch (Exception e) {
            throw paimonMetadataException(format("Failed to get Paimon schema owner for '%s'", schemaName), e);
        }
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String schemaName, TrinoPrincipal principal)
    {
        requireNonNull(session, "session is null");
        requireNonNull(principal, "principal is null");
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(schemaName), "schemaName cannot be null or empty");
        rejectSystemSchemaWrite(schemaName, "set schema authorization");

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.alterDatabase(schemaName, List.of(
                    PropertyChange.setProperty(OWNER_PROPERTY, principal.getName()),
                    PropertyChange.setProperty(TRINO_SCHEMA_OWNER_TYPE_PROPERTY, principal.getType().name())), false);
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaName), e);
        }
        catch (Exception e) {
            throw paimonMetadataException(format("Failed to set authorization on Paimon schema '%s'", schemaName), e);
        }
    }

    private static Map<String, Object> supportedSchemaProperties(Map<String, String> properties)
    {
        Map<String, Object> result = new HashMap<>();
        copySchemaProperty(properties, result, LOCATION_PROPERTY);
        copySchemaProperty(properties, result, COMMENT_PROPERTY);
        return Map.copyOf(result);
    }

    private static void copySchemaProperty(Map<String, String> properties, Map<String, Object> result, String property)
    {
        String value = properties.get(property);
        if (value != null && !value.isBlank()) {
            result.put(property, value);
        }
    }

    private static Map<String, String> schemaProperties(Map<String, Object> properties, TrinoPrincipal owner)
    {
        Map<String, String> result = new HashMap<>();
        boolean ownerPropertyProvided = false;
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String propertyName = requireNonNull(entry.getKey(), "properties contains null property name");
            checkArgument(!StringUtils.isNullOrWhitespaceOnly(propertyName), "properties contains blank property name");
            if (OWNER_PROPERTY.equals(propertyName)) {
                ownerPropertyProvided = true;
            }
            Object value = entry.getValue();
            if (value == null) {
                continue;
            }
            if (!(value instanceof String stringValue)) {
                throw new IllegalArgumentException("properties value for property '%s' must be a string".formatted(propertyName));
            }
            if (stringValue.isBlank()) {
                throw new IllegalArgumentException("properties value for property '%s' is blank".formatted(propertyName));
            }
            result.put(propertyName, stringValue);
        }
        if (owner != null) {
            result.putIfAbsent(OWNER_PROPERTY, owner.getName());
            if (!ownerPropertyProvided || owner.getName().equals(result.get(OWNER_PROPERTY))) {
                result.put(TRINO_SCHEMA_OWNER_TYPE_PROPERTY, owner.getType().name());
            }
        }
        return Map.copyOf(result);
    }

    private static PrincipalType schemaOwnerType(Map<String, String> properties)
    {
        String ownerType = properties.get(TRINO_SCHEMA_OWNER_TYPE_PROPERTY);
        if (ownerType == null || ownerType.isBlank()) {
            return PrincipalType.USER;
        }
        try {
            return PrincipalType.valueOf(ownerType.toUpperCase(Locale.ENGLISH));
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(
                    PAIMON_METADATA_ERROR,
                    "Invalid Paimon schema owner type '%s'".formatted(ownerType),
                    e);
        }
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        requireNonNull(session, "session is null");
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(schemaName), "schemaName cannot be null or empty");
        rejectSystemSchemaWrite(schemaName, "drop schema");
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.dropDatabase(schemaName, false, cascade);
        }
        catch (Catalog.DatabaseNotEmptyException e) {
            throw new TrinoException(SCHEMA_NOT_EMPTY, format("Schema '%s' is not empty", schemaName), e);
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaName));
        }
        catch (Exception e) {
            throw paimonMetadataException(format("Failed to drop Paimon schema '%s'", schemaName), e);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(startVersion, "startVersion is null");
        requireNonNull(endVersion, "endVersion is null");
        if (startVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Read paimon table with start version is not supported");
        }
        if (endVersion.isPresent() && !PaimonTableHandle.supportsHistoricalRead(
                Identifier.create(tableName.getSchemaName(), tableName.getTableName()))) {
            throw new TrinoException(NOT_SUPPORTED, PaimonTableHandle.UNSUPPORTED_HISTORICAL_READ_MESSAGE);
        }

        Map<String, String> dynamicOptions = new HashMap<>();
        if (endVersion.isPresent()) {
            ConnectorTableVersion version = endVersion.get();
            Type versionType = version.getVersionType();
            switch (version.getPointerType()) {
                case TEMPORAL -> {
                    if (!(versionType instanceof TimestampWithTimeZoneType timeZonedVersionType)) {
                        throw new TrinoException(
                                NOT_SUPPORTED,
                                "Unsupported type for table version: " + versionType.getDisplayName());
                    }
                    long epochMillis = timeZonedVersionType.isShort()
                            ? unpackMillisUtc((long) version.getVersion())
                            : ((LongTimestampWithTimeZone) version.getVersion()).getEpochMillis();
                    dynamicOptions.put(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), String.valueOf(epochMillis));
                }
                case TARGET_ID -> {
                    String versionValue;
                    if (versionType instanceof VarcharType) {
                        versionValue = BinaryString.fromBytes(((Slice) version.getVersion()).getBytes()).toString();
                    }
                    else {
                        versionValue = version.getVersion().toString();
                    }
                    if (versionValue.isBlank()) {
                        throw new TrinoException(INVALID_ARGUMENTS, "Paimon table version may not be blank");
                    }
                    dynamicOptions.put(CoreOptions.SCAN_VERSION.key(), versionValue);
                }
            }
        }
        return getTableHandle(session, tableName, dynamicOptions);
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        requireNonNull(session, "session is null");
        getTableHandle("table properties", table);
        return new ConnectorTableProperties();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("table statistics", tableHandle);
        if (paimonTableHandle.getFilter().isNone()
                || (paimonTableHandle.getLimit().isPresent() && paimonTableHandle.getLimit().orElseThrow() == 0)) {
            return TableStatistics.builder().setRowCount(Estimate.zero()).build();
        }
        if (!paimonTableHandle.getFilter().isAll() || paimonTableHandle.getLimit().isPresent()) {
            return TableStatistics.empty();
        }
        if (paimonTableHandle.hasIncrementalReadMode()) {
            return TableStatistics.empty();
        }

        Catalog sessionCatalog = catalog.forSession(session);
        Table table = PaimonTableHandle.schemaAwareReadTable(
                paimonTableHandle.tableWithDynamicOptions(sessionCatalog, session),
                !paimonTableHandle.usesHistoricalReadSchema(session));

        Optional<Statistics> statistics;
        try {
            statistics = table.statistics();
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            Optional<TrinoException> mappedFailure = nestedTrinoException(e);
            if (mappedFailure.isPresent()) {
                throw mappedFailure.get();
            }
            return TableStatistics.empty();
        }
        if (statistics.isPresent()) {
            return fallbackTableStatistics(table, toTableStatistics(table, statistics.get()));
        }
        return fallbackTableStatistics(table, TableStatistics.empty());
    }

    private static TableStatistics fallbackTableStatistics(Table table, TableStatistics tableStatistics)
    {
        if (!tableStatistics.getRowCount().isUnknown() || !(table instanceof FileStoreTable fileStoreTable)) {
            return tableStatistics;
        }

        try {
            OptionalLong rowCount = visibleRowCount(fileStoreTable);
            if (rowCount.isEmpty()) {
                return tableStatistics;
            }
            TableStatistics.Builder builder = TableStatistics.builder().setRowCount(Estimate.of(rowCount.orElseThrow()));
            tableStatistics.getColumnStatistics().forEach(builder::setColumnStatistics);
            return builder.build();
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            Optional<TrinoException> mappedFailure = nestedTrinoException(e);
            if (mappedFailure.isPresent()) {
                throw mappedFailure.get();
            }
            return tableStatistics;
        }
    }

    private static OptionalLong visibleRowCount(FileStoreTable fileStoreTable)
    {
        long rowCount = 0;
        List<Split> splits = fileStoreTable.newReadBuilder().dropStats().newScan().plan().splits();
        for (Split split : splits) {
            OptionalLong mergedRowCount = split.mergedRowCount();
            long splitRowCount;
            if (mergedRowCount.isPresent()) {
                splitRowCount = mergedRowCount.orElseThrow();
            }
            else if (fileStoreTable.primaryKeys().isEmpty()) {
                splitRowCount = split.rowCount();
            }
            else {
                return OptionalLong.empty();
            }
            if (splitRowCount < 0 || Long.MAX_VALUE - rowCount < splitRowCount) {
                return OptionalLong.empty();
            }
            rowCount += splitRowCount;
        }
        return OptionalLong.of(rowCount);
    }

    private static Optional<TrinoException> nestedTrinoException(RuntimeException exception)
    {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = exception.getCause();
        while (current != null && visited.add(current)) {
            if (current instanceof TrinoException trinoException) {
                return Optional.of(trinoException);
            }
            current = current.getCause();
        }
        return Optional.empty();
    }

    private TableStatistics toTableStatistics(Table table, Statistics statistics)
    {
        TableStatistics.Builder builder = TableStatistics.builder();

        OptionalLong mergedRecordCount = statistics.mergedRecordCount();
        mergedRecordCount.ifPresent(rowCount -> {
            if (rowCount >= 0) {
                builder.setRowCount(Estimate.of(rowCount));
            }
        });

        Map<String, ColStats<?>> colStats = statistics.colStats();
        if (colStats == null || colStats.isEmpty()) {
            return builder.build();
        }

        Map<Integer, ColStats<?>> colStatsById = new HashMap<>();
        Set<Integer> duplicateColStatIds = new HashSet<>();
        for (ColStats<?> stats : colStats.values()) {
            if (stats == null) {
                continue;
            }
            ColStats<?> previous = colStatsById.putIfAbsent(stats.colId(), stats);
            if (previous != null) {
                duplicateColStatIds.add(stats.colId());
            }
        }

        for (DataField field : PaimonTableHandle.effectiveReadRowType(table).getFields()) {
            ColStats<?> columnStats = colStats.get(field.name());
            if (columnStats != null && columnStats.colId() != field.id()) {
                columnStats = null;
            }
            if (columnStats == null && !duplicateColStatIds.contains(field.id())) {
                columnStats = colStatsById.get(field.id());
            }
            if (columnStats != null) {
                builder.setColumnStatistics(
                        toPaimonColumnHandle(field),
                        toColumnStatistics(field.type(), columnStats, mergedRecordCount, typeManager));
            }
        }
        return builder.build();
    }

    private static ColumnStatistics toColumnStatistics(
            DataType logicalType,
            ColStats<?> stats,
            OptionalLong rowCount,
            TypeManager typeManager)
    {
        ColumnStatistics.Builder builder = ColumnStatistics.builder();

        stats.distinctCount().ifPresent(distinctCount -> {
            if (distinctCount < 0) {
                return;
            }
            if (rowCount.isPresent()) {
                long records = rowCount.orElseThrow();
                if (records >= 0 && distinctCount > records) {
                    return;
                }
            }
            builder.setDistinctValuesCount(Estimate.of(distinctCount));
        });
        if (rowCount.isPresent()) {
            long records = rowCount.orElseThrow();
            stats.nullCount().ifPresent(nullCount -> {
                if (records == 0) {
                    builder.setNullsFraction(Estimate.zero());
                }
                else if (records > 0 && nullCount >= 0 && nullCount <= records) {
                    builder.setNullsFraction(Estimate.of((double) nullCount / records));
                }
            });
            stats.avgLen().ifPresent(avgLen -> {
                if (records >= 0 && avgLen >= 0) {
                    OptionalLong nullCount = stats.nullCount();
                    long nullCountValue = nullCount.orElse(0);
                    if (nullCount.isPresent() && (nullCountValue < 0 || nullCountValue > records)) {
                        return;
                    }
                    long nonNullRecords = records - nullCountValue;
                    builder.setDataSize(Estimate.of((double) nonNullRecords * avgLen));
                }
            });
        }
        toRange(logicalType, stats, typeManager).ifPresent(builder::setRange);

        return builder.build();
    }

    private static Optional<DoubleRange> toRange(DataType logicalType, ColStats<?> stats, TypeManager typeManager)
    {
        Optional<?> min = stats.min();
        Optional<?> max = stats.max();
        if (min.isEmpty() || max.isEmpty()) {
            return Optional.empty();
        }

        try {
            Type trinoType = PaimonTypeUtils.fromPaimonType(logicalType, typeManager);
            Object minValue = toTrinoNativeStatsValue(trinoType, logicalType, min.get());
            Object maxValue = toTrinoNativeStatsValue(trinoType, logicalType, max.get());
            return DoubleRange.from(trinoType, minValue, maxValue);
        }
        catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static Object toTrinoNativeStatsValue(Type trinoType, DataType logicalType, Object value)
    {
        return switch (logicalType.getTypeRoot()) {
            case BOOLEAN -> value;
            case TINYINT, SMALLINT, INTEGER, BIGINT, DATE -> ((Number) value).longValue();
            case FLOAT -> (long) Float.floatToIntBits(((Number) value).floatValue());
            case DOUBLE -> ((Number) value).doubleValue();
            case DECIMAL -> toTrinoNativeDecimalValue((DecimalType) trinoType, (Decimal) value);
            case TIMESTAMP_WITHOUT_TIME_ZONE -> paimonTimestampToTrino(trinoType, (Timestamp) value);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> paimonTimestampToTrinoTimestampWithTimeZone(trinoType, value);
            default -> throw new IllegalArgumentException("Unsupported Paimon statistics range type: " + logicalType);
        };
    }

    private static Object toTrinoNativeDecimalValue(DecimalType trinoType, Decimal value)
    {
        if (trinoType.isShort()) {
            return Decimals.encodeShortScaledValue(value.toBigDecimal(), trinoType.getScale());
        }
        return Decimals.encodeScaledValue(value.toBigDecimal(), trinoType.getScale());
    }

    public PaimonTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Map<String, String> dynamicOptions)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(dynamicOptions, "dynamicOptions is null");
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                dynamicOptions);
        Catalog sessionCatalog = catalog.forSession(session);
        try {
            Table table = PaimonTableSupport.requireSupportedTable(
                    sessionCatalog.getTable(Identifier.create(tableName.getSchemaName(), tableName.getTableName())));
            tableHandle.cacheTable(sessionCatalog, table);
            return tableHandle;
        }
        catch (Catalog.TableNotExistException e) {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("table metadata", tableHandle);
        Catalog sessionCatalog = catalog.forSession(session);
        return paimonTableHandle.tableMetadata(sessionCatalog, typeManager, session);
    }

    @Override
    public void setTableProperties(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Map<String, Optional<Object>> properties)
    {
        requireNonNull(session, "session is null");
        requireNonNull(properties, "properties is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("set table properties", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "set table properties");
        if (properties.isEmpty()) {
            return;
        }
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        rejectUnsupportedTablePropertyUpdates(properties);
        Catalog sessionCatalog = catalog.forSession(session);
        Optional<PaimonTableOptionSnapshot> tableOptionSnapshot = Optional.empty();
        List<SchemaChange> changes = new ArrayList<>();
        Set<String> updatedPaimonOptionKeys = new HashSet<>();

        // Handle both setting and removing options
        // When SET PROPERTIES x = DEFAULT is used, the value will be Optional.empty()
        for (Map.Entry<String, Optional<Object>> entry : properties.entrySet()) {
            String propertyName = requireNonNull(entry.getKey(), "properties contains null property name");
            checkArgument(!StringUtils.isNullOrWhitespaceOnly(propertyName), "properties contains blank property name");
            String key = PaimonTableOptionUtils.toPaimonOptionKey(propertyName);
            if (!updatedPaimonOptionKeys.add(key)) {
                throw new TrinoException(
                        INVALID_TABLE_PROPERTY,
                        "Multiple table properties map to Paimon option '%s'".formatted(key));
            }
            Optional<Object> value = requireNonNull(
                    entry.getValue(),
                    "properties contains null value for property '%s'".formatted(propertyName));

            if (value.isPresent()) {
                // Set the property to the specified value
                String optionValue = PaimonTableOptionUtils.normalizeOptionValue(propertyName, key, value.get());
                if (requiresExistingOptionsForPaimonOptionUpdate(key)) {
                    tableOptionSnapshot = tableOptionSnapshot.or(() ->
                            getPaimonTableOptionSnapshotIfAvailable(sessionCatalog, paimonTableHandle));
                    tableOptionSnapshot.ifPresent(snapshot ->
                            validatePaimonTableOptionUpdate(snapshot.options(), snapshot::hasSnapshots, key, optionValue));
                }
                else {
                    validatePaimonTableOptionUpdate(Map.of(), () -> true, key, optionValue);
                }
                changes.add(SchemaChange.setOption(key, optionValue));
            }
            else {
                // Remove the property (SET PROPERTIES x = DEFAULT)
                if (requiresExistingOptionsForPaimonOptionRemove(key)) {
                    tableOptionSnapshot = tableOptionSnapshot.or(() ->
                            getPaimonTableOptionSnapshotIfAvailable(sessionCatalog, paimonTableHandle));
                    tableOptionSnapshot.ifPresent(snapshot ->
                            validatePaimonTableOptionRemove(snapshot.options(), snapshot::hasSnapshots, key));
                }
                else {
                    validatePaimonTableOptionRemove(Map.of(), () -> true, key);
                }
                changes.add(SchemaChange.removeOption(key));
            }
        }

        try {
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    private static boolean requiresExistingOptionsForPaimonOptionUpdate(String key)
    {
        return PAIMON_OPTION_UPDATES_REQUIRING_EXISTING_OPTIONS.contains(key);
    }

    private static boolean requiresExistingOptionsForPaimonOptionRemove(String key)
    {
        return PAIMON_OPTION_REMOVES_REQUIRING_EXISTING_OPTIONS.contains(key);
    }

    private static Optional<PaimonTableOptionSnapshot> getPaimonTableOptionSnapshotIfAvailable(
            Catalog sessionCatalog,
            PaimonTableHandle tableHandle)
    {
        try {
            Table table = sessionCatalog.getTable(Identifier.create(
                    tableHandle.getSchemaName(),
                    tableHandle.getTableName()));
            Map<String, String> options = table instanceof FileStoreTable fileStoreTable
                    ? fileStoreTable.schema().options()
                    : table.options();
            return Optional.of(new PaimonTableOptionSnapshot(options, table));
        }
        catch (Catalog.TableNotExistException e) {
            return Optional.empty();
        }
    }

    private static void validatePaimonTableOptionUpdate(
            Map<String, String> existingOptions,
            BooleanSupplier hasSnapshots,
            String key,
            String value)
    {
        String oldValue = existingOptions.get(key);
        if (Objects.equals(oldValue, value) || !hasSnapshots.getAsBoolean()) {
            return;
        }
        try {
            SchemaManager.checkAlterTableOption(existingOptions, key, oldValue, value);
        }
        catch (UnsupportedOperationException e) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    unsupportedOperationMessageOrFallback(
                            "Paimon table option '%s' update is not supported".formatted(key), e),
                    e);
        }
    }

    private static void validatePaimonTableOptionRemove(
            Map<String, String> existingOptions,
            BooleanSupplier hasSnapshots,
            String key)
    {
        if (!hasSnapshots.getAsBoolean()) {
            return;
        }
        try {
            SchemaManager.checkResetTableOption(existingOptions, key);
        }
        catch (UnsupportedOperationException e) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    unsupportedOperationMessageOrFallback(
                            "Paimon table option '%s' reset is not supported".formatted(key), e),
                    e);
        }
    }

    private static final class PaimonTableOptionSnapshot
    {
        private final Map<String, String> options;
        private final Table table;
        private Boolean hasSnapshots;

        private PaimonTableOptionSnapshot(Map<String, String> options, Table table)
        {
            this.options = Map.copyOf(requireNonNull(options, "options is null"));
            this.table = requireNonNull(table, "table is null");
        }

        private Map<String, String> options()
        {
            return options;
        }

        private boolean hasSnapshots()
        {
            if (hasSnapshots == null) {
                hasSnapshots = table.latestSnapshot().isPresent();
            }
            return hasSnapshots;
        }
    }

    private static void rejectUnsupportedTablePropertyUpdates(Map<String, Optional<Object>> properties)
    {
        List<String> unsupportedProperties = properties.keySet().stream()
                .peek(property -> requireNonNull(property, "properties contains null property name"))
                .peek(property -> checkArgument(
                        !StringUtils.isNullOrWhitespaceOnly(property),
                        "properties contains blank property name"))
                .filter(property -> PaimonTableOptions.PRIMARY_KEY_IDENTIFIER.equals(property)
                        || PaimonTableOptions.PARTITIONED_BY_PROPERTY.equals(property)
                        || CoreOptions.PRIMARY_KEY.key().equals(property)
                        || CoreOptions.PARTITION.key().equals(property)
                        || CoreOptions.IMMUTABLE_OPTIONS.contains(PaimonTableOptionUtils.toPaimonOptionKey(property))
                        || PaimonTableOptionUtils.isRuntimeOnlyTableProperty(property))
                .sorted()
                .toList();
        if (!unsupportedProperties.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "The following properties cannot be updated: " + String.join(", ", unsupportedProperties));
        }
    }

    @Override
    public void setTableAuthorization(ConnectorSession session, SchemaTableName tableName, TrinoPrincipal principal)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(principal, "principal is null");
        rejectSystemTableWrite(tableName, "set table authorization");

        Identifier identifier = new Identifier(tableName.getSchemaName(), tableName.getTableName());
        List<SchemaChange> changes = List.of(SchemaChange.setOption(OWNER_PROPERTY, principal.getName()));
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(tableName, e);
        }
    }

    private static void rejectSystemSchemaWrite(String schemaName, String operation)
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(operation, "operation is null");
        if (SYSTEM_DATABASE_NAME.equals(schemaName)) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon " + operation + " is not supported for the system schema '" + SYSTEM_DATABASE_NAME + "'");
        }
    }

    private static void rejectSystemTableWrite(PaimonTableHandle tableHandle, String operation)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        rejectSystemTableWrite(schemaTableName(tableHandle), operation);
    }

    private static void rejectSystemTableWrite(SchemaTableName tableName, String operation)
    {
        requireNonNull(tableName, "tableName is null");
        rejectSystemSchemaWrite(tableName.getSchemaName(), operation);
        rejectSystemTableWrite(Identifier.create(tableName.getSchemaName(), tableName.getTableName()), operation);
    }

    private static void rejectSystemTableWrite(Identifier identifier, String operation)
    {
        requireNonNull(identifier, "identifier is null");
        requireNonNull(operation, "operation is null");
        if (identifier.isSystemTable()) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon " + operation + " is not supported for system table '" + identifier.getFullName() + "'");
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(schemaName, "schemaName is null");
        schemaName.ifPresent(schema -> checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(schema),
                "schemaName cannot be null or empty"));
        Catalog sessionCatalog = catalog.forSession(session);
        List<SchemaTableName> tables = new ArrayList<>();
        schemaName.map(Collections::singletonList)
                .orElseGet(() -> listSchemaNames(session))
                .forEach(schema -> {
                    tables.addAll(listTables(sessionCatalog, schema));
                    tables.addAll(listViewsIfSupported(sessionCatalog, schema));
                });
        return tables;
    }

    private List<SchemaTableName> listTables(Catalog sessionCatalog, String schema)
    {
        if (SYSTEM_DATABASE_NAME.equals(schema)) {
            return SystemTableLoader.loadGlobalTableNames(catalog.catalogOptions()).stream()
                    .map(table -> new SchemaTableName(schema, table))
                    .collect(toList());
        }
        try {
            return sessionCatalog.listTables(schema).stream().map(table -> new SchemaTableName(schema, table))
                    .collect(toList());
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schema), e);
        }
    }

    private List<SchemaTableName> listViewsIfSupported(Catalog sessionCatalog, String schemaName)
    {
        try {
            return listViews(sessionCatalog, schemaName);
        }
        catch (TrinoException e) {
            if (isUnsupportedViewListOperation(e)) {
                return List.of();
            }
            throw e;
        }
    }

    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(ConnectorSession session, Optional<String> schemaName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(schemaName, "schemaName is null");
        schemaName.ifPresent(schema -> checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(schema),
                "schemaName cannot be null or empty"));
        Catalog sessionCatalog = catalog.forSession(session);
        Map<SchemaTableName, RelationType> relationTypes = new LinkedHashMap<>();
        schemaName.map(Collections::singletonList)
                .orElseGet(() -> listSchemaNames(session))
                .forEach(schema -> {
                    listTables(sessionCatalog, schema).forEach(tableName -> relationTypes.put(tableName, RelationType.TABLE));
                    listViewsIfSupported(sessionCatalog, schema).forEach(viewName -> relationTypes.put(viewName, RelationType.VIEW));
                });
        return Collections.unmodifiableMap(new LinkedHashMap<>(relationTypes));
    }

    @Override
    public void createTable(
            ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            SaveMode saveMode)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableMetadata, "tableMetadata is null");
        requireNonNull(saveMode, "saveMode is null");
        SchemaTableName table = tableMetadata.getTable();
        rejectSystemTableWrite(table, "create table");
        Identifier identifier = Identifier.create(table.getSchemaName(), table.getTableName());
        Schema schema = prepareSchema(tableMetadata, false);

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            if (saveMode == SaveMode.REPLACE) {
                replaceOrCreateTable(sessionCatalog, identifier, schema);
                return;
            }
            sessionCatalog.createTable(identifier, schema, saveMode == SaveMode.IGNORE);
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", table.getSchemaName()));
        }
        catch (Catalog.TableAlreadyExistException e) {
            if (saveMode == SaveMode.IGNORE) {
                return;
            }
            throw new TrinoException(TABLE_ALREADY_EXISTS, format("Table '%s' already exists", table), e);
        }
        catch (UnsupportedOperationException e) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    unsupportedOperationMessageWithDetail(
                            format("Paimon create or replace table '%s' is not supported", table), e),
                    e);
        }
        catch (Exception e) {
            throw paimonMetadataException(format("Failed to create Paimon table '%s'", table), e);
        }
    }

    private static void replaceOrCreateTable(Catalog sessionCatalog, Identifier identifier, Schema schema)
            throws Catalog.TableAlreadyExistException, Catalog.DatabaseNotExistException, Catalog.TableNotExistException
    {
        try {
            sessionCatalog.replaceTable(identifier, schema, false);
        }
        catch (Catalog.TableNotExistException e) {
            sessionCatalog.createTable(identifier, schema, false);
        }
    }

    private Schema prepareSchema(ConnectorTableMetadata tableMetadata, boolean applyColumnCommentDirectives)
    {
        Map<String, Object> properties = new HashMap<>(tableMetadata.getProperties());
        List<String> primaryKeys = PaimonTableOptions.getPrimaryKeys(properties);
        List<String> partitionKeys = PaimonTableOptions.getPartitionedKeys(properties);
        primaryKeys.forEach(column -> rejectPaimonSystemColumnName("create table primary key", column));
        partitionKeys.forEach(column -> rejectPaimonSystemColumnName("create table partition key", column));
        List<String> columnNames = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toList());
        Map<String, String> canonicalColumnNames = canonicalColumnNames(columnNames);
        primaryKeys = canonicalKeyColumns(PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, primaryKeys, canonicalColumnNames);
        partitionKeys = canonicalKeyColumns(PaimonTableOptions.PARTITIONED_BY_PROPERTY, partitionKeys, canonicalColumnNames);
        Map<String, String> options = PaimonTableOptionUtils.buildOptionMap(properties);
        Schema.Builder builder = Schema.newBuilder().primaryKey(primaryKeys)
                .partitionKeys(partitionKeys)
                .comment(tableMetadata.getComment().orElse(null));

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            rejectPaimonSystemColumnName("create table", column.getName());
            DataType dataType = toPaimonType(column);
            String comment = column.getComment().orElse(null);
            if (applyColumnCommentDirectives) {
                ConvertedColumn convertedColumn = applyAddColumnCommentDirective(column, dataType, options);
                if (convertedColumn != null) {
                    dataType = convertedColumn.type();
                    comment = convertedColumn.comment();
                }
            }
            else {
                validateAddColumnCommentDirective(column, dataType);
            }
            builder.column(column.getName(), dataType, comment);
        }

        builder.options(options);

        return builder.build();
    }

    private static Map<String, String> canonicalColumnNames(List<String> columnNames)
    {
        requireNonNull(columnNames, "columnNames is null");
        Map<String, String> canonicalColumnNames = new LinkedHashMap<>();
        Set<String> duplicateColumns = new LinkedHashSet<>();
        for (String columnName : columnNames) {
            String lowerColumnName = FieldNameUtils.toLowerCase(columnName);
            if (canonicalColumnNames.putIfAbsent(lowerColumnName, columnName) != null) {
                duplicateColumns.add(lowerColumnName);
            }
        }
        if (!duplicateColumns.isEmpty()) {
            throw new TrinoException(
                    INVALID_TABLE_PROPERTY,
                    "Paimon table columns must not contain case-insensitive duplicate columns: " + duplicateColumns);
        }
        return canonicalColumnNames;
    }

    private static List<String> canonicalKeyColumns(
            String propertyName,
            List<String> keyColumns,
            Map<String, String> canonicalColumnNames)
    {
        requireNonNull(propertyName, "propertyName is null");
        requireNonNull(keyColumns, "keyColumns is null");
        requireNonNull(canonicalColumnNames, "canonicalColumnNames is null");
        if (keyColumns.isEmpty()) {
            return List.of();
        }

        Set<String> duplicateColumns = duplicates(FieldNameUtils.toLowerCase(keyColumns));
        if (!duplicateColumns.isEmpty()) {
            throw new TrinoException(
                    INVALID_TABLE_PROPERTY,
                    "Paimon " + propertyName + " must not contain duplicate columns: " + duplicateColumns);
        }

        List<String> missingColumns = keyColumns.stream()
                .filter(column -> !canonicalColumnNames.containsKey(FieldNameUtils.toLowerCase(column)))
                .toList();
        if (!missingColumns.isEmpty()) {
            throw new TrinoException(
                    INVALID_TABLE_PROPERTY,
                    "Paimon " + propertyName + " columns not present in schema: " + missingColumns);
        }

        return keyColumns.stream()
                .map(column -> canonicalColumnNames.get(FieldNameUtils.toLowerCase(column)))
                .collect(toList());
    }

    private static Set<String> duplicates(List<String> values)
    {
        Set<String> seen = new HashSet<>();
        Set<String> duplicates = new LinkedHashSet<>();
        for (String value : values) {
            if (!seen.add(value)) {
                duplicates.add(value);
            }
        }
        return duplicates;
    }

    private static DataType toPaimonType(ColumnMetadata column)
    {
        return toPaimonType(column.getType()).copy(column.isNullable());
    }

    private static ConvertedColumn applyAddColumnCommentDirective(
            ColumnMetadata column,
            DataType dataType,
            Map<String, String> options)
    {
        try {
            return applyAddColumnDirective(column.getComment().orElse(null), column.getName(), dataType, options);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Invalid Paimon column comment directive for column '%s': %s"
                            .formatted(column.getName(), e.getMessage()),
                    e);
        }
    }

    private static void validateAddColumnCommentDirective(ColumnMetadata column, DataType dataType)
    {
        applyAddColumnCommentDirective(column, dataType, new HashMap<>());
    }

    private PaimonColumnHandle toPaimonColumnHandle(DataField field)
    {
        try {
            return PaimonColumnHandle.of(field.name(), field.type(), typeManager);
        }
        catch (UnsupportedOperationException e) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    unsupportedOperationMessageOrFallback(
                            "Unsupported Paimon column '%s' with type %s: %s"
                                    .formatted(field.name(), paimonDataTypeName(field.type()), unsupportedOperationMessage(e)),
                            e),
                    e);
        }
    }

    private static DataType toPaimonType(Type type)
    {
        try {
            return PaimonTypeUtils.toPaimonType(requireNonNull(type, "type is null"));
        }
        catch (UnsupportedOperationException e) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    unsupportedOperationMessageOrFallback(
                            "Unsupported Trino type %s: %s".formatted(trinoTypeName(type), unsupportedOperationMessage(e)),
                            e),
                    e);
        }
    }

    private static String unsupportedOperationMessageWithDetail(String prefix, UnsupportedOperationException exception)
    {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            return prefix;
        }
        return prefix + ": " + message;
    }

    private static String unsupportedOperationMessageOrFallback(String fallback, UnsupportedOperationException exception)
    {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            return fallback;
        }
        return message;
    }

    private static String unsupportedOperationMessage(UnsupportedOperationException exception)
    {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            return exception.getClass().getSimpleName();
        }
        return message;
    }

    private static String trinoTypeName(Type type)
    {
        try {
            String displayName = type.getDisplayName();
            if (displayName != null && !displayName.isBlank()) {
                return displayName;
            }
        }
        catch (RuntimeException ignored) {
            // Fall through to toString/class name while formatting an unsupported type failure.
        }
        return objectName(type);
    }

    private static String paimonDataTypeName(DataType type)
    {
        try {
            String name = type.toString();
            if (name != null && !name.isBlank()) {
                return name;
            }
        }
        catch (RuntimeException ignored) {
            // Fall through to the implementation class while formatting an unsupported type failure.
        }
        return type.getClass().getName();
    }

    private static String objectName(Object value)
    {
        try {
            String name = value.toString();
            if (name != null && !name.isBlank()) {
                return name;
            }
        }
        catch (RuntimeException ignored) {
            // Fall through to the implementation class while formatting an unsupported value.
        }
        return value.getClass().getName();
    }

    private static RuntimeException paimonAlterTableException(SchemaTableName tableName, Exception exception)
    {
        if (exception instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (exception instanceof Catalog.TableNotExistException) {
            return new TrinoException(TABLE_NOT_FOUND, format("Table '%s' does not exist", tableName), exception);
        }
        if (exception instanceof Catalog.ColumnAlreadyExistException columnAlreadyExistException) {
            return new TrinoException(
                    COLUMN_ALREADY_EXISTS,
                    format("Column '%s' already exists in table '%s'", columnAlreadyExistException.column(), tableName),
                    exception);
        }
        if (exception instanceof Catalog.ColumnNotExistException columnNotExistException) {
            return new TrinoException(
                    COLUMN_NOT_FOUND,
                    format("Column '%s' does not exist in table '%s'", columnNotExistException.column(), tableName),
                    exception);
        }
        if (exception instanceof Catalog.DatabaseNotExistException) {
            return new TrinoException(
                    SCHEMA_NOT_FOUND,
                    format("Schema '%s' does not exist", tableName.getSchemaName()),
                    exception);
        }
        return paimonMetadataException(format("Failed to alter Paimon table '%s'", tableName), exception);
    }

    private static boolean isColumnAlreadyExistsException(Exception exception)
    {
        return exception instanceof Catalog.ColumnAlreadyExistException
                || (exception instanceof TrinoException trinoException
                && trinoException.getErrorCode().equals(COLUMN_ALREADY_EXISTS.toErrorCode()));
    }

    private static SchemaTableName schemaTableName(PaimonTableHandle tableHandle)
    {
        return new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle oldTableHandle = getTableHandle("rename table", tableHandle);
        requireNonNull(newTableName, "newTableName is null");
        rejectSystemTableWrite(oldTableHandle, "rename table");
        rejectSystemTableWrite(newTableName, "rename table");
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.renameTable(
                    new Identifier(oldTableHandle.getSchemaName(), oldTableHandle.getTableName()),
                    new Identifier(newTableName.getSchemaName(), newTableName.getTableName()),
                    false);
        }
        catch (Catalog.TableNotExistException e) {
            throw new TrinoException(TABLE_NOT_FOUND, format(
                    "Table '%s.%s' does not exist",
                    oldTableHandle.getSchemaName(),
                    oldTableHandle.getTableName()), e);
        }
        catch (Catalog.TableAlreadyExistException e) {
            throw new TrinoException(TABLE_ALREADY_EXISTS, format("Table '%s' already exists", newTableName), e);
        }
        catch (Exception e) {
            throw paimonMetadataException(
                    format("Failed to rename Paimon table '%s' to '%s'",
                            schemaTableName(oldTableHandle),
                            newTableName),
                    e);
        }
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("drop table", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "drop table");
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.dropTable(new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName()), false);
        }
        catch (Catalog.TableNotExistException e) {
            throw new TrinoException(TABLE_NOT_FOUND, format(
                    "Table '%s.%s' does not exist",
                    paimonTableHandle.getSchemaName(),
                    paimonTableHandle.getTableName()), e);
        }
        catch (Exception e) {
            throw paimonMetadataException(
                    format("Failed to drop Paimon table '%s'", schemaTableName(paimonTableHandle)),
                    e);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle table = getTableHandle("column handles", tableHandle);
        Catalog sessionCatalog = catalog.forSession(session);
        Map<String, ColumnHandle> handleMap = new HashMap<>();
        for (ColumnMetadata column : table.columnMetadatas(sessionCatalog, typeManager, session)) {
            handleMap.put(column.getName(), table.columnHandle(sessionCatalog, typeManager, session, column.getName()));
        }
        return handleMap;
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("column metadata", tableHandle);
        PaimonColumnHandle paimonColumnHandle = getColumnHandle("column metadata", columnHandle);
        if (paimonColumnHandle.isRowId()) {
            return paimonColumnHandle.getColumnMetadata();
        }
        Catalog sessionCatalog = catalog.forSession(session);
        Table table = PaimonTableHandle.schemaAwareReadTable(
                paimonTableHandle.tableWithDynamicOptions(sessionCatalog, session),
                !paimonTableHandle.usesHistoricalReadSchema(session));
        try {
            return PaimonTableHandle.columnMetadata(
                    table,
                    paimonColumnHandle.getColumnName(),
                    typeManager);
        }
        catch (TrinoException e) {
            if (e.getErrorCode().equals(COLUMN_NOT_FOUND.toErrorCode())
                    && !PaimonColumnHandle.isHiddenColumnName(paimonColumnHandle.getColumnName())) {
                // Trino may ask for metadata using a stale ordinary column handle immediately after
                // rename/drop DDL has already changed the table schema.
                return paimonColumnHandle.getColumnMetadata();
            }
            throw e;
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(session, "session is null");
        requireNonNull(prefix, "prefix is null");
        List<SchemaTableName> tableNames = prefix.getTable()
                .map(_ -> Collections.singletonList(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        return tableNames.stream()
                .map(tableName -> getTableColumnsMetadata(session, tableName)
                        .map(columns -> Map.entry(tableName, columns)))
                .flatMap(Optional::stream)
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> List.copyOf(entry.getValue())));
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(
            ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(session, "session is null");
        requireNonNull(prefix, "prefix is null");
        List<SchemaTableName> tableNames = prefix.getTable()
                .map(_ -> Collections.singletonList(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        return tableNames.stream()
                .map(tableName -> getTableColumnsMetadata(session, tableName)
                        .map(columns -> TableColumnsMetadata.forTable(tableName, columns)))
                .flatMap(Optional::stream)
                .iterator();
    }

    private Optional<List<ColumnMetadata>> getTableColumnsMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        PaimonTableHandle tableHandle = getTableHandle(session, tableName, Collections.emptyMap());
        if (tableHandle == null) {
            return getViewColumnsMetadata(session, tableName);
        }
        Catalog sessionCatalog = catalog.forSession(session);
        return Optional.of(tableHandle.columnMetadatas(sessionCatalog, typeManager, session));
    }

    private Optional<List<ColumnMetadata>> getViewColumnsMetadata(ConnectorSession session, SchemaTableName viewName)
    {
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            Optional<View> view = getPaimonView(sessionCatalog, viewName);
            if (view.isEmpty()) {
                return Optional.empty();
            }
            View paimonView = view.get();
            if (!hasTrinoViewDialect(paimonView)) {
                return Optional.empty();
            }
            return Optional.of(viewColumnsMetadata(paimonView));
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (UnsupportedOperationException e) {
            throw unsupportedViewOperation("read", e);
        }
        catch (Exception e) {
            throw paimonViewException(format("Failed to get view '%s'", viewName), e);
        }
    }

    private List<ColumnMetadata> viewColumnsMetadata(View view)
    {
        return view.rowType().getFields().stream()
                .map(field -> ColumnMetadata.builder()
                        .setName(field.name())
                        .setType(PaimonTypeUtils.fromPaimonType(field.type(), typeManager))
                        .setComment(Optional.ofNullable(field.description()).filter(comment -> !comment.isEmpty()))
                        .build())
                .collect(toList());
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column, ColumnPosition position)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("add column", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "add column");
        requireNonNull(column, "column is null");
        rejectPaimonSystemColumnName("add column", column.getName());
        if (!column.isNullable()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding not null columns");
        }

        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        List<SchemaChange> changes = new ArrayList<>();
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            tryLatestWriteFileStoreTable(paimonTableHandle, sessionCatalog, "add column")
                    .ifPresent(table -> validateNoCaseInsensitiveDuplicateColumnName(
                            table, schemaTableName(paimonTableHandle), column.getName(), Optional.empty()));
            DataType dataType = toPaimonType(column);
            validateAddColumnCommentDirective(column, dataType);
            changes.add(SchemaChange.addColumn(column.getName(), dataType, column.getComment().orElse(null), null));
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void renameColumn(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle source,
            String target)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("rename column", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "rename column");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = getColumnHandle("rename column", source);
        rejectPaimonSystemColumn(paimonColumnHandle, "rename column");
        validateFieldName("target", target);
        rejectPaimonSystemColumnName("rename column", target);
        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable table = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, "rename column");
        DataField sourceField = canonicalColumn(table, schemaTableName(paimonTableHandle), paimonColumnHandle);
        String sourceColumnName = sourceField.name();
        PaimonSchemaEvolutionKeys schemaEvolutionKeys = schemaEvolutionKeys(table);
        rejectPartitionKeyChange("rename column", "rename", paimonColumnHandle, schemaEvolutionKeys);
        rejectPrimaryKeyChange("rename column", "rename", paimonColumnHandle, schemaEvolutionKeys);
        rejectBlobColumnRename(sourceField);
        validateNoCaseInsensitiveDuplicateColumnName(
                table,
                schemaTableName(paimonTableHandle),
                target,
                Optional.of(sourceColumnName));
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.renameColumn(sourceColumnName, target));
        try {
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("drop column", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "drop column");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = getColumnHandle("drop column", column);
        rejectPaimonSystemColumn(paimonColumnHandle, "drop column");
        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable table = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, "drop column");
        String columnName = canonicalColumn(table, schemaTableName(paimonTableHandle), paimonColumnHandle).name();
        PaimonSchemaEvolutionKeys schemaEvolutionKeys = schemaEvolutionKeys(table);
        rejectPartitionOrPrimaryKeyDrop(paimonColumnHandle, schemaEvolutionKeys);
        rejectDropAllFields(table.rowType().getFields().size(), "drop column");
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.dropColumn(columnName));
        try {
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("set table comment", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "set table comment");
        requireNonNull(comment, "comment is null");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.updateComment(comment.orElse(null)));
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void setColumnComment(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle column,
            Optional<String> comment)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("set column comment", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "set column comment");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = getColumnHandle("set column comment", column);
        rejectPaimonSystemColumn(paimonColumnHandle, "set column comment");
        requireNonNull(comment, "comment is null");
        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable table = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, "set column comment");
        String columnName = canonicalColumn(table, schemaTableName(paimonTableHandle), paimonColumnHandle).name();
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.updateColumnComment(columnName, comment.orElse(null)));
        try {
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void setColumnType(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle column,
            Type type)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("set column type", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "set column type");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = getColumnHandle("set column type", column);
        rejectPaimonSystemColumn(paimonColumnHandle, "set column type");
        requireNonNull(type, "type is null");
        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable table = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, "set column type");
        DataField field = canonicalColumn(table, schemaTableName(paimonTableHandle), paimonColumnHandle);
        String columnName = field.name();
        PaimonSchemaEvolutionKeys schemaEvolutionKeys = schemaEvolutionKeys(table);
        rejectPartitionKeyChange("set column type", "update", paimonColumnHandle, schemaEvolutionKeys);
        rejectPrimaryKeyChange("set column type", "update", paimonColumnHandle, schemaEvolutionKeys);

        DataType paimonType = toPaimonType(type)
                .copy(field.type().isNullable());
        rejectBlobColumnTypeChange(field, paimonType);

        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.updateColumnType(columnName, paimonType, true));

        try {
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("drop not null constraint", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "drop not null constraint");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = getColumnHandle("drop not null constraint", column);
        rejectPaimonSystemColumn(paimonColumnHandle, "drop not null constraint");
        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable table = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, "drop not null constraint");
        String columnName = canonicalColumn(table, schemaTableName(paimonTableHandle), paimonColumnHandle).name();
        PaimonSchemaEvolutionKeys schemaEvolutionKeys = schemaEvolutionKeys(table);
        rejectPrimaryKeyChange(
                "drop not null constraint",
                "change nullability of",
                paimonColumnHandle,
                schemaEvolutionKeys);

        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.updateColumnNullability(columnName, true));

        try {
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void addField(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<String> parentPath,
            String fieldName,
            Type type,
            boolean ignoreExisting)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("add field", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "add field");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());

        // Build field path: parentPath + fieldName
        String[] fieldNames = buildFieldNamesArray(parentPath, fieldName);
        rejectPaimonSystemRootField("add field", fieldNames[0]);

        // Convert Trino Type to Paimon DataType
        DataType paimonType = toPaimonType(type);

        List<SchemaChange> changes = new ArrayList<>();

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            Optional<FileStoreTable> table = tryLatestWriteFileStoreTable(
                    paimonTableHandle,
                    sessionCatalog,
                    "nested field schema change");
            if (table.isPresent()) {
                if (fieldNames.length > 1) {
                    fieldNames = canonicalNestedFieldNames(table.get(), paimonTableHandle, fieldNames, false);
                    validateNoCaseInsensitiveDuplicateNestedFieldName(
                            table.get(),
                            paimonTableHandle,
                            fieldNames,
                            fieldName,
                            Optional.empty());
                }
                else {
                    validateNoCaseInsensitiveDuplicateColumnName(
                            table.get(),
                            schemaTableName(paimonTableHandle),
                            fieldName,
                            Optional.empty());
                }
            }
            changes.add(SchemaChange.addColumn(fieldNames, paimonType, null, null));
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            if (ignoreExisting && isColumnAlreadyExistsException(e)) {
                return;
            }
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void dropField(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle column,
            List<String> fieldPath)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("drop field", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "drop field");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = getColumnHandle("drop field", column);
        rejectPaimonSystemColumn(paimonColumnHandle, "drop field");
        validateRelativeFieldPath("drop field", fieldPath);

        // Build full field path: columnName + fieldPath
        String[] fieldNames = buildFieldNamesArray(List.of(paimonColumnHandle.getColumnName()), fieldPath);

        List<SchemaChange> changes = new ArrayList<>();

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            FileStoreTable table = latestWriteFileStoreTable(
                    paimonTableHandle,
                    sessionCatalog,
                    "nested field schema change");
            fieldNames = canonicalNestedFieldNames(table, paimonTableHandle, fieldNames, true);
            rejectDropAllFields(parentRowFieldCount(table, paimonTableHandle, fieldNames), "drop field");
            changes.add(SchemaChange.dropColumn(fieldNames));
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void renameField(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<String> fieldPath,
            String target)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("rename field", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "rename field");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        validateAbsoluteFieldPath("rename field", fieldPath);
        rejectPaimonSystemRootField("rename field", fieldPath.get(0));
        validateFieldName("target", target);

        // fieldPath includes column name and nested path
        String[] fieldNames = fieldPath.toArray(new String[0]);

        List<SchemaChange> changes = new ArrayList<>();

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            FileStoreTable table = latestWriteFileStoreTable(
                    paimonTableHandle,
                    sessionCatalog,
                    "nested field schema change");
            fieldNames = canonicalNestedFieldNames(table, paimonTableHandle, fieldNames, true);
            if (fieldNames.length == 1) {
                validateNoCaseInsensitiveDuplicateColumnName(
                        table,
                        schemaTableName(paimonTableHandle),
                        target,
                        Optional.of(fieldNames[0]));
            }
            else {
                validateNoCaseInsensitiveDuplicateNestedFieldName(
                        table,
                        paimonTableHandle,
                        fieldNames,
                        target,
                        Optional.of(fieldNames[fieldNames.length - 1]));
            }
            changes.add(SchemaChange.renameColumn(fieldNames, target));
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    @Override
    public void setFieldType(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<String> fieldPath,
            Type type)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("set field type", tableHandle);
        rejectSystemTableWrite(paimonTableHandle, "set field type");
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        validateAbsoluteFieldPath("set field type", fieldPath);
        rejectPaimonSystemRootField("set field type", fieldPath.get(0));

        // fieldPath includes column name and nested path
        String[] fieldNames = fieldPath.toArray(new String[0]);

        // Convert Trino Type to Paimon DataType
        DataType paimonType = toPaimonType(type);

        List<SchemaChange> changes = new ArrayList<>();

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            FileStoreTable table = latestWriteFileStoreTable(
                    paimonTableHandle,
                    sessionCatalog,
                    "nested field schema change");
            fieldNames = canonicalNestedFieldNames(table, paimonTableHandle, fieldNames, true);
            changes.add(SchemaChange.updateColumnType(fieldNames, paimonType, true));
            sessionCatalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw paimonAlterTableException(schemaTableName(paimonTableHandle), e);
        }
    }

    private static void rejectBlobColumnRename(DataField field)
    {
        requireNonNull(field, "field is null");
        if (field.type().is(DataTypeRoot.BLOB)) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon rename column is not supported: Cannot rename BLOB column: [" + field.name() + "]");
        }
    }

    private static void rejectBlobColumnTypeChange(DataField field, DataType newType)
    {
        requireNonNull(field, "field is null");
        requireNonNull(newType, "newType is null");
        if (field.type().is(DataTypeRoot.BLOB) || newType.is(DataTypeRoot.BLOB)) {
            throw new TrinoException(NOT_SUPPORTED, format(
                    "Paimon set column type is not supported: Cannot change column type involving BLOB: [%s] %s -> %s",
                    field.name(),
                    field.type(),
                    newType));
        }
    }

    private static PaimonSchemaEvolutionKeys schemaEvolutionKeys(FileStoreTable table)
    {
        requireNonNull(table, "table is null");
        return new PaimonSchemaEvolutionKeys(table.partitionKeys(), table.primaryKeys());
    }

    private static String[] canonicalNestedFieldNames(
            FileStoreTable table,
            PaimonTableHandle tableHandle,
            String[] fieldNames,
            boolean includeLeaf)
            throws Catalog.ColumnNotExistException
    {
        requireNonNull(table, "table is null");
        String[] canonicalFieldNames = fieldNames.clone();
        DataType currentType = table.rowType();
        int limit = includeLeaf ? canonicalFieldNames.length : canonicalFieldNames.length - 1;
        boolean lastSegmentWasCollectionMarker = false;
        for (int index = 0; index < limit; index++) {
            switch (currentType.getTypeRoot()) {
                case ROW -> {
                    DataField field = canonicalNestedField(tableHandle, currentType, canonicalFieldNames, index);
                    canonicalFieldNames[index] = field.name();
                    currentType = field.type();
                    lastSegmentWasCollectionMarker = false;
                }
                case ARRAY -> {
                    canonicalFieldNames[index] = canonicalNestedCollectionField(
                            canonicalFieldNames,
                            index,
                            "element",
                            "array element");
                    currentType = ((ArrayType) currentType).getElementType();
                    lastSegmentWasCollectionMarker = true;
                }
                case MAP -> {
                    canonicalFieldNames[index] = canonicalNestedCollectionField(
                            canonicalFieldNames,
                            index,
                            "value",
                            "map value");
                    currentType = ((MapType) currentType).getValueType();
                    lastSegmentWasCollectionMarker = true;
                }
                default -> throw unsupportedNestedFieldPath(canonicalFieldNames);
            }
        }
        if (includeLeaf && lastSegmentWasCollectionMarker) {
            throw new TrinoException(NOT_SUPPORTED, format(
                    "Paimon nested field schema change must target a row field, not collection marker '%s' in field path '%s'",
                    canonicalFieldNames[canonicalFieldNames.length - 1],
                    String.join(".", canonicalFieldNames)));
        }
        return canonicalFieldNames;
    }

    private static int parentRowFieldCount(
            FileStoreTable table,
            PaimonTableHandle tableHandle,
            String[] fieldNames)
            throws Catalog.ColumnNotExistException
    {
        requireNonNull(table, "table is null");
        requireNonNull(fieldNames, "fieldNames is null");
        DataType currentType = table.rowType();
        for (int index = 0; index < fieldNames.length - 1; index++) {
            currentType = nextNestedType(tableHandle, currentType, fieldNames, index);
        }
        if (!(currentType instanceof RowType rowType)) {
            throw unsupportedNestedFieldPath(fieldNames);
        }
        return rowType.getFields().size();
    }

    private static void validateNoCaseInsensitiveDuplicateNestedFieldName(
            FileStoreTable table,
            PaimonTableHandle tableHandle,
            String[] fieldNames,
            String fieldName,
            Optional<String> existingFieldName)
            throws Catalog.ColumnNotExistException
    {
        requireNonNull(table, "table is null");
        requireNonNull(fieldNames, "fieldNames is null");
        if (fieldNames.length < 2) {
            throw new IllegalArgumentException("fieldNames must contain a parent path and field name");
        }

        DataType currentType = table.rowType();
        for (int index = 0; index < fieldNames.length - 1; index++) {
            currentType = nextNestedType(tableHandle, currentType, fieldNames, index);
        }
        if (!(currentType instanceof RowType rowType)) {
            throw unsupportedNestedFieldPath(fieldNames);
        }
        validateNoCaseInsensitiveDuplicateFieldName(
                rowType.getFields(),
                String.join(".", fieldNames),
                fieldName,
                existingFieldName);
    }

    private static DataType nextNestedType(
            PaimonTableHandle tableHandle,
            DataType currentType,
            String[] fieldNames,
            int index)
            throws Catalog.ColumnNotExistException
    {
        return switch (currentType.getTypeRoot()) {
            case ROW -> canonicalNestedField(tableHandle, currentType, fieldNames, index).type();
            case ARRAY -> {
                canonicalNestedCollectionField(fieldNames, index, "element", "array element");
                yield ((ArrayType) currentType).getElementType();
            }
            case MAP -> {
                canonicalNestedCollectionField(fieldNames, index, "value", "map value");
                yield ((MapType) currentType).getValueType();
            }
            default -> throw unsupportedNestedFieldPath(fieldNames);
        };
    }

    private static DataField canonicalNestedField(
            PaimonTableHandle tableHandle,
            DataType currentType,
            String[] fieldNames,
            int index)
            throws Catalog.ColumnNotExistException
    {
        if (!(currentType instanceof RowType rowType)) {
            throw unsupportedNestedFieldPath(fieldNames);
        }

        String requestedFieldName = fieldNames[index];
        DataField matchedField = null;
        for (DataField field : rowType.getFields()) {
            if (field.name().equalsIgnoreCase(requestedFieldName)) {
                if (matchedField != null) {
                    throw new TrinoException(NOT_SUPPORTED,
                            "Paimon nested field schema change is ambiguous for field path '"
                                    + String.join(".", fieldNames) + "'");
                }
                matchedField = field;
            }
        }
        if (matchedField == null) {
            throw new Catalog.ColumnNotExistException(
                    new Identifier(tableHandle.getSchemaName(), tableHandle.getTableName()),
                    String.join(".", Arrays.asList(fieldNames).subList(0, index + 1)));
        }
        return matchedField;
    }

    private static String canonicalNestedCollectionField(
            String[] fieldNames,
            int index,
            String expectedName,
            String fieldDescription)
    {
        String requestedFieldName = fieldNames[index];
        if (expectedName.equalsIgnoreCase(requestedFieldName)) {
            return expectedName;
        }
        throw new TrinoException(NOT_SUPPORTED, format(
                "Paimon nested field schema change for %s must use '%s' in field path '%s'",
                fieldDescription,
                expectedName,
                String.join(".", fieldNames)));
    }

    private static TrinoException unsupportedNestedFieldPath(String[] fieldNames)
    {
        return new TrinoException(NOT_SUPPORTED,
                "Paimon nested field schema change is not supported for non-row field path '"
                        + String.join(".", fieldNames) + "'");
    }

    private static void rejectPartitionOrPrimaryKeyDrop(
            PaimonColumnHandle columnHandle,
            PaimonSchemaEvolutionKeys schemaEvolutionKeys)
    {
        if (schemaEvolutionKeys.isPartitionKey(columnHandle) || schemaEvolutionKeys.isPrimaryKey(columnHandle)) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Cannot drop partition key or primary key: [" + columnHandle.getColumnName() + "]");
        }
    }

    private static void rejectDropAllFields(int fieldCount, String operation)
    {
        if (fieldCount <= 1) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon " + operation + " is not supported: Cannot drop all fields in table");
        }
    }

    private static void rejectPartitionKeyChange(
            String trinoOperation,
            String paimonOperation,
            PaimonColumnHandle columnHandle,
            PaimonSchemaEvolutionKeys schemaEvolutionKeys)
    {
        if (schemaEvolutionKeys.isPartitionKey(columnHandle)) {
            throw new TrinoException(NOT_SUPPORTED,
                    "Paimon " + trinoOperation + " is not supported: Cannot " + paimonOperation
                            + " partition column: [" + columnHandle.getColumnName() + "]");
        }
    }

    private static void rejectPrimaryKeyChange(
            String trinoOperation,
            String paimonOperation,
            PaimonColumnHandle columnHandle,
            PaimonSchemaEvolutionKeys schemaEvolutionKeys)
    {
        if (schemaEvolutionKeys.isPrimaryKey(columnHandle)) {
            throw new TrinoException(NOT_SUPPORTED,
                    "Paimon " + trinoOperation + " is not supported: Cannot " + paimonOperation
                            + " primary key");
        }
    }

    private record PaimonSchemaEvolutionKeys(Set<String> partitionKeys, Set<String> primaryKeys)
    {
        private PaimonSchemaEvolutionKeys(List<String> partitionKeys, List<String> primaryKeys)
        {
            this(FieldNameUtils.toLowerCase(partitionKeys).stream().collect(Collectors.toUnmodifiableSet()),
                    FieldNameUtils.toLowerCase(primaryKeys).stream().collect(Collectors.toUnmodifiableSet()));
        }

        private boolean isPartitionKey(PaimonColumnHandle columnHandle)
        {
            return partitionKeys.contains(FieldNameUtils.toLowerCase(columnHandle.getColumnName()));
        }

        private boolean isPrimaryKey(PaimonColumnHandle columnHandle)
        {
            return primaryKeys.contains(FieldNameUtils.toLowerCase(columnHandle.getColumnName()));
        }
    }

    /**
     * Helper method to build field names array from parent path and field name.
     * Used for nested field operations.
     */
    private String[] buildFieldNamesArray(List<String> parentPath, String fieldName)
    {
        requireNonNull(parentPath, "parentPath is null");
        parentPath.forEach(field -> validateFieldName("parentPath", field));
        validateFieldName("fieldName", fieldName);
        List<String> fullPath = new ArrayList<>(parentPath);
        fullPath.add(fieldName);
        return fullPath.toArray(new String[0]);
    }

    /**
     * Helper method to build field names array from column name and field path.
     * Used for nested field operations where we have a column handle and a nested
     * path.
     */
    private String[] buildFieldNamesArray(List<String> columnList, List<String> fieldPath)
    {
        requireNonNull(columnList, "columnList is null");
        requireNonNull(fieldPath, "fieldPath is null");
        columnList.forEach(field -> validateFieldName("columnList", field));
        List<String> fullPath = new ArrayList<>(columnList);
        fullPath.addAll(fieldPath);
        return fullPath.toArray(new String[0]);
    }

    private static void validateRelativeFieldPath(String operation, List<String> fieldPath)
    {
        requireNonNull(fieldPath, operation + " fieldPath is null");
        checkArgument(!fieldPath.isEmpty(), operation + " fieldPath is empty");
        fieldPath.forEach(field -> validateFieldName(operation + " fieldPath", field));
    }

    private static void validateAbsoluteFieldPath(String operation, List<String> fieldPath)
    {
        requireNonNull(fieldPath, operation + " fieldPath is null");
        checkArgument(fieldPath.size() >= 2, operation + " fieldPath must include a column name and nested field");
        fieldPath.forEach(field -> validateFieldName(operation + " fieldPath", field));
    }

    private static void validateFieldName(String label, String fieldName)
    {
        requireNonNull(fieldName, label + " contains null field");
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(fieldName), label + " contains blank field");
    }

    private static void rejectPaimonSystemColumn(PaimonColumnHandle columnHandle, String operation)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        if (PaimonColumnHandle.isPaimonSystemColumnName(columnHandle.getColumnName())) {
            throw new TrinoException(NOT_SUPPORTED,
                    "Paimon " + operation + " is not supported for system column '"
                            + columnHandle.getColumnName() + "'");
        }
    }

    private static void rejectPaimonSystemColumnName(String operation, String columnName)
    {
        requireNonNull(columnName, "columnName is null");
        if (PaimonColumnHandle.isPaimonSystemColumnName(columnName)) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon " + operation + " is not supported for system column '" + columnName + "'");
        }
    }

    private static void rejectPaimonSystemRootField(String operation, String rootField)
    {
        requireNonNull(rootField, "rootField is null");
        if (PaimonColumnHandle.isPaimonSystemColumnName(rootField)) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon " + operation + " is not supported for system column '" + rootField + "'");
        }
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("truncate table", tableHandle);
        truncatePaimonTable(session, paimonTableHandle, "truncate table", "truncate");
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("delete", handle);
        rejectSystemTableWrite(paimonTableHandle, "delete");

        Catalog sessionCatalog = catalog.forSession(session);
        FileStoreTable fileStoreTable = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, "delete");
        if (isSafeFullTableDeleteHandle(paimonTableHandle)) {
            return Optional.of(paimonTableHandle);
        }
        return partitionDeleteSpecs(paimonTableHandle, fileStoreTable)
                .map(paimonTableHandle::withDeletePartitionSpecs)
                .map(ConnectorTableHandle.class::cast);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("delete", handle);
        rejectSystemTableWrite(paimonTableHandle, "delete");
        if (!paimonTableHandle.getFilter().isAll() && paimonTableHandle.getDeletePartitionSpecs().isEmpty()) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon delete requires an unfiltered table handle or a validated partition delete handle");
        }
        truncatePaimonTable(
                session,
                paimonTableHandle,
                "delete",
                "delete rows from",
                paimonTableHandle.getDeletePartitionSpecs());
        return OptionalLong.empty();
    }

    private static List<Map<String, String>> validatedDeletePartitionSpecs(
            PaimonTableHandle tableHandle,
            FileStoreTable fileStoreTable,
            List<Map<String, String>> deletePartitionSpecs)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(fileStoreTable, "fileStoreTable is null");
        requireNonNull(deletePartitionSpecs, "deletePartitionSpecs is null");
        if (deletePartitionSpecs.isEmpty() || deletePartitionSpecs.size() > MAX_PARTITION_DELETE_SPECS) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon partition delete requires between 1 and " + MAX_PARTITION_DELETE_SPECS + " partition specs");
        }
        if (fileStoreTable.partitionKeys().isEmpty()) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon partition delete requires a partitioned table");
        }

        RowType partitionType = partitionType(fileStoreTable);
        InternalRowPartitionComputer partitionComputer = partitionComputer(fileStoreTable, partitionType);
        Set<String> partitionKeys = new LinkedHashSet<>(fileStoreTable.partitionKeys());
        List<Map<String, String>> normalizedSpecs = new ArrayList<>(deletePartitionSpecs.size());
        for (Map<String, String> partitionSpec : deletePartitionSpecs) {
            if (!partitionSpec.keySet().equals(partitionKeys)) {
                throw new TrinoException(
                        NOT_SUPPORTED,
                        "Paimon partition delete requires complete partition specs for keys: " + partitionKeys);
            }
            try {
                GenericRow partitionRow = InternalRowPartitionComputer.convertSpecToInternalRow(
                        partitionSpec,
                        partitionType,
                        fileStoreTable.coreOptions().partitionDefaultName());
                normalizedSpecs.add(partitionComputer.generatePartValues(partitionRow));
            }
            catch (RuntimeException e) {
                throw new TrinoException(
                        NOT_SUPPORTED,
                        "Paimon partition delete requires valid Paimon partition values",
                        e);
            }
        }
        List<Map<String, String>> normalizedDeletePartitionSpecs = List.copyOf(normalizedSpecs);
        Optional<List<Map<String, String>>> expectedDeletePartitionSpecs = partitionDeleteSpecs(tableHandle, fileStoreTable);
        Set<Map<String, String>> actualPartitions = new LinkedHashSet<>(normalizedDeletePartitionSpecs);
        if (expectedDeletePartitionSpecs.isEmpty()
                || !actualPartitions.equals(new LinkedHashSet<>(expectedDeletePartitionSpecs.orElseThrow()))) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Paimon delete requires partition delete specs to match the table handle filter");
        }
        return List.copyOf(actualPartitions);
    }

    private static boolean isSafeFullTableDeleteHandle(PaimonTableHandle tableHandle)
    {
        return tableHandle.getFilter().isAll()
                && tableHandle.getLimit().isEmpty()
                && tableHandle.getDeletePartitionSpecs().isEmpty();
    }

    private static Optional<List<Map<String, String>>> partitionDeleteSpecs(
            PaimonTableHandle tableHandle,
            FileStoreTable fileStoreTable)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(fileStoreTable, "fileStoreTable is null");
        if (tableHandle.getLimit().isPresent()
                || tableHandle.getFilter().isNone() || fileStoreTable.partitionKeys().isEmpty()) {
            return Optional.empty();
        }

        Optional<Map<PaimonColumnHandle, Domain>> domains = tableHandle.getFilter().getDomains();
        if (domains.isEmpty() || domains.get().size() != fileStoreTable.partitionKeys().size()) {
            return Optional.empty();
        }

        Map<String, Domain> domainsByName = new HashMap<>();
        Map<String, PaimonColumnHandle> columnsByName = new HashMap<>();
        for (Map.Entry<PaimonColumnHandle, Domain> entry : domains.get().entrySet()) {
            String columnName = FieldNameUtils.toLowerCase(entry.getKey().getColumnName());
            domainsByName.put(columnName, entry.getValue());
            columnsByName.put(columnName, entry.getKey());
        }

        RowType partitionType = partitionType(fileStoreTable);
        InternalRowPartitionComputer partitionComputer = partitionComputer(fileStoreTable, partitionType);
        List<List<Object>> partitionValueRows = List.of(List.of());
        for (String partitionKey : fileStoreTable.partitionKeys()) {
            String lowerPartitionKey = FieldNameUtils.toLowerCase(partitionKey);
            Domain domain = domainsByName.get(lowerPartitionKey);
            PaimonColumnHandle columnHandle = columnsByName.get(lowerPartitionKey);
            if (domain == null || columnHandle == null || !domain.isNullableDiscreteSet()) {
                return Optional.empty();
            }
            Optional<List<Object>> partitionValues = partitionValues(columnHandle, domain);
            if (partitionValues.isEmpty()) {
                return Optional.empty();
            }
            if (partitionValues.get().isEmpty()
                    || partitionValueRows.size() > MAX_PARTITION_DELETE_SPECS / partitionValues.get().size()) {
                return Optional.empty();
            }
            partitionValueRows = appendPartitionValues(partitionValueRows, partitionValues.get());
        }

        return Optional.of(partitionValueRows.stream()
                .map(values -> partitionComputer.generatePartValues(GenericRow.of(values.toArray())))
                .collect(toList()));
    }

    private static RowType partitionType(FileStoreTable fileStoreTable)
    {
        requireNonNull(fileStoreTable, "fileStoreTable is null");
        return new RowType(fileStoreTable.partitionKeys().stream()
                .map(partitionKey -> fileStoreTable.rowType().getField(partitionKey))
                .collect(toList()));
    }

    private static InternalRowPartitionComputer partitionComputer(FileStoreTable fileStoreTable, RowType partitionType)
    {
        requireNonNull(fileStoreTable, "fileStoreTable is null");
        requireNonNull(partitionType, "partitionType is null");
        return new InternalRowPartitionComputer(
                fileStoreTable.coreOptions().partitionDefaultName(),
                partitionType,
                fileStoreTable.partitionKeys().toArray(new String[0]),
                fileStoreTable.coreOptions().legacyPartitionName());
    }

    private static Optional<List<Object>> partitionValues(PaimonColumnHandle columnHandle, Domain domain)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        requireNonNull(domain, "domain is null");
        List<Object> values = new ArrayList<>();
        Domain.DiscreteSet discreteSet = domain.getNullableDiscreteSet();
        for (Object value : discreteSet.getNonNullValues()) {
            Optional<Object> partitionValue = partitionValue(columnHandle, value);
            if (partitionValue.isEmpty()) {
                return Optional.empty();
            }
            values.add(partitionValue.get());
        }
        if (discreteSet.containsNull()) {
            values.add(null);
        }
        return Optional.of(Collections.unmodifiableList(new ArrayList<>(values)));
    }

    private static List<List<Object>> appendPartitionValues(
            List<List<Object>> partitionValueRows,
            List<Object> partitionValues)
    {
        requireNonNull(partitionValueRows, "partitionValueRows is null");
        requireNonNull(partitionValues, "partitionValues is null");
        List<List<Object>> result = new ArrayList<>(partitionValueRows.size() * partitionValues.size());
        for (List<Object> partitionValueRow : partitionValueRows) {
            for (Object partitionValue : partitionValues) {
                List<Object> newPartitionValueRow = new ArrayList<>(partitionValueRow);
                newPartitionValueRow.add(partitionValue);
                result.add(Collections.unmodifiableList(newPartitionValueRow));
            }
        }
        return List.copyOf(result);
    }

    private static Optional<Object> partitionValue(PaimonColumnHandle columnHandle, Object value)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        requireNonNull(value, "value is null");
        try {
            return Optional.of(PaimonFilterConverter.getLiteralValue(columnHandle.getTrinoType(), value));
        }
        catch (UnsupportedOperationException | ClassCastException | ArithmeticException | IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    private void truncatePaimonTable(
            ConnectorSession session,
            PaimonTableHandle paimonTableHandle,
            String operation,
            String failureOperation)
    {
        truncatePaimonTable(session, paimonTableHandle, operation, failureOperation, Optional.empty());
    }

    private void truncatePaimonTable(
            ConnectorSession session,
            PaimonTableHandle paimonTableHandle,
            String operation,
            String failureOperation,
            Optional<List<Map<String, String>>> deletePartitionSpecs)
    {
        rejectSystemTableWrite(paimonTableHandle, operation);

        boolean keyDynamic = false;
        try {
            Catalog sessionCatalog = catalog.forSession(session);
            FileStoreTable fileStoreTable = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, operation);
            keyDynamic = fileStoreTable.bucketMode() == BucketMode.KEY_DYNAMIC;
            if (keyDynamic) {
                keyDynamicWriteCoordinator.acquire(session.getQueryId(), fileStoreTable.name());
                // Refresh after taking the local slot so the bootstrap snapshot and commit use one schema view.
                fileStoreTable = latestWriteFileStoreTable(paimonTableHandle, sessionCatalog, operation);
                PaimonKeyDynamicBootstrap.validateAtomicCommitCapability(fileStoreTable);
            }
            FileStoreTable operationTable = fileStoreTable;
            Optional<List<Map<String, String>>> validatedDeletePartitionSpecs = deletePartitionSpecs
                    .map(specs -> validatedDeletePartitionSpecs(paimonTableHandle, operationTable, specs));
            truncatePaimonTable(
                    operationTable,
                    paimonTableHandle,
                    operation,
                    failureOperation,
                    validatedDeletePartitionSpecs,
                    keyDynamic ? keyDynamicTruncateValidator(session, operationTable) : null);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (UnsupportedOperationException e) {
            String detail = e.getMessage();
            throw new TrinoException(
                    NOT_SUPPORTED,
                    detail == null || detail.isBlank()
                            ? "Paimon " + operation + " uses features which are not supported by the Trino connector"
                            : "Paimon " + operation + " uses features which are not supported by the Trino connector: " + detail,
                    e);
        }
        catch (Exception e) {
            throw paimonMetadataException(
                    format("Failed to %s Paimon table '%s'", failureOperation, paimonTableHandle.getTableName()),
                    e);
        }
        finally {
            if (keyDynamic) {
                keyDynamicWriteCoordinator.releaseQuery(session.getQueryId());
            }
        }
    }

    private Runnable keyDynamicTruncateValidator(
            ConnectorSession session,
            FileStoreTable fileStoreTable)
    {
        requireNonNull(session, "session is null");
        requireNonNull(fileStoreTable, "fileStoreTable is null");
        int assignerParallelism = dynamicBucketAssignerParallelism(fileStoreTable)
                .orElseThrow(() -> new IllegalStateException(
                        "Paimon KEY_DYNAMIC truncate is missing assigner parallelism"));
        PaimonKeyDynamicBootstrap.OptionalSnapshot expectedSnapshot =
                PaimonKeyDynamicBootstrap.OptionalSnapshot.pinned(
                        PaimonKeyDynamicBootstrap.latestSnapshot(fileStoreTable));
        return () -> {
            try {
                PaimonKeyDynamicBootstrap.validateSnapshotForAtomicCommit(
                        fileStoreTable,
                        session.getQueryId(),
                        expectedSnapshot,
                        assignerParallelism,
                        fileStoreTable.store().snapshotManager().latestSnapshot(),
                        true);
            }
            catch (RuntimeException e) {
                throw e;
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static void truncatePaimonTable(
            FileStoreTable fileStoreTable,
            PaimonTableHandle paimonTableHandle,
            String operation,
            String failureOperation,
            Optional<List<Map<String, String>>> deletePartitionSpecs,
            @Nullable Runnable preCommitValidation)
    {
        try {
            if (preCommitValidation != null) {
                preCommitValidation.run();
            }
            // Use BatchTableCommit to truncate the table
            try (BatchTableCommit commit = fileStoreTable.newBatchWriteBuilder().newCommit()) {
                if (deletePartitionSpecs.isPresent()) {
                    commit.truncatePartitions(deletePartitionSpecs.get());
                }
                else {
                    commit.truncateTable();
                }
            }
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (UnsupportedOperationException e) {
            String detail = e.getMessage();
            throw new TrinoException(
                    NOT_SUPPORTED,
                    detail == null || detail.isBlank()
                            ? "Paimon " + operation + " uses features which are not supported by the Trino connector"
                            : "Paimon " + operation + " uses features which are not supported by the Trino connector: " + detail,
                    e);
        }
        catch (Exception e) {
            throw paimonMetadataException(
                    format("Failed to %s Paimon table '%s'", failureOperation, paimonTableHandle.getTableName()),
                    e);
        }
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint constraint)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("filter pushdown", handle);
        requireNonNull(constraint, "constraint is null");
        validateFilterColumns(constraint);
        if (paimonTableHandle.getFilter().isNone()) {
            return Optional.empty();
        }
        if (constraint.getSummary().isNone()) {
            return Optional.of(new ConstraintApplicationResult<>(
                    paimonTableHandle.copy(TupleDomain.none()),
                    TupleDomain.all(),
                    TRUE,
                    false));
        }
        if (constraint.getSummary().isAll() && constraint.getExpression().equals(TRUE)) {
            return Optional.empty();
        }
        Catalog sessionCatalog = catalog.forSession(session);
        Optional<PaimonFilterExtractor.TrinoFilter> extract = PaimonFilterExtractor.extract(
                sessionCatalog,
                paimonTableHandle,
                session,
                constraint);
        if (extract.isPresent()) {
            PaimonFilterExtractor.TrinoFilter trinoFilter = extract.get();
            if (paimonTableHandle.getLimit().isPresent()
                    && !canApplyFilterAfterLimit(trinoFilter)) {
                return Optional.empty();
            }
            return Optional.of(new ConstraintApplicationResult<>(
                    paimonTableHandle.copy(trinoFilter.filter()),
                    trinoFilter.remainFilter(),
                    trinoFilter.remainingExpression(),
                    false));
        }
        else {
            return Optional.empty();
        }
    }

    private static boolean canApplyFilterAfterLimit(PaimonFilterExtractor.TrinoFilter trinoFilter)
    {
        if (trinoFilter.filter().isNone()) {
            return true;
        }
        return trinoFilter.partitionOnlyPushedFilter()
                && trinoFilter.remainFilter().isAll()
                && trinoFilter.remainingExpression().equals(TRUE);
    }

    private static void validateFilterColumns(Constraint constraint)
    {
        constraint.getSummary().transformKeys(column -> getColumnHandle("filter pushdown", column));
        constraint.getAssignments().values().forEach(column -> getColumnHandle("filter pushdown", column));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle paimonTableHandle = getTableHandle("projection pushdown", handle);
        requireNonNull(projections, "projections is null");
        requireNonNull(assignments, "assignments is null");
        assignments.forEach((name, column) -> {
            requireNonNull(name, "assignments contains null variable");
            getColumnHandle("projection pushdown", column);
        });
        LinkedHashMap<String, PaimonColumnHandle> projectedAssignments = projectedAssignments(
                projections,
                assignments);
        if (projectedAssignments.isEmpty()) {
            return Optional.empty();
        }

        List<ColumnHandle> newColumns = new ArrayList<>(projectedAssignments.values());

        if (paimonTableHandle.getProjectedColumns().isPresent()
                && newColumns.equals(paimonTableHandle.getProjectedColumns().get())) {
            return Optional.empty();
        }

        List<Assignment> assignmentList = new ArrayList<>();
        projectedAssignments.forEach((name, column) -> assignmentList
                .add(new Assignment(name, column, column.getTrinoType())));

        return Optional.of(new ProjectionApplicationResult<>(
                paimonTableHandle.copy(Optional.of(newColumns)),
                projections,
                assignmentList,
                false));
    }

    private static LinkedHashMap<String, PaimonColumnHandle> projectedAssignments(
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        LinkedHashMap<String, PaimonColumnHandle> projectedAssignments = new LinkedHashMap<>();
        projections.forEach(projection -> collectProjectionVariables(projection, assignments, projectedAssignments));
        return projectedAssignments;
    }

    private static void collectProjectionVariables(
            ConnectorExpression projection,
            Map<String, ColumnHandle> assignments,
            LinkedHashMap<String, PaimonColumnHandle> projectedAssignments)
    {
        requireNonNull(projection, "projections contains null expression");
        if (projection instanceof Variable variable) {
            if (!assignments.containsKey(variable.getName())) {
                throw new IllegalStateException("Paimon projection pushdown assignments missing variable: "
                        + variable.getName());
            }
            projectedAssignments.putIfAbsent(
                    variable.getName(),
                    getColumnHandle("projection pushdown", assignments.get(variable.getName())));
            return;
        }
        projection.getChildren().forEach(child -> collectProjectionVariables(child, assignments, projectedAssignments));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle handle,
            long limit)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle table = getTableHandle("limit pushdown", handle);
        checkArgument(limit >= 0, "limit must be non-negative");

        if (table.getLimit().isPresent() && table.getLimit().orElseThrow() <= limit) {
            return Optional.empty();
        }

        if (table.getFilter().isNone()) {
            return Optional.of(new LimitApplicationResult<>(table.copy(OptionalLong.of(limit)), false, false));
        }

        if (!table.getFilter().isAll()) {
            Catalog sessionCatalog = catalog.forSession(session);
            Table paimonTable = PaimonTableHandle.schemaAwareReadTable(
                    table.tableWithDynamicOptions(sessionCatalog, session),
                    !table.usesHistoricalReadSchema(session));
            HashMap<PaimonColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
            HashMap<PaimonColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();
            new PaimonFilterConverter(PaimonTableHandle.effectiveReadRowType(paimonTable)).convert(
                    table.getFilter(), acceptedDomains, unsupportedDomains);
            Set<String> acceptedFields = acceptedDomains.keySet().stream()
                    .map(PaimonColumnHandle::getColumnName)
                    .map(FieldNameUtils::toLowerCase)
                    .collect(Collectors.toSet());
            Set<String> partitionKeys = paimonTable.partitionKeys().stream()
                    .map(FieldNameUtils::toLowerCase)
                    .collect(Collectors.toSet());
            if (!unsupportedDomains.isEmpty()
                    || !partitionKeys.containsAll(acceptedFields)) {
                return Optional.empty();
            }
        }

        table = table.copy(OptionalLong.of(limit));

        return Optional.of(new LimitApplicationResult<>(table, false, false));
    }

    @Override
    public void createView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorViewDefinition definition,
            Map<String, Object> viewProperties,
            boolean replace)
    {
        requireNonNull(session, "session is null");
        requireNonNull(viewName, "viewName is null");
        requireNonNull(definition, "definition is null");
        rejectSystemSchemaWrite(viewName.getSchemaName(), "create view");
        Identifier identifier = new Identifier(viewName.getSchemaName(), viewName.getTableName());
        View paimonView = toPaimonView(identifier, definition);

        try {
            Catalog sessionCatalog = catalog.forSession(session);
            if (replace) {
                Optional<View> existingView = existingView(sessionCatalog, identifier);
                sessionCatalog.dropView(identifier, true);
                try {
                    sessionCatalog.createView(identifier, paimonView, false);
                }
                catch (Exception e) {
                    restoreReplacedView(sessionCatalog, identifier, existingView, e);
                    throw e;
                }
            }
            else {
                sessionCatalog.createView(identifier, paimonView, false);
            }
        }
        catch (Catalog.ViewAlreadyExistException e) {
            throw new TrinoException(
                    ALREADY_EXISTS,
                    format("View '%s' already exists", viewName));
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(
                    SCHEMA_NOT_FOUND,
                    format("Schema '%s' does not exist", viewName.getSchemaName()));
        }
        catch (UnsupportedOperationException e) {
            throw unsupportedViewOperation("create", e);
        }
        catch (Exception e) {
            throw paimonViewException(format("Failed to create view '%s'", viewName), e);
        }
    }

    private static Optional<View> existingView(Catalog catalog, Identifier identifier)
    {
        try {
            return Optional.of(catalog.getView(identifier));
        }
        catch (Catalog.ViewNotExistException e) {
            return Optional.empty();
        }
    }

    private static void restoreReplacedView(
            Catalog catalog,
            Identifier identifier,
            Optional<View> existingView,
            Exception failure)
    {
        if (existingView.isEmpty()) {
            return;
        }
        try {
            catalog.createView(identifier, existingView.get(), true);
        }
        catch (Exception restoreFailure) {
            failure.addSuppressed(restoreFailure);
            addSuppressedToCauses(failure, restoreFailure);
        }
    }

    private static void addSuppressedToCauses(Exception exception, Exception suppressed)
    {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = exception.getCause();
        while (current != null && visited.add(current)) {
            current.addSuppressed(suppressed);
            current = current.getCause();
        }
    }

    private View toPaimonView(Identifier identifier, ConnectorViewDefinition definition)
    {
        List<DataField> fields = IntStream.range(0, definition.getColumns().size())
                .mapToObj(index -> {
                    ConnectorViewDefinition.ViewColumn column = definition.getColumns().get(index);
                    return new DataField(
                            index,
                            column.getName(),
                            toPaimonType(typeManager.getType(column.getType())),
                            column.getComment().orElse(null));
                })
                .collect(toList());

        Map<String, String> dialects = new HashMap<>();
        dialects.put("trino", definition.getOriginalSql());

        Map<String, String> options = new HashMap<>();
        definition.getComment().ifPresent(c -> options.put("comment", c));
        definition.getOwner().ifPresent(owner -> options.put(OWNER_PROPERTY, owner));

        return new ViewImpl(
                identifier,
                fields,
                definition.getOriginalSql(),
                dialects,
                definition.getComment().orElse(null),
                options);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(viewName, "viewName is null");
        rejectSystemSchemaWrite(viewName.getSchemaName(), "drop view");
        Catalog sessionCatalog = catalog.forSession(session);
        Identifier identifier = new Identifier(viewName.getSchemaName(), viewName.getTableName());

        try {
            sessionCatalog.dropView(identifier, false);
        }
        catch (Catalog.ViewNotExistException e) {
            throw new TrinoException(
                    TABLE_NOT_FOUND,
                    format("View '%s' does not exist", viewName));
        }
        catch (UnsupportedOperationException e) {
            throw unsupportedViewOperation("drop", e);
        }
        catch (Exception e) {
            throw paimonViewException(format("Failed to drop view '%s'", viewName), e);
        }
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        requireNonNull(session, "session is null");
        requireNonNull(source, "source is null");
        requireNonNull(target, "target is null");
        rejectSystemSchemaWrite(source.getSchemaName(), "rename view");
        rejectSystemSchemaWrite(target.getSchemaName(), "rename view");
        Catalog sessionCatalog = catalog.forSession(session);
        Identifier sourceIdentifier = new Identifier(source.getSchemaName(), source.getTableName());
        Identifier targetIdentifier = new Identifier(target.getSchemaName(), target.getTableName());

        try {
            sessionCatalog.renameView(sourceIdentifier, targetIdentifier, false);
        }
        catch (Catalog.ViewNotExistException e) {
            throw new TrinoException(
                    TABLE_NOT_FOUND,
                    format("View '%s' does not exist", source));
        }
        catch (Catalog.ViewAlreadyExistException e) {
            throw new TrinoException(
                    ALREADY_EXISTS,
                    format("View '%s' already exists", target));
        }
        catch (UnsupportedOperationException e) {
            throw unsupportedViewOperation("rename", e);
        }
        catch (Exception e) {
            throw paimonViewException(format("Failed to rename view '%s' to '%s'", source, target), e);
        }
    }

    @Override
    public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal)
    {
        requireNonNull(session, "session is null");
        requireNonNull(viewName, "viewName is null");
        requireNonNull(principal, "principal is null");
        rejectSystemSchemaWrite(viewName.getSchemaName(), "set view authorization");

        Catalog sessionCatalog = catalog.forSession(session);
        Identifier identifier = new Identifier(viewName.getSchemaName(), viewName.getTableName());

        try {
            sessionCatalog.alterView(
                    identifier,
                    List.of(ViewChange.setOption(OWNER_PROPERTY, principal.getName())),
                    false);
        }
        catch (Catalog.ViewNotExistException e) {
            throw new TrinoException(
                    TABLE_NOT_FOUND,
                    format("View '%s' does not exist", viewName));
        }
        catch (UnsupportedOperationException e) {
            throw unsupportedViewOperation("alter", e);
        }
        catch (Exception e) {
            throw paimonViewException(format("Failed to set authorization on view '%s'", viewName), e);
        }
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(viewName, "viewName is null");
        if (SYSTEM_DATABASE_NAME.equals(viewName.getSchemaName())) {
            return Optional.empty();
        }
        Catalog sessionCatalog = catalog.forSession(session);
        Optional<View> view = getPaimonView(sessionCatalog, viewName);
        if (view.isEmpty()) {
            return Optional.empty();
        }
        View paimonView = view.get();

        if (!hasTrinoViewDialect(paimonView)) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    format("Paimon view '%s' does not contain a Trino SQL dialect", viewName));
        }

        // Convert Paimon View to Trino ConnectorViewDefinition
        List<ConnectorViewDefinition.ViewColumn> columns = paimonView.rowType().getFields().stream()
                .map(field -> new ConnectorViewDefinition.ViewColumn(
                        field.name(),
                        PaimonTypeUtils.fromPaimonType(field.type(), typeManager).getTypeId(),
                        Optional.ofNullable(field.description()).filter(comment -> !comment.isEmpty())))
                .collect(toList());

        String originalSql = paimonView.dialects().get("trino");
        return Optional.of(new ConnectorViewDefinition(
                originalSql,
                Optional.empty(), // catalog
                Optional.empty(), // schema
                columns,
                paimonView.comment(), // comment
                Optional.ofNullable(paimonView.options().get(OWNER_PROPERTY)), // owner
                false, // runAsInvoker
                List.of())); // path
    }

    private Optional<View> getPaimonView(Catalog sessionCatalog, SchemaTableName viewName)
    {
        requireNonNull(sessionCatalog, "sessionCatalog is null");
        requireNonNull(viewName, "viewName is null");
        try {
            Identifier identifier = new Identifier(viewName.getSchemaName(), viewName.getTableName());
            return Optional.of(sessionCatalog.getView(identifier));
        }
        catch (Catalog.ViewNotExistException e) {
            return Optional.empty();
        }
        catch (UnsupportedOperationException e) {
            throw unsupportedViewOperation("read", e);
        }
        catch (Exception e) {
            throw paimonViewException(format("Failed to get view '%s'", viewName), e);
        }
    }

    private static boolean hasTrinoViewDialect(View view)
    {
        requireNonNull(view, "view is null");
        return !StringUtils.isNullOrWhitespaceOnly(view.dialects().get("trino"));
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(schemaName, "schemaName is null");
        schemaName.ifPresent(schema -> checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(schema),
                "schemaName cannot be null or empty"));
        Catalog sessionCatalog = catalog.forSession(session);

        return schemaName.map(Collections::singletonList)
                .orElseGet(sessionCatalog::listDatabases).stream()
                .flatMap(schema -> listViews(sessionCatalog, schema).stream())
                .collect(toList());
    }

    private List<SchemaTableName> listViews(Catalog sessionCatalog, String schemaName)
    {
        return listViewNames(sessionCatalog, schemaName).stream()
                .map(viewName -> new SchemaTableName(schemaName, viewName))
                .filter(viewName -> isTrinoView(sessionCatalog, viewName))
                .collect(toList());
    }

    private List<String> listViewNames(Catalog sessionCatalog, String schemaName)
    {
        if (SYSTEM_DATABASE_NAME.equals(schemaName)) {
            return List.of();
        }
        try {
            return sessionCatalog.listViews(schemaName);
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(
                    SCHEMA_NOT_FOUND,
                    format("Schema '%s' does not exist", schemaName));
        }
        catch (UnsupportedOperationException e) {
            throw unsupportedViewOperation("list", e);
        }
        catch (Exception e) {
            throw paimonViewException(format("Failed to list views in schema '%s'", schemaName), e);
        }
    }

    private boolean isTrinoView(Catalog sessionCatalog, SchemaTableName viewName)
    {
        return getPaimonView(sessionCatalog, viewName)
                .filter(PaimonMetadata::hasTrinoViewDialect)
                .isPresent();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(schemaName, "schemaName is null");
        schemaName.ifPresent(schema -> checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(schema),
                "schemaName cannot be null or empty"));
        Catalog sessionCatalog = catalog.forSession(session);

        List<String> schemas = schemaName.map(Collections::singletonList).orElseGet(sessionCatalog::listDatabases);
        Map<SchemaTableName, ConnectorViewDefinition> views = new LinkedHashMap<>();
        for (String schema : schemas) {
            views.putAll(getViews(sessionCatalog, session, schema));
        }
        return views;
    }

    private Map<SchemaTableName, ConnectorViewDefinition> getViews(Catalog sessionCatalog, ConnectorSession session, String schemaName)
    {
        if (SYSTEM_DATABASE_NAME.equals(schemaName)) {
            return Map.of();
        }
        List<String> viewNames = listViewNames(sessionCatalog, schemaName);

        Map<SchemaTableName, ConnectorViewDefinition> views = new HashMap<>();
        for (String viewName : viewNames) {
            SchemaTableName tableName = new SchemaTableName(schemaName, viewName);
            try {
                getView(session, tableName).ifPresent(def -> views.put(tableName, def));
            }
            catch (TrinoException e) {
                if (!isMissingTrinoViewDialect(e, tableName)) {
                    throw e;
                }
            }
        }
        return views;
    }

    private static boolean isMissingTrinoViewDialect(TrinoException exception, SchemaTableName viewName)
    {
        return exception.getErrorCode().equals(NOT_SUPPORTED.toErrorCode())
                && exception.getMessage().equals(format("Paimon view '%s' does not contain a Trino SQL dialect", viewName));
    }

    private static boolean isUnsupportedViewListOperation(TrinoException exception)
    {
        return exception.getErrorCode().equals(NOT_SUPPORTED.toErrorCode())
                && exception.getMessage().equals("Paimon catalog does not support view list operations");
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        requireNonNull(session, "session is null");
        requireNonNull(viewName, "viewName is null");
        requireNonNull(comment, "comment is null");
        rejectSystemSchemaWrite(viewName.getSchemaName(), "set view comment");
        Catalog sessionCatalog = catalog.forSession(session);
        Identifier identifier = new Identifier(viewName.getSchemaName(), viewName.getTableName());

        try {
            List<ViewChange> changes = List
                    .of(ViewChange.updateComment(comment.orElse(null)));
            sessionCatalog.alterView(identifier, changes, false);
        }
        catch (Catalog.ViewNotExistException e) {
            throw new TrinoException(
                    TABLE_NOT_FOUND,
                    format("View '%s' does not exist", viewName));
        }
        catch (UnsupportedOperationException e) {
            throw unsupportedViewOperation("alter", e);
        }
        catch (Exception e) {
            throw paimonViewException(format("Failed to set comment on view '%s'", viewName), e);
        }
    }

    private static RuntimeException paimonViewException(String message, Exception exception)
    {
        return paimonMetadataException(message, exception);
    }

    public static RuntimeException paimonMetadataException(String message, Exception exception)
    {
        Throwable recognizedFailure = firstRecognizedMetadataFailure(exception);
        if (recognizedFailure instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (recognizedFailure instanceof Exception recognizedException) {
            Optional<RuntimeException> catalogException = paimonCatalogException(recognizedException);
            if (catalogException.isPresent()) {
                return catalogException.get();
            }
        }
        if (recognizedFailure instanceof UnsupportedOperationException unsupportedOperationException) {
            return new TrinoException(
                    NOT_SUPPORTED,
                    unsupportedOperationException.getMessage() == null || unsupportedOperationException.getMessage().isBlank()
                            ? message
                            : unsupportedOperationException.getMessage(),
                    unsupportedOperationException);
        }
        if (recognizedFailure instanceof CommitValidationException validationException) {
            return new TrinoException(
                    PAIMON_COMMIT_ERROR,
                    validationException.getMessage() == null || validationException.getMessage().isBlank()
                            ? message
                            : validationException.getMessage(),
                    validationException);
        }
        if (exception instanceof RuntimeException runtimeException) {
            Throwable cause = runtimeException.getCause();
            if (cause instanceof Exception nestedException) {
                Optional<RuntimeException> nestedCatalogException = paimonCatalogException(nestedException);
                if (nestedCatalogException.isPresent()) {
                    return nestedCatalogException.get();
                }
                return new TrinoException(PAIMON_METADATA_ERROR, message, nestedException);
            }
            return new TrinoException(PAIMON_METADATA_ERROR, message, runtimeException);
        }
        return new TrinoException(PAIMON_METADATA_ERROR, message, exception);
    }

    private static Throwable firstRecognizedMetadataFailure(Exception exception)
    {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = exception;
        while (current != null && visited.add(current)) {
            if (current instanceof TrinoException
                    || current instanceof UnsupportedOperationException
                    || current instanceof CommitValidationException) {
                return current;
            }
            if (current instanceof Exception currentException && paimonCatalogException(currentException).isPresent()) {
                return current;
            }
            current = current.getCause();
        }
        return exception;
    }

    private static Optional<RuntimeException> paimonCatalogException(Exception exception)
    {
        if (exception instanceof Catalog.DatabaseAlreadyExistException databaseAlreadyExistException) {
            return Optional.of(new TrinoException(
                    SCHEMA_ALREADY_EXISTS,
                    format("Schema '%s' already exists", databaseAlreadyExistException.database()),
                    exception));
        }
        if (exception instanceof Catalog.DatabaseNotExistException databaseNotExistException) {
            return Optional.of(new TrinoException(
                    SCHEMA_NOT_FOUND,
                    format("Schema '%s' does not exist", databaseNotExistException.database()),
                    exception));
        }
        if (exception instanceof Catalog.DatabaseNotEmptyException databaseNotEmptyException) {
            return Optional.of(new TrinoException(
                    SCHEMA_NOT_EMPTY,
                    format("Schema '%s' is not empty", databaseNotEmptyException.database()),
                    exception));
        }
        if (exception instanceof Catalog.TableAlreadyExistException tableAlreadyExistException) {
            return Optional.of(new TrinoException(
                    TABLE_ALREADY_EXISTS,
                    format("Table '%s' already exists", tableAlreadyExistException.identifier().getFullName()),
                    exception));
        }
        if (exception instanceof Catalog.TableNotExistException tableNotExistException) {
            return Optional.of(new TrinoException(
                    TABLE_NOT_FOUND,
                    format("Table '%s' does not exist", tableNotExistException.identifier().getFullName()),
                    exception));
        }
        if (exception instanceof Catalog.ViewAlreadyExistException viewAlreadyExistException) {
            return Optional.of(new TrinoException(
                    ALREADY_EXISTS,
                    format("View '%s' already exists", viewAlreadyExistException.identifier().getFullName()),
                    exception));
        }
        if (exception instanceof Catalog.ViewNotExistException viewNotExistException) {
            return Optional.of(new TrinoException(
                    TABLE_NOT_FOUND,
                    format("View '%s' does not exist", viewNotExistException.identifier().getFullName()),
                    exception));
        }
        if (exception instanceof Catalog.ColumnAlreadyExistException columnAlreadyExistException) {
            return Optional.of(new TrinoException(
                    COLUMN_ALREADY_EXISTS,
                    format("Column '%s' already exists in table '%s'",
                            columnAlreadyExistException.column(),
                            columnAlreadyExistException.identifier().getFullName()),
                    exception));
        }
        if (exception instanceof Catalog.ColumnNotExistException columnNotExistException) {
            return Optional.of(new TrinoException(
                    COLUMN_NOT_FOUND,
                    format("Column '%s' does not exist in table '%s'",
                            columnNotExistException.column(),
                            columnNotExistException.identifier().getFullName()),
                    exception));
        }
        return Optional.empty();
    }

    private static TrinoException unsupportedViewOperation(String operation, UnsupportedOperationException cause)
    {
        String message = "Paimon catalog does not support view " + operation + " operations";
        if (operation.equals("create")) {
            message = "This connector does not support creating views: " + message;
        }
        return new TrinoException(NOT_SUPPORTED, message, cause);
    }
}
