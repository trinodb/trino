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

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.paimon.ClassLoaderUtils.runWithContextClassLoader;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;

public class PaimonTableHandle
        implements ConnectorInsertTableHandle,
                   ConnectorOutputTableHandle,
                   ConnectorTableFunctionHandle,
                   ConnectorTableHandle
{
    static final String UNSUPPORTED_HISTORICAL_READ_MESSAGE = "Paimon system tables do not support historical reads";
    static final String CREATE_TABLE_AS_SELECT_OPERATION = "CREATE_TABLE_AS_SELECT";
    static final String CREATE_OR_REPLACE_TABLE_AS_SELECT_OPERATION = "CREATE_OR_REPLACE_TABLE_AS_SELECT";
    private static final Set<String> INCREMENTAL_READ_OPTION_KEYS = Set.of(
            CoreOptions.INCREMENTAL_BETWEEN.key(),
            CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key(),
            CoreOptions.INCREMENTAL_TO_AUTO_TAG.key());
    private static final Set<String> EXPLICIT_STARTUP_OPTION_KEYS = Set.of(
            CoreOptions.SCAN_VERSION.key(),
            CoreOptions.SCAN_SNAPSHOT_ID.key(),
            CoreOptions.SCAN_TAG_NAME.key(),
            CoreOptions.SCAN_TIMESTAMP.key(),
            CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
            CoreOptions.SCAN_WATERMARK.key(),
            CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key(),
            CoreOptions.SCAN_CREATION_TIME_MILLIS.key(),
            CoreOptions.INCREMENTAL_BETWEEN.key(),
            CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key(),
            CoreOptions.INCREMENTAL_TO_AUTO_TAG.key());
    private static final Set<String> HISTORICAL_READ_OPTION_KEYS = Set.of(
            CoreOptions.SCAN_VERSION.key(),
            CoreOptions.SCAN_SNAPSHOT_ID.key(),
            CoreOptions.SCAN_TAG_NAME.key(),
            CoreOptions.SCAN_TIMESTAMP.key(),
            CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
            CoreOptions.SCAN_WATERMARK.key(),
            CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key(),
            CoreOptions.SCAN_CREATION_TIME_MILLIS.key());

    private final String schemaName;
    private final String tableName;
    private final TupleDomain<PaimonColumnHandle> filter;
    private final Optional<List<PaimonColumnHandle>> projectedColumns;
    private final Optional<List<PaimonColumnHandle>> writeColumns;
    private final OptionalLong limit;
    private final Optional<List<Map<String, String>>> deletePartitionSpecs;
    private final Optional<String> createTableOperation;
    private final OptionalInt dynamicBucketAssignerParallelism;
    private final boolean keyDynamicBootstrapSnapshotPlanned;
    private final OptionalLong keyDynamicBootstrapSnapshot;
    private final Map<String, String> dynamicOptions;

    private final transient Map<Catalog, Table> tablesByCatalog = Collections.synchronizedMap(new IdentityHashMap<>());
    private transient OptionalInt plannedInsertDynamicBucketAssignerParallelism = OptionalInt.empty();
    private transient OptionalInt plannedRowLevelDynamicBucketAssignerParallelism = OptionalInt.empty();

    public PaimonTableHandle(String schemaName, String tableName, Map<String, String> dynamicOptions)
    {
        this(schemaName,
                tableName,
                dynamicOptions,
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty(),
                OptionalInt.empty());
    }

    public PaimonTableHandle(
            String schemaName,
            String tableName,
            Map<String, String> dynamicOptions,
            TupleDomain<PaimonColumnHandle> filter,
            Optional<List<PaimonColumnHandle>> projectedColumns,
            Optional<List<PaimonColumnHandle>> writeColumns,
            OptionalLong limit)
    {
        this(schemaName,
                tableName,
                dynamicOptions,
                filter,
                projectedColumns,
                writeColumns,
                limit,
                Optional.empty(),
                Optional.empty(),
                OptionalInt.empty());
    }

    public PaimonTableHandle(
            String schemaName,
            String tableName,
            Map<String, String> dynamicOptions,
            TupleDomain<PaimonColumnHandle> filter,
            Optional<List<PaimonColumnHandle>> projectedColumns,
            Optional<List<PaimonColumnHandle>> writeColumns,
            OptionalLong limit,
            Optional<List<Map<String, String>>> deletePartitionSpecs)
    {
        this(schemaName,
                tableName,
                dynamicOptions,
                filter,
                projectedColumns,
                writeColumns,
                limit,
                deletePartitionSpecs,
                Optional.empty(),
                OptionalInt.empty());
    }

    public PaimonTableHandle(
            String schemaName,
            String tableName,
            Map<String, String> dynamicOptions,
            TupleDomain<PaimonColumnHandle> filter,
            Optional<List<PaimonColumnHandle>> projectedColumns,
            Optional<List<PaimonColumnHandle>> writeColumns,
            OptionalLong limit,
            Optional<List<Map<String, String>>> deletePartitionSpecs,
            Optional<String> createTableOperation,
            OptionalInt dynamicBucketAssignerParallelism)
    {
        this(schemaName,
                tableName,
                dynamicOptions,
                filter,
                projectedColumns,
                writeColumns,
                limit,
                deletePartitionSpecs,
                createTableOperation,
                dynamicBucketAssignerParallelism,
                false,
                OptionalLong.empty());
    }

    @JsonCreator
    public PaimonTableHandle(
            @JsonProperty(value = "schemaName", required = true) String schemaName,
            @JsonProperty(value = "tableName", required = true) String tableName,
            @JsonProperty(value = "dynamicOptions", required = true) Map<String, String> dynamicOptions,
            @JsonProperty(value = "filter", required = true) TupleDomain<PaimonColumnHandle> filter,
            @JsonProperty(value = "projectedColumns", required = true) Optional<List<PaimonColumnHandle>> projectedColumns,
            @JsonProperty(value = "writeColumns", required = true) Optional<List<PaimonColumnHandle>> writeColumns,
            @JsonProperty(value = "limit", required = true) OptionalLong limit,
            @JsonProperty("deletePartitionSpecs") Optional<List<Map<String, String>>> deletePartitionSpecs,
            @JsonProperty("createTableOperation") Optional<String> createTableOperation,
            @JsonProperty("dynamicBucketAssignerParallelism") OptionalInt dynamicBucketAssignerParallelism,
            @JsonProperty("keyDynamicBootstrapSnapshotPlanned") Boolean keyDynamicBootstrapSnapshotPlanned,
            @JsonProperty("keyDynamicBootstrapSnapshot") OptionalLong keyDynamicBootstrapSnapshot)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        checkArgument(!this.schemaName.isBlank(), "schemaName is blank");
        this.tableName = requireNonNull(tableName, "tableName is null");
        checkArgument(!this.tableName.isBlank(), "tableName is blank");
        this.dynamicOptions = copyDynamicOptions(dynamicOptions);
        validateDynamicOptionsSemantics(this.dynamicOptions);
        this.filter = requireNonNull(filter, "filter is null");
        this.projectedColumns = copyColumnHandles(projectedColumns, "projectedColumns");
        this.writeColumns = copyColumnHandles(writeColumns, "writeColumns");
        this.limit = requireNonNull(limit, "limit is null");
        this.deletePartitionSpecs = copyDeletePartitionSpecs(deletePartitionSpecs);
        this.createTableOperation = copyCreateTableOperation(createTableOperation);
        this.dynamicBucketAssignerParallelism = copyDynamicBucketAssignerParallelism(
                dynamicBucketAssignerParallelism);
        this.keyDynamicBootstrapSnapshotPlanned = Boolean.TRUE.equals(keyDynamicBootstrapSnapshotPlanned);
        this.keyDynamicBootstrapSnapshot = keyDynamicBootstrapSnapshot == null
                ? OptionalLong.empty()
                : keyDynamicBootstrapSnapshot;
        if (!this.keyDynamicBootstrapSnapshotPlanned) {
            checkArgument(this.keyDynamicBootstrapSnapshot.isEmpty(),
                    "keyDynamicBootstrapSnapshot must be empty when planning is disabled");
        }
        this.keyDynamicBootstrapSnapshot.ifPresent(value ->
                checkArgument(value >= 0, "keyDynamicBootstrapSnapshot must be non-negative: %s", value));
        checkArgument(this.limit.isEmpty() || this.limit.orElseThrow() >= 0, "limit must be non-negative");
    }

    private static Map<String, String> copyDynamicOptions(Map<String, String> dynamicOptions)
    {
        Map<String, String> copiedOptions = new LinkedHashMap<>();
        requireNonNull(dynamicOptions, "dynamicOptions is null").forEach((key, value) -> {
            requireNonNull(key, "dynamicOptions contains null key");
            String optionKey = key.trim();
            checkArgument(!optionKey.isBlank(), "dynamicOptions contains blank key");
            requireNonNull(value, "dynamicOptions contains null value for key '%s'".formatted(key));
            checkArgument(!value.isBlank(), "dynamicOptions contains blank value for key '%s'", key);
            String optionValue = PaimonTableOptionUtils.normalizeDynamicOptionValue(optionKey, value);
            checkArgument(copiedOptions.putIfAbsent(optionKey, optionValue) == null,
                    "dynamicOptions contains duplicate key after normalization: '%s'",
                    optionKey);
        });
        return Map.copyOf(copiedOptions);
    }

    private static Optional<String> copyCreateTableOperation(Optional<String> createTableOperation)
    {
        Optional<String> operation = createTableOperation == null ? Optional.empty() : createTableOperation;
        operation.ifPresent(value -> {
            checkArgument(!value.isBlank(), "createTableOperation is blank");
            checkArgument(value.equals(CREATE_TABLE_AS_SELECT_OPERATION)
                            || value.equals(CREATE_OR_REPLACE_TABLE_AS_SELECT_OPERATION),
                    "Unsupported createTableOperation: %s",
                    value);
        });
        return operation;
    }

    private static OptionalInt copyDynamicBucketAssignerParallelism(OptionalInt dynamicBucketAssignerParallelism)
    {
        OptionalInt parallelism = dynamicBucketAssignerParallelism == null
                ? OptionalInt.empty()
                : dynamicBucketAssignerParallelism;
        parallelism.ifPresent(value ->
                checkArgument(value > 0, "dynamicBucketAssignerParallelism must be positive: %s", value));
        return parallelism;
    }

    private static void validateDynamicOptionsSemantics(Map<String, String> dynamicOptions)
    {
        requireNonNull(dynamicOptions, "dynamicOptions is null");

        checkArgument(!dynamicOptions.containsKey(CoreOptions.SCAN_MODE.key()),
                "dynamicOptions key '%s' is not supported; use explicit scan selector keys instead",
                CoreOptions.SCAN_MODE.key());

        List<String> startupSelections = EXPLICIT_STARTUP_OPTION_KEYS.stream()
                .filter(dynamicOptions::containsKey)
                .sorted()
                .toList();
        checkArgument(startupSelections.size() <= 1,
                "dynamicOptions may contain only one startup selection, got keys: %s",
                startupSelections);

        String incrementalBetweenScanMode = dynamicOptions.get(CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key());
        if (incrementalBetweenScanMode != null) {
            checkArgument(
                    dynamicOptions.containsKey(CoreOptions.INCREMENTAL_BETWEEN.key())
                            || dynamicOptions.containsKey(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key()),
                    "dynamicOptions key '%s' requires '%s' or '%s'",
                    CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(),
                    CoreOptions.INCREMENTAL_BETWEEN.key(),
                    CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key());
            validateDynamicOptionValue(CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE, incrementalBetweenScanMode);
        }

        String incrementalBetweenTagToSnapshot = dynamicOptions.get(CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key());
        if (incrementalBetweenTagToSnapshot != null) {
            checkArgument(
                    dynamicOptions.containsKey(CoreOptions.INCREMENTAL_BETWEEN.key()),
                    "dynamicOptions key '%s' requires '%s'",
                    CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key(),
                    CoreOptions.INCREMENTAL_BETWEEN.key());
            validateDynamicOptionValue(CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT, incrementalBetweenTagToSnapshot);
        }
    }

    private static <T> void validateDynamicOptionValue(ConfigOption<T> option, String value)
    {
        requireNonNull(option, "option is null");
        requireNonNull(value, "value is null");
        try {
            Options.fromMap(Map.of(option.key(), value)).get(option);
        }
        catch (RuntimeException e) {
            throw new IllegalArgumentException("dynamicOptions contains invalid value for key '%s'".formatted(option.key()), e);
        }
    }

    private static Optional<List<PaimonColumnHandle>> copyColumnHandles(
            Optional<List<PaimonColumnHandle>> columns,
            String fieldName)
    {
        return requireNonNull(columns, fieldName + " is null")
                .map(columnHandles -> {
                    List<PaimonColumnHandle> copiedColumns = copyColumnHandlesList(columnHandles, fieldName);
                    if (fieldName.equals("writeColumns")) {
                        checkArgument(!copiedColumns.isEmpty(), "writeColumns is empty");
                    }
                    return copiedColumns;
                });
    }

    private static Optional<List<Map<String, String>>> copyDeletePartitionSpecs(
            Optional<List<Map<String, String>>> deletePartitionSpecs)
    {
        Optional<List<Map<String, String>>> specs = deletePartitionSpecs == null ? Optional.empty() : deletePartitionSpecs;
        return specs.map(partitionSpecs -> {
            checkArgument(!partitionSpecs.isEmpty(), "deletePartitionSpecs is empty");
            return partitionSpecs.stream()
                    .map(PaimonTableHandle::copyDeletePartitionSpec)
                    .toList();
        });
    }

    private static Map<String, String> copyDeletePartitionSpec(Map<String, String> partitionSpec)
    {
        requireNonNull(partitionSpec, "deletePartitionSpecs contains null partitionSpec");
        checkArgument(!partitionSpec.isEmpty(), "deletePartitionSpecs contains empty partitionSpec");
        partitionSpec.forEach((key, value) -> {
            requireNonNull(key, "deletePartitionSpecs contains null partition key");
            checkArgument(!key.isBlank(), "deletePartitionSpecs contains blank partition key");
            requireNonNull(value, "deletePartitionSpecs contains null value for key '%s'".formatted(key));
        });
        return Collections.unmodifiableMap(new LinkedHashMap<>(partitionSpec));
    }

    private static List<PaimonColumnHandle> copyColumnHandlesList(List<?> columns, String fieldName)
    {
        requireNonNull(columns, fieldName + " is null");
        return columns.stream()
                .map(column -> {
                    requireNonNull(column, fieldName + " contains null column");
                    if (!(column instanceof PaimonColumnHandle paimonColumnHandle)) {
                        throw new IllegalStateException("%s requires PaimonColumnHandle, got: %s"
                                .formatted(fieldName, column.getClass().getName()));
                    }
                    return paimonColumnHandle;
                })
                .toList();
    }

    @JsonAnySetter
    public void rejectUnknownJsonField(String name, Object value)
    {
        PaimonHandleJsonUtils.rejectUnknownHandleJsonField("PaimonTableHandle", name, value);
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Map<String, String> getDynamicOptions()
    {
        return dynamicOptions;
    }

    @JsonProperty
    public TupleDomain<PaimonColumnHandle> getFilter()
    {
        return filter;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.ALWAYS)
    public Optional<List<PaimonColumnHandle>> getProjectedColumns()
    {
        return projectedColumns;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.ALWAYS)
    public Optional<List<PaimonColumnHandle>> getWriteColumns()
    {
        return writeColumns;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.ALWAYS)
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.ALWAYS)
    public Optional<List<Map<String, String>>> getDeletePartitionSpecs()
    {
        return deletePartitionSpecs;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.ALWAYS)
    public Optional<String> getCreateTableOperation()
    {
        return createTableOperation;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.ALWAYS)
    public OptionalInt getDynamicBucketAssignerParallelism()
    {
        return dynamicBucketAssignerParallelism;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.ALWAYS)
    public boolean isKeyDynamicBootstrapSnapshotPlanned()
    {
        return keyDynamicBootstrapSnapshotPlanned;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.ALWAYS)
    public OptionalLong getKeyDynamicBootstrapSnapshot()
    {
        return keyDynamicBootstrapSnapshot;
    }

    synchronized void rememberPlannedInsertDynamicBucketAssignerParallelism(OptionalInt dynamicBucketAssignerParallelism)
    {
        OptionalInt parallelism = copyDynamicBucketAssignerParallelism(dynamicBucketAssignerParallelism);
        if (parallelism.isEmpty() || plannedInsertDynamicBucketAssignerParallelism.isPresent()) {
            return;
        }
        plannedInsertDynamicBucketAssignerParallelism = parallelism;
    }

    synchronized OptionalInt getPlannedInsertDynamicBucketAssignerParallelism()
    {
        return plannedInsertDynamicBucketAssignerParallelism;
    }

    synchronized void rememberPlannedRowLevelDynamicBucketAssignerParallelism(OptionalInt dynamicBucketAssignerParallelism)
    {
        OptionalInt parallelism = copyDynamicBucketAssignerParallelism(dynamicBucketAssignerParallelism);
        if (parallelism.isEmpty() || plannedRowLevelDynamicBucketAssignerParallelism.isPresent()) {
            return;
        }
        plannedRowLevelDynamicBucketAssignerParallelism = parallelism;
    }

    synchronized OptionalInt getPlannedRowLevelDynamicBucketAssignerParallelism()
    {
        return plannedRowLevelDynamicBucketAssignerParallelism;
    }

    public Table tableWithDynamicOptions(Catalog catalog, ConnectorSession session)
    {
        requireNonNull(catalog, "catalog is null");
        requireNonNull(session, "session is null");
        return runWithContextClassLoader(() -> {
            Table paimonTable = rawTable(catalog);
            Map<String, String> dynamicOptions = readDynamicOptions(session);
            validateHistoricalReadSupported(dynamicOptions);
            return requireSupportedTable(!dynamicOptions.isEmpty() ? paimonTable.copy(dynamicOptions) : paimonTable);
        }, PaimonTableHandle.class.getClassLoader());
    }

    public boolean usesHistoricalReadSchema(ConnectorSession session)
    {
        requireNonNull(session, "session is null");
        Map<String, String> dynamicOptions = readDynamicOptions(session);
        return HISTORICAL_READ_OPTION_KEYS.stream().anyMatch(dynamicOptions::containsKey);
    }

    boolean hasIncrementalReadMode()
    {
        return INCREMENTAL_READ_OPTION_KEYS.stream().anyMatch(dynamicOptions::containsKey);
    }

    public Table tableWithWriteDynamicOptions(Catalog catalog)
    {
        requireNonNull(catalog, "catalog is null");
        return runWithContextClassLoader(() -> {
            Table paimonTable = rawTable(catalog);
            if (!(paimonTable instanceof FileStoreTable fileStoreTable)) {
                return requireSupportedTable(paimonTable);
            }

            Map<String, String> dynamicOptions = new HashMap<>(this.dynamicOptions);
            dynamicOptions.keySet().removeIf(PaimonTableOptionUtils::isRuntimeOnlyPaimonOptionKeyForWrite);
            return requireSupportedTable(!dynamicOptions.isEmpty()
                    ? fileStoreTable.copyWithoutTimeTravel(dynamicOptions)
                    : fileStoreTable);
        }, PaimonTableHandle.class.getClassLoader());
    }

    private static boolean hasExplicitStartupSelection(Map<String, String> dynamicOptions)
    {
        requireNonNull(dynamicOptions, "dynamicOptions is null");
        return EXPLICIT_STARTUP_OPTION_KEYS.stream().anyMatch(dynamicOptions::containsKey);
    }

    private static boolean hasHistoricalReadSelection(Map<String, String> dynamicOptions)
    {
        requireNonNull(dynamicOptions, "dynamicOptions is null");
        return HISTORICAL_READ_OPTION_KEYS.stream().anyMatch(dynamicOptions::containsKey);
    }

    private Map<String, String> readDynamicOptions(ConnectorSession session)
    {
        requireNonNull(session, "session is null");

        // see TrinoConnector.getSessionProperties
        Map<String, String> dynamicOptions = new HashMap<>(this.dynamicOptions);
        if (!hasExplicitStartupSelection(dynamicOptions)) {
            Long scanTimestampMills = PaimonSessionProperties.getScanTimestampMillis(session);
            Long scanSnapshotId = PaimonSessionProperties.getScanSnapshotId(session);
            String scanTagName = PaimonSessionProperties.getScanTagName(session);
            Long scanFileCreationTimeMillis = PaimonSessionProperties.getScanFileCreationTimeMillis(session);
            Long scanCreationTimeMillis = PaimonSessionProperties.getScanCreationTimeMillis(session);
            validateSessionScanSelection(
                    scanTimestampMills,
                    scanSnapshotId,
                    scanTagName,
                    scanFileCreationTimeMillis,
                    scanCreationTimeMillis);
            if (scanTimestampMills != null) {
                dynamicOptions.put(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), scanTimestampMills.toString());
            }
            if (scanSnapshotId != null) {
                dynamicOptions.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), scanSnapshotId.toString());
            }
            if (scanTagName != null) {
                dynamicOptions.put(CoreOptions.SCAN_TAG_NAME.key(), scanTagName);
            }
            if (scanFileCreationTimeMillis != null) {
                dynamicOptions.put(CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key(), scanFileCreationTimeMillis.toString());
            }
            if (scanCreationTimeMillis != null) {
                dynamicOptions.put(CoreOptions.SCAN_CREATION_TIME_MILLIS.key(), scanCreationTimeMillis.toString());
            }
        }
        validateDynamicOptionsSemantics(dynamicOptions);
        return dynamicOptions;
    }

    private static void validateSessionScanSelection(
            Long scanTimestampMills,
            Long scanSnapshotId,
            String scanTagName,
            Long scanFileCreationTimeMillis,
            Long scanCreationTimeMillis)
    {
        int selections = 0;
        if (scanTimestampMills != null) {
            selections++;
        }
        if (scanSnapshotId != null) {
            selections++;
        }
        if (scanTagName != null) {
            selections++;
        }
        if (scanFileCreationTimeMillis != null) {
            selections++;
        }
        if (scanCreationTimeMillis != null) {
            selections++;
        }
        if (selections > 1) {
            throw new TrinoException(INVALID_SESSION_PROPERTY,
                    "Only one of %s, %s, %s, %s or %s session properties may be set"
                            .formatted(
                                    PaimonSessionProperties.SCAN_TIMESTAMP,
                                    PaimonSessionProperties.SCAN_SNAPSHOT,
                                    PaimonSessionProperties.SCAN_TAG,
                                    PaimonSessionProperties.SCAN_FILE_CREATION_TIME,
                                    PaimonSessionProperties.SCAN_CREATION_TIME));
        }
    }

    public Table table(Catalog catalog)
    {
        requireNonNull(catalog, "catalog is null");
        return runWithContextClassLoader(() -> {
            validateHistoricalReadSupported(dynamicOptions);
            Table paimonTable = rawTable(catalog);
            return requireSupportedTable(!dynamicOptions.isEmpty() ? paimonTable.copy(dynamicOptions) : paimonTable);
        }, PaimonTableHandle.class.getClassLoader());
    }

    static boolean supportsHistoricalRead(Identifier identifier)
    {
        requireNonNull(identifier, "identifier is null");
        return !SYSTEM_DATABASE_NAME.equals(identifier.getDatabaseName()) && !identifier.isSystemTable();
    }

    private Table rawTable(Catalog catalog)
    {
        requireNonNull(catalog, "catalog is null");
        Table table = tablesByCatalog.get(catalog);
        if (table != null) {
            return requireSupportedTable(table);
        }
        try {
            table = catalog.getTable(Identifier.create(schemaName, tableName));
            cacheTable(catalog, table);
            return requireSupportedTable(table);
        }
        catch (Catalog.TableNotExistException e) {
            throw new TrinoException(TABLE_NOT_FOUND, "Paimon table '%s.%s' does not exist".formatted(
                    schemaName,
                    tableName), e);
        }
    }

    private void validateHistoricalReadSupported(Map<String, String> dynamicOptions)
    {
        requireNonNull(dynamicOptions, "dynamicOptions is null");
        if (hasHistoricalReadSelection(dynamicOptions) && !supportsHistoricalRead(Identifier.create(schemaName, tableName))) {
            throw new TrinoException(NOT_SUPPORTED, UNSUPPORTED_HISTORICAL_READ_MESSAGE);
        }
    }

    void cacheTable(Catalog catalog, Table table)
    {
        requireNonNull(catalog, "catalog is null");
        requireNonNull(table, "table is null");
        tablesByCatalog.put(catalog, table);
    }

    private static Table requireSupportedTable(Table table)
    {
        return PaimonTableSupport.requireSupportedTable(table);
    }

    public ConnectorTableMetadata tableMetadata(Catalog catalog, TypeManager typeManager, ConnectorSession session)
    {
        requireNonNull(catalog, "catalog is null");
        requireNonNull(typeManager, "typeManager is null");
        Table table = metadataTable(catalog, session);
        return new ConnectorTableMetadata(
                SchemaTableName.schemaTableName(schemaName, tableName),
                columnMetadatas(table, typeManager),
                PaimonTableOptionUtils.tableProperties(table),
                normalizeComment(table.comment()));
    }

    public List<ColumnMetadata> columnMetadatas(Catalog catalog, TypeManager typeManager, ConnectorSession session)
    {
        requireNonNull(catalog, "catalog is null");
        requireNonNull(typeManager, "typeManager is null");
        return columnMetadatas(metadataTable(catalog, session), typeManager);
    }

    private static List<ColumnMetadata> columnMetadatas(Table table, TypeManager typeManager)
    {
        requireNonNull(table, "table is null");
        requireNonNull(typeManager, "typeManager is null");
        return effectiveReadRowType(table).getFields().stream()
                .map(column -> columnMetadata(table, column, typeManager))
                .collect(Collectors.toList());
    }

    private static Optional<String> normalizeComment(Optional<String> comment)
    {
        return requireNonNull(comment, "comment is null").flatMap(PaimonTableHandle::normalizeComment);
    }

    private static Optional<String> normalizeComment(String comment)
    {
        return Optional.ofNullable(comment).filter(value -> !value.isEmpty());
    }

    public PaimonColumnHandle columnHandle(
            Catalog catalog,
            TypeManager typeManager,
            ConnectorSession session,
            String field)
    {
        requireNonNull(catalog, "catalog is null");
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(field, "field is null");
        Table paimonTable = metadataTable(catalog, session);
        RowType readRowType = effectiveReadRowType(paimonTable);
        Map<String, Integer> lowerCaseFieldIndexes = FieldNameUtils.fieldNameIndexes(readRowType);
        List<String> originFieldNames = readRowType.getFieldNames();
        // Fix case-sensitivity: lowerCaseFieldIndexes contains lowercase names, so convert field to lowercase for lookup
        Integer index = lowerCaseFieldIndexes.get(FieldNameUtils.toLowerCase(field));
        if (index == null) {
            throw new TrinoException(
                    COLUMN_NOT_FOUND,
                    String.format("Column '%s' does not exist in Paimon table '%s.%s'", field, schemaName, tableName));
        }
        return columnHandle(originFieldNames.get(index), readRowType.getTypeAt(index), typeManager);
    }

    private Table metadataTable(Catalog catalog, ConnectorSession session)
    {
        Table table = tableWithDynamicOptions(catalog, session);
        return schemaAwareReadTable(table, !usesHistoricalReadSchema(session));
    }

    static Table schemaAwareReadTable(Table table, boolean refreshToLatestSchema)
    {
        requireNonNull(table, "table is null");
        if (refreshToLatestSchema && table instanceof FileStoreTable fileStoreTable) {
            return fileStoreTable.copyWithLatestSchema();
        }
        return table;
    }

    static RowType effectiveReadRowType(Table table)
    {
        requireNonNull(table, "table is null");
        RowType rowType = table.rowType();
        if (!(table instanceof FileStoreTable fileStoreTable) || !fileStoreTable.coreOptions().rowTrackingEnabled()) {
            return rowType;
        }
        if (rowType.containsField(SpecialFields.ROW_ID.name()) || rowType.containsField(SpecialFields.SEQUENCE_NUMBER.name())) {
            return rowType;
        }
        return SpecialFields.rowTypeWithRowTracking(rowType, true, true);
    }

    static ColumnMetadata columnMetadata(Table table, String fieldName, TypeManager typeManager)
    {
        requireNonNull(table, "table is null");
        requireNonNull(fieldName, "fieldName is null");
        requireNonNull(typeManager, "typeManager is null");
        RowType readRowType = effectiveReadRowType(table);
        Map<String, Integer> lowerCaseFieldIndexes = FieldNameUtils.fieldNameIndexes(readRowType);
        Integer index = lowerCaseFieldIndexes.get(FieldNameUtils.toLowerCase(fieldName));
        if (index == null) {
            throw new TrinoException(
                    COLUMN_NOT_FOUND,
                    "Column '%s' does not exist in Paimon table '%s'".formatted(fieldName, table.name()));
        }
        return columnMetadata(table, readRowType.getFields().get(index), typeManager);
    }

    private static ColumnMetadata columnMetadata(Table table, DataField column, TypeManager typeManager)
    {
        requireNonNull(column, "column is null");
        requireNonNull(typeManager, "typeManager is null");
        return ColumnMetadata.builder()
                .setName(column.name())
                .setType(fromPaimonType(column.type(), typeManager))
                .setNullable(column.type().isNullable())
                .setComment(normalizeComment(column.description()))
                .setHidden(isHiddenColumn(table, column.name()))
                .build();
    }

    private static PaimonColumnHandle columnHandle(String columnName, DataType columnType, TypeManager typeManager)
    {
        try {
            return PaimonColumnHandle.of(columnName, columnType, typeManager);
        }
        catch (UnsupportedOperationException e) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    unsupportedColumnConversionMessage(columnName, columnType, e),
                    e);
        }
    }

    private static io.trino.spi.type.Type fromPaimonType(DataType type, TypeManager typeManager)
    {
        try {
            return PaimonTypeUtils.fromPaimonType(type, typeManager);
        }
        catch (UnsupportedOperationException e) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    unsupportedPaimonTypeMessage(type, e),
                    e);
        }
    }

    private static String unsupportedColumnConversionMessage(String columnName, DataType columnType, UnsupportedOperationException cause)
    {
        if (hasMessage(cause)) {
            return cause.getMessage();
        }
        return "Unsupported Paimon column '%s' with type %s: %s"
                .formatted(columnName, dataTypeName(columnType), unsupportedOperationMessage(cause));
    }

    private static String unsupportedPaimonTypeMessage(DataType type, UnsupportedOperationException cause)
    {
        if (hasMessage(cause)) {
            return cause.getMessage();
        }
        return "Unsupported Paimon type %s: %s".formatted(dataTypeName(type), unsupportedOperationMessage(cause));
    }

    private static boolean hasMessage(UnsupportedOperationException cause)
    {
        String message = cause.getMessage();
        return message != null && !message.isBlank();
    }

    private static String dataTypeName(DataType type)
    {
        try {
            String name = type.toString();
            if (name != null && !name.isBlank()) {
                return name;
            }
        }
        catch (RuntimeException ignored) {
            // Fall through to the implementation class; this path is already formatting an unsupported type failure.
        }
        return type.getClass().getName();
    }

    private static String unsupportedOperationMessage(UnsupportedOperationException cause)
    {
        String message = cause.getMessage();
        if (message == null || message.isBlank()) {
            return cause.getClass().getSimpleName();
        }
        return message;
    }

    private static boolean isHiddenColumn(Table table, String columnName)
    {
        if (!PaimonColumnHandle.isHiddenColumnName(columnName)) {
            return false;
        }
        return !FieldNameUtils.fieldNameIndexes(requireNonNull(table, "table is null").rowType())
                .containsKey(FieldNameUtils.toLowerCase(columnName));
    }

    public PaimonTableHandle copy(TupleDomain<PaimonColumnHandle> filter)
    {
        return new PaimonTableHandle(
                schemaName,
                tableName,
                dynamicOptions,
                filter,
                projectedColumns,
                writeColumns,
                limit,
                Optional.empty(),
                createTableOperation,
                dynamicBucketAssignerParallelism,
                keyDynamicBootstrapSnapshotPlanned,
                keyDynamicBootstrapSnapshot);
    }

    public PaimonTableHandle copy(Optional<List<ColumnHandle>> projectedColumns)
    {
        return new PaimonTableHandle(
                schemaName,
                tableName,
                dynamicOptions,
                filter,
                toPaimonColumnHandles(projectedColumns),
                writeColumns,
                limit,
                Optional.empty(),
                createTableOperation,
                dynamicBucketAssignerParallelism,
                keyDynamicBootstrapSnapshotPlanned,
                keyDynamicBootstrapSnapshot);
    }

    public PaimonTableHandle withWriteColumns(List<ColumnHandle> writeColumns)
    {
        requireNonNull(writeColumns, "writeColumns is null");
        checkArgument(!writeColumns.isEmpty(), "writeColumns is empty");
        return new PaimonTableHandle(
                schemaName,
                tableName,
                dynamicOptions,
                filter,
                projectedColumns,
                Optional.of(toPaimonColumnHandles(writeColumns)),
                limit,
                Optional.empty(),
                createTableOperation,
                dynamicBucketAssignerParallelism,
                keyDynamicBootstrapSnapshotPlanned,
                keyDynamicBootstrapSnapshot);
    }

    public PaimonTableHandle withCreateTableOperation(String createTableOperation)
    {
        return new PaimonTableHandle(
                schemaName,
                tableName,
                dynamicOptions,
                filter,
                projectedColumns,
                writeColumns,
                limit,
                deletePartitionSpecs,
                Optional.of(requireNonNull(
                        createTableOperation,
                        "createTableOperation is null")),
                dynamicBucketAssignerParallelism,
                keyDynamicBootstrapSnapshotPlanned,
                keyDynamicBootstrapSnapshot);
    }

    public PaimonTableHandle withDynamicBucketAssignerParallelism(
            OptionalInt dynamicBucketAssignerParallelism)
    {
        return new PaimonTableHandle(
                schemaName,
                tableName,
                dynamicOptions,
                filter,
                projectedColumns,
                writeColumns,
                limit,
                deletePartitionSpecs,
                createTableOperation,
                copyDynamicBucketAssignerParallelism(dynamicBucketAssignerParallelism),
                keyDynamicBootstrapSnapshotPlanned,
                keyDynamicBootstrapSnapshot);
    }

    private static Optional<List<PaimonColumnHandle>> toPaimonColumnHandles(Optional<List<ColumnHandle>> columns)
    {
        return requireNonNull(columns, "columns is null").map(PaimonTableHandle::toPaimonColumnHandles);
    }

    private static List<PaimonColumnHandle> toPaimonColumnHandles(List<? extends ColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");
        return columns.stream()
                .map(column -> requireNonNull(column, "column is null"))
                .map(column -> {
                    if (!(column instanceof PaimonColumnHandle paimonColumnHandle)) {
                        throw new IllegalStateException("Paimon table handle requires PaimonColumnHandle, got: "
                                + column.getClass().getName());
                    }
                    return paimonColumnHandle;
                })
                .toList();
    }

    public PaimonTableHandle copy(OptionalLong limit)
    {
        return new PaimonTableHandle(
                schemaName,
                tableName,
                dynamicOptions,
                filter,
                projectedColumns,
                writeColumns,
                limit,
                Optional.empty(),
                createTableOperation,
                dynamicBucketAssignerParallelism,
                keyDynamicBootstrapSnapshotPlanned,
                keyDynamicBootstrapSnapshot);
    }

    public PaimonTableHandle withDeletePartitionSpecs(List<Map<String, String>> deletePartitionSpecs)
    {
        requireNonNull(deletePartitionSpecs, "deletePartitionSpecs is null");
        return new PaimonTableHandle(
                schemaName,
                tableName,
                dynamicOptions,
                filter,
                projectedColumns,
                writeColumns,
                limit,
                Optional.of(deletePartitionSpecs),
                createTableOperation,
                dynamicBucketAssignerParallelism,
                keyDynamicBootstrapSnapshotPlanned,
                keyDynamicBootstrapSnapshot);
    }

    PaimonTableHandle withKeyDynamicBootstrapSnapshot(OptionalLong snapshot)
    {
        requireNonNull(snapshot, "snapshot is null");
        return new PaimonTableHandle(
                schemaName,
                tableName,
                dynamicOptions,
                filter,
                projectedColumns,
                writeColumns,
                limit,
                deletePartitionSpecs,
                createTableOperation,
                dynamicBucketAssignerParallelism,
                true,
                snapshot);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PaimonTableHandle that = (PaimonTableHandle) o;
        return Objects.equals(dynamicOptions, that.dynamicOptions) && Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName) && Objects.equals(filter, that.filter)
                && Objects.equals(projectedColumns, that.projectedColumns)
                && Objects.equals(writeColumns, that.writeColumns)
                && Objects.equals(limit, that.limit)
                && Objects.equals(deletePartitionSpecs, that.deletePartitionSpecs)
                && Objects.equals(createTableOperation, that.createTableOperation)
                && Objects.equals(dynamicBucketAssignerParallelism, that.dynamicBucketAssignerParallelism)
                && keyDynamicBootstrapSnapshotPlanned == that.keyDynamicBootstrapSnapshotPlanned
                && Objects.equals(keyDynamicBootstrapSnapshot, that.keyDynamicBootstrapSnapshot);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                schemaName,
                tableName,
                filter,
                projectedColumns,
                writeColumns,
                limit,
                deletePartitionSpecs,
                createTableOperation,
                dynamicBucketAssignerParallelism,
                keyDynamicBootstrapSnapshotPlanned,
                keyDynamicBootstrapSnapshot,
                dynamicOptions);
    }
}
