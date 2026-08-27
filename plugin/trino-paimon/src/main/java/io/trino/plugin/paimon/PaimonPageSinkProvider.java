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

import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.plugin.paimon.format.TrinoPaimonFileFormat;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.crosspartition.GlobalIndexAssigner;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.index.HashBucketAssigner;
import org.apache.paimon.index.SimpleHashBucketAssigner;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.BatchWriteBuilderImpl;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.paimon.ClassLoaderUtils.runWithContextClassLoader;
import static io.trino.plugin.paimon.PaimonDynamicBucketUtils.dynamicBucketAssignerParallelism;
import static io.trino.plugin.paimon.PaimonDynamicBucketUtils.dynamicBucketNumAssigners;
import static io.trino.plugin.paimon.PaimonDynamicBucketUtils.keyDynamicAssignerParallelism;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_WRITER_DATA_ERROR;
import static io.trino.plugin.paimon.PaimonLongUtils.saturatedAdd;
import static java.util.Arrays.fill;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.utils.DefaultValueUtils.convertDefaultValue;

public class PaimonPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final PaimonCatalog paimonCatalog;
    private final TypeManager typeManager;
    private final Supplier<IOManager> ioManagerFactory;
    private final IntSupplier dynamicBucketWorkerCountSupplier;

    @Inject
    public PaimonPageSinkProvider(
            PaimonMetadataFactory paimonMetadataFactory,
            PaimonConfig config,
            NodeManager nodeManager,
            PaimonConnectorStats stats)
    {
        this(paimonMetadataFactory, () -> createIoManager(requireNonNull(config, "config is null")
                .getWriteSpillPath()), createWorkerNodeCountSupplier(requireNonNull(nodeManager, "nodeManager is null")), stats);
    }

    private static IntSupplier createWorkerNodeCountSupplier(NodeManager nodeManager)
    {
        try {
            // Resolve eagerly: the supplier may be invoked on a worker node where
            // the NodeManager cannot list all nodes.
            int workerCount = nodeManager.getRequiredWorkerNodes().size();
            return () -> workerCount;
        }
        catch (UnsupportedOperationException | TrinoException e) {
            // Worker nodes cannot list all nodes; default to 1 (this node).
            return () -> 1;
        }
    }

    public PaimonPageSinkProvider(PaimonMetadataFactory paimonMetadataFactory, PaimonConfig config)
    {
        this(paimonMetadataFactory, () -> createIoManager(requireNonNull(config, "config is null")
                .getWriteSpillPath()), () -> 1, new PaimonConnectorStats());
    }

    public PaimonPageSinkProvider(PaimonMetadataFactory paimonMetadataFactory)
    {
        this(paimonMetadataFactory, new PaimonConfig());
    }

    PaimonPageSinkProvider(PaimonMetadataFactory paimonMetadataFactory, Supplier<IOManager> ioManagerFactory)
    {
        this(paimonMetadataFactory, ioManagerFactory, () -> 1, new PaimonConnectorStats());
    }

    PaimonPageSinkProvider(
            PaimonMetadataFactory paimonMetadataFactory,
            Supplier<IOManager> ioManagerFactory,
            IntSupplier dynamicBucketWorkerCountSupplier,
            PaimonConnectorStats stats)
    {
        requireNonNull(paimonMetadataFactory, "trinoMetadataFactory is null");
        this.paimonCatalog = paimonMetadataFactory.create().catalog();
        this.typeManager = paimonMetadataFactory.typeManager();
        this.ioManagerFactory = requireNonNull(ioManagerFactory, "ioManagerFactory is null");
        this.dynamicBucketWorkerCountSupplier = requireNonNull(
                dynamicBucketWorkerCountSupplier,
                "dynamicBucketWorkerCountSupplier is null");
        requireNonNull(stats, "stats is null");
    }

    static void validateWriteBucketMode(Table table)
    {
        BucketMode mode = requireFileStoreTable(table, "writes").bucketMode();
        switch (mode) {
            case HASH_FIXED, HASH_DYNAMIC, KEY_DYNAMIC, BUCKET_UNAWARE -> {}
            default -> throw PaimonTableSupport.unsupportedBucketMode("writes", mode);
        }
    }

    static void validateMergeBucketMode(Table table)
    {
        BucketMode mode = requireFileStoreTable(table, "merge writes").bucketMode();
        switch (mode) {
            case HASH_FIXED, HASH_DYNAMIC, KEY_DYNAMIC -> {}
            default -> throw PaimonTableSupport.unsupportedBucketMode("merge writes", mode);
        }
    }

    private static FileStoreTable requireFileStoreTable(Table table, String operation)
    {
        return PaimonTableSupport.requireFileStoreTable(table, operation);
    }

    static FileStoreTable latestFileStoreTable(Table table, String operation)
    {
        return requireFileStoreTable(table, operation).copyWithLatestSchema();
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        requireNonNull(session, "session is null");
        return createOutputPageSink(getOutputTableHandle(outputTableHandle), session, pageSinkId);
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        requireNonNull(session, "session is null");
        return createInsertPageSink(getInsertTableHandle(insertTableHandle), session, pageSinkId);
    }

    private ConnectorPageSink createOutputPageSink(
            PaimonTableHandle tableHandle,
            ConnectorSession session,
            ConnectorPageSinkId pageSinkId)
    {
        requireNonNull(session, "session is null");
        List<PaimonColumnHandle> writeColumns = getWriteColumns(tableHandle);
        return runWithContextClassLoader(() -> {
            Catalog catalog = paimonCatalog.forSession(session);
            FileStoreTable table = latestFileStoreTable(
                    tableHandle.tableWithWriteDynamicOptions(catalog),
                    "writes");
            validateWriteBucketMode(table);
            validateWriteColumns(table, writeColumns);
            return createPageSink(
                    table,
                    false,
                    writeLayout(table, writeColumns, typeManager),
                    pageSinkId,
                    tableHandle.getDynamicBucketAssignerParallelism(),
                    session.getQueryId(),
                    tableHandle);
        }, PaimonPageSinkProvider.class.getClassLoader());
    }

    private ConnectorPageSink createInsertPageSink(
            PaimonTableHandle tableHandle,
            ConnectorSession session,
            ConnectorPageSinkId pageSinkId)
    {
        requireNonNull(session, "session is null");
        List<PaimonColumnHandle> writeColumns = getWriteColumns(tableHandle);
        return runWithContextClassLoader(() -> {
            Catalog catalog = paimonCatalog.forSession(session);
            FileStoreTable table = latestFileStoreTable(
                    tableHandle.tableWithWriteDynamicOptions(catalog),
                    "writes");
            validateWriteBucketMode(table);
            validateWriteColumns(table, writeColumns);
            boolean overwrite = PaimonSessionProperties.enableInsertOverwrite(session);
            if (overwrite) {
                PaimonTableSupport.validateInsertOverwrite(table);
            }
            return createPageSink(
                    table,
                    overwrite,
                    writeLayout(table, writeColumns, typeManager),
                    pageSinkId,
                    tableHandle.getDynamicBucketAssignerParallelism(),
                    session.getQueryId(),
                    tableHandle);
        }, PaimonPageSinkProvider.class.getClassLoader());
    }

    private ConnectorPageSink createMergePageSink(
            PaimonTableHandle tableHandle,
            ConnectorSession session,
            ConnectorPageSinkId pageSinkId)
    {
        requireNonNull(session, "session is null");
        List<PaimonColumnHandle> writeColumns = getWriteColumns(tableHandle);
        return runWithContextClassLoader(() -> {
            Catalog catalog = paimonCatalog.forSession(session);
            FileStoreTable table = latestFileStoreTable(
                    tableHandle.tableWithWriteDynamicOptions(catalog),
                    "merge writes");
            validateMergeBucketMode(table);
            PaimonTableSupport.validateRowLevelDelete(table, "merge writes");
            validateMergeWriteColumns(table, writeColumns);
            return createPageSink(
                    table,
                    false,
                    writeLayout(table, writeColumns, typeManager),
                    pageSinkId,
                    tableHandle.getDynamicBucketAssignerParallelism(),
                    session.getQueryId(),
                    tableHandle);
        }, PaimonPageSinkProvider.class.getClassLoader());
    }

    static List<PaimonColumnHandle> getWriteColumns(PaimonTableHandle tableHandle)
    {
        return requireNonNull(tableHandle, "tableHandle is null").getWriteColumns()
                .orElseThrow(() -> new IllegalStateException("Paimon page sink requires explicit write columns"))
                .stream()
                .map(PaimonPageSinkProvider::getWriteColumn)
                .collect(Collectors.toList());
    }

    static PaimonTableHandle getOutputTableHandle(ConnectorOutputTableHandle outputTableHandle)
    {
        if (!(requireNonNull(outputTableHandle, "outputTableHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon create table page sink requires PaimonTableHandle, got: "
                    + outputTableHandle.getClass().getName());
        }
        return paimonTableHandle;
    }

    static PaimonTableHandle getInsertTableHandle(ConnectorInsertTableHandle insertTableHandle)
    {
        if (!(requireNonNull(insertTableHandle, "insertTableHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon insert page sink requires PaimonTableHandle, got: "
                    + insertTableHandle.getClass().getName());
        }
        return paimonTableHandle;
    }

    private static PaimonColumnHandle getWriteColumn(ColumnHandle column)
    {
        if (!(requireNonNull(column, "writeColumns contains null column") instanceof PaimonColumnHandle paimonColumnHandle)) {
            throw new IllegalStateException("Paimon page sink requires PaimonColumnHandle, got: "
                    + column.getClass().getName());
        }
        return paimonColumnHandle;
    }

    static void validateWriteColumns(FileStoreTable table, List<PaimonColumnHandle> writeColumns)
    {
        validateWriteColumnsAndGetLatestFields(table, writeColumns);
    }

    private static LatestFields validateWriteColumnsAndGetLatestFields(FileStoreTable table, List<PaimonColumnHandle> writeColumns)
    {
        requireNonNull(table, "table is null");
        requireNonNull(writeColumns, "writeColumns is null");
        if (writeColumns.isEmpty()) {
            throw new IllegalStateException("Paimon page sink requires non-empty write columns");
        }
        List<DataField> fields = table.rowType().getFields();
        Map<String, Integer> fieldIndexes = latestFieldIndexes(fields);
        Set<String> seenColumnNames = new HashSet<>();
        for (PaimonColumnHandle column : writeColumns) {
            requireNonNull(column, "writeColumns contains null column");
            String columnName = column.getColumnName();
            String lowerColumnName = FieldNameUtils.toLowerCase(columnName);
            if (!seenColumnNames.add(lowerColumnName)) {
                throw new IllegalStateException("Write column '%s' appears more than once".formatted(columnName));
            }
            DataField latestField = latestField(fields, fieldIndexes, columnName);
            if (!latestField.type().equals(column.logicalType())) {
                throw new IllegalStateException("Write column '%s' type %s does not match latest Paimon table schema type %s"
                        .formatted(columnName, column.logicalType().asSQLString(), latestField.type().asSQLString()));
            }
        }
        return new LatestFields(fields, fieldIndexes);
    }

    static WriteLayout writeLayout(FileStoreTable table, List<PaimonColumnHandle> writeColumns, TypeManager typeManager)
    {
        requireNonNull(table, "table is null");
        requireNonNull(writeColumns, "writeColumns is null");
        requireNonNull(typeManager, "typeManager is null");
        LatestFields latestFields = validateWriteColumnsAndGetLatestFields(table, writeColumns);
        List<DataField> fields = latestFields.fields();
        int[] inputChannels = new int[fields.size()];
        fill(inputChannels, -1);
        for (int inputChannel = 0; inputChannel < writeColumns.size(); inputChannel++) {
            PaimonColumnHandle column = requireNonNull(writeColumns.get(inputChannel), "writeColumns contains null column");
            int fieldIndex = latestFieldIndex(fields, latestFields.fieldIndexes(), column.getColumnName());
            inputChannels[fieldIndex] = inputChannel;
        }
        for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
            DataField field = fields.get(fieldIndex);
            if (inputChannels[fieldIndex] < 0 && field.defaultValue() == null && !field.type().isNullable()) {
                throw new TrinoException(PAIMON_WRITER_DATA_ERROR,
                        "Write column '%s' is missing, has no default value, and latest Paimon table schema type %s is not nullable"
                                .formatted(field.name(), paimonTypeName(field.type())));
            }
        }
        return new WriteLayout(
                fields.stream()
                        .map(field -> PaimonTypeUtils.fromPaimonType(field.type(), typeManager))
                        .collect(Collectors.toList()),
                fields.stream()
                        .map(DataField::type)
                        .collect(Collectors.toList()),
                inputChannels,
                fields.stream()
                        .map(PaimonPageSinkProvider::defaultValue)
                        .toArray());
    }

    private static Object defaultValue(DataField field)
    {
        String defaultValue = field.defaultValue();
        if (defaultValue == null) {
            return null;
        }
        try {
            return convertDefaultValue(field.type(), defaultValue);
        }
        catch (RuntimeException e) {
            throw new TrinoException(
                    PAIMON_WRITER_DATA_ERROR,
                    "Failed to convert Paimon default value for column '%s' with Paimon type %s"
                            .formatted(field.name(), paimonTypeName(field.type())),
                    e);
        }
    }

    private static String paimonTypeName(DataType type)
    {
        try {
            String name = type.asSQLString();
            if (name != null && !name.isBlank()) {
                return name;
            }
        }
        catch (RuntimeException ignored) {
            // Fall through to toString/class name while already formatting a default-value conversion failure.
        }
        try {
            String name = type.toString();
            if (name != null && !name.isBlank()) {
                return name;
            }
        }
        catch (RuntimeException ignored) {
            // Fall through to implementation class name.
        }
        return type.getClass().getName();
    }

    record WriteLayout(List<Type> columnTypes, List<DataType> logicalTypes, int[] inputChannels, Object[] defaultValues)
    {
        WriteLayout
        {
            columnTypes = List.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
            logicalTypes = List.copyOf(requireNonNull(logicalTypes, "logicalTypes is null"));
            inputChannels = requireNonNull(inputChannels, "inputChannels is null").clone();
            defaultValues = requireNonNull(defaultValues, "defaultValues is null").clone();
        }

        @Override
        public int[] inputChannels()
        {
            return inputChannels.clone();
        }

        @Override
        public Object[] defaultValues()
        {
            return defaultValues.clone();
        }
    }

    private record LatestFields(List<DataField> fields, Map<String, Integer> fieldIndexes)
    {
        private LatestFields
        {
            requireNonNull(fields, "fields is null");
            requireNonNull(fieldIndexes, "fieldIndexes is null");
        }
    }

    private static DataField latestField(List<DataField> fields, Map<String, Integer> fieldIndexes, String columnName)
    {
        return fields.get(latestFieldIndex(fields, fieldIndexes, columnName));
    }

    private static int latestFieldIndex(List<DataField> fields, Map<String, Integer> fieldIndexes, String columnName)
    {
        String lowerColumnName = FieldNameUtils.toLowerCase(columnName);
        Integer fieldIndex = fieldIndexes.get(lowerColumnName);
        if (fieldIndex == null) {
            throw new IllegalStateException("Write column '%s' is not present in latest Paimon table schema %s"
                    .formatted(columnName, fields.stream().map(DataField::name).collect(Collectors.toList())));
        }
        return fieldIndex;
    }

    static Map<String, Integer> latestFieldIndexes(List<DataField> fields)
    {
        requireNonNull(fields, "fields is null");
        Map<String, Integer> indexes = new LinkedHashMap<>();
        for (int index = 0; index < fields.size(); index++) {
            DataField field = requireNonNull(fields.get(index), "fields contains null field");
            String lowerFieldName = FieldNameUtils.toLowerCase(field.name());
            if (indexes.putIfAbsent(lowerFieldName, index) != null) {
                throw new IllegalStateException(
                        "Latest Paimon table schema contains case-insensitive duplicate field name '%s'"
                                .formatted(lowerFieldName));
            }
        }
        return Collections.unmodifiableMap(indexes);
    }

    static void validateNoCaseInsensitiveDuplicateFieldNames(List<DataField> fields)
    {
        latestFieldIndexes(fields);
    }

    static void validateMergeWriteColumns(FileStoreTable table, List<PaimonColumnHandle> writeColumns)
    {
        validateWriteColumns(table, writeColumns);
        List<String> latestFieldNames = table.rowType().getFieldNames();
        List<String> writeColumnNames = writeColumns.stream()
                .map(PaimonColumnHandle::getColumnName)
                .collect(Collectors.toList());
        if (!writeColumnNames.equals(latestFieldNames)) {
            throw new IllegalStateException("Merge write columns %s must match latest Paimon table schema columns %s"
                    .formatted(writeColumnNames, latestFieldNames));
        }
    }

    private PaimonPageSink createPageSink(
            FileStoreTable table,
            boolean overwrite,
            WriteLayout writeLayout,
            ConnectorPageSinkId pageSinkId,
            OptionalInt dynamicBucketAssignerParallelism,
            String queryId,
            PaimonTableHandle tableHandle)
    {
        BatchTableWrite write = null;
        IOManager ioManager = null;
        try {
            validateTrinoManagedFileFormatWriteType(table);
            BatchWriteBuilder batchWriteBuilder = table.newBatchWriteBuilder();
            enableKeyDynamicConflictCheck(table, batchWriteBuilder);
            if (overwrite) {
                batchWriteBuilder.withOverwrite();
            }
            write = batchWriteBuilder.newWrite();
            ioManager = requireNonNull(ioManagerFactory.get(), "ioManagerFactory returned null");
            write.withIOManager(ioManager);
            MemoryPoolFactory memoryPoolFactory = memoryPoolFactory(table);
            write.withMemoryPoolFactory(memoryPoolFactory);
            if (table.bucketMode() == BucketMode.HASH_DYNAMIC) {
                return new PaimonPageSink(
                        write,
                        writeLayout.columnTypes(),
                        writeLayout.logicalTypes(),
                        writeLayout.inputChannels(),
                        writeLayout.defaultValues(),
                        dynamicBucketWriter(
                                table,
                                overwrite,
                                pageSinkId,
                                dynamicBucketAssignerParallelism,
                                dynamicBucketWorkerCountSupplier.getAsInt()),
                        memoryPoolFactory,
                        ioManager);
            }
            if (table.bucketMode() == BucketMode.KEY_DYNAMIC) {
                PaimonPageSink.KeyDynamicWriter keyDynamicWriter = null;
                try {
                    keyDynamicWriter = keyDynamicWriter(
                            table,
                            write,
                            ioManager,
                            pageSinkId,
                            dynamicBucketAssignerParallelism,
                            dynamicBucketWorkerCountSupplier.getAsInt(),
                            queryId,
                            PaimonKeyDynamicBootstrap.snapshotFor(tableHandle),
                            tableHandle.getCreateTableOperation().isPresent());
                    return new PaimonPageSink(
                            write,
                            writeLayout.columnTypes(),
                            writeLayout.logicalTypes(),
                            writeLayout.inputChannels(),
                            writeLayout.defaultValues(),
                            null,
                            keyDynamicWriter,
                            memoryPoolFactory,
                            ioManager,
                            keyDynamicMemoryUsage(table));
                }
                catch (Exception e) {
                    RuntimeException failure = PaimonPageSink.wrapWriteException(e);
                    if (keyDynamicWriter != null) {
                        try {
                            keyDynamicWriter.abort();
                        }
                        catch (Exception closeFailure) {
                            failure.addSuppressed(PaimonPageSink.wrapWriterCloseException(closeFailure));
                        }
                    }
                    throw failure;
                }
            }
            return new PaimonPageSink(
                    write,
                    writeLayout.columnTypes(),
                    writeLayout.logicalTypes(),
                    writeLayout.inputChannels(),
                    writeLayout.defaultValues(),
                    null,
                    memoryPoolFactory,
                    ioManager);
        }
        catch (Exception e) {
            RuntimeException failure = PaimonPageSink.wrapWriteException(e);
            if (write != null) {
                failure = PaimonPageSink.closeWriter(write, failure);
            }
            failure = PaimonPageSink.closeIoManager(ioManager, failure);
            throw failure;
        }
    }

    private static void enableKeyDynamicConflictCheck(FileStoreTable table, BatchWriteBuilder batchWriteBuilder)
    {
        if (batchWriteBuilder instanceof BatchWriteBuilderImpl builder && table.bucketMode() == BucketMode.KEY_DYNAMIC) {
            builder.appendCommitCheckConflict(true);
        }
    }

    static PaimonPageSink.KeyDynamicWriter keyDynamicWriter(
            FileStoreTable table,
            BatchTableWrite writer,
            IOManager ioManager,
            ConnectorPageSinkId pageSinkId,
            OptionalInt plannedAssignerParallelism,
            int workerCount)
            throws Exception
    {
        return keyDynamicWriter(
                table,
                writer,
                ioManager,
                pageSinkId,
                plannedAssignerParallelism,
                workerCount,
                "legacy-key-dynamic-writer",
                PaimonKeyDynamicBootstrap.OptionalSnapshot.unpinned(),
                false);
    }

    static PaimonPageSink.KeyDynamicWriter keyDynamicWriter(
            FileStoreTable table,
            BatchTableWrite writer,
            IOManager ioManager,
            ConnectorPageSinkId pageSinkId,
            OptionalInt plannedAssignerParallelism,
            int workerCount,
            String queryId,
            PaimonKeyDynamicBootstrap.OptionalSnapshot expectedSnapshot,
            boolean emptyCreateTable)
            throws Exception
    {
        requireNonNull(table, "table is null");
        requireNonNull(writer, "writer is null");
        requireNonNull(ioManager, "ioManager is null");
        CoreOptions coreOptions = table.coreOptions();
        int assignerParallelism = requireNonNull(plannedAssignerParallelism, "plannedAssignerParallelism is null")
                .orElseGet(() -> keyDynamicAssignerParallelism(coreOptions, workerCount));
        checkArgument(workerCount > 0, "workerCount must be positive: %s", workerCount);
        // When plannedAssignerParallelism is set from the coordinator, it has already been validated
        // against the actual worker count. Skip the worker count check on worker nodes where the
        // NodeManager may not be available (workerCount defaults to 1).
        if (plannedAssignerParallelism.isEmpty()) {
            checkArgument(assignerParallelism <= workerCount,
                    "Paimon KEY_DYNAMIC assigner parallelism %s exceeds available workers %s",
                    assignerParallelism,
                    workerCount);
        }
        int assignId = pageSinkTaskPartitionId(pageSinkId);
        if (assignId >= assignerParallelism) {
            throw new IllegalStateException(
                    "Paimon KEY_DYNAMIC writer task partition %s is outside assigner parallelism %s"
                            .formatted(assignId, assignerParallelism));
        }

        PaimonKeyDynamicBootstrap.Artifact bootstrapArtifact = null;
        PaimonKeyDynamicBootstrap.KeyFingerprintWriter keyFingerprintWriter = null;
        GlobalIndexAssigner assigner = new GlobalIndexAssigner(table);
        RowPartitionKeyExtractor keyExtractor = new RowPartitionKeyExtractor(table.schema());
        PaimonPageSink.KeyDynamicWriter keyDynamicWriter = null;
        try {
            // A newly created CTAS table has no snapshot and therefore no existing keys to bootstrap.
            // Any table with existing state must have been planned by the coordinator and publish a
            // shared artifact; workers must never independently scan it.
            if (!emptyCreateTable && (expectedSnapshot.pinned() || PaimonKeyDynamicBootstrap.latestSnapshot(table).isPresent())) {
                bootstrapArtifact = PaimonKeyDynamicBootstrap.open(table, queryId, expectedSnapshot, assignerParallelism);
            }
            if (expectedSnapshot.pinned()) {
                keyFingerprintWriter = PaimonKeyDynamicBootstrap.openKeyFingerprintWriter(
                        table, queryId, expectedSnapshot, assignerParallelism, assignId, pageSinkId.getId());
            }
            keyDynamicWriter = new PaimonPageSink.KeyDynamicWriter(
                    writer,
                    assigner,
                    expectedSnapshot.pinned() ? keyExtractor : null,
                    keyFingerprintWriter);
            assigner.open(0L, MoreExecutors.newDirectExecutorService(), ioManager, assignerParallelism, assignId, keyDynamicWriter::writeAssignedRow);
            if (bootstrapArtifact != null) {
                try (PaimonKeyDynamicBootstrap.ShardReader reader = bootstrapArtifact.openShard(assignId)) {
                    InternalRow row;
                    while ((row = reader.next()) != null) {
                        assigner.bootstrapKey(row);
                    }
                }
            }
            assigner.endBoostrap(true);
            return keyDynamicWriter;
        }
        catch (Exception e) {
            if (keyDynamicWriter != null) {
                try {
                    keyDynamicWriter.abort();
                }
                catch (Exception closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            else {
                try {
                    assigner.close();
                }
                catch (Exception closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            if (keyDynamicWriter == null && keyFingerprintWriter != null) {
                keyFingerprintWriter.abort();
            }
            throw e;
        }
    }

    static long keyDynamicMemoryUsage(FileStoreTable table)
    {
        requireNonNull(table, "table is null");
        long blockCacheSize = table.coreOptions().lookupCacheMaxMemory().getBytes();
        // GlobalIndexAssigner owns a local KV cache and two bootstrap buffers outside the
        // normal Paimon writer memory pool. Reserve the configured cache plus one write-buffer
        // budget so Trino does not undercount native/index memory during bootstrap.
        return saturatedAdd(
                blockCacheSize,
                table.coreOptions().writeBufferSize(),
                "Paimon KEY_DYNAMIC memory reservation");
    }

    private static void validateTrinoManagedFileFormatWriteType(FileStoreTable table)
    {
        CoreOptions coreOptions = table.coreOptions();
        if (coreOptions.rowTrackingEnabled()) {
            return;
        }
        String fileFormat = coreOptions.fileFormatString();
        if (CoreOptions.FILE_FORMAT_PARQUET.equals(fileFormat) || CoreOptions.FILE_FORMAT_ORC.equals(fileFormat)) {
            TrinoPaimonFileFormat.validateWriteType(fileFormat, table.rowType());
        }
    }

    static IOManager createIoManager(String writeSpillPath)
    {
        return new IOManagerImpl(PaimonWriteSpillPaths.split(writeSpillPath));
    }

    private static MemoryPoolFactory memoryPoolFactory(FileStoreTable table)
    {
        CoreOptions coreOptions = table.coreOptions();
        return new MemoryPoolFactory(new HeapMemorySegmentPool(
                coreOptions.writeBufferSize(),
                coreOptions.pageSize()));
    }

    static PaimonPageSink.DynamicBucketWriter dynamicBucketWriter(
            FileStoreTable table,
            boolean overwrite,
            ConnectorPageSinkId pageSinkId,
            int workerCount)
    {
        return dynamicBucketWriter(table, overwrite, pageSinkId, OptionalInt.empty(), workerCount);
    }

    static PaimonPageSink.DynamicBucketWriter dynamicBucketWriter(
            FileStoreTable table,
            boolean overwrite,
            ConnectorPageSinkId pageSinkId,
            OptionalInt plannedAssignerParallelism,
            int workerCount)
    {
        CoreOptions coreOptions = table.coreOptions();
        int assignerParallelism = requireNonNull(plannedAssignerParallelism, "plannedAssignerParallelism is null")
                .orElseGet(() -> dynamicBucketAssignerParallelism(coreOptions, workerCount));
        checkArgument(workerCount > 0, "workerCount must be positive: %s", workerCount);
        // When plannedAssignerParallelism is set from the coordinator, it has already been validated
        // against the actual worker count. Skip the worker count check on worker nodes where the
        // NodeManager may not be available (workerCount defaults to 1).
        if (plannedAssignerParallelism.isEmpty()) {
            checkArgument(assignerParallelism <= workerCount,
                    "Paimon HASH_DYNAMIC assigner parallelism %s exceeds available workers %s",
                    assignerParallelism,
                    workerCount);
        }
        int numAssigners = dynamicBucketNumAssigners(coreOptions, assignerParallelism);
        int assignId = pageSinkTaskPartitionId(pageSinkId);
        if (assignId >= assignerParallelism) {
            throw new IllegalStateException(
                    "Paimon HASH_DYNAMIC writer task partition %s is outside assigner parallelism %s"
                            .formatted(assignId, assignerParallelism));
        }
        BucketAssigner bucketAssigner;
        if (overwrite) {
            bucketAssigner = new SimpleHashBucketAssigner(
                    assignerParallelism,
                    assignId,
                    coreOptions.dynamicBucketTargetRowNum(),
                    coreOptions.dynamicBucketMaxBuckets());
        }
        else {
            Options options = new Options(table.options());
            bucketAssigner = new HashBucketAssigner(
                    table.store().snapshotManager(),
                    CoreOptions.createCommitUser(options),
                    table.store().newIndexFileHandler(),
                    assignerParallelism,
                    numAssigners,
                    assignId,
                    coreOptions.dynamicBucketTargetRowNum(),
                    coreOptions.dynamicBucketMaxBuckets());
        }
        return new PaimonPageSink.DynamicBucketWriter(new RowPartitionKeyExtractor(table.schema()), bucketAssigner);
    }

    static int pageSinkTaskPartitionId(ConnectorPageSinkId pageSinkId)
    {
        long id = requireNonNull(pageSinkId, "pageSinkId is null").getId();
        return (int) ((id >>> 8) & 0x00FF_FFFFL);
    }

    @Override
    public ConnectorMergeSink createMergeSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorMergeTableHandle mergeHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        requireNonNull(session, "session is null");
        PaimonMergeTableHandle paimonMergeTableHandle = getPaimonMergeTableHandle(mergeHandle);
        if (paimonMergeTableHandle.isMetadataDeleteFallback()) {
            return new PaimonMetadataDeleteMergeSink();
        }
        PaimonTableHandle paimonTableHandle = paimonMergeTableHandle.paimonTableHandle();
        int dataColumnCount = getWriteColumns(paimonTableHandle).size();
        return runWithContextClassLoader(() -> new PaimonMergeSink(
                createMergePageSink(paimonTableHandle, session, pageSinkId),
                dataColumnCount), PaimonPageSinkProvider.class.getClassLoader());
    }

    static PaimonTableHandle getMergeTableHandle(ConnectorMergeTableHandle mergeHandle)
    {
        return getPaimonMergeTableHandle(mergeHandle).paimonTableHandle();
    }

    static PaimonMergeTableHandle getPaimonMergeTableHandle(ConnectorMergeTableHandle mergeHandle)
    {
        ConnectorTableHandle tableHandle = requireNonNull(mergeHandle, "mergeHandle is null").getTableHandle();
        if (!(requireNonNull(tableHandle, "mergeHandle tableHandle is null") instanceof PaimonTableHandle)) {
            throw new IllegalStateException("Paimon merge sink requires PaimonTableHandle, got: "
                    + tableHandle.getClass().getName());
        }
        if (!(mergeHandle instanceof PaimonMergeTableHandle paimonMergeTableHandle)) {
            throw new IllegalStateException("Paimon merge sink requires PaimonMergeTableHandle, got: "
                    + mergeHandle.getClass().getName());
        }
        return paimonMergeTableHandle;
    }
}
