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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcRecordReader;
import io.trino.orc.TupleDomainOrcPredicate;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ParquetTypeUtils;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.TransformConnectorPageSource;
import io.trino.plugin.hive.orc.OrcPageSource;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.IndexFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.RowType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource;
import static io.trino.plugin.paimon.ClassLoaderUtils.runWithContextClassLoader;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_BAD_DATA;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_CURSOR_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.fileindex.FileIndexOptions.topLevelIndexOfNested;

public class PaimonPageSourceProvider
        implements ConnectorPageSourceProvider
{
    static final int EMPTY_PROJECTION_MAX_PAGE_SIZE = DEFAULT_MAX_PAGE_SIZE_IN_BYTES / SIZE_OF_LONG;

    private final TrinoFileSystemFactory fileSystemFactory;
    private final PaimonCatalog paimonCatalog;
    private final OrcReaderOptions orcReaderOptions;
    private final ParquetReaderOptions parquetReaderOptions;
    private final Supplier<IOManager> ioManagerFactory;

    @Inject
    public PaimonPageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            PaimonMetadataFactory paimonMetadataFactory,
            OrcReaderConfig orcReaderConfig,
            ParquetReaderConfig parquetReaderConfig,
            PaimonConfig config)
    {
        this(fileSystemFactory, paimonMetadataFactory, orcReaderConfig, parquetReaderConfig,
                () -> PaimonPageSinkProvider.createIoManager(requireNonNull(config, "config is null")
                        .getWriteSpillPath()));
    }

    public PaimonPageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            PaimonMetadataFactory paimonMetadataFactory,
            OrcReaderConfig orcReaderConfig,
            ParquetReaderConfig parquetReaderConfig)
    {
        this(fileSystemFactory,
                paimonMetadataFactory,
                orcReaderConfig,
                parquetReaderConfig,
                () -> PaimonPageSinkProvider.createIoManager(new PaimonConfig().getWriteSpillPath()));
    }

    PaimonPageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            PaimonMetadataFactory paimonMetadataFactory,
            OrcReaderConfig orcReaderConfig,
            ParquetReaderConfig parquetReaderConfig,
            Supplier<IOManager> ioManagerFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.paimonCatalog = requireNonNull(paimonMetadataFactory, "trinoMetadataFactory is null").create().catalog();
        this.orcReaderOptions = requireNonNull(orcReaderConfig, "orcReaderConfig is null").toOrcReaderOptions()
                // Default tiny stripe size 8 M is too big for paimon.
                // Cache stripe will cause more read (I want to read one column,
                // but not the whole stripe)
                .withTinyStripeThreshold(DataSize.of(4, DataSize.Unit.KILOBYTE));
        this.parquetReaderOptions = requireNonNull(parquetReaderConfig, "parquetReaderConfig is null").toParquetReaderOptions();
        this.ioManagerFactory = requireNonNull(ioManagerFactory, "ioManagerFactory is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle tableHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            MemoryContext memoryContext)
    {
        requireNonNull(session, "session is null");
        requireNonNull(dynamicFilter, "dynamicFilter is null");
        PaimonTableHandle paimonTableHandle = getTableHandle(tableHandle);
        PaimonSplit paimonSplit = getSplit(split);
        List<PaimonColumnHandle> paimonColumns = getColumnHandles(columns);
        TupleDomain<PaimonColumnHandle> effectiveFilter = effectiveFilter(paimonTableHandle, dynamicFilter);
        if (effectiveFilter.isNone()) {
            return emptyPageSource();
        }
        Catalog catalog = paimonCatalog.forSession(session);
        Table table = paimonTableHandle.tableWithDynamicOptions(catalog, session);
        boolean refreshToLatestSchema = !paimonTableHandle.usesHistoricalReadSchema(session);
        return runWithContextClassLoader(() -> {
            Optional<PaimonColumnHandle> rowId = rowIdColumn(paimonColumns);
            if (rowId.isPresent()) {
                List<PaimonColumnHandle> dataColumns = paimonColumns.stream()
                        .filter(column -> !column.isRowId()).collect(Collectors.toList());
                List<String> rowIdFields = rowIdFieldNames(rowId.get().getTrinoType());
                RowIdReadColumns rowIdReadColumns = rowIdReadColumns(rowId.get(), dataColumns, rowIdFields);
                return PaimonMergePageSourceWrapper.wrap(
                        createPageSource(
                                session,
                                paimonTableHandle,
                                table,
                                effectiveFilter,
                                paimonSplit,
                                rowIdReadColumns.readColumns(),
                                paimonTableHandle.getLimit(),
                                refreshToLatestSchema),
                        rowIdFields,
                        rowIdReadColumns.fieldToIndex(),
                        rowIdReadColumns.outputChannels());
            }
            else {
                return createPageSource(
                        session,
                        paimonTableHandle,
                        table,
                        effectiveFilter,
                        paimonSplit,
                        paimonColumns,
                        paimonTableHandle.getLimit(),
                        refreshToLatestSchema);
            }
        }, PaimonPageSourceProvider.class.getClassLoader());
    }

    static TupleDomain<PaimonColumnHandle> effectiveFilter(PaimonTableHandle tableHandle, DynamicFilter dynamicFilter)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(dynamicFilter, "dynamicFilter is null");
        return PaimonSplitManager.effectivePredicate(tableHandle, dynamicFilter);
    }

    static Optional<PaimonColumnHandle> rowIdColumn(List<PaimonColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");
        List<PaimonColumnHandle> rowIdColumns = columns.stream()
                .map(column -> requireNonNull(column, "columns contains null column"))
                .filter(PaimonColumnHandle::isRowId)
                .toList();
        if (rowIdColumns.size() > 1) {
            throw new IllegalStateException("Paimon page source expected at most one row id column, got: "
                    + rowIdColumns.size());
        }
        return rowIdColumns.stream().findFirst();
    }

    static PaimonTableHandle getTableHandle(ConnectorTableHandle tableHandle)
    {
        if (!(requireNonNull(tableHandle, "tableHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon page source requires PaimonTableHandle, got: "
                    + tableHandle.getClass().getName());
        }
        return paimonTableHandle;
    }

    static PaimonSplit getSplit(ConnectorSplit split)
    {
        if (!(requireNonNull(split, "split is null") instanceof PaimonSplit paimonSplit)) {
            throw new IllegalStateException("Paimon page source requires PaimonSplit, got: "
                    + split.getClass().getName());
        }
        return paimonSplit;
    }

    static List<PaimonColumnHandle> getColumnHandles(List<? extends ColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");
        return columns.stream()
                .map(column -> {
                    if (!(requireNonNull(column, "columns contains null column") instanceof PaimonColumnHandle paimonColumnHandle)) {
                        throw new IllegalStateException("Paimon page source requires PaimonColumnHandle, got: "
                                + column.getClass().getName());
                    }
                    return paimonColumnHandle;
                })
                .toList();
    }

    static List<String> rowIdFieldNames(Type rowIdType)
    {
        requireNonNull(rowIdType, "rowIdType is null");
        if (!(rowIdType instanceof io.trino.spi.type.RowType trinoRowIdType)) {
            throw new IllegalArgumentException("Paimon row id column must be ROW, got: "
                    + rowIdType.getDisplayName());
        }
        List<String> rowIdFields = new ArrayList<>();
        Set<String> seenFields = new HashSet<>();
        for (int index = 0; index < trinoRowIdType.getFields().size(); index++) {
            int fieldIndex = index;
            io.trino.spi.type.RowType.Field field = trinoRowIdType.getFields().get(index);
            String fieldName = field.getName()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Paimon row id field at index %s must be named".formatted(fieldIndex)));
            if (fieldName.isBlank()) {
                throw new IllegalArgumentException("Paimon row id field at index %s is blank".formatted(fieldIndex));
            }
            if (!seenFields.add(fieldName)) {
                throw new IllegalArgumentException("Paimon row id field '%s' appears more than once".formatted(fieldName));
            }
            rowIdFields.add(fieldName);
        }
        return List.copyOf(rowIdFields);
    }

    static RowIdReadColumns rowIdReadColumns(
            PaimonColumnHandle rowIdColumn,
            List<PaimonColumnHandle> dataColumns,
            List<String> rowIdFields)
    {
        requireNonNull(rowIdColumn, "rowIdColumn is null");
        requireNonNull(dataColumns, "dataColumns is null");
        requireNonNull(rowIdFields, "rowIdFields is null");
        List<PaimonColumnHandle> readColumns = new ArrayList<>(dataColumns);
        Set<String> rowIdFieldSet = Set.copyOf(rowIdFields);
        HashMap<String, Integer> fieldToIndex = new HashMap<>();
        for (int i = 0; i < dataColumns.size(); i++) {
            PaimonColumnHandle paimonColumnHandle = requireNonNull(
                    dataColumns.get(i),
                    "dataColumns contains null column");
            if (rowIdFieldSet.contains(paimonColumnHandle.getColumnName())) {
                fieldToIndex.putIfAbsent(paimonColumnHandle.getColumnName(), i);
            }
        }
        for (String rowIdField : rowIdFields) {
            requireNonNull(rowIdField, "rowIdFields contains null field");
            if (PaimonMergePageSourceWrapper.METADATA_DELETE_ROW_ID_FIELD.equals(rowIdField)
                    || fieldToIndex.containsKey(rowIdField)) {
                continue;
            }
            fieldToIndex.put(rowIdField, readColumns.size());
            readColumns.add(rowIdFieldColumn(rowIdColumn, rowIdField));
        }
        int[] outputChannels = new int[dataColumns.size()];
        for (int i = 0; i < outputChannels.length; i++) {
            outputChannels[i] = i;
        }
        return new RowIdReadColumns(readColumns, fieldToIndex, outputChannels);
    }

    private static PaimonColumnHandle rowIdFieldColumn(PaimonColumnHandle rowIdColumn, String rowIdField)
    {
        if (!(rowIdColumn.logicalType() instanceof RowType rowIdLogicalType)) {
            throw new IllegalArgumentException("Paimon row id logical type must be ROW, got: "
                    + rowIdColumn.logicalType().asSQLString());
        }
        if (!(rowIdColumn.getTrinoType() instanceof io.trino.spi.type.RowType trinoRowIdType)) {
            throw new IllegalArgumentException("Paimon row id Trino type must be ROW, got: "
                    + rowIdColumn.getTrinoType().getDisplayName());
        }
        List<String> logicalFieldNames = rowIdLogicalType.getFieldNames();
        int fieldIndex = logicalFieldNames.indexOf(rowIdField);
        if (fieldIndex < 0) {
            throw new IllegalArgumentException("Paimon row id field '%s' is not present in row id logical type"
                    .formatted(rowIdField));
        }
        return PaimonColumnHandle.of(
                rowIdField,
                rowIdLogicalType.getTypeAt(fieldIndex),
                trinoRowIdType.getFields().get(fieldIndex).getType());
    }

    record RowIdReadColumns(
            List<PaimonColumnHandle> readColumns,
            Map<String, Integer> fieldToIndex,
            int[] outputChannels)
    {
        RowIdReadColumns
        {
            readColumns = List.copyOf(requireNonNull(readColumns, "readColumns is null"));
            fieldToIndex = Map.copyOf(requireNonNull(fieldToIndex, "fieldToIndex is null"));
            outputChannels = requireNonNull(outputChannels, "outputChannels is null").clone();
        }

        @Override
        public int[] outputChannels()
        {
            return outputChannels.clone();
        }
    }

    private ConnectorPageSource createPageSource(
            ConnectorSession session,
            PaimonTableHandle tableHandle,
            Table table,
            TupleDomain<PaimonColumnHandle> filter,
            PaimonSplit split,
            List<PaimonColumnHandle> columns,
            OptionalLong limit,
            boolean refreshToLatestSchema)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        if (filter.isNone()) {
            return emptyPageSource();
        }

        List<String> projectedFields = columns.stream().map(PaimonColumnHandle::getColumnName).toList();
        TupleDomain<PaimonColumnHandle> readerFilter = readerFilter(filter);

        try {
            Split paimonSplit = split.decodeSplit();
            Optional<List<RawFile>> optionalRawFiles = paimonSplit.convertToRawFiles();
            if (checkRawFile(tableHandle, optionalRawFiles, columns, filter)) {
                List<String> partitionKeys = table.partitionKeys();
                if (!directReaderSupportsFilter(projectedFields, partitionKeys, filter)) {
                    return createPaimonReaderPageSource(
                            table,
                            refreshToLatestSchema,
                            readerFilter,
                            paimonSplit,
                            columns,
                            limit,
                            projectedFields);
                }
                List<RawFile> files = optionalRawFiles.orElseThrow();
                if (projectedFields.isEmpty()) {
                    return createEmptyProjectionRawFilePageSource(
                            table,
                            refreshToLatestSchema,
                            paimonSplit,
                            files,
                            limit);
                }

                DirectReadTableContext directReadTableContext = directReadTableContext(
                        table,
                        filter,
                        refreshToLatestSchema);
                FileStoreTable fileStoreTable = directReadTableContext.table();
                RowType rowType = directReadTableContext.rowType();
                boolean readIndex = fileStoreTable.coreOptions().fileIndexReadEnabled();
                List<Domain> filterDomains = orderDomains(projectedFields, filter);
                List<Domain> noPredicateDomains = Collections.nCopies(projectedFields.size(), null);

                Optional<List<DeletionFile>> deletionFiles = paimonSplit.deletionFiles();
                Optional<List<IndexFile>> indexFiles = readIndex ? paimonSplit.indexFiles() : Optional.empty();
                Optional<Predicate> fileIndexFilter = directReadTableContext.fileIndexFilter();

                try {
                    validateAlignedMetadataFiles("indexFiles", indexFiles, files.size());
                    validateAlignedMetadataFiles("deletionFiles", deletionFiles, files.size());
                    // Raw-file predicate pushdown can change row positions before the deletion vector is applied.
                    if (requiresPaimonReaderForDeletionVectorFilter(filter, deletionFiles, partitionKeys)) {
                        return createPaimonReaderPageSource(
                                table,
                                refreshToLatestSchema,
                                readerFilter,
                                paimonSplit,
                                columns,
                                limit,
                                projectedFields);
                    }

                    SchemaManager schemaManager = new SchemaManager(fileStoreTable.fileIO(), fileStoreTable.location());
                    long tableSchemaId = fileStoreTable.schema().id();
                    List<DataField> tableFields = rowType.getFields();
                    Map<Long, DirectReadSchemaPlan> schemaPlans = new HashMap<>();
                    schemaPlans.put(tableSchemaId, directReadSchemaPlan(
                            projectedFields,
                            filterDomains,
                            tableFields,
                            tableFields));
                    List<Type> type = columns.stream().map(PaimonColumnHandle::getTrinoType)
                            .collect(Collectors.toList());
                    TrinoFileSystem fileSystem = fileSystemFactory.create(session);
                    List<Supplier<ConnectorPageSource>> sources = new ArrayList<>(files.size());

                    // if file index exists, do the filter.
                    for (int i = 0; i < files.size(); i++) {
                        RawFile rawFile = files.get(i);
                        if (indexFiles.isPresent()) {
                            IndexFile indexFile = indexFiles.get().get(i);
                            if (indexFile != null && fileIndexFilter.isPresent()) {
                                try (FileIndexPredicate fileIndexPredicate = new FileIndexPredicate(
                                        new Path(indexFile.path()), fileStoreTable.fileIO(), rowType)) {
                                    if (!fileIndexPredicate.evaluate(fileIndexFilter.get()).remain()) {
                                        continue;
                                    }
                                }
                            }
                        }

                        Optional<DeletionFile> deletionFile = deletionFileAt(deletionFiles, i);
                        long fileSchemaId = rawFile.schemaId();
                        DirectReadSchemaPlan schemaPlan = schemaPlans.get(fileSchemaId);
                        if (schemaPlan == null) {
                            schemaPlan = directReadSchemaPlan(
                                    projectedFields,
                                    filterDomains,
                                    tableFields,
                                    schemaManager.schema(fileSchemaId).fields());
                            schemaPlans.put(fileSchemaId, schemaPlan);
                        }

                        if (!schemaPlan.directReaderSupported()) {
                            return createPaimonReaderPageSource(
                                    table,
                                    refreshToLatestSchema,
                                    readerFilter,
                                    paimonSplit,
                                    columns,
                                    limit,
                                    projectedFields);
                        }
                        if (schemaPlan.skipFile()) {
                            continue;
                        }

                        DirectReadSchemaPlan fileSchemaPlan = schemaPlan;
                        Supplier<ConnectorPageSource> sourceSupplier = () -> {
                            ConnectorPageSource source = createDataPageSource(
                                    rawFile.format(),
                                    rawFileInputFile(fileSystem, rawFile),
                                    fileSchemaPlan.dataFileColumns(),
                                    type,
                                    directReaderDomains(filterDomains, noPredicateDomains, deletionFile.isPresent()));

                            return wrapWithDeletionVector(source, fileStoreTable, deletionFile);
                        };
                        sources.add(sourceSupplier);
                    }

                    return DirectTrinoPageSource.lazyPageSources(sources, limit);
                }
                catch (Exception e) {
                    throw wrapPaimonReadException(e);
                }
            }
            return createPaimonReaderPageSource(
                    table,
                    refreshToLatestSchema,
                    readerFilter,
                    paimonSplit,
                    columns,
                    limit,
                    projectedFields);
        }
        catch (Exception e) {
            throw wrapPaimonReadException(e);
        }
    }

    private ConnectorPageSource createPaimonReaderPageSource(
            Table table,
            boolean refreshToLatestSchema,
            TupleDomain<PaimonColumnHandle> readerFilter,
            Split paimonSplit,
            List<PaimonColumnHandle> columns,
            OptionalLong limit,
            List<String> projectedFields)
            throws IOException
    {
        Table readTable = PaimonTableHandle.schemaAwareReadTable(table, refreshToLatestSchema);
        RowType rowType = PaimonTableHandle.effectiveReadRowType(readTable);
        List<String> fieldNames = rowType.getFieldNames();
        Optional<Predicate> paimonFilter = new PaimonFilterConverter(rowType).convert(readerFilter);
        List<String> readFields = readerFields(fieldNames, projectedFields, readerFilter);
        int[] columnIndex = projectionIndexes(fieldNames, readFields);
        RowType paimonReadType = isIdentityProjection(columnIndex, fieldNames.size())
                ? rowType
                : rowType.project(columnIndex);
        ReadBuilder read = readTable.newReadBuilder();
        paimonFilter.ifPresent(read::withFilter);
        if (!readTable.rowType().equals(paimonReadType)) {
            read.withReadType(paimonReadType);
        }

        IOManager ioManager = requireNonNull(ioManagerFactory.get(), "ioManagerFactory returned null");
        RecordReader<InternalRow> reader;
        try {
            TableRead tableRead = read.newRead().withIOManager(ioManager).executeFilter();
            reader = tableRead.createReader(paimonSplit);
        }
        catch (IOException | RuntimeException | Error e) {
            closeAllSuppress(e, ioManager);
            throw e;
        }
        return new PaimonPageSource(reader, columns, limit, List.of(ioManager));
    }

    static List<String> readerFields(List<String> fieldNames, List<String> projectedFields, TupleDomain<PaimonColumnHandle> readerFilter)
    {
        requireNonNull(fieldNames, "fieldNames is null");
        requireNonNull(projectedFields, "projectedFields is null");
        requireNonNull(readerFilter, "readerFilter is null");
        List<String> readFields = new ArrayList<>(projectedFields);
        Set<String> readFieldNames = readFields.stream()
                .map(field -> requireNonNull(field, "projectedFields contains null field"))
                .map(FieldNameUtils::toLowerCase)
                .collect(Collectors.toCollection(HashSet::new));
        if (readerFilter.isNone() || readerFilter.isAll()) {
            return List.copyOf(readFields);
        }

        Set<String> requiredFilterFields = readerFilter.getDomains()
                .orElseThrow(() -> new IllegalStateException("Expected reader filter domains for non-trivial TupleDomain"))
                .keySet().stream()
                .map(PaimonColumnHandle::getColumnName)
                .map(PaimonPageSourceProvider::requiredProjectedFieldName)
                .collect(Collectors.toSet());
        fieldNames.stream()
                .map(field -> requireNonNull(field, "fieldNames contains null field"))
                .filter(field -> requiredFilterFields.contains(FieldNameUtils.toLowerCase(field)))
                .filter(field -> readFieldNames.add(FieldNameUtils.toLowerCase(field)))
                .forEach(readFields::add);
        return List.copyOf(readFields);
    }

    static TupleDomain<PaimonColumnHandle> readerFilter(TupleDomain<PaimonColumnHandle> filter)
    {
        requireNonNull(filter, "filter is null");
        return PaimonRowRangeExtractor.removeRowIdPredicate(filter);
    }

    static boolean directReaderSupportsFilter(List<String> projectedFields, TupleDomain<PaimonColumnHandle> filter)
    {
        return directReaderSupportsFilter(projectedFields, List.of(), filter);
    }

    static boolean directReaderSupportsFilter(
            List<String> projectedFields,
            List<String> partitionKeys,
            TupleDomain<PaimonColumnHandle> filter)
    {
        requireNonNull(projectedFields, "projectedFields is null");
        requireNonNull(partitionKeys, "partitionKeys is null");
        requireNonNull(filter, "filter is null");
        if (filter.isAll()) {
            return true;
        }
        if (filter.isNone()) {
            return false;
        }

        Set<String> projectedFieldNames = projectedFields.stream()
                .map(field -> requireNonNull(field, "projectedFields contains null field"))
                .map(FieldNameUtils::toLowerCase)
                .collect(Collectors.toSet());
        Set<String> partitionKeyNames = partitionKeys.stream()
                .map(key -> requireNonNull(key, "partitionKeys contains null key"))
                .map(FieldNameUtils::toLowerCase)
                .collect(Collectors.toSet());

        return filter.getDomains()
                .orElseThrow(() -> new IllegalStateException("Expected filter domains for non-trivial TupleDomain"))
                .keySet().stream()
                .map(PaimonColumnHandle::getColumnName)
                .map(PaimonPageSourceProvider::requiredProjectedFieldName)
                .allMatch(field -> projectedFieldNames.contains(field) || partitionKeyNames.contains(field));
    }

    private static String requiredProjectedFieldName(String fieldName)
    {
        requireNonNull(fieldName, "fieldName is null");
        return topLevelIndexOfNested(fieldName)
                .map(index -> FieldNameUtils.toLowerCase(fieldName.substring(0, index)))
                .orElseGet(() -> FieldNameUtils.toLowerCase(fieldName));
    }

    static boolean canSkipDirectReadFile(List<String> dataFileColumns, List<Domain> filterDomains, List<DataField> dataSchemaFields)
    {
        requireNonNull(dataFileColumns, "dataFileColumns is null");
        requireNonNull(filterDomains, "filterDomains is null");
        requireNonNull(dataSchemaFields, "dataSchemaFields is null");
        if (dataFileColumns.size() != filterDomains.size()) {
            throw new IllegalArgumentException("filterDomains count (%s) must match dataFileColumns count (%s)"
                    .formatted(filterDomains.size(), dataFileColumns.size()));
        }

        Set<String> dataFieldNames = new HashSet<>();
        for (DataField field : dataSchemaFields) {
            requireNonNull(field, "dataSchemaFields contains null field");
            String lowerFieldName = FieldNameUtils.toLowerCase(field.name());
            if (!dataFieldNames.add(lowerFieldName)) {
                throw new IllegalStateException("Paimon data file schema contains case-insensitive duplicate field name '%s'"
                        .formatted(lowerFieldName));
            }
        }
        List<String> lowercaseDataFileColumns = new ArrayList<>(dataFileColumns.size());
        for (String dataFileColumn : dataFileColumns) {
            if (dataFileColumn == null) {
                lowercaseDataFileColumns.add(null);
            }
            else {
                lowercaseDataFileColumns.add(FieldNameUtils.toLowerCase(dataFileColumn));
            }
        }
        return canSkipDirectReadFile(lowercaseDataFileColumns, filterDomains, dataFieldNames);
    }

    // Callers pass lower-case data file column names to avoid repeated normalization
    // while planning direct reads.
    private static boolean canSkipDirectReadFile(List<String> dataFileColumns, List<Domain> filterDomains, Set<String> dataFieldNames)
    {
        requireNonNull(dataFileColumns, "dataFileColumns is null");
        requireNonNull(filterDomains, "filterDomains is null");
        requireNonNull(dataFieldNames, "dataFieldNames is null");
        if (dataFileColumns.size() != filterDomains.size()) {
            throw new IllegalArgumentException("filterDomains count (%s) must match dataFileColumns count (%s)"
                    .formatted(filterDomains.size(), dataFileColumns.size()));
        }
        for (int index = 0; index < dataFileColumns.size(); index++) {
            Domain domain = filterDomains.get(index);
            if (domain == null) {
                continue;
            }
            String dataFileColumn = dataFileColumns.get(index);
            if ((dataFileColumn == null || !dataFieldNames.contains(dataFileColumn))
                    && !domain.includesNullableValue(null)) {
                return true;
            }
        }
        return false;
    }

    static boolean requiresPaimonReaderForDeletionVectorFilter(
            TupleDomain<PaimonColumnHandle> filter,
            Optional<List<DeletionFile>> deletionFiles)
    {
        return requiresPaimonReaderForDeletionVectorFilter(filter, deletionFiles, List.of());
    }

    static boolean requiresPaimonReaderForDeletionVectorFilter(
            TupleDomain<PaimonColumnHandle> filter,
            Optional<List<DeletionFile>> deletionFiles,
            List<String> partitionKeys)
    {
        requireNonNull(filter, "filter is null");
        requireNonNull(deletionFiles, "deletionFiles is null");
        requireNonNull(partitionKeys, "partitionKeys is null");
        if (filter.isAll() || filter.isNone() || deletionFiles.isEmpty()) {
            return false;
        }
        if (deletionFiles.orElseThrow().stream().noneMatch(deletionFile -> deletionFile != null)) {
            return false;
        }
        Set<String> partitionKeyNames = partitionKeys.stream()
                .map(key -> requireNonNull(key, "partitionKeys contains null key"))
                .map(FieldNameUtils::toLowerCase)
                .collect(Collectors.toSet());
        return filter.getDomains()
                .orElseThrow(() -> new IllegalStateException("Expected filter domains for non-trivial TupleDomain"))
                .keySet().stream()
                .map(PaimonColumnHandle::getColumnName)
                .map(PaimonPageSourceProvider::requiredProjectedFieldName)
                .anyMatch(field -> !partitionKeyNames.contains(field));
    }

    static DirectReadSchemaPlan directReadSchemaPlan(
            List<String> projectedFields,
            List<Domain> filterDomains,
            List<DataField> tableFields,
            List<DataField> dataFields)
    {
        requireNonNull(projectedFields, "projectedFields is null");
        requireNonNull(filterDomains, "filterDomains is null");
        requireNonNull(tableFields, "tableFields is null");
        requireNonNull(dataFields, "dataFields is null");

        // Paimon stores column names in lowercase in ORC/Parquet files. For schema evolution,
        // resolve current table fields to historical data-file field names by stable field id.
        Map<String, DataField> tableFieldsByName = tableFieldsByLowercaseName(tableFields);
        DirectReadDataSchemaFields dataSchemaFields = directReadDataSchemaFields(dataFields);
        List<String> dataFileColumns = new ArrayList<>(projectedFields.size());
        boolean directReaderSupported = true;
        for (String projectedField : projectedFields) {
            String lowerFieldName = FieldNameUtils.toLowerCase(projectedField);
            DataField tableField = tableFieldsByName.get(lowerFieldName);
            if (tableField == null) {
                throw new IllegalStateException("Projected field '%s' does not exist in current Paimon table fields %s"
                        .formatted(projectedField, tableFields.stream().map(DataField::name).toList()));
            }
            DataField dataField = dataSchemaFields.fieldById().get(tableField.id());
            if (dataField == null) {
                if (dataSchemaFields.fieldIdByName().containsKey(lowerFieldName)) {
                    // A same-name field with a different ID belongs to an old dropped column.
                    dataFileColumns.add(null);
                }
                else {
                    dataFileColumns.add(lowerFieldName);
                }
                if (tableField.defaultValue() != null || !tableField.type().isNullable()) {
                    directReaderSupported = false;
                }
                continue;
            }

            dataFileColumns.add(FieldNameUtils.toLowerCase(dataField.name()));
            if (!dataField.type().equalsIgnoreNullable(tableField.type())) {
                directReaderSupported = false;
            }
        }
        boolean skipFile = directReaderSupported
                && canSkipDirectReadFile(dataFileColumns, filterDomains, dataSchemaFields.fieldNames());
        return new DirectReadSchemaPlan(dataFields, dataFileColumns, directReaderSupported, skipFile);
    }

    record DirectReadSchemaPlan(
            List<DataField> dataSchemaFields,
            List<String> dataFileColumns,
            boolean directReaderSupported,
            boolean skipFile)
    {
        DirectReadSchemaPlan
        {
            dataSchemaFields = List.copyOf(requireNonNull(dataSchemaFields, "dataSchemaFields is null"));
            dataFileColumns = Collections.unmodifiableList(new ArrayList<>(
                    requireNonNull(dataFileColumns, "dataFileColumns is null")));
        }
    }

    static boolean directReaderSupportsSchemaEvolution(
            List<String> projectedFields,
            List<DataField> tableFields,
            List<DataField> dataFields)
    {
        requireNonNull(projectedFields, "projectedFields is null");
        Map<String, DataField> tableFieldByName = tableFieldsByLowercaseName(tableFields);
        Map<Integer, DataField> dataFieldById = dataFieldsById(dataFields);

        for (String projectedField : projectedFields) {
            DataField tableField = tableFieldByName.get(FieldNameUtils.toLowerCase(projectedField));
            if (tableField == null) {
                throw new IllegalStateException("Projected field '%s' does not exist in current Paimon table fields %s"
                        .formatted(projectedField, tableFields.stream().map(DataField::name).toList()));
            }
            DataField dataField = dataFieldById.get(tableField.id());
            if (dataField == null) {
                if (tableField.defaultValue() != null || !tableField.type().isNullable()) {
                    return false;
                }
                continue;
            }
            if (!dataField.type().equalsIgnoreNullable(tableField.type())) {
                return false;
            }
        }
        return true;
    }

    private static Map<String, DataField> tableFieldsByLowercaseName(List<DataField> tableFields)
    {
        requireNonNull(tableFields, "tableFields is null");
        Map<String, DataField> tableFieldByName = new HashMap<>();
        for (DataField field : tableFields) {
            requireNonNull(field, "tableFields contains null field");
            String lowerName = FieldNameUtils.toLowerCase(field.name());
            DataField previous = tableFieldByName.putIfAbsent(lowerName, field);
            if (previous != null) {
                throw new IllegalStateException("Current Paimon table schema contains case-insensitive duplicate field name '%s'"
                        .formatted(lowerName));
            }
        }
        return tableFieldByName;
    }

    private static Map<Integer, DataField> dataFieldsById(List<DataField> dataFields)
    {
        requireNonNull(dataFields, "dataFields is null");
        Map<Integer, DataField> dataFieldById = new HashMap<>();
        for (DataField field : dataFields) {
            requireNonNull(field, "dataFields contains null field");
            DataField previous = dataFieldById.putIfAbsent(field.id(), field);
            if (previous != null) {
                throw new IllegalStateException("Paimon data file schema contains duplicate field id %s"
                        .formatted(field.id()));
            }
        }
        return dataFieldById;
    }

    private static DirectReadDataSchemaFields directReadDataSchemaFields(List<DataField> dataFields)
    {
        requireNonNull(dataFields, "dataFields is null");
        Map<String, Integer> fieldIdByName = new HashMap<>();
        Map<Integer, DataField> fieldById = new HashMap<>();
        for (DataField field : dataFields) {
            requireNonNull(field, "dataFields contains null field");
            String lowerName = FieldNameUtils.toLowerCase(field.name());
            Integer previousId = fieldIdByName.putIfAbsent(lowerName, field.id());
            if (previousId != null) {
                throw new IllegalStateException("Paimon data file schema contains case-insensitive duplicate field name '%s'"
                        .formatted(lowerName));
            }
            DataField previousField = fieldById.putIfAbsent(field.id(), field);
            if (previousField != null) {
                throw new IllegalStateException("Paimon data file schema contains duplicate field id %s"
                        .formatted(field.id()));
            }
        }
        return new DirectReadDataSchemaFields(fieldIdByName, fieldById);
    }

    private record DirectReadDataSchemaFields(Map<String, Integer> fieldIdByName, Map<Integer, DataField> fieldById)
    {
        private DirectReadDataSchemaFields
        {
            fieldIdByName = Map.copyOf(requireNonNull(fieldIdByName, "fieldIdByName is null"));
            fieldById = Map.copyOf(requireNonNull(fieldById, "fieldById is null"));
        }

        private Set<String> fieldNames()
        {
            return fieldIdByName.keySet();
        }
    }

    private static final class EmptyProjectionPageSource
            implements ConnectorPageSource
    {
        private final long rowCount;
        private long completedPositions;
        private boolean closed;

        private EmptyProjectionPageSource(long rowCount)
        {
            if (rowCount < 0) {
                throw new IllegalArgumentException("rowCount is negative: " + rowCount);
            }
            this.rowCount = rowCount;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public OptionalLong getCompletedPositions()
        {
            return OptionalLong.of(completedPositions);
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return closed || completedPositions == rowCount;
        }

        @Override
        public SourcePage getNextSourcePage()
        {
            if (isFinished()) {
                close();
                return null;
            }
            int pageSize = toIntExact(min(EMPTY_PROJECTION_MAX_PAGE_SIZE, rowCount - completedPositions));
            completedPositions += pageSize;
            return SourcePage.create(new Page(pageSize));
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    static DirectReadTableContext directReadTableContext(
            Table table,
            TupleDomain<PaimonColumnHandle> filter,
            boolean refreshToLatestSchema)
    {
        requireNonNull(filter, "filter is null");
        FileStoreTable fileStoreTable = fileStoreTableForDirectRead(table, refreshToLatestSchema);
        RowType rowType = fileStoreTable.rowType();
        return new DirectReadTableContext(
                fileStoreTable,
                rowType,
                new PaimonFilterConverter(rowType).convertForFileIndex(filter));
    }

    record DirectReadTableContext(FileStoreTable table, RowType rowType, Optional<Predicate> fileIndexFilter)
    {
        DirectReadTableContext
        {
            requireNonNull(table, "table is null");
            requireNonNull(rowType, "rowType is null");
            requireNonNull(fileIndexFilter, "fileIndexFilter is null");
        }
    }

    static RuntimeException wrapPaimonReadException(Exception exception)
    {
        return wrapPaimonReadException(
                "Failed to open or read Paimon split",
                "Paimon page read uses features which are not supported by the Trino connector",
                exception);
    }

    static RuntimeException wrapPaimonReadException(String message, Exception exception)
    {
        return wrapPaimonReadException(message, message, exception);
    }

    private static RuntimeException wrapPaimonReadException(
            String cannotOpenSplitMessage,
            String unsupportedReadMessage,
            Exception exception)
    {
        requireNonNull(exception, "exception is null");
        Throwable readFailure = firstRecognizedReadFailure(exception);
        if (readFailure instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (readFailure instanceof UnsupportedOperationException unsupportedOperationException) {
            return unsupportedReadException(unsupportedReadMessage, unsupportedOperationException);
        }
        if (readFailure instanceof OrcCorruptionException || readFailure instanceof ParquetCorruptionException) {
            return new TrinoException(PAIMON_BAD_DATA, readFailure);
        }
        if (readFailure instanceof UncheckedIOException uncheckedIOException) {
            return cannotOpenSplitException(cannotOpenSplitMessage, uncheckedIOException.getCause());
        }
        if (readFailure instanceof IOException ioException) {
            return cannotOpenSplitException(cannotOpenSplitMessage, ioException);
        }
        return cannotOpenSplitException(cannotOpenSplitMessage, exception);
    }

    private static Throwable firstRecognizedReadFailure(Throwable exception)
    {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = exception;
        while (current != null && visited.add(current)) {
            if (current instanceof TrinoException ||
                    current instanceof UnsupportedOperationException ||
                    current instanceof OrcCorruptionException ||
                    current instanceof ParquetCorruptionException ||
                    current instanceof UncheckedIOException ||
                    current instanceof IOException) {
                return current;
            }
            current = current.getCause();
        }
        return exception;
    }

    static TrinoException unsupportedReadException(String message, UnsupportedOperationException exception)
    {
        requireNonNull(message, "message is null");
        return new TrinoException(NOT_SUPPORTED, message, requireNonNull(exception, "exception is null"));
    }

    static TrinoException cannotOpenSplitException(String message, Exception exception)
    {
        requireNonNull(message, "message is null");
        return new TrinoException(PAIMON_CANNOT_OPEN_SPLIT, message, requireNonNull(exception, "exception is null"));
    }

    static ConnectorPageSource emptyPageSource()
    {
        return new FixedPageSource(List.of());
    }

    static ConnectorPageSource emptyProjectionPageSource(long rowCount)
    {
        return new EmptyProjectionPageSource(rowCount);
    }

    private static ConnectorPageSource createEmptyProjectionRawFilePageSource(
            Table table,
            boolean refreshToLatestSchema,
            Split paimonSplit,
            List<RawFile> files,
            OptionalLong limit)
    {
        requireNonNull(paimonSplit, "paimonSplit is null");
        requireNonNull(files, "files is null");
        Optional<List<DeletionFile>> deletionFiles = paimonSplit.deletionFiles();
        validateAlignedMetadataFiles("deletionFiles", deletionFiles, files.size());
        FileStoreTable fileStoreTable = fileStoreTableForDirectRead(table, refreshToLatestSchema);
        List<Supplier<ConnectorPageSource>> sources = new ArrayList<>(files.size());
        for (int i = 0; i < files.size(); i++) {
            RawFile rawFile = files.get(i);
            Optional<DeletionFile> deletionFile = deletionFileAt(deletionFiles, i);
            sources.add(() -> wrapWithDeletionVector(
                    emptyProjectionPageSource(rawFile.rowCount()),
                    fileStoreTable,
                    deletionFile));
        }
        return DirectTrinoPageSource.lazyPageSources(sources, limit);
    }

    private static ConnectorPageSource wrapWithDeletionVector(
            ConnectorPageSource source,
            FileStoreTable fileStoreTable,
            Optional<DeletionFile> deletionFile)
    {
        requireNonNull(source, "source is null");
        requireNonNull(fileStoreTable, "fileStoreTable is null");
        requireNonNull(deletionFile, "deletionFile is null");
        if (deletionFile.isEmpty()) {
            return source;
        }
        return PaimonPageSourceWrapper.wrap(source, deletionFile.map(file -> {
            try {
                return DeletionVector.read(fileStoreTable.fileIO(), file);
            }
            catch (IOException e) {
                throw cannotOpenSplitException(
                        "Failed to read deletion vector file: " + file.path(),
                        e);
            }
        }));
    }

    static FileStoreTable fileStoreTableForDirectRead(Table table, boolean refreshToLatestSchema)
    {
        FileStoreTable fileStoreTable = requireFileStoreTableForDirectRead(table);
        if (refreshToLatestSchema) {
            return fileStoreTable.copyWithLatestSchema();
        }
        return fileStoreTable;
    }

    static FileStoreTable requireFileStoreTableForDirectRead(Table table)
    {
        return PaimonTableSupport.requireFileStoreTable(table, "direct raw-file reads");
    }

    static void validateAlignedMetadataFiles(String name, Optional<? extends List<?>> files, int rawFileCount)
    {
        requireNonNull(name, "name is null");
        requireNonNull(files, "files is null");
        if (rawFileCount < 0) {
            throw new IllegalArgumentException("rawFileCount is negative: " + rawFileCount);
        }
        if (files.isPresent() && files.get().size() != rawFileCount) {
            throw new IllegalStateException("%s count (%s) must match raw file count (%s)"
                    .formatted(name, files.get().size(), rawFileCount));
        }
    }

    // make domains(filters) to be ordered by projected fields' order.
    static List<Domain> orderDomains(List<String> projectedFields, TupleDomain<PaimonColumnHandle> filter)
    {
        requireNonNull(projectedFields, "projectedFields is null");
        requireNonNull(filter, "filter is null");
        Optional<Map<PaimonColumnHandle, Domain>> optionalFilter = filter.getDomains();
        Map<String, Domain> domainMap = new HashMap<>();
        optionalFilter.ifPresent(trinoColumnHandleDomainMap -> trinoColumnHandleDomainMap
                .forEach((k, v) -> {
                    String fieldName = FieldNameUtils.toLowerCase(k.getColumnName());
                    Domain previous = domainMap.putIfAbsent(fieldName, v);
                    if (previous != null && !previous.equals(v)) {
                        throw new IllegalStateException("Filter contains conflicting domains for field '%s'"
                                .formatted(fieldName));
                    }
                }));

        return projectedFields.stream()
                .map(FieldNameUtils::toLowerCase)
                .map(name -> domainMap.getOrDefault(name, null))
                .collect(Collectors.toList());
    }

    static List<Domain> directReaderDomains(
            List<String> projectedFields,
            TupleDomain<PaimonColumnHandle> filter,
            boolean hasDeletionVectors)
    {
        requireNonNull(projectedFields, "projectedFields is null");
        requireNonNull(filter, "filter is null");
        if (filter.isNone()) {
            throw new IllegalStateException("Direct raw-file reads must not receive TupleDomain.none()");
        }
        List<Domain> orderedFilterDomains = orderDomains(projectedFields, filter);
        return directReaderDomains(
                orderedFilterDomains,
                Collections.nCopies(orderedFilterDomains.size(), null),
                hasDeletionVectors);
    }

    static List<Domain> directReaderDomains(
            List<Domain> orderedFilterDomains,
            List<Domain> noPredicateDomains,
            boolean hasDeletionVectors)
    {
        requireNonNull(orderedFilterDomains, "orderedFilterDomains is null");
        requireNonNull(noPredicateDomains, "noPredicateDomains is null");
        if (orderedFilterDomains.size() != noPredicateDomains.size()) {
            throw new IllegalArgumentException("noPredicateDomains count (%s) must match orderedFilterDomains count (%s)"
                    .formatted(noPredicateDomains.size(), orderedFilterDomains.size()));
        }
        return hasDeletionVectors ? noPredicateDomains : orderedFilterDomains;
    }

    static Optional<DeletionFile> deletionFileAt(Optional<List<DeletionFile>> deletionFiles, int fileIndex)
    {
        requireNonNull(deletionFiles, "deletionFiles is null");
        if (fileIndex < 0) {
            throw new IllegalArgumentException("fileIndex is negative: " + fileIndex);
        }
        return deletionFiles.flatMap(files -> {
            if (fileIndex >= files.size()) {
                throw new IllegalArgumentException("fileIndex %s is out of range for deletionFiles count %s"
                        .formatted(fileIndex, files.size()));
            }
            return Optional.ofNullable(files.get(fileIndex));
        });
    }

    static int[] projectionIndexes(List<String> fieldNames, List<String> projectedFields)
    {
        requireNonNull(fieldNames, "fieldNames is null");
        requireNonNull(projectedFields, "projectedFields is null");
        Map<String, Integer> fieldIndexes = new HashMap<>();
        for (int index = 0; index < fieldNames.size(); index++) {
            String fieldName = requireNonNull(fieldNames.get(index), "fieldNames contains null field");
            Integer previousIndex = fieldIndexes.putIfAbsent(FieldNameUtils.toLowerCase(fieldName), index);
            if (previousIndex != null) {
                throw new IllegalStateException("Table fields contain case-insensitive duplicate field name '%s': %s"
                        .formatted(fieldName, fieldNames));
            }
        }

        int[] indexes = new int[projectedFields.size()];
        for (int projectedIndex = 0; projectedIndex < projectedFields.size(); projectedIndex++) {
            String projectedField = requireNonNull(projectedFields.get(projectedIndex), "projectedFields contains null field");
            Integer fieldIndex = fieldIndexes.get(FieldNameUtils.toLowerCase(projectedField));
            if (fieldIndex == null) {
                throw new IllegalStateException("Projected field '%s' does not exist in table fields %s"
                        .formatted(projectedField, fieldNames));
            }
            indexes[projectedIndex] = fieldIndex;
        }
        return indexes;
    }

    static boolean isIdentityProjection(int[] projectionIndexes, int fieldCount)
    {
        requireNonNull(projectionIndexes, "projectionIndexes is null");
        if (projectionIndexes.length != fieldCount) {
            return false;
        }
        for (int index = 0; index < projectionIndexes.length; index++) {
            if (projectionIndexes[index] != index) {
                return false;
            }
        }
        return true;
    }

    private boolean checkRawFile(
            PaimonTableHandle tableHandle,
            Optional<List<RawFile>> optionalRawFiles,
            List<? extends ColumnHandle> columns,
            TupleDomain<PaimonColumnHandle> filter)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(filter, "filter is null");
        return optionalRawFiles.isPresent() && canUseTrinoPageSource(tableHandle, optionalRawFiles.get(), columns)
                && PaimonRowRangeExtractor.extractRowIdRanges(filter).isEmpty();
    }

    // Support ORC and Parquet direct reads. Other formats, including Avro, fall back to Paimon's reader.
    static boolean canUseTrinoPageSource(
            PaimonTableHandle tableHandle,
            List<RawFile> rawFiles,
            List<? extends ColumnHandle> columns)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        // Incremental window reads back the system.table_changes contract. Keep them on Paimon's
        // reader path until the raw-file fast path is explicitly validated for those semantics.
        return !tableHandle.hasIncrementalReadMode() && canUseTrinoPageSource(rawFiles, columns);
    }

    // Support ORC and Parquet direct reads. Other formats, including Avro, fall back to Paimon's reader.
    static boolean canUseTrinoPageSource(List<RawFile> rawFiles, List<? extends ColumnHandle> columns)
    {
        requireNonNull(rawFiles, "rawFiles is null");
        if (rawFiles.isEmpty()) {
            return false;
        }
        boolean hasOrcRawFiles = false;
        for (RawFile rawFile : rawFiles) {
            requireNonNull(rawFile, "rawFiles contains null file");
            String format = requireNonNull(rawFile.format(), "rawFiles contains file with null format");
            if (format.isBlank()) {
                throw new IllegalArgumentException("rawFiles contains file with blank format");
            }
            if (!"orc".equalsIgnoreCase(format) && !"parquet".equalsIgnoreCase(format)) {
                return false;
            }
            if (!isWholeRawFile(rawFile)) {
                return false;
            }
            hasOrcRawFiles = hasOrcRawFiles || "orc".equalsIgnoreCase(format);
        }
        for (PaimonColumnHandle paimonColumn : getColumnHandles(columns)) {
            if (isUnsupportedDirectReadColumn(paimonColumn)
                    || containsUnsupportedDirectReadType(paimonColumn.logicalType(), hasOrcRawFiles)) {
                return false;
            }
        }
        for (RawFile rawFile : rawFiles) {
            String path = requireNonNull(rawFile.path(), "rawFiles contains file with null path");
            if (path.isBlank()) {
                throw new IllegalArgumentException("rawFiles contains file with blank path");
            }
        }
        return true;
    }

    static TrinoInputFile rawFileInputFile(TrinoFileSystem fileSystem, RawFile rawFile)
    {
        requireNonNull(fileSystem, "fileSystem is null");
        requireNonNull(rawFile, "rawFile is null");
        return fileSystem.newInputFile(Location.of(rawFile.path()), rawFile.fileSize());
    }

    private static boolean isWholeRawFile(RawFile rawFile)
    {
        return rawFile.fileSize() >= 0
                && rawFile.offset() == 0
                && rawFile.length() == rawFile.fileSize();
    }

    private static boolean isUnsupportedDirectReadColumn(PaimonColumnHandle column)
    {
        requireNonNull(column, "column is null");
        String columnName = column.getColumnName();
        if (PaimonColumnHandle.isHiddenColumnName(columnName)) {
            return true;
        }
        if (columnName.regionMatches(true, 0, SpecialFields.KEY_FIELD_PREFIX, 0, SpecialFields.KEY_FIELD_PREFIX.length())) {
            return true;
        }
        return SpecialFields.SYSTEM_FIELD_NAMES.stream()
                .anyMatch(systemField -> systemField.equalsIgnoreCase(columnName));
    }

    private static boolean containsUnsupportedDirectReadType(DataType type, boolean hasOrcRawFiles)
    {
        return switch (type.getTypeRoot()) {
            case BLOB, VARIANT, VECTOR, MULTISET -> true;
            case ARRAY, MAP, ROW -> DataTypeChecks.getNestedTypes(type).stream()
                    .anyMatch(nestedType -> containsUnsupportedDirectReadType(nestedType, hasOrcRawFiles));
            // Paimon ORC stores TIME as int millis. Trino's ORC TimeType reader only accepts Iceberg-style
            // long time columns, while the Parquet TIME(MILLIS) path performs the millis-to-picos conversion.
            case TIME_WITHOUT_TIME_ZONE -> hasOrcRawFiles;
            case CHAR, VARCHAR, BOOLEAN, BINARY, VARBINARY, DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE,
                 DATE, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> false;
        };
    }

    // map the table schema column names to data schema column names
    // Paimon stores column names in lowercase, so we return lowercase names
    static List<String> currentSchemaFieldNames(List<String> fieldNames, List<DataField> tableFields)
    {
        requireNonNull(fieldNames, "fieldNames is null");
        requireNonNull(tableFields, "tableFields is null");
        return schemaEvolutionFieldNames(fieldNames, tableFields, tableFields);
    }

    static List<String> schemaEvolutionFieldNames(
            List<String> fieldNames,
            List<DataField> tableFields,
            List<DataField> dataFields)
    {
        requireNonNull(fieldNames, "fieldNames is null");
        requireNonNull(tableFields, "tableFields is null");
        requireNonNull(dataFields, "dataFields is null");
        Map<String, Integer> fieldNameToId = new HashMap<>();
        Map<String, Integer> dataFieldNameToId = new HashMap<>();
        Map<Integer, String> idToFieldName = new HashMap<>();
        List<String> result = new ArrayList<>();

        // Build maps: lowercase name -> field ID (from table), field ID -> lowercase field name (from data file)
        tableFields.forEach(field -> {
            requireNonNull(field, "tableFields contains null field");
            String lowerName = FieldNameUtils.toLowerCase(field.name());
            Integer previous = fieldNameToId.putIfAbsent(lowerName, field.id());
            if (previous != null) {
                throw new IllegalStateException("Current Paimon table schema contains case-insensitive duplicate field name '%s'"
                        .formatted(lowerName));
            }
        });
        dataFields.forEach(field -> {
            requireNonNull(field, "dataFields contains null field");
            // Store lowercase field name because Paimon writes files with lowercase column names
            String lowerName = FieldNameUtils.toLowerCase(field.name());
            Integer previousId = dataFieldNameToId.putIfAbsent(lowerName, field.id());
            if (previousId != null) {
                throw new IllegalStateException("Paimon data file schema contains case-insensitive duplicate field name '%s'"
                        .formatted(lowerName));
            }
            String previous = idToFieldName.putIfAbsent(field.id(), lowerName);
            if (previous != null) {
                throw new IllegalStateException("Paimon data file schema contains duplicate field id %s"
                        .formatted(field.id()));
            }
        });

        for (String fieldName : fieldNames) {
            // Convert to lowercase for case-insensitive lookup
            String lowerFieldName = FieldNameUtils.toLowerCase(fieldName);
            Integer id = fieldNameToId.get(lowerFieldName);
            if (id == null) {
                throw new IllegalStateException("Projected field '%s' does not exist in current Paimon table fields %s"
                        .formatted(fieldName, tableFields.stream().map(DataField::name).toList()));
            }
            if (idToFieldName.containsKey(id)) {
                // Return the lowercase field name for file reading
                result.add(idToFieldName.get(id));
            }
            else if (dataFieldNameToId.containsKey(lowerFieldName)) {
                // A same-name field with a different ID belongs to an old dropped column.
                result.add(null);
            }
            else {
                result.add(lowerFieldName);
            }
        }
        return result;
    }

    private ConnectorPageSource createDataPageSource(
            String format,
            TrinoInputFile inputFile,
            List<String> columns,
            List<Type> types,
            List<Domain> domains)
    {
        return createNativeDataPageSource(
                format,
                inputFile,
                columns,
                types,
                domains,
                orcReaderOptions,
                parquetReaderOptions);
    }

    public static ConnectorPageSource createNativeDataPageSource(
            String format,
            TrinoInputFile inputFile,
            List<String> columns,
            List<Type> types,
            List<Domain> domains)
    {
        return createNativeDataPageSource(
                format,
                inputFile,
                columns,
                types,
                domains,
                new OrcReaderOptions().withTinyStripeThreshold(DataSize.of(4, DataSize.Unit.KILOBYTE)),
                ParquetReaderOptions.defaultOptions());
    }

    private static ConnectorPageSource createNativeDataPageSource(
            String format,
            TrinoInputFile inputFile,
            List<String> columns,
            List<Type> types,
            List<Domain> domains,
            OrcReaderOptions orcReaderOptions,
            ParquetReaderOptions parquetReaderOptions)
    {
        validateDirectPageSourceInputs(format, inputFile, columns, types, domains);
        switch (format.toLowerCase(Locale.ENGLISH)) {
            case "orc" -> {
                return createOrcDataPageSource(inputFile, orcReaderOptions, columns, types, domains);
            }
            case "parquet" -> {
                try {
                    return createParquetDataPageSource(
                            inputFile,
                            parquetReaderOptions,
                            columns,
                            types,
                            domains,
                            inputFile.length());
                }
                catch (IOException e) {
                    throw cannotOpenSplitException(
                            "Failed to get file length for Parquet file: " + inputFile.location(),
                            e);
                }
            }
            default -> throw new IllegalArgumentException("Unsupported direct file format: " + format);
        }
    }

    static void validateDirectPageSourceInputs(
            String format,
            TrinoInputFile inputFile,
            List<String> columns,
            List<Type> types,
            List<Domain> domains)
    {
        requireNonNull(format, "format is null");
        if (format.isBlank()) {
            throw new IllegalArgumentException("format is blank");
        }
        requireNonNull(inputFile, "inputFile is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(types, "types is null");
        requireNonNull(domains, "domains is null");
        if (types.size() != columns.size()) {
            throw new IllegalArgumentException("types count (%s) must match columns count (%s)"
                    .formatted(types.size(), columns.size()));
        }
        if (domains.size() != columns.size()) {
            throw new IllegalArgumentException("domains count (%s) must match columns count (%s)"
                    .formatted(domains.size(), columns.size()));
        }
        for (String column : columns) {
            if (column != null && column.isBlank()) {
                throw new IllegalArgumentException("columns contains blank column");
            }
        }
        for (Type type : types) {
            requireNonNull(type, "types contains null type");
        }
    }

    private static ConnectorPageSource createOrcDataPageSource(
            TrinoInputFile inputFile,
            OrcReaderOptions options,
            List<String> columns,
            List<Type> types,
            List<Domain> domains)
    {
        OrcDataSource orcDataSource = null;
        try {
            orcDataSource = new PaimonOrcDataSource(inputFile, options);
            OrcReader reader = OrcReader.createOrcReader(orcDataSource, options)
                    .orElseThrow(() -> new TrinoException(
                            PAIMON_BAD_DATA,
                            "ORC file is zero length: " + inputFile.location()));

            List<OrcColumn> fileColumns = reader.getRootColumn().getNestedColumns();
            // Use case-insensitive map for column name lookup
            Map<String, OrcColumn> fieldsMap = orcFieldsByLowercaseName(fileColumns);
            TupleDomainOrcPredicate.TupleDomainOrcPredicateBuilder predicateBuilder = TupleDomainOrcPredicate.builder();
            TransformConnectorPageSource.Builder transforms = TransformConnectorPageSource.builder();
            List<OrcColumn> fileReadColumns = new ArrayList<>(columns.size());
            List<Type> fileReadTypes = new ArrayList<>(columns.size());

            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i) != null) {
                    OrcColumn orcColumn = fieldsMap.get(FieldNameUtils.toLowerCase(columns.get(i)));
                    if (orcColumn == null) {
                        transforms.constantValue(types.get(i).createNullBlock());
                        continue;
                    }
                    transforms.column(fileReadColumns.size());
                    fileReadColumns.add(orcColumn);
                    fileReadTypes.add(types.get(i));
                    if (domains.get(i) != null) {
                        predicateBuilder.addColumn(orcColumn.getColumnId(), domains.get(i));
                    }
                }
                else {
                    transforms.constantValue(types.get(i).createNullBlock());
                }
            }

            AggregatedMemoryContext memoryUsage = newSimpleAggregatedMemoryContext();
            OrcDataSourceId dataSourceId = orcDataSource.getId();
            OrcRecordReader recordReader = reader.createRecordReader(
                    fileReadColumns,
                    fileReadTypes,
                    false,
                    predicateBuilder.build(),
                    DateTimeZone.UTC,
                    memoryUsage,
                    INITIAL_BATCH_SIZE,
                    exception -> handleOrcException(dataSourceId, exception));

            return transforms.build(new OrcPageSource(
                    recordReader,
                    orcDataSource,
                    Optional.empty(),
                    Optional.empty(),
                    memoryUsage,
                    new FileFormatDataSourceStats(),
                    reader.getCompressionKind()));
        }
        catch (Exception e) {
            closeDataSourceSuppressingException(orcDataSource, e);
            throw wrapPaimonReadException("Failed to create ORC page source for " + inputFile.location(), e);
        }
    }

    private static ConnectorPageSource createParquetDataPageSource(
            TrinoInputFile inputFile,
            ParquetReaderOptions options,
            List<String> columns,
            List<Type> types,
            List<Domain> domains,
            long fileSize)
    {
        ParquetDataSource dataSource = null;
        try {
            AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
            dataSource = createDataSource(
                    inputFile,
                    OptionalLong.of(fileSize),
                    options,
                    memoryContext,
                    new FileFormatDataSourceStats());

            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, options, Optional.empty(), Optional.empty());
            FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            // Build column name to Parquet field mapping (case-insensitive)
            Map<String, org.apache.parquet.schema.Type> fieldsByName = parquetFieldsByLowercaseName(
                    fileSchema.getFields());

            // Build requested schema from requested columns
            List<org.apache.parquet.schema.Type> requestedFields = new ArrayList<>();
            for (String columnName : columns) {
                if (columnName != null) {
                    org.apache.parquet.schema.Type field = fieldsByName.get(FieldNameUtils.toLowerCase(columnName));
                    if (field != null) {
                        requestedFields.add(field);
                    }
                }
            }

            MessageType requestedSchema = new MessageType(fileSchema.getName(), requestedFields);
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);

            // Build predicate for row group filtering
            TupleDomain<ColumnDescriptor> parquetTupleDomain = buildParquetTupleDomain(
                    descriptorsByPath,
                    columns,
                    domains,
                    fieldsByName);
            TupleDomainParquetPredicate parquetPredicate = buildPredicate(
                    requestedSchema,
                    parquetTupleDomain,
                    descriptorsByPath,
                    DateTimeZone.UTC);

            // Filter row groups based on predicate
            List<RowGroupInfo> rowGroups = getFilteredRowGroups(
                    0,
                    fileSize,
                    dataSource,
                    parquetMetadata,
                    ImmutableList.of(parquetTupleDomain),
                    ImmutableList.of(parquetPredicate),
                    descriptorsByPath,
                    DateTimeZone.UTC,
                    100,
                    options);

            // Build ParquetPageSource
            TransformConnectorPageSource.Builder transforms = TransformConnectorPageSource.builder();
            List<Column> parquetColumns = new ArrayList<>();
            int parquetSourceChannel = 0;

            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i);
                Type type = types.get(i);
                String lowerColumnName = columnName == null ? null : FieldNameUtils.toLowerCase(columnName);

                if (lowerColumnName == null || !fieldsByName.containsKey(lowerColumnName)) {
                    parquetSourceChannel = addParquetColumn(
                            columnName,
                            type,
                            Optional.empty(),
                            Optional.empty(),
                            transforms,
                            parquetColumns,
                            parquetSourceChannel);
                }
                else {
                    org.apache.parquet.schema.Type parquetField = fieldsByName.get(lowerColumnName);
                    ColumnIO columnIO = messageColumnIO.getChild(parquetField.getName());

                    // Convert Parquet field to Trino Field
                    Optional<Field> field = constructField(type, columnIO);
                    parquetSourceChannel = addParquetColumn(
                            columnName,
                            type,
                            Optional.of(parquetField.getName()),
                            field,
                            transforms,
                            parquetColumns,
                            parquetSourceChannel);
                }
            }

            ParquetDataSourceId dataSourceId = dataSource.getId();
            ParquetReader parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    parquetColumns,
                    false,
                    rowGroups,
                    dataSource,
                    DateTimeZone.UTC,
                    memoryContext,
                    options,
                    exception -> handleParquetException(dataSourceId, exception),
                    Optional.of(parquetPredicate),
                    Optional.empty(),
                    parquetMetadata.getDecryptionContext());

            return transforms.build(new ParquetPageSource(parquetReader));
        }
        catch (Exception e) {
            closeDataSourceSuppressingException(dataSource, e);
            throw wrapPaimonReadException("Failed to create Parquet page source for " + inputFile.location(), e);
        }
    }

    static int addParquetColumn(
            String columnName,
            Type type,
            Optional<String> parquetFieldName,
            Optional<Field> field,
            TransformConnectorPageSource.Builder transforms,
            List<Column> parquetColumns,
            int parquetSourceChannel)
    {
        requireNonNull(type, "type is null");
        requireNonNull(parquetFieldName, "parquetFieldName is null");
        requireNonNull(field, "field is null");
        requireNonNull(transforms, "transforms is null");
        requireNonNull(parquetColumns, "parquetColumns is null");
        if (parquetSourceChannel < 0) {
            throw new IllegalArgumentException("parquetSourceChannel is negative: " + parquetSourceChannel);
        }
        if (parquetFieldName.isEmpty()) {
            transforms.constantValue(type.createNullBlock());
            return parquetSourceChannel;
        }
        if (field.isEmpty()) {
            throw new IllegalStateException("Parquet file column '%s' exists but cannot be read as %s"
                    .formatted(columnName, type.getDisplayName()));
        }
        parquetColumns.add(new Column(parquetFieldName.get(), field.get()));
        transforms.column(parquetSourceChannel);
        return parquetSourceChannel + 1;
    }

    static TupleDomain<ColumnDescriptor> buildParquetTupleDomain(
            Map<List<String>, ColumnDescriptor> descriptorsByPath,
            List<String> columns,
            List<Domain> domains,
            Map<String, org.apache.parquet.schema.Type> fieldsByName)
    {
        Map<ColumnDescriptor, Domain> predicateDomains = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i) != null && domains.get(i) != null) {
                String columnName = FieldNameUtils.toLowerCase(columns.get(i));
                if (fieldsByName.containsKey(columnName)) {
                    org.apache.parquet.schema.Type parquetType = fieldsByName.get(columnName);
                    if (parquetType.isPrimitive()) {
                        ColumnDescriptor descriptor = descriptorsByPath
                                .get(ImmutableList.of(parquetType.getName()));
                        if (descriptor != null) {
                            Domain previous = predicateDomains.putIfAbsent(descriptor, domains.get(i));
                            if (previous != null && !previous.equals(domains.get(i))) {
                                throw new IllegalStateException("Parquet predicate contains conflicting domains for field '%s'"
                                        .formatted(columnName));
                            }
                        }
                    }
                }
            }
        }
        return TupleDomain.withColumnDomains(predicateDomains);
    }

    static Map<String, OrcColumn> orcFieldsByLowercaseName(List<OrcColumn> columns)
    {
        requireNonNull(columns, "columns is null");
        Map<String, OrcColumn> fieldsByName = new HashMap<>();
        for (OrcColumn column : columns) {
            requireNonNull(column, "columns contains null column");
            String lowerColumnName = FieldNameUtils.toLowerCase(column.getColumnName());
            OrcColumn previous = fieldsByName.putIfAbsent(lowerColumnName, column);
            if (previous != null) {
                throw new IllegalStateException("ORC file schema contains case-insensitive duplicate field name '%s'"
                        .formatted(lowerColumnName));
            }
        }
        return fieldsByName;
    }

    static Map<String, org.apache.parquet.schema.Type> parquetFieldsByLowercaseName(
            List<org.apache.parquet.schema.Type> fields)
    {
        requireNonNull(fields, "fields is null");
        Map<String, org.apache.parquet.schema.Type> fieldsByName = new HashMap<>();
        for (org.apache.parquet.schema.Type field : fields) {
            requireNonNull(field, "fields contains null field");
            String lowerFieldName = FieldNameUtils.toLowerCase(field.getName());
            org.apache.parquet.schema.Type previous = fieldsByName.putIfAbsent(lowerFieldName, field);
            if (previous != null) {
                throw new IllegalStateException("Parquet file schema contains case-insensitive duplicate field name '%s'"
                        .formatted(lowerFieldName));
            }
        }
        return fieldsByName;
    }

    private static Optional<Field> constructField(Type type, ColumnIO columnIO)
    {
        if (columnIO == null) {
            return Optional.empty();
        }
        return ParquetTypeUtils.constructField(type, columnIO);
    }

    static TrinoException handleOrcException(OrcDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (exception instanceof OrcCorruptionException) {
            return new TrinoException(PAIMON_BAD_DATA, exception);
        }
        return new TrinoException(PAIMON_CURSOR_ERROR, "Failed to read ORC file: " + dataSourceId, exception);
    }

    static TrinoException handleParquetException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (exception instanceof ParquetCorruptionException) {
            return new TrinoException(PAIMON_BAD_DATA, exception);
        }
        return new TrinoException(PAIMON_CURSOR_ERROR, "Failed to read Parquet file: " + dataSourceId, exception);
    }

    private static void closeDataSourceSuppressingException(AutoCloseable dataSource, Exception failure)
    {
        if (dataSource == null) {
            return;
        }
        requireNonNull(failure, "failure is null");
        try {
            dataSource.close();
        }
        catch (Exception closeException) {
            if (closeException != failure) {
                failure.addSuppressed(closeException);
            }
        }
    }
}
