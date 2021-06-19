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
package io.trino.plugin.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.OrcType;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HivePageSource.BucketValidator;
import io.trino.plugin.hive.HiveRecordCursorProvider.ReaderRecordCursorWithProjections;
import io.trino.plugin.hive.HiveSplit.BucketConversion;
import io.trino.plugin.hive.HiveSplit.BucketValidation;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.orc.OrcFileWriterFactory;
import io.trino.plugin.hive.orc.OrcPageSource;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static io.trino.plugin.hive.HiveColumnHandle.isRowIdColumnHandle;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping.toColumnHandles;
import static io.trino.plugin.hive.HiveUpdatablePageSource.ACID_ROW_STRUCT_COLUMN_ID;
import static io.trino.plugin.hive.HiveUpdatablePageSource.ORIGINAL_FILE_PATH_MATCHER;
import static io.trino.plugin.hive.orc.OrcTypeToHiveTypeTranslator.fromOrcTypeToHiveType;
import static io.trino.plugin.hive.util.HiveBucketing.HiveBucketFilter;
import static io.trino.plugin.hive.util.HiveBucketing.getHiveBucketFilter;
import static io.trino.plugin.hive.util.HiveUtil.getPrefilledColumnValue;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;

public class HivePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final TypeManager typeManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final int domainCompactionThreshold;
    private final Set<HivePageSourceFactory> pageSourceFactories;
    private final Set<HiveRecordCursorProvider> cursorProviders;
    private final Optional<OrcFileWriterFactory> orcFileWriterFactory;

    @Inject
    public HivePageSourceProvider(
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment,
            HiveConfig hiveConfig,
            Set<HivePageSourceFactory> pageSourceFactories,
            Set<HiveRecordCursorProvider> cursorProviders,
            GenericHiveRecordCursorProvider genericCursorProvider,
            OrcFileWriterFactory orcFileWriterFactory)
    {
        this(typeManager, hdfsEnvironment, hiveConfig, pageSourceFactories, cursorProviders, genericCursorProvider, Optional.of(orcFileWriterFactory));
    }

    public HivePageSourceProvider(
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment,
            HiveConfig hiveConfig,
            Set<HivePageSourceFactory> pageSourceFactories,
            Set<HiveRecordCursorProvider> cursorProviders,
            GenericHiveRecordCursorProvider genericCursorProvider,
            Optional<OrcFileWriterFactory> orcFileWriterFactory)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.domainCompactionThreshold = requireNonNull(hiveConfig, "hiveConfig is null").getDomainCompactionThreshold();
        this.pageSourceFactories = ImmutableSet.copyOf(requireNonNull(pageSourceFactories, "pageSourceFactories is null"));
        this.cursorProviders = ImmutableSet.<HiveRecordCursorProvider>builder()
                .addAll(requireNonNull(cursorProviders, "cursorProviders is null"))
                .add(genericCursorProvider) // generic should be last, as a fallback option
                .build();
        this.orcFileWriterFactory = requireNonNull(orcFileWriterFactory, "orcFileWriterFactory is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        HiveTableHandle hiveTable = (HiveTableHandle) tableHandle;
        HiveSplit hiveSplit = (HiveSplit) split;

        if (shouldSkipBucket(hiveTable, hiveSplit, dynamicFilter)) {
            return new EmptyPageSource();
        }

        List<HiveColumnHandle> hiveColumns = columns.stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toList());

        List<HiveColumnHandle> dependencyColumns = hiveColumns.stream()
                .filter(HiveColumnHandle::isBaseColumn)
                .collect(toImmutableList());

        if (hiveTable.isAcidUpdate()) {
            hiveColumns = hiveTable.getUpdateProcessor()
                    .orElseThrow(() -> new IllegalArgumentException("update processor not present"))
                    .mergeWithNonUpdatedColumns(hiveColumns);
        }

        Path path = new Path(hiveSplit.getPath());
        boolean originalFile = ORIGINAL_FILE_PATH_MATCHER.matcher(path.toString()).matches();

        Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsContext(session), path);

        TupleDomain<HiveColumnHandle> simplifiedDynamicFilter = dynamicFilter
                .getCurrentPredicate()
                .transformKeys(HiveColumnHandle.class::cast).simplify(domainCompactionThreshold);
        Optional<ConnectorPageSource> pageSource = createHivePageSource(
                pageSourceFactories,
                cursorProviders,
                configuration,
                session,
                path,
                hiveSplit.getBucketNumber(),
                hiveSplit.getStart(),
                hiveSplit.getLength(),
                hiveSplit.getEstimatedFileSize(),
                hiveSplit.getFileModifiedTime(),
                hiveSplit.getSchema(),
                hiveTable.getCompactEffectivePredicate().intersect(simplifiedDynamicFilter),
                hiveColumns,
                hiveSplit.getPartitionName(),
                hiveSplit.getPartitionKeys(),
                typeManager,
                hiveSplit.getTableToPartitionMapping(),
                hiveSplit.getBucketConversion(),
                hiveSplit.getBucketValidation(),
                hiveSplit.isS3SelectPushdownEnabled(),
                hiveSplit.getAcidInfo(),
                originalFile,
                hiveTable.getTransaction());

        if (pageSource.isPresent()) {
            ConnectorPageSource source = pageSource.get();
            if (hiveTable.isAcidDelete() || hiveTable.isAcidUpdate()) {
                checkArgument(orcFileWriterFactory.isPresent(), "orcFileWriterFactory not supplied but required for DELETE and UPDATE");
                HivePageSource hivePageSource = (HivePageSource) source;
                OrcPageSource orcPageSource = (OrcPageSource) hivePageSource.getDelegate();
                ColumnMetadata<OrcType> columnMetadata = orcPageSource.getColumnTypes();
                int acidRowColumnId = originalFile ? 0 : ACID_ROW_STRUCT_COLUMN_ID;
                HiveType rowType = fromOrcTypeToHiveType(columnMetadata.get(new OrcColumnId(acidRowColumnId)), columnMetadata);
                return new HiveUpdatablePageSource(
                        hiveTable,
                        hiveSplit.getPartitionName(),
                        hiveSplit.getStatementId(),
                        source,
                        typeManager,
                        hiveSplit.getBucketNumber(),
                        path,
                        originalFile,
                        orcFileWriterFactory.get(),
                        configuration,
                        session,
                        rowType,
                        dependencyColumns,
                        hiveTable.getTransaction().getOperation());
            }

            return source;
        }
        throw new RuntimeException("Could not find a file reader for split " + hiveSplit);
    }

    public static Optional<ConnectorPageSource> createHivePageSource(
            Set<HivePageSourceFactory> pageSourceFactories,
            Set<HiveRecordCursorProvider> cursorProviders,
            Configuration configuration,
            ConnectorSession session,
            Path path,
            OptionalInt bucketNumber,
            long start,
            long length,
            long estimatedFileSize,
            long fileModifiedTime,
            Properties schema,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HiveColumnHandle> columns,
            String partitionName,
            List<HivePartitionKey> partitionKeys,
            TypeManager typeManager,
            TableToPartitionMapping tableToPartitionMapping,
            Optional<BucketConversion> bucketConversion,
            Optional<BucketValidation> bucketValidation,
            boolean s3SelectPushdownEnabled,
            Optional<AcidInfo> acidInfo,
            boolean originalFile,
            AcidTransaction transaction)
    {
        if (effectivePredicate.isNone()) {
            return Optional.of(new EmptyPageSource());
        }

        List<ColumnMapping> columnMappings = ColumnMapping.buildColumnMappings(
                partitionName,
                partitionKeys,
                columns,
                bucketConversion.map(BucketConversion::getBucketColumnHandles).orElse(ImmutableList.of()),
                tableToPartitionMapping,
                path,
                bucketNumber,
                estimatedFileSize,
                fileModifiedTime);
        List<ColumnMapping> regularAndInterimColumnMappings = ColumnMapping.extractRegularAndInterimColumnMappings(columnMappings);

        Optional<BucketAdaptation> bucketAdaptation = createBucketAdaptation(bucketConversion, bucketNumber, regularAndInterimColumnMappings);
        Optional<BucketValidator> bucketValidator = createBucketValidator(path, bucketValidation, bucketNumber, regularAndInterimColumnMappings);

        for (HivePageSourceFactory pageSourceFactory : pageSourceFactories) {
            List<HiveColumnHandle> desiredColumns = toColumnHandles(regularAndInterimColumnMappings, true, typeManager);

            Optional<ReaderPageSource> readerWithProjections = pageSourceFactory.createPageSource(
                    configuration,
                    session,
                    path,
                    start,
                    length,
                    estimatedFileSize,
                    schema,
                    desiredColumns,
                    effectivePredicate,
                    acidInfo,
                    bucketNumber,
                    originalFile,
                    transaction);

            if (readerWithProjections.isPresent()) {
                ConnectorPageSource pageSource = readerWithProjections.get().get();

                Optional<ReaderColumns> readerProjections = readerWithProjections.get().getReaderColumns();
                Optional<ReaderProjectionsAdapter> adapter = Optional.empty();
                if (readerProjections.isPresent()) {
                    adapter = Optional.of(hiveProjectionsAdapter(desiredColumns, readerProjections.get()));
                }

                return Optional.of(new HivePageSource(
                        columnMappings,
                        bucketAdaptation,
                        bucketValidator,
                        adapter,
                        typeManager,
                        pageSource));
            }
        }

        for (HiveRecordCursorProvider provider : cursorProviders) {
            // GenericHiveRecordCursor will automatically do the coercion without HiveCoercionRecordCursor
            boolean doCoercion = !(provider instanceof GenericHiveRecordCursorProvider);

            List<HiveColumnHandle> desiredColumns = toColumnHandles(regularAndInterimColumnMappings, doCoercion, typeManager);
            Optional<ReaderRecordCursorWithProjections> readerWithProjections = provider.createRecordCursor(
                    configuration,
                    session,
                    path,
                    start,
                    length,
                    estimatedFileSize,
                    schema,
                    desiredColumns,
                    effectivePredicate,
                    typeManager,
                    s3SelectPushdownEnabled);

            if (readerWithProjections.isPresent()) {
                RecordCursor delegate = readerWithProjections.get().getRecordCursor();
                Optional<ReaderColumns> projections = readerWithProjections.get().getProjectedReaderColumns();

                if (projections.isPresent()) {
                    ReaderProjectionsAdapter projectionsAdapter = hiveProjectionsAdapter(desiredColumns, projections.get());
                    delegate = new HiveReaderProjectionsAdaptingRecordCursor(delegate, projectionsAdapter);
                }

                checkArgument(acidInfo.isEmpty(), "Acid is not supported");

                if (bucketAdaptation.isPresent()) {
                    delegate = new HiveBucketAdapterRecordCursor(
                            bucketAdaptation.get().getBucketColumnIndices(),
                            bucketAdaptation.get().getBucketColumnHiveTypes(),
                            bucketAdaptation.get().getBucketingVersion(),
                            bucketAdaptation.get().getTableBucketCount(),
                            bucketAdaptation.get().getPartitionBucketCount(),
                            bucketAdaptation.get().getBucketToKeep(),
                            typeManager,
                            delegate);
                }

                // Need to wrap RcText and RcBinary into a wrapper, which will do the coercion for mismatch columns
                if (doCoercion) {
                    delegate = new HiveCoercionRecordCursor(regularAndInterimColumnMappings, typeManager, delegate);
                }

                // bucket adaptation already validates that data is in the right bucket
                if (bucketAdaptation.isEmpty() && bucketValidator.isPresent()) {
                    delegate = bucketValidator.get().wrapRecordCursor(delegate, typeManager);
                }

                HiveRecordCursor hiveRecordCursor = new HiveRecordCursor(columnMappings, delegate);
                List<Type> columnTypes = columns.stream()
                        .map(HiveColumnHandle::getType)
                        .collect(toList());

                return Optional.of(new RecordPageSource(columnTypes, hiveRecordCursor));
            }
        }

        return Optional.empty();
    }

    private static boolean shouldSkipBucket(HiveTableHandle hiveTable, HiveSplit hiveSplit, DynamicFilter dynamicFilter)
    {
        if (hiveSplit.getBucketNumber().isEmpty()) {
            return false;
        }
        Optional<HiveBucketFilter> hiveBucketFilter = getHiveBucketFilter(hiveTable, dynamicFilter.getCurrentPredicate());
        return hiveBucketFilter.map(filter -> !filter.getBucketsToKeep().contains(hiveSplit.getBucketNumber().getAsInt())).orElse(false);
    }

    private static ReaderProjectionsAdapter hiveProjectionsAdapter(List<HiveColumnHandle> expectedColumns, ReaderColumns readColumns)
    {
        return new ReaderProjectionsAdapter(
                expectedColumns.stream().map(ColumnHandle.class::cast).collect(toImmutableList()),
                readColumns,
                column -> ((HiveColumnHandle) column).getType(),
                HivePageSourceProvider::getProjection);
    }

    @VisibleForTesting
    static List<Integer> getProjection(ColumnHandle expected, ColumnHandle read)
    {
        HiveColumnHandle expectedColumn = (HiveColumnHandle) expected;
        HiveColumnHandle readColumn = (HiveColumnHandle) read;

        checkArgument(expectedColumn.getBaseColumn().equals(readColumn.getBaseColumn()), "reader column is not valid for expected column");

        List<Integer> expectedDereferences = expectedColumn.getHiveColumnProjectionInfo()
                .map(HiveColumnProjectionInfo::getDereferenceIndices)
                .orElse(ImmutableList.of());

        List<Integer> readerDereferences = readColumn.getHiveColumnProjectionInfo()
                .map(HiveColumnProjectionInfo::getDereferenceIndices)
                .orElse(ImmutableList.of());

        checkArgument(readerDereferences.size() <= expectedDereferences.size(), "Field returned by the reader should include expected field");
        checkArgument(expectedDereferences.subList(0, readerDereferences.size()).equals(readerDereferences), "Field returned by the reader should be a prefix of expected field");

        return expectedDereferences.subList(readerDereferences.size(), expectedDereferences.size());
    }

    public static class ColumnMapping
    {
        private final ColumnMappingKind kind;
        private final HiveColumnHandle hiveColumnHandle;
        private final Optional<String> prefilledValue;
        /**
         * ordinal of this column in the underlying page source or record cursor
         */
        private final OptionalInt index;
        private final Optional<HiveType> baseTypeCoercionFrom;

        public static ColumnMapping regular(HiveColumnHandle hiveColumnHandle, int index, Optional<HiveType> baseTypeCoercionFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == REGULAR);
            return new ColumnMapping(ColumnMappingKind.REGULAR, hiveColumnHandle, Optional.empty(), OptionalInt.of(index), baseTypeCoercionFrom);
        }

        public static ColumnMapping synthesized(HiveColumnHandle hiveColumnHandle, int index, Optional<HiveType> baseTypeCoercionFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == SYNTHESIZED);
            return new ColumnMapping(ColumnMappingKind.SYNTHESIZED, hiveColumnHandle, Optional.empty(), OptionalInt.of(index), baseTypeCoercionFrom);
        }

        public static ColumnMapping prefilled(HiveColumnHandle hiveColumnHandle, String prefilledValue, Optional<HiveType> baseTypeCoercionFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == PARTITION_KEY || hiveColumnHandle.getColumnType() == SYNTHESIZED);
            checkArgument(hiveColumnHandle.isBaseColumn(), "prefilled values not supported for projected columns");
            return new ColumnMapping(ColumnMappingKind.PREFILLED, hiveColumnHandle, Optional.of(prefilledValue), OptionalInt.empty(), baseTypeCoercionFrom);
        }

        public static ColumnMapping interim(HiveColumnHandle hiveColumnHandle, int index, Optional<HiveType> baseTypeCoercionFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == REGULAR);
            return new ColumnMapping(ColumnMappingKind.INTERIM, hiveColumnHandle, Optional.empty(), OptionalInt.of(index), baseTypeCoercionFrom);
        }

        public static ColumnMapping empty(HiveColumnHandle hiveColumnHandle)
        {
            checkArgument(hiveColumnHandle.getColumnType() == REGULAR);
            return new ColumnMapping(ColumnMappingKind.EMPTY, hiveColumnHandle, Optional.empty(), OptionalInt.empty(), Optional.empty());
        }

        private ColumnMapping(
                ColumnMappingKind kind,
                HiveColumnHandle hiveColumnHandle,
                Optional<String> prefilledValue,
                OptionalInt index,
                Optional<HiveType> baseTypeCoercionFrom)
        {
            this.kind = requireNonNull(kind, "kind is null");
            this.hiveColumnHandle = requireNonNull(hiveColumnHandle, "hiveColumnHandle is null");
            this.prefilledValue = requireNonNull(prefilledValue, "prefilledValue is null");
            this.index = requireNonNull(index, "index is null");
            this.baseTypeCoercionFrom = requireNonNull(baseTypeCoercionFrom, "baseTypeCoercionFrom is null");
        }

        public ColumnMappingKind getKind()
        {
            return kind;
        }

        public String getPrefilledValue()
        {
            checkState(kind == ColumnMappingKind.PREFILLED);
            return prefilledValue.get();
        }

        public HiveColumnHandle getHiveColumnHandle()
        {
            return hiveColumnHandle;
        }

        public int getIndex()
        {
            checkState(kind == ColumnMappingKind.REGULAR || kind == ColumnMappingKind.INTERIM || isRowIdColumnHandle(hiveColumnHandle));
            return index.getAsInt();
        }

        public Optional<HiveType> getBaseTypeCoercionFrom()
        {
            return baseTypeCoercionFrom;
        }

        public static List<ColumnMapping> buildColumnMappings(
                String partitionName,
                List<HivePartitionKey> partitionKeys,
                List<HiveColumnHandle> columns,
                List<HiveColumnHandle> requiredInterimColumns,
                TableToPartitionMapping tableToPartitionMapping,
                Path path,
                OptionalInt bucketNumber,
                long estimatedFileSize,
                long fileModifiedTime)
        {
            Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey::getName);

            // Maintain state about hive columns added to the mapping as we iterate (for validation)
            Set<Integer> baseColumnHiveIndices = new HashSet<>();
            Map<Integer, Set<Optional<HiveColumnProjectionInfo>>> projectionsForColumn = new HashMap<>();

            ImmutableList.Builder<ColumnMapping> columnMappings = ImmutableList.builder();
            int regularIndex = 0;

            for (HiveColumnHandle column : columns) {
                Optional<HiveType> baseTypeCoercionFrom = tableToPartitionMapping.getCoercion(column.getBaseHiveColumnIndex());
                if (column.getColumnType() == REGULAR) {
                    if (column.isBaseColumn()) {
                        baseColumnHiveIndices.add(column.getBaseHiveColumnIndex());
                    }

                    checkArgument(
                            projectionsForColumn.computeIfAbsent(column.getBaseHiveColumnIndex(), columnIndex -> new HashSet<>()).add(column.getHiveColumnProjectionInfo()),
                            "duplicate column in columns list");

                    // Add regular mapping if projection is valid for partition schema, otherwise add an empty mapping
                    if (baseTypeCoercionFrom.isEmpty()
                            || projectionValidForType(baseTypeCoercionFrom.get(), column.getHiveColumnProjectionInfo())) {
                        columnMappings.add(regular(column, regularIndex, baseTypeCoercionFrom));
                        regularIndex++;
                    }
                    else {
                        columnMappings.add(empty(column));
                    }
                }
                else if (isRowIdColumnHandle(column)) {
                    baseColumnHiveIndices.add(column.getBaseHiveColumnIndex());
                    checkArgument(
                            projectionsForColumn.computeIfAbsent(column.getBaseHiveColumnIndex(), index -> new HashSet()).add(column.getHiveColumnProjectionInfo()),
                            "duplicate column in columns list");

                    if (baseTypeCoercionFrom.isEmpty()
                            || projectionValidForType(baseTypeCoercionFrom.get(), column.getHiveColumnProjectionInfo())) {
                        columnMappings.add(synthesized(column, regularIndex, baseTypeCoercionFrom));
                    }
                    else {
                        throw new RuntimeException("baseTypeCoercisionFrom was empty for the rowId column");
                    }
                    regularIndex++;
                }
                else {
                    columnMappings.add(prefilled(
                            column,
                            getPrefilledColumnValue(column, partitionKeysByName.get(column.getName()), path, bucketNumber, estimatedFileSize, fileModifiedTime, partitionName),
                            baseTypeCoercionFrom));
                }
            }

            for (HiveColumnHandle column : requiredInterimColumns) {
                checkArgument(column.getColumnType() == REGULAR);
                checkArgument(column.isBaseColumn(), "bucketed columns should be base columns");
                if (baseColumnHiveIndices.contains(column.getBaseHiveColumnIndex())) {
                    continue; // This column exists in columns. Do not add it again.
                }

                if (projectionsForColumn.containsKey(column.getBaseHiveColumnIndex())) {
                    columnMappings.add(interim(column, regularIndex, tableToPartitionMapping.getCoercion(column.getBaseHiveColumnIndex())));
                }
                else {
                    // If coercion does not affect bucket number calculation, coercion doesn't need to be applied here.
                    // Otherwise, read of this partition should not be allowed.
                    // (Alternatively, the partition could be read as an unbucketed partition. This is not implemented.)
                    columnMappings.add(interim(column, regularIndex, Optional.empty()));
                }
                regularIndex++;
            }
            return columnMappings.build();
        }

        private static boolean projectionValidForType(HiveType baseType, Optional<HiveColumnProjectionInfo> projection)
        {
            List<Integer> dereferences = projection.map(HiveColumnProjectionInfo::getDereferenceIndices).orElse(ImmutableList.of());
            Optional<HiveType> targetType = baseType.getHiveTypeForDereferences(dereferences);
            return targetType.isPresent();
        }

        public static List<ColumnMapping> extractRegularAndInterimColumnMappings(List<ColumnMapping> columnMappings)
        {
            return columnMappings.stream()
                    .filter(columnMapping -> columnMapping.getKind() == ColumnMappingKind.REGULAR || columnMapping.getKind() == ColumnMappingKind.INTERIM)
                    .collect(toImmutableList());
        }

        public static List<HiveColumnHandle> toColumnHandles(List<ColumnMapping> regularColumnMappings, boolean doCoercion, TypeManager typeManager)
        {
            return regularColumnMappings.stream()
                    .map(columnMapping -> {
                        HiveColumnHandle columnHandle = columnMapping.getHiveColumnHandle();
                        if (!doCoercion || columnMapping.getBaseTypeCoercionFrom().isEmpty()) {
                            return columnHandle;
                        }
                        HiveType fromHiveTypeBase = columnMapping.getBaseTypeCoercionFrom().get();

                        Optional<HiveColumnProjectionInfo> newColumnProjectionInfo = columnHandle.getHiveColumnProjectionInfo().map(projectedColumn -> {
                            HiveType fromHiveType = fromHiveTypeBase.getHiveTypeForDereferences(projectedColumn.getDereferenceIndices()).get();
                            return new HiveColumnProjectionInfo(
                                    projectedColumn.getDereferenceIndices(),
                                    projectedColumn.getDereferenceNames(),
                                    fromHiveType,
                                    fromHiveType.getType(typeManager));
                        });

                        return new HiveColumnHandle(
                                columnHandle.getBaseColumnName(),
                                columnHandle.getBaseHiveColumnIndex(),
                                fromHiveTypeBase,
                                fromHiveTypeBase.getType(typeManager),
                                newColumnProjectionInfo,
                                columnHandle.getColumnType(),
                                columnHandle.getComment());
                    })
                    .collect(toList());
        }
    }

    public enum ColumnMappingKind
    {
        REGULAR,
        PREFILLED,
        INTERIM,
        SYNTHESIZED,
        EMPTY
    }

    private static Optional<BucketAdaptation> createBucketAdaptation(Optional<BucketConversion> bucketConversion, OptionalInt bucketNumber, List<ColumnMapping> columnMappings)
    {
        return bucketConversion.map(conversion -> {
            List<ColumnMapping> baseColumnMapping = columnMappings.stream()
                    .filter(mapping -> mapping.getHiveColumnHandle().isBaseColumn())
                    .collect(toList());
            Map<Integer, ColumnMapping> baseHiveColumnToBlockIndex = uniqueIndex(baseColumnMapping, mapping -> mapping.getHiveColumnHandle().getBaseHiveColumnIndex());

            int[] bucketColumnIndices = conversion.getBucketColumnHandles().stream()
                    .mapToInt(columnHandle -> baseHiveColumnToBlockIndex.get(columnHandle.getBaseHiveColumnIndex()).getIndex())
                    .toArray();
            List<HiveType> bucketColumnHiveTypes = conversion.getBucketColumnHandles().stream()
                    .map(columnHandle -> baseHiveColumnToBlockIndex.get(columnHandle.getBaseHiveColumnIndex()).getHiveColumnHandle().getHiveType())
                    .collect(toImmutableList());
            return new BucketAdaptation(
                    bucketColumnIndices,
                    bucketColumnHiveTypes,
                    conversion.getBucketingVersion(),
                    conversion.getTableBucketCount(),
                    conversion.getPartitionBucketCount(),
                    bucketNumber.getAsInt());
        });
    }

    public static class BucketAdaptation
    {
        private final int[] bucketColumnIndices;
        private final List<HiveType> bucketColumnHiveTypes;
        private final BucketingVersion bucketingVersion;
        private final int tableBucketCount;
        private final int partitionBucketCount;
        private final int bucketToKeep;

        public BucketAdaptation(
                int[] bucketColumnIndices,
                List<HiveType> bucketColumnHiveTypes,
                BucketingVersion bucketingVersion,
                int tableBucketCount,
                int partitionBucketCount,
                int bucketToKeep)
        {
            this.bucketColumnIndices = bucketColumnIndices;
            this.bucketColumnHiveTypes = bucketColumnHiveTypes;
            this.bucketingVersion = bucketingVersion;
            this.tableBucketCount = tableBucketCount;
            this.partitionBucketCount = partitionBucketCount;
            this.bucketToKeep = bucketToKeep;
        }

        public int[] getBucketColumnIndices()
        {
            return bucketColumnIndices;
        }

        public List<HiveType> getBucketColumnHiveTypes()
        {
            return bucketColumnHiveTypes;
        }

        public BucketingVersion getBucketingVersion()
        {
            return bucketingVersion;
        }

        public int getTableBucketCount()
        {
            return tableBucketCount;
        }

        public int getPartitionBucketCount()
        {
            return partitionBucketCount;
        }

        public int getBucketToKeep()
        {
            return bucketToKeep;
        }
    }

    private static Optional<BucketValidator> createBucketValidator(Path path, Optional<BucketValidation> bucketValidation, OptionalInt bucketNumber, List<ColumnMapping> columnMappings)
    {
        return bucketValidation.flatMap(validation -> {
            Map<Integer, ColumnMapping> baseHiveColumnToBlockIndex = columnMappings.stream()
                    .filter(mapping -> mapping.getHiveColumnHandle().isBaseColumn())
                    .collect(toImmutableMap(mapping -> mapping.getHiveColumnHandle().getBaseHiveColumnIndex(), identity()));

            int[] bucketColumnIndices = new int[validation.getBucketColumns().size()];

            List<TypeInfo> bucketColumnTypes = new ArrayList<>();
            for (int i = 0; i < validation.getBucketColumns().size(); i++) {
                HiveColumnHandle column = validation.getBucketColumns().get(i);
                ColumnMapping mapping = baseHiveColumnToBlockIndex.get(column.getBaseHiveColumnIndex());
                if (mapping == null) {
                    // The bucket column is not read by the query, and thus invalid bucketing cannot
                    // affect the results. Filtering on the hidden $bucket column still correctly
                    // partitions the table by bucket, even if the bucket has the wrong data.
                    return Optional.empty();
                }
                bucketColumnIndices[i] = mapping.getIndex();
                bucketColumnTypes.add(mapping.getHiveColumnHandle().getHiveType().getTypeInfo());
            }

            return Optional.of(new BucketValidator(
                    path,
                    bucketColumnIndices,
                    bucketColumnTypes,
                    validation.getBucketingVersion(),
                    validation.getBucketCount(),
                    bucketNumber.orElseThrow()));
        });
    }

    /**
     * Creates a mapping between the input {@param columns} and base columns if required.
     */
    public static Optional<ReaderColumns> projectBaseColumns(List<HiveColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");

        // No projection is required if all columns are base columns
        if (columns.stream().allMatch(HiveColumnHandle::isBaseColumn)) {
            return Optional.empty();
        }

        ImmutableList.Builder<ColumnHandle> projectedColumns = ImmutableList.builder();
        ImmutableList.Builder<Integer> outputColumnMapping = ImmutableList.builder();
        Map<Integer, Integer> mappedHiveColumnIndices = new HashMap<>();
        int projectedColumnCount = 0;

        for (HiveColumnHandle column : columns) {
            int hiveColumnIndex = column.getBaseHiveColumnIndex();
            Integer mapped = mappedHiveColumnIndices.get(hiveColumnIndex);

            if (mapped == null) {
                projectedColumns.add(column.getBaseColumn());
                mappedHiveColumnIndices.put(hiveColumnIndex, projectedColumnCount);
                outputColumnMapping.add(projectedColumnCount);
                projectedColumnCount++;
            }
            else {
                outputColumnMapping.add(mapped);
            }
        }

        return Optional.of(new ReaderColumns(projectedColumns.build(), outputColumnMapping.build()));
    }

    /**
     * Creates a set of sufficient columns for the input projected columns and prepares a mapping between the two. For example,
     * if input {@param columns} include columns "a.b" and "a.b.c", then they will be projected from a single column "a.b".
     */
    public static Optional<ReaderColumns> projectSufficientColumns(List<HiveColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");

        if (columns.stream().allMatch(HiveColumnHandle::isBaseColumn)) {
            return Optional.empty();
        }

        ImmutableBiMap.Builder<DereferenceChain, HiveColumnHandle> dereferenceChainsBuilder = ImmutableBiMap.builder();

        for (HiveColumnHandle column : columns) {
            List<Integer> indices = column.getHiveColumnProjectionInfo()
                    .map(HiveColumnProjectionInfo::getDereferenceIndices)
                    .orElse(ImmutableList.of());

            DereferenceChain dereferenceChain = new DereferenceChain(column.getBaseColumnName(), indices);
            dereferenceChainsBuilder.put(dereferenceChain, column);
        }

        BiMap<DereferenceChain, HiveColumnHandle> dereferenceChains = dereferenceChainsBuilder.build();

        List<ColumnHandle> sufficientColumns = new ArrayList<>();
        ImmutableList.Builder<Integer> outputColumnMapping = ImmutableList.builder();

        Map<DereferenceChain, Integer> pickedColumns = new HashMap<>();

        // Pick a covering column for every column
        for (HiveColumnHandle columnHandle : columns) {
            DereferenceChain column = dereferenceChains.inverse().get(columnHandle);
            List<DereferenceChain> orderedPrefixes = column.getOrderedPrefixes();
            DereferenceChain chosenColumn = null;

            // Shortest existing prefix is chosen as the input.
            for (DereferenceChain prefix : orderedPrefixes) {
                if (dereferenceChains.containsKey(prefix)) {
                    chosenColumn = prefix;
                    break;
                }
            }

            checkState(chosenColumn != null, "chosenColumn is null");
            int inputBlockIndex;

            if (pickedColumns.containsKey(chosenColumn)) {
                // Use already picked column
                inputBlockIndex = pickedColumns.get(chosenColumn);
            }
            else {
                // Add a new column for the reader
                sufficientColumns.add(dereferenceChains.get(chosenColumn));
                pickedColumns.put(chosenColumn, sufficientColumns.size() - 1);
                inputBlockIndex = sufficientColumns.size() - 1;
            }

            outputColumnMapping.add(inputBlockIndex);
        }

        return Optional.of(new ReaderColumns(sufficientColumns, outputColumnMapping.build()));
    }

    private static class DereferenceChain
    {
        private final String name;
        private final List<Integer> indices;

        public DereferenceChain(String name, List<Integer> indices)
        {
            this.name = requireNonNull(name, "name is null");
            this.indices = ImmutableList.copyOf(requireNonNull(indices, "indices is null"));
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

            DereferenceChain that = (DereferenceChain) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(indices, that.indices);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, indices);
        }

        /**
         * Get Prefixes of this Dereference chain in increasing order of lengths
         */
        public List<DereferenceChain> getOrderedPrefixes()
        {
            ImmutableList.Builder<DereferenceChain> prefixes = ImmutableList.builder();

            for (int prefixLen = 0; prefixLen <= indices.size(); prefixLen++) {
                prefixes.add(new DereferenceChain(name, indices.subList(0, prefixLen)));
            }

            return prefixes.build();
        }
    }
}
