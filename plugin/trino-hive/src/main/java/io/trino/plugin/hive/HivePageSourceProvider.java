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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.metastore.HiveType;
import io.trino.metastore.HiveTypeName;
import io.trino.metastore.type.TypeInfo;
import io.trino.plugin.hive.HiveSplit.BucketConversion;
import io.trino.plugin.hive.HiveSplit.BucketValidation;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.coercions.CoercionUtils.CoercionContext;
import io.trino.plugin.hive.coercions.TypeCoercer;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.Utils;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static io.trino.plugin.hive.HiveColumnHandle.isRowIdColumnHandle;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping.toColumnHandles;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMappingKind.PREFILLED;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.plugin.hive.coercions.CoercionUtils.createTypeFromCoercer;
import static io.trino.plugin.hive.coercions.CoercionUtils.extractHiveStorageFormat;
import static io.trino.plugin.hive.util.HiveBucketing.HiveBucketFilter;
import static io.trino.plugin.hive.util.HiveBucketing.getHiveBucketFilter;
import static io.trino.plugin.hive.util.HiveTypeUtil.getHiveTypeForDereferences;
import static io.trino.plugin.hive.util.HiveUtil.getInputFormatName;
import static io.trino.plugin.hive.util.HiveUtil.getPrefilledColumnValue;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;

public class HivePageSourceProvider
        implements ConnectorPageSourceProvider
{
    public static final int ORIGINAL_TRANSACTION_CHANNEL = 0;
    public static final int BUCKET_CHANNEL = 1;
    public static final int ROW_ID_CHANNEL = 2;
    // The original file path looks like this: /root/dir/nnnnnnn_m(_copy_ccc)?
    private static final Pattern ORIGINAL_FILE_PATH_MATCHER = Pattern.compile("(?s)(?<rootDir>.*)/(?<filename>(?<bucketNumber>\\d+)_(?<rest>.*)?)$");

    private final TypeManager typeManager;
    private final int domainCompactionThreshold;
    private final Set<HivePageSourceFactory> pageSourceFactories;

    @Inject
    public HivePageSourceProvider(TypeManager typeManager, HiveConfig hiveConfig, Set<HivePageSourceFactory> pageSourceFactories)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.domainCompactionThreshold = hiveConfig.getDomainCompactionThreshold();
        this.pageSourceFactories = ImmutableSet.copyOf(requireNonNull(pageSourceFactories, "pageSourceFactories is null"));
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

        boolean originalFile = ORIGINAL_FILE_PATH_MATCHER.matcher(hiveSplit.getPath()).matches();

        List<ColumnMapping> columnMappings = ColumnMapping.buildColumnMappings(
                hiveSplit.getPartitionName(),
                hiveSplit.getPartitionKeys(),
                hiveColumns,
                hiveSplit.getBucketConversion().map(BucketConversion::bucketColumnHandles).orElse(ImmutableList.of()),
                hiveSplit.getHiveColumnCoercions(),
                hiveSplit.getPath(),
                hiveSplit.getTableBucketNumber(),
                hiveSplit.getEstimatedFileSize(),
                hiveSplit.getFileModifiedTime());

        // Perform dynamic partition pruning in case coordinator didn't prune split.
        // This can happen when dynamic filters are collected after partition splits were listed.
        if (shouldSkipSplit(columnMappings, dynamicFilter)) {
            return new EmptyPageSource();
        }

        Optional<ConnectorPageSource> pageSource = createHivePageSource(
                pageSourceFactories,
                session,
                Location.of(hiveSplit.getPath()),
                hiveSplit.getTableBucketNumber(),
                hiveSplit.getStart(),
                hiveSplit.getLength(),
                hiveSplit.getEstimatedFileSize(),
                hiveSplit.getFileModifiedTime(),
                hiveSplit.getSchema(),
                hiveTable.getCompactEffectivePredicate().intersect(
                                dynamicFilter.getCurrentPredicate().transformKeys(HiveColumnHandle.class::cast))
                        .simplify(domainCompactionThreshold),
                typeManager,
                hiveSplit.getBucketConversion(),
                hiveSplit.getBucketValidation(),
                hiveSplit.getAcidInfo(),
                originalFile,
                hiveTable.getTransaction(),
                columnMappings);

        if (pageSource.isPresent()) {
            return pageSource.get();
        }

        throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Unsupported input format: serde=%s, format=%s, partition=%s, path=%s".formatted(
                hiveSplit.getSchema().serializationLibraryName(),
                getInputFormatName(hiveSplit.getSchema().serdeProperties()).orElse(null),
                hiveSplit.getPartitionName(),
                hiveSplit.getPath()));
    }

    public static Optional<ConnectorPageSource> createHivePageSource(
            Set<HivePageSourceFactory> pageSourceFactories,
            ConnectorSession session,
            Location path,
            OptionalInt tableBucketNumber,
            long start,
            long length,
            long estimatedFileSize,
            long fileModifiedTime,
            Schema schema,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            TypeManager typeManager,
            Optional<BucketConversion> bucketConversion,
            Optional<BucketValidation> bucketValidation,
            Optional<AcidInfo> acidInfo,
            boolean originalFile,
            AcidTransaction transaction,
            List<ColumnMapping> columnMappings)
    {
        if (effectivePredicate.isNone()) {
            return Optional.of(new EmptyPageSource());
        }

        List<ColumnMapping> regularAndInterimColumnMappings = ColumnMapping.extractRegularAndInterimColumnMappings(columnMappings);

        Optional<BucketAdaptation> bucketAdaptation = createBucketAdaptation(bucketConversion, tableBucketNumber, regularAndInterimColumnMappings);
        Optional<BucketValidator> bucketValidator = createBucketValidator(path, bucketValidation, tableBucketNumber, regularAndInterimColumnMappings);

        CoercionContext coercionContext = new CoercionContext(getTimestampPrecision(session), extractHiveStorageFormat(schema.serializationLibraryName()));

        for (HivePageSourceFactory pageSourceFactory : pageSourceFactories) {
            List<HiveColumnHandle> desiredColumns = toColumnHandles(regularAndInterimColumnMappings, typeManager, coercionContext);

            Optional<ConnectorPageSource> pageSource = pageSourceFactory.createPageSource(
                    session,
                    path,
                    start,
                    length,
                    estimatedFileSize,
                    fileModifiedTime,
                    schema,
                    desiredColumns,
                    effectivePredicate,
                    acidInfo,
                    tableBucketNumber,
                    originalFile,
                    transaction);

            if (pageSource.isPresent()) {
                return Optional.of(createHivePageSource(columnMappings,
                        bucketAdaptation,
                        bucketValidator,
                        typeManager,
                        coercionContext,
                        pageSource.get()));
            }
        }

        return Optional.empty();
    }

    @VisibleForTesting
    static ConnectorPageSource createHivePageSource(
            List<ColumnMapping> columnMappings,
            Optional<BucketAdaptation> bucketAdaptation,
            Optional<BucketValidator> bucketValidator,
            TypeManager typeManager,
            CoercionContext coercionContext,
            ConnectorPageSource pageSource)
    {
        if (bucketAdaptation.isPresent()) {
            BucketAdapter bucketAdapter = new BucketAdapter(bucketAdaptation.get());
            pageSource = TransformConnectorPageSource.create(pageSource, bucketAdapter::filterPageToEligibleRowsOrDiscard);
        }
        else if (bucketValidator.isPresent()) {
            BucketValidator validator = bucketValidator.get();
            pageSource = TransformConnectorPageSource.create(pageSource, page -> {
                validator.validate(page);
                return page;
            });
        }

        TransformConnectorPageSource.Builder transforms = TransformConnectorPageSource.builder();
        for (ColumnMapping columnMapping : columnMappings) {
            HiveColumnHandle column = columnMapping.getHiveColumnHandle();

            Type type = column.getType();
            switch (columnMapping.getKind()) {
                case PREFILLED -> transforms.constantValue(Utils.nativeValueToBlock(type, columnMapping.getPrefilledValue().getValue()));
                case EMPTY -> transforms.constantValue(type.createNullBlock());
                case REGULAR, SYNTHESIZED -> {
                    Optional<TypeCoercer<? extends Type, ? extends Type>> coercer = Optional.empty();
                    if (columnMapping.getBaseTypeCoercionFrom().isPresent()) {
                        List<Integer> dereferenceIndices = column.getHiveColumnProjectionInfo()
                                .map(HiveColumnProjectionInfo::getDereferenceIndices)
                                .orElse(ImmutableList.of());
                        HiveType fromType = getHiveTypeForDereferences(columnMapping.getBaseTypeCoercionFrom().get(), dereferenceIndices).orElseThrow();
                        HiveType toType = columnMapping.getHiveColumnHandle().getHiveType();
                        coercer = createCoercer(typeManager, fromType, toType, coercionContext);
                    }

                    int inputChannel = columnMapping.getIndex();
                    if (coercer.isPresent()) {
                        transforms.transform(inputChannel, coercer.get());
                    }
                    else {
                        transforms.column(inputChannel);
                    }
                }
                case INTERIM -> {
                    // interim columns don't show up in output
                }
            }
        }
        return transforms.build(pageSource);
    }

    private static boolean shouldSkipBucket(HiveTableHandle hiveTable, HiveSplit hiveSplit, DynamicFilter dynamicFilter)
    {
        if (hiveSplit.getTableBucketNumber().isEmpty()) {
            return false;
        }
        Optional<HiveBucketFilter> hiveBucketFilter = getHiveBucketFilter(hiveTable, dynamicFilter.getCurrentPredicate());
        return hiveBucketFilter.map(filter -> !filter.getBucketsToKeep().contains(hiveSplit.getTableBucketNumber().getAsInt())).orElse(false);
    }

    private static boolean shouldSkipSplit(List<ColumnMapping> columnMappings, DynamicFilter dynamicFilter)
    {
        TupleDomain<ColumnHandle> predicate = dynamicFilter.getCurrentPredicate();
        if (predicate.isNone()) {
            return true;
        }
        Map<ColumnHandle, Domain> domains = predicate.getDomains().get();
        for (ColumnMapping columnMapping : columnMappings) {
            if (columnMapping.getKind() != PREFILLED) {
                continue;
            }
            Object value = columnMapping.getPrefilledValue().getValue();
            Domain allowedDomain = domains.get(columnMapping.getHiveColumnHandle());
            if (allowedDomain != null && !allowedDomain.includesNullableValue(value)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Create a page source using base columns and project the final shape from the base columns.
     * This utility is used for page sources that do not handle column dereferences directly.
     */
    public static ConnectorPageSource projectColumnDereferences(List<HiveColumnHandle> columns, Function<List<HiveColumnHandle>, ConnectorPageSource> pageSourceFactory)
    {
        // determine base columns and create transform to project the final shape from the base columns
        List<HiveColumnHandle> baseColumns = new ArrayList<>();
        TransformConnectorPageSource.Builder transforms = TransformConnectorPageSource.builder();
        Map<Integer, Integer> baseColumnOrdinalByColumnIndex = new HashMap<>();
        for (HiveColumnHandle column : columns) {
            HiveColumnHandle baseColumn = column.getBaseColumn();
            Integer ordinal = baseColumnOrdinalByColumnIndex.get(baseColumn.getBaseHiveColumnIndex());
            if (ordinal == null) {
                ordinal = baseColumns.size();
                baseColumnOrdinalByColumnIndex.put(baseColumn.getBaseHiveColumnIndex(), ordinal);
                baseColumns.add(baseColumn);
            }

            if (column.isBaseColumn()) {
                transforms.column(ordinal);
            }
            else {
                transforms.dereferenceField(ImmutableList.<Integer>builder()
                        .add(ordinal)
                        .addAll(getProjection(column, baseColumn))
                        .build());
            }
        }

        ConnectorPageSource connectorPageSource = pageSourceFactory.apply(baseColumns);
        return transforms.build(connectorPageSource);
    }

    public static List<Integer> getProjection(ColumnHandle expected, ColumnHandle read)
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
        private final Optional<NullableValue> prefilledValue;
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

        public static ColumnMapping prefilled(HiveColumnHandle hiveColumnHandle, NullableValue prefilledValue, Optional<HiveType> baseTypeCoercionFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == PARTITION_KEY || hiveColumnHandle.getColumnType() == SYNTHESIZED);
            checkArgument(hiveColumnHandle.isBaseColumn(), "prefilled values not supported for projected columns");
            return new ColumnMapping(PREFILLED, hiveColumnHandle, Optional.of(prefilledValue), OptionalInt.empty(), baseTypeCoercionFrom);
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
                Optional<NullableValue> prefilledValue,
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

        public NullableValue getPrefilledValue()
        {
            checkState(kind == PREFILLED);
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
                Map<Integer, HiveTypeName> hiveColumnCoercions,
                String path,
                OptionalInt bucketNumber,
                long estimatedFileSize,
                long fileModifiedTime)
        {
            Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey::name);

            // Maintain state about hive columns added to the mapping as we iterate (for validation)
            Set<Integer> baseColumnHiveIndices = new HashSet<>();
            Map<Integer, Set<Optional<HiveColumnProjectionInfo>>> projectionsForColumn = new HashMap<>();

            ImmutableList.Builder<ColumnMapping> columnMappings = ImmutableList.builder();
            int regularIndex = 0;

            for (HiveColumnHandle column : columns) {
                Optional<HiveType> baseTypeCoercionFrom = Optional.ofNullable(hiveColumnCoercions.get(column.getBaseHiveColumnIndex())).map(HiveTypeName::toHiveType);
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
                            projectionsForColumn.computeIfAbsent(column.getBaseHiveColumnIndex(), index -> new HashSet<>()).add(column.getHiveColumnProjectionInfo()),
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
                    Optional<HiveType> baseTypeCoercionFrom = Optional.ofNullable(hiveColumnCoercions.get(column.getBaseHiveColumnIndex())).map(HiveTypeName::toHiveType);
                    columnMappings.add(interim(column, regularIndex, baseTypeCoercionFrom));
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
            Optional<HiveType> targetType = getHiveTypeForDereferences(baseType, dereferences);
            return targetType.isPresent();
        }

        public static List<ColumnMapping> extractRegularAndInterimColumnMappings(List<ColumnMapping> columnMappings)
        {
            return columnMappings.stream()
                    .filter(columnMapping -> columnMapping.getKind() == ColumnMappingKind.REGULAR || columnMapping.getKind() == ColumnMappingKind.INTERIM)
                    .collect(toImmutableList());
        }

        public static List<HiveColumnHandle> toColumnHandles(List<ColumnMapping> regularColumnMappings, TypeManager typeManager, CoercionContext coercionContext)
        {
            return regularColumnMappings.stream()
                    .map(columnMapping -> {
                        HiveColumnHandle columnHandle = columnMapping.getHiveColumnHandle();
                        if (columnMapping.getBaseTypeCoercionFrom().isEmpty()) {
                            return columnHandle;
                        }
                        HiveType fromHiveTypeBase = columnMapping.getBaseTypeCoercionFrom().get();

                        Optional<HiveColumnProjectionInfo> newColumnProjectionInfo = columnHandle.getHiveColumnProjectionInfo().map(projectedColumn -> {
                            HiveType fromHiveType = getHiveTypeForDereferences(fromHiveTypeBase, projectedColumn.getDereferenceIndices()).get();
                            return new HiveColumnProjectionInfo(
                                    projectedColumn.getDereferenceIndices(),
                                    projectedColumn.getDereferenceNames(),
                                    fromHiveType,
                                    createTypeFromCoercer(typeManager, fromHiveType, columnHandle.getHiveType(), coercionContext));
                        });

                        return new HiveColumnHandle(
                                columnHandle.getBaseColumnName(),
                                columnHandle.getBaseHiveColumnIndex(),
                                fromHiveTypeBase,
                                createTypeFromCoercer(typeManager, fromHiveTypeBase, columnHandle.getBaseHiveType(), coercionContext),
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

            int[] bucketColumnIndices = conversion.bucketColumnHandles().stream()
                    .mapToInt(columnHandle -> baseHiveColumnToBlockIndex.get(columnHandle.getBaseHiveColumnIndex()).getIndex())
                    .toArray();
            List<HiveType> bucketColumnHiveTypes = conversion.bucketColumnHandles().stream()
                    .map(columnHandle -> baseHiveColumnToBlockIndex.get(columnHandle.getBaseHiveColumnIndex()).getHiveColumnHandle().getHiveType())
                    .collect(toImmutableList());
            return new BucketAdaptation(
                    bucketColumnIndices,
                    bucketColumnHiveTypes,
                    conversion.bucketingVersion(),
                    conversion.tableBucketCount(),
                    conversion.partitionBucketCount(),
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

    static Optional<BucketValidator> createBucketValidator(Location path, Optional<BucketValidation> bucketValidation, OptionalInt bucketNumber, List<ColumnMapping> columnMappings)
    {
        return bucketValidation.flatMap(validation -> {
            Map<Integer, ColumnMapping> baseHiveColumnToBlockIndex = columnMappings.stream()
                    .filter(mapping -> mapping.getHiveColumnHandle().isBaseColumn())
                    .collect(toImmutableMap(mapping -> mapping.getHiveColumnHandle().getBaseHiveColumnIndex(), identity()));

            int[] bucketColumnIndices = new int[validation.bucketColumns().size()];

            List<TypeInfo> bucketColumnTypes = new ArrayList<>();
            for (int i = 0; i < validation.bucketColumns().size(); i++) {
                HiveColumnHandle column = validation.bucketColumns().get(i);
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
                    validation.bucketingVersion(),
                    validation.bucketCount(),
                    bucketNumber.orElseThrow()));
        });
    }
}
