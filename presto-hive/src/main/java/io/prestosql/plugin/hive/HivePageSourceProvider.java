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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HivePageSourceFactory.ReaderPageSourceWithProjections;
import io.prestosql.plugin.hive.HiveRecordCursorProvider.ReaderRecordCursorWithProjections;
import io.prestosql.plugin.hive.HiveSplit.BucketConversion;
import io.prestosql.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.EmptyPageSource;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static io.prestosql.plugin.hive.HivePageSourceProvider.ColumnMapping.toColumnHandles;
import static io.prestosql.plugin.hive.util.HiveUtil.getPrefilledColumnValue;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HivePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final TypeManager typeManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final Set<HivePageSourceFactory> pageSourceFactories;
    private final Set<HiveRecordCursorProvider> cursorProviders;

    @Inject
    public HivePageSourceProvider(
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment,
            Set<HivePageSourceFactory> pageSourceFactories,
            Set<HiveRecordCursorProvider> cursorProviders,
            GenericHiveRecordCursorProvider genericCursorProvider)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.pageSourceFactories = ImmutableSet.copyOf(requireNonNull(pageSourceFactories, "pageSourceFactories is null"));
        this.cursorProviders = ImmutableSet.<HiveRecordCursorProvider>builder()
                .addAll(requireNonNull(cursorProviders, "cursorProviders is null"))
                .add(genericCursorProvider) // generic should be last, as a fallback option
                .build();
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, TupleDomain<ColumnHandle> dynamicFilter)
    {
        HiveTableHandle hiveTable = (HiveTableHandle) table;

        List<HiveColumnHandle> hiveColumns = columns.stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toList());

        HiveSplit hiveSplit = (HiveSplit) split;
        Path path = new Path(hiveSplit.getPath());

        Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsContext(session, hiveSplit.getDatabase(), hiveSplit.getTable()), path);

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
                hiveTable.getCompactEffectivePredicate().intersect(dynamicFilter.transform(HiveColumnHandle.class::cast).simplify()),
                hiveColumns,
                hiveSplit.getPartitionName(),
                hiveSplit.getPartitionKeys(),
                typeManager,
                hiveSplit.getTableToPartitionMapping(),
                hiveSplit.getBucketConversion(),
                hiveSplit.isS3SelectPushdownEnabled(),
                hiveSplit.getAcidInfo());
        if (pageSource.isPresent()) {
            return pageSource.get();
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
            boolean s3SelectPushdownEnabled,
            Optional<AcidInfo> acidInfo)
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

        for (HivePageSourceFactory pageSourceFactory : pageSourceFactories) {
            List<HiveColumnHandle> desiredColumns = toColumnHandles(regularAndInterimColumnMappings, true, typeManager);

            Optional<ReaderPageSourceWithProjections> readerWithProjections = pageSourceFactory.createPageSource(
                    configuration,
                    session,
                    path,
                    start,
                    length,
                    estimatedFileSize,
                    schema,
                    desiredColumns,
                    effectivePredicate,
                    acidInfo);

            if (readerWithProjections.isPresent()) {
                ConnectorPageSource pageSource = readerWithProjections.get().getConnectorPageSource();

                Optional<ReaderProjections> readerProjections = readerWithProjections.get().getProjectedReaderColumns();
                Optional<ReaderProjectionsAdapter> adapter = Optional.empty();
                if (readerProjections.isPresent()) {
                    adapter = Optional.of(new ReaderProjectionsAdapter(desiredColumns, readerProjections.get()));
                }

                return Optional.of(new HivePageSource(
                        columnMappings,
                        bucketAdaptation,
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
                Optional<ReaderProjections> projections = readerWithProjections.get().getProjectedReaderColumns();

                if (projections.isPresent()) {
                    ReaderProjectionsAdapter projectionsAdapter = new ReaderProjectionsAdapter(desiredColumns, projections.get());
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

                HiveRecordCursor hiveRecordCursor = new HiveRecordCursor(columnMappings, delegate);
                List<Type> columnTypes = columns.stream()
                        .map(HiveColumnHandle::getType)
                        .collect(toList());

                return Optional.of(new RecordPageSource(columnTypes, hiveRecordCursor));
            }
        }

        return Optional.empty();
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
            checkState(kind == ColumnMappingKind.REGULAR || kind == ColumnMappingKind.INTERIM);
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
}
