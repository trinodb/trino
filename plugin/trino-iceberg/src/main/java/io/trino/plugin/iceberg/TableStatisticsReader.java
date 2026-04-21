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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ParallelIterable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionDomain;
import static io.trino.plugin.iceberg.IcebergUtil.getPathDomain;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Long.parseLong;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.apache.iceberg.IcebergManifestUtils.liveEntries;

public final class TableStatisticsReader
{
    private static final Logger log = Logger.get(TableStatisticsReader.class);

    public static final String APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY = "ndv";

    private final TypeManager typeManager;
    private final ExecutorService icebergPlanningExecutor;

    @Inject
    public TableStatisticsReader(
            TypeManager typeManager,
            @ForIcebergPlanning ExecutorService icebergPlanningExecutor)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.icebergPlanningExecutor = requireNonNull(icebergPlanningExecutor, "icebergPlanningExecutor is null");
    }

    public TableStatistics getTableStatistics(
            IcebergTableHandle tableHandle,
            Set<IcebergColumnHandle> projectedColumns,
            Table icebergTable)
    {
        return makeTableStatistics(
                typeManager,
                icebergTable,
                tableHandle.getSnapshotId(),
                tableHandle.getEnforcedPredicate(),
                tableHandle.getUnenforcedPredicate(),
                projectedColumns,
                icebergPlanningExecutor);
    }

    @VisibleForTesting
    public static TableStatistics makeTableStatistics(
            TypeManager typeManager,
            Table icebergTable,
            OptionalLong snapshot,
            TupleDomain<IcebergColumnHandle> enforcedConstraint,
            TupleDomain<IcebergColumnHandle> unenforcedConstraint,
            Set<IcebergColumnHandle> projectedColumns,
            ExecutorService icebergPlanningExecutor)
    {
        if (snapshot.isEmpty()) {
            // No snapshot, so no data.
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .build();
        }
        long snapshotId = snapshot.getAsLong();

        // Including both enforced and unenforced constraint matches how Splits will eventually be generated and allows
        // us to provide more accurate estimates. Stats will be estimated again by FilterStatsCalculator based on the
        // unenforced constraint.
        TupleDomain<IcebergColumnHandle> effectivePredicate = enforcedConstraint.intersect(unenforcedConstraint);
        if (effectivePredicate.isNone()) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .build();
        }

        Set<Integer> columnIds = projectedColumns.stream()
                .map(IcebergColumnHandle::getId)
                .collect(toImmutableSet());

        Domain partitionDomain = getPartitionDomain(effectivePredicate);
        Domain pathDomain = getPathDomain(effectivePredicate);
        Expression filter = toIcebergExpression(effectivePredicate.filter((column, domain) -> !isMetadataColumnId(column.getId())));

        NonEvictableLoadingCache<Integer, ManifestEvaluator> manifestPartitionFilterEvaluators = buildNonEvictableCache(
                CacheBuilder.newBuilder().maximumSize(1000),
                CacheLoader.from(specId -> {
                    PartitionSpec partitionSpec = icebergTable.specs().get(specId);
                    return ManifestEvaluator.forRowFilter(filter, partitionSpec, true);
                }));
        List<ManifestFile> filteredManifests = icebergTable.snapshot(snapshotId)
                // We only need to look at data files to estimate statistics
                .dataManifests(icebergTable.io())
                .stream()
                // remove any manifests that don't have any existing or added files
                .filter(manifest -> manifest.hasAddedFiles() || manifest.hasExistingFiles())
                // remove manifests that don't match the scan filter
                .filter(manifestFile -> {
                    ManifestEvaluator evaluator = manifestPartitionFilterEvaluators.getUnchecked(manifestFile.partitionSpecId());
                    return evaluator.eval(manifestFile);
                })
                .collect(toImmutableList());
        Iterable<CloseableIterable<DataFile>> dataFileIterables = Iterables.transform(filteredManifests, manifestFile -> readManifest(icebergTable, manifestFile, filter, columnIds));

        List<Types.NestedField> columns = icebergTable.schema().columns()
                .stream()
                .filter(column -> columnIds.contains(column.fieldId()))
                .collect(toImmutableList());
        IcebergStatistics.Builder icebergStatisticsBuilder = new IcebergStatistics.Builder(columns, typeManager);
        try (CloseableIterable<DataFile> dataFiles = new ParallelIterable<>(dataFileIterables, icebergPlanningExecutor)) {
            dataFiles.forEach(dataFile -> {
                PartitionSpec spec = icebergTable.specs().get(dataFile.specId());
                if (!partitionDomain.isAll() && !partitionDomain.includesNullableValue(utf8Slice(spec.partitionToPath(dataFile.partition())))) {
                    return;
                }
                if (!pathDomain.isAll() && !pathDomain.includesNullableValue(utf8Slice(dataFile.location()))) {
                    return;
                }

                // Filtering by $file_modified_time is skipped to avoid making filesystem calls on each matched data file
                icebergStatisticsBuilder.acceptDataFile(dataFile, spec);
            });
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        IcebergStatistics summary = icebergStatisticsBuilder.build();

        if (summary.fileCount() == 0) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .build();
        }

        Map<Integer, Long> ndvs = readNdvs(
                icebergTable,
                snapshotId,
                columnIds);

        Map<Integer, org.apache.iceberg.types.Type> idToType = columns.stream()
                .map(column -> Maps.immutableEntry(column.fieldId(), column.type()))
                .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

        ImmutableMap.Builder<ColumnHandle, ColumnStatistics> columnHandleBuilder = ImmutableMap.builder();
        double recordCount = summary.recordCount();
        for (IcebergColumnHandle columnHandle : projectedColumns) {
            int fieldId = columnHandle.getId();
            ColumnStatistics.Builder columnBuilder = new ColumnStatistics.Builder();
            Long nullCount = summary.nullCounts().get(fieldId);
            if (nullCount != null) {
                columnBuilder.setNullsFraction(Estimate.of(nullCount / recordCount));
            }
            if (idToType.get(columnHandle.getId()) instanceof Types.FixedType fixedType) {
                long columnSize = fixedType.length();
                columnBuilder.setDataSize(Estimate.of(columnSize));
            }
            else if (summary.columnSizes() != null) {
                Long columnSize = summary.columnSizes().get(fieldId);
                if (columnSize != null) {
                    // columnSize is the size on disk and Trino column stats is size in memory.
                    // The relation between the two is type and data dependent.
                    // However, Trino currently does not use data size statistics for fixed-width types
                    // (it's not needed for them), so do not report it at all, to avoid reporting some bogus value.
                    if (!(columnHandle.getBaseType() instanceof FixedWidthType)) {
                        if (columnHandle.getBaseType() == VARCHAR) {
                            // Tested using item table from TPCDS benchmark
                            // compared column size of item_desc column stored inside files
                            // with length of values in that column reported by trino
                            columnSize = (long) (columnSize * 2.7);
                        }
                        else if (columnHandle.getBaseType() == VARBINARY) {
                            // Tested using VARBINARY columns with random, both in length and content, data
                            // compared column size stored inside parquet files with length of bytes written to it
                            // Data used for testing came from alpha numeric characters so it was not very real life like scenario
                            // In the future better heuristic could be used here
                            columnSize = (long) (columnSize * 1.4);
                        }
                        columnBuilder.setDataSize(Estimate.of(columnSize));
                    }
                }
            }
            Object min = summary.minValues().get(fieldId);
            Object max = summary.maxValues().get(fieldId);
            if (min != null && max != null) {
                columnBuilder.setRange(DoubleRange.from(columnHandle.getType(), min, max));
            }
            columnBuilder.setDistinctValuesCount(
                    Optional.ofNullable(ndvs.get(fieldId))
                            .map(Estimate::of)
                            .orElseGet(Estimate::unknown));
            columnHandleBuilder.put(columnHandle, columnBuilder.build());
        }
        return new TableStatistics(Estimate.of(recordCount), columnHandleBuilder.buildOrThrow());
    }

    public static Map<Integer, Long> readNdvs(Table icebergTable, long snapshotId, Set<Integer> columnIds)
    {
        ImmutableMap.Builder<Integer, Long> ndvByColumnId = ImmutableMap.builder();

        getLatestStatisticsFile(icebergTable, snapshotId).ifPresent(statisticsFile -> {
            Map<Integer, BlobMetadata> thetaBlobsByFieldId = statisticsFile.blobMetadata().stream()
                    .filter(blobMetadata -> blobMetadata.type().equals(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1))
                    .filter(blobMetadata -> blobMetadata.fields().size() == 1)
                    .filter(blobMetadata -> columnIds.contains(getOnlyElement(blobMetadata.fields())))
                    // Fail loud upon duplicates (there must be none)
                    .collect(toImmutableMap(blobMetadata -> getOnlyElement(blobMetadata.fields()), identity()));

            for (Entry<Integer, BlobMetadata> entry : thetaBlobsByFieldId.entrySet()) {
                int fieldId = entry.getKey();
                BlobMetadata blobMetadata = entry.getValue();
                String ndv = blobMetadata.properties().get(APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY);
                if (ndv == null) {
                    log.debug("Blob %s is missing %s property", blobMetadata.type(), APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY);
                }
                else {
                    ndvByColumnId.put(fieldId, parseLong(ndv));
                }
            }
        });

        return ndvByColumnId.buildOrThrow();
    }

    /**
     * Returns most recent statistics file for the given {@code snapshotId}
     */
    public static Optional<StatisticsFile> getLatestStatisticsFile(Table icebergTable, long snapshotId)
    {
        if (icebergTable.statisticsFiles().isEmpty()) {
            return Optional.empty();
        }

        Map<Long, StatisticsFile> statsFileBySnapshot = icebergTable.statisticsFiles().stream()
                .collect(toMap(
                        StatisticsFile::snapshotId,
                        identity(),
                        (file1, file2) -> {
                            throw new TrinoException(
                                    ICEBERG_INVALID_METADATA,
                                    "Table '%s' has duplicate statistics files '%s' and '%s' for snapshot ID %s"
                                            .formatted(icebergTable, file1.path(), file2.path(), file1.snapshotId()));
                        }));

        return stream(walkSnapshots(icebergTable, snapshotId))
                .map(statsFileBySnapshot::get)
                .filter(Objects::nonNull)
                .findFirst();
    }

    /**
     * Iterates over parent snapshot chain, starting at {@code startingSnapshotId} (inclusive).
     */
    private static Iterator<Long> walkSnapshots(Table icebergTable, long startingSnapshotId)
    {
        return new AbstractSequentialIterator<>(startingSnapshotId)
        {
            @Override
            protected Long computeNext(Long previous)
            {
                requireNonNull(previous, "previous is null");
                @Nullable
                Snapshot snapshot = icebergTable.snapshot(previous);
                if (snapshot == null) {
                    // Snapshot referenced by `previous` is expired from table history
                    return null;
                }
                if (snapshot.parentId() == null) {
                    // Snapshot referenced by `previous` had no parent.
                    return null;
                }
                return verifyNotNull(snapshot.parentId(), "snapshot.parentId()");
            }
        };
    }

    private static CloseableIterable<DataFile> readManifest(Table icebergTable, ManifestFile manifestFile, Expression filter, Set<Integer> columnIds)
    {
        return CloseableIterable.transform(
                liveEntries(
                        ManifestFiles.read(manifestFile, icebergTable.io(), icebergTable.specs())
                                .select(ImmutableSet.of(
                                        "partition",
                                        "file_path",
                                        "column_sizes",
                                        "value_counts",
                                        "null_value_counts",
                                        "nan_value_counts",
                                        "lower_bounds",
                                        "upper_bounds",
                                        "record_count"))
                                .filterRows(filter)),
                dataFile -> dataFile.copyWithStats(columnIds));
    }
}
