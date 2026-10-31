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
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.cache.NonEvictableCache;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatistics;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.iceberg.TableStatisticsReader.collectManifestStatistics;
import static java.lang.Long.parseLong;
import static java.lang.Math.toIntExact;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.FileFormat.AVRO;
import static org.apache.iceberg.FileFormat.PARQUET;

/**
 * Computes row counts from Iceberg partition statistics files and snapshot summaries instead of walking every
 * data file entry. Avro and Parquet files are read through the Iceberg library, files in other formats are
 * ignored. Decoded partition statistics files are cached up to an estimate of their retained size, and files that do
 * not fit are not used.
 */
public final class PartitionStatisticsReader
{
    private static final Logger log = Logger.get(PartitionStatisticsReader.class);
    // the library reads Parquet through the reader IcebergInternalData registers, and it has no ORC implementation
    private static final Set<FileFormat> READABLE_FORMATS = ImmutableSet.of(AVRO, PARQUET);

    // null fractions and data sizes are ratios, so a bounded sample of data files is enough to estimate them
    @VisibleForTesting
    static final int SAMPLE_DATA_FILE_LIMIT = 1000;

    private static final DataSize MAXIMUM_CACHE_SIZE = DataSize.of(64, MEGABYTE);
    private static final Duration CACHE_EXPIRATION = Duration.ofHours(1);
    // a file that cannot be used, for example after a transient read failure, is tried again after this time
    @VisibleForTesting
    static final Duration UNUSABLE_FILE_EXPIRATION = Duration.ofMinutes(5);
    private static final long MAXIMUM_UNUSABLE_FILES = 10_000;
    // estimated retained size of a row, its partition record and the record's array of values
    private static final long ROW_ESTIMATED_BYTES = 100;
    // estimated retained size of a boxed partition value, or of the object holding a string or binary value
    private static final long PARTITION_VALUE_ESTIMATED_BYTES = 48;

    private final DataSize maximumCacheSize;
    private final NonEvictableCache<CacheKey, CachedPartitionStatistics> cache;
    private final NonEvictableCache<CacheKey, Boolean> unusableFiles;
    private final AtomicLong failures = new AtomicLong();

    @Inject
    public PartitionStatisticsReader()
    {
        this(MAXIMUM_CACHE_SIZE, Ticker.systemTicker());
    }

    @VisibleForTesting
    PartitionStatisticsReader(DataSize maximumCacheSize)
    {
        this(maximumCacheSize, Ticker.systemTicker());
    }

    @VisibleForTesting
    PartitionStatisticsReader(DataSize maximumCacheSize, Ticker ticker)
    {
        this.maximumCacheSize = requireNonNull(maximumCacheSize, "maximumCacheSize is null");
        this.cache = buildNonEvictableCache(CacheBuilder.newBuilder()
                // a single segment, so that the maximum weight bounds the whole cache instead of each segment
                .concurrencyLevel(1)
                .maximumWeight(maximumCacheSize.toBytes())
                // an entry is never larger than the maximum weight
                .weigher((CacheKey _, CachedPartitionStatistics statistics) -> toIntExact(statistics.estimatedBytes()))
                .expireAfterAccess(CACHE_EXPIRATION));
        this.unusableFiles = buildNonEvictableCache(CacheBuilder.newBuilder()
                .ticker(ticker)
                .maximumSize(MAXIMUM_UNUSABLE_FILES)
                .expireAfterWrite(UNUSABLE_FILE_EXPIRATION));
    }

    /**
     * Returns whether {@code file} can be read. Files in other formats are ignored.
     */
    static boolean isReadable(PartitionStatisticsFile file)
    {
        return READABLE_FORMATS.contains(FileFormat.fromFileName(file.path()));
    }

    /**
     * Returns the partition statistics file recorded for exactly {@code snapshotId}, if it can be read.
     */
    static Optional<PartitionStatisticsFile> findPartitionStatisticsFile(Table table, long snapshotId)
    {
        return table.partitionStatisticsFiles().stream()
                .filter(file -> file.snapshotId() == snapshotId)
                .filter(PartitionStatisticsReader::isReadable)
                .findFirst();
    }

    /**
     * Returns the row count of the whole snapshot, or empty if the summary does not have usable totals.
     */
    static OptionalLong unfilteredRowCount(Map<String, String> snapshotSummary, int formatVersion)
    {
        try {
            String totalRecords = snapshotSummary.get(SnapshotSummary.TOTAL_RECORDS_PROP);
            if (totalRecords == null) {
                return OptionalLong.empty();
            }
            if (subtractPositionDeletes(formatVersion)) {
                // the totals do not tell deletion vectors from position delete files, which are not subtracted
                String totalPositionDeletes = snapshotSummary.get(SnapshotSummary.TOTAL_POS_DELETES_PROP);
                if (totalPositionDeletes == null || parseLong(totalPositionDeletes) != 0) {
                    return OptionalLong.empty();
                }
            }
            long rowCount = parseLong(totalRecords);
            if (rowCount < 0) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(rowCount);
        }
        catch (NumberFormatException _) {
            return OptionalLong.empty();
        }
    }

    /**
     * Returns the number of live rows in the partitions of the file recorded for {@code snapshotId} that match
     * {@code filter} and {@code partitionDomain}, or empty if the answer cannot be derived.
     */
    public OptionalLong filteredRowCount(Table table, long snapshotId, Expression filter, Domain partitionDomain, int formatVersion)
    {
        Optional<PartitionStatisticsFile> file = findPartitionStatisticsFile(table, snapshotId);
        if (file.isEmpty()) {
            return OptionalLong.empty();
        }
        String path = file.get().path();
        try {
            Types.StructType partitionType = Partitioning.partitionType(table);
            // the file path is unique per file and immutable, and the partition type the rows are read with changes when
            // the type of a partition source column changes, which does not add a snapshot
            CacheKey key = new CacheKey(path, partitionType);
            Optional<CachedPartitionStatistics> loaded = loadPartitionStatistics(key, table, file.get().snapshotId());
            if (loaded.isEmpty()) {
                return OptionalLong.empty();
            }
            CachedPartitionStatistics statistics = loaded.get();
            boolean subtractPositionDeletes = subtractPositionDeletes(formatVersion);
            Map<Integer, SpecFilter> specFilters = new HashMap<>();
            long rowCount = 0;
            int matchedPartitions = 0;
            for (PartitionStatisticsRow row : statistics.rows()) {
                SpecFilter specFilter = specFilters.get(row.specId());
                if (specFilter == null) {
                    PartitionSpec spec = table.specs().get(row.specId());
                    if (spec == null) {
                        log.debug("Partition statistics file %s references unknown partition spec %s", path, row.specId());
                        return OptionalLong.empty();
                    }
                    specFilter = new SpecFilter(
                            spec,
                            new Evaluator(spec.partitionType(), Projections.inclusive(spec, true).project(filter), true),
                            // strict: throws if a field of the spec is missing from the rows, which falls back to the manifest walk
                            StructProjection.create(statistics.partitionType(), spec.partitionType()));
                    specFilters.put(row.specId(), specFilter);
                }
                StructLike partition = specFilter.projection().wrap(row.partition());
                boolean matches = specFilter.evaluator().eval(partition)
                        && (partitionDomain.isAll() || partitionDomain.includesNullableValue(utf8Slice(specFilter.spec().partitionToPath(partition))));
                if (matches) {
                    rowCount += row.rowCount(subtractPositionDeletes);
                    matchedPartitions++;
                }
            }
            log.debug(
                    "Using partition statistics file %s for row count of %s at snapshot %s: %s rows in %s of %s partitions",
                    path,
                    table.name(),
                    snapshotId,
                    rowCount,
                    matchedPartitions,
                    statistics.rows().size());
            return OptionalLong.of(rowCount);
        }
        catch (ExecutionError e) {
            if (e.getCause() instanceof LinkageError) {
                failures.incrementAndGet();
                log.debug(e, "Failed to use partition statistics file %s", path);
                return OptionalLong.empty();
            }
            throw e;
        }
        // linkage errors mean a reader dependency is missing at runtime, which must not fail planning
        catch (ExecutionException | RuntimeException | LinkageError e) {
            failures.incrementAndGet();
            log.debug(e, "Failed to use partition statistics file %s", path);
            return OptionalLong.empty();
        }
    }

    @VisibleForTesting
    long failureCount()
    {
        return failures.get();
    }

    /**
     * Returns the rows of the file, or empty if the file is too large or cannot be read, which is remembered for
     * {@link #UNUSABLE_FILE_EXPIRATION}, so that a popular file is neither read on every planning call, nor ignored for
     * long after a transient failure.
     */
    private Optional<CachedPartitionStatistics> loadPartitionStatistics(CacheKey key, Table table, long snapshotId)
            throws ExecutionException
    {
        if (unusableFiles.getIfPresent(key) != null) {
            return Optional.empty();
        }
        try {
            // single flight per key; a file that is too large or cannot be read is not cached here
            return Optional.of(cache.get(key, () -> readPartitionStatistics(table, snapshotId, key.partitionType())
                    .orElseThrow(() -> new FileTooLargeException(key.path()))));
        }
        catch (UncheckedExecutionException e) {
            unusableFiles.put(key, true);
            if (e.getCause() instanceof FileTooLargeException) {
                log.debug("Partition statistics file %s exceeds %s, not using it", key.path(), maximumCacheSize);
                return Optional.empty();
            }
            throw e;
        }
        catch (ExecutionException | ExecutionError e) {
            unusableFiles.put(key, true);
            throw e;
        }
    }

    /**
     * Returns the rows of the file, or empty if their estimated retained size exceeds the maximum cache size,
     * in which case reading stops there.
     */
    private Optional<CachedPartitionStatistics> readPartitionStatistics(Table table, long snapshotId, Types.StructType partitionType)
    {
        ImmutableList.Builder<PartitionStatisticsRow> rows = ImmutableList.builder();
        long estimatedBytes = 0;
        try (CloseableIterable<PartitionStatistics> statistics = table.newPartitionStatisticsScan().useSnapshot(snapshotId).scan()) {
            for (PartitionStatistics statistic : statistics) {
                // A partition without rows adds nothing to a row count. Incremental computation keeps a partition whose
                // data files were all removed, with no rows, so a file can hold many of them.
                if (statistic.dataRecordCount() == null || statistic.dataRecordCount() == 0) {
                    continue;
                }
                // copy, so that no reader-owned containers are retained
                GenericRecord partition = GenericRecord.create(partitionType);
                for (int i = 0; i < partitionType.fields().size(); i++) {
                    partition.set(i, statistic.partition().get(i, Object.class));
                }
                PartitionStatisticsRow row = new PartitionStatisticsRow(
                        statistic.specId(),
                        partition,
                        statistic.dataRecordCount(),
                        statistic.positionDeleteRecordCount() == null ? 0 : statistic.positionDeleteRecordCount(),
                        // an unknown count could include position delete files
                        statistic.positionDeleteFileCount() == null ? 1 : statistic.positionDeleteFileCount());
                estimatedBytes += row.estimatedBytes();
                if (estimatedBytes > maximumCacheSize.toBytes()) {
                    return Optional.empty();
                }
                rows.add(row);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Optional.of(new CachedPartitionStatistics(partitionType, rows.build(), estimatedBytes));
    }

    /**
     * Position deletes are subtracted only for format version 3 tables, and only for partitions whose position
     * deletes are all deletion vectors. Deletion vectors always reference live data files, while position delete
     * files, which tables upgraded from format version 2 can still have, can outlive the data files they reference,
     * for example after compaction, and subtracting them could underestimate the row count.
     */
    private static boolean subtractPositionDeletes(int formatVersion)
    {
        return formatVersion >= 3;
    }

    /**
     * Number of data files the manifests list as added or existing, from manifest list metadata only.
     * Empty when a manifest does not report both counts.
     */
    static OptionalLong countDataFiles(List<ManifestFile> manifests)
    {
        long count = 0;
        for (ManifestFile manifest : manifests) {
            if (manifest.addedFilesCount() == null || manifest.existingFilesCount() == null) {
                return OptionalLong.empty();
            }
            count += manifest.addedFilesCount() + manifest.existingFilesCount();
        }
        return OptionalLong.of(count);
    }

    /**
     * Collects statistics from at most {@link #SAMPLE_DATA_FILE_LIMIT} matching data files of the smallest of the
     * given manifests, as a representative sample of column shape. Returns empty if the sample has no rows.
     */
    static Optional<IcebergStatistics> sampleManifest(
            Table table,
            List<ManifestFile> filteredManifests,
            Expression filter,
            Set<Integer> columnIds,
            Domain partitionDomain,
            Domain pathDomain,
            List<Types.NestedField> columns,
            List<Type> columnTypes,
            TypeManager typeManager)
    {
        return sampleManifest(table, filteredManifests, filter, columnIds, partitionDomain, pathDomain, columns, columnTypes, typeManager, SAMPLE_DATA_FILE_LIMIT);
    }

    @VisibleForTesting
    static Optional<IcebergStatistics> sampleManifest(
            Table table,
            List<ManifestFile> filteredManifests,
            Expression filter,
            Set<Integer> columnIds,
            Domain partitionDomain,
            Domain pathDomain,
            List<Types.NestedField> columns,
            List<Type> columnTypes,
            TypeManager typeManager,
            int dataFileLimit)
    {
        // the smallest manifest is the cheapest to read; manifests are merged into large files on big tables
        Optional<ManifestFile> smallest = filteredManifests.stream().min(comparingLong(ManifestFile::length));
        if (smallest.isEmpty()) {
            return Optional.empty();
        }
        IcebergStatistics.Builder builder = new IcebergStatistics.Builder(columns, columnTypes, typeManager);
        collectManifestStatistics(builder, table, smallest.get(), filter, columnIds, partitionDomain, pathDomain, dataFileLimit);
        IcebergStatistics sample = builder.build();
        if (sample.recordCount() == 0) {
            return Optional.empty();
        }
        return Optional.of(sample);
    }

    private record CacheKey(String path, Types.StructType partitionType) {}

    record CachedPartitionStatistics(Types.StructType partitionType, List<PartitionStatisticsRow> rows, long estimatedBytes) {}

    private static final class FileTooLargeException
            extends RuntimeException
    {
        private FileTooLargeException(String path)
        {
            super("Partition statistics file is too large to cache: " + path);
        }
    }

    record PartitionStatisticsRow(int specId, StructLike partition, long dataRecordCount, long positionDeleteRecordCount, int positionDeleteFileCount)
    {
        long rowCount(boolean subtractPositionDeletes)
        {
            // negative counts mean a corrupt file; the caller falls back to the manifest walk
            verify(positionDeleteRecordCount >= 0, "negative position delete record count %s for partition %s", positionDeleteRecordCount, partition);
            long rowCount = dataRecordCount;
            // positionDeleteRecordCount includes deletion vectors, positionDeleteFileCount does not count them
            if (subtractPositionDeletes && positionDeleteFileCount == 0) {
                rowCount -= positionDeleteRecordCount;
            }
            verify(rowCount >= 0, "negative row count %s for partition %s", rowCount, partition);
            return rowCount;
        }

        long estimatedBytes()
        {
            long bytes = ROW_ESTIMATED_BYTES;
            for (int i = 0; i < partition.size(); i++) {
                bytes += switch (partition.get(i, Object.class)) {
                    case null -> 0;
                    case CharSequence value -> PARTITION_VALUE_ESTIMATED_BYTES + value.length();
                    case ByteBuffer value -> PARTITION_VALUE_ESTIMATED_BYTES + value.remaining();
                    default -> PARTITION_VALUE_ESTIMATED_BYTES;
                };
            }
            return bytes;
        }
    }

    // the projection is reused for every row of the spec, so a wrapped partition must not be retained
    private record SpecFilter(PartitionSpec spec, Evaluator evaluator, StructProjection projection) {}
}
