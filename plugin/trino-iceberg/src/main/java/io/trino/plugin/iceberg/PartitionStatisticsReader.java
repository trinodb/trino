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
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.cache.NonEvictableCache;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.cache.ParquetFooterCache;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.iceberg.fileio.ForwardingInputFile;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatistics;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.InternalReader;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.iceberg.IcebergPageSink.getIcebergValue;
import static io.trino.plugin.iceberg.IcebergPageSourceProvider.createParquetPageSource;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.TableStatisticsReader.collectManifestStatistics;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Long.parseLong;
import static java.lang.Math.toIntExact;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.FileFormat.AVRO;
import static org.apache.iceberg.FileFormat.PARQUET;

/**
 * Computes row counts from Iceberg partition statistics files and snapshot summaries instead of walking every
 * data file entry. Avro files are read with the Iceberg library and Parquet files with Trino's Parquet reader, files
 * in other formats are ignored. Decoded partition statistics files are cached up to an estimate of their retained
 * size, and files that do not fit are not used.
 */
public final class PartitionStatisticsReader
{
    private static final Logger log = Logger.get(PartitionStatisticsReader.class);
    // the Iceberg library writes partition statistics files only in Avro and Parquet
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

    // positions of the fields in the records read with readSchema
    private static final int READ_PARTITION = 0;
    private static final int READ_SPEC_ID = 1;
    private static final int READ_DATA_RECORD_COUNT = 2;
    private static final int READ_POSITION_DELETE_RECORD_COUNT = 3;
    private static final int READ_POSITION_DELETE_FILE_COUNT = 4;

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
     * Returns the number of live rows in the partitions of {@code file} that match {@code filter} and
     * {@code partitionFieldPredicate}, or empty if the answer cannot be derived.
     */
    public OptionalLong filteredRowCount(Table table, PartitionStatisticsFile file, Expression filter, PartitionFieldPredicate partitionFieldPredicate, int formatVersion, TypeManager typeManager)
    {
        String path = file.path();
        try {
            Types.StructType partitionType = Partitioning.partitionType(table);
            // the file path is unique per file and immutable, and the partition type the rows are read with changes when
            // the type of a partition source column changes, which does not add a snapshot
            CacheKey key = new CacheKey(path, partitionType);
            Optional<CachedPartitionStatistics> loaded = loadPartitionStatistics(key, table, file, typeManager);
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
                        && (partitionFieldPredicate.isAll() || partitionFieldPredicate.matches(specFilter.spec(), partition));
                if (matches) {
                    rowCount += row.rowCount(subtractPositionDeletes);
                    matchedPartitions++;
                }
            }
            log.debug(
                    "Using partition statistics file %s for row count of %s at snapshot %s: %s rows in %s of %s partitions",
                    path,
                    table.name(),
                    file.snapshotId(),
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
    private Optional<CachedPartitionStatistics> loadPartitionStatistics(CacheKey key, Table table, PartitionStatisticsFile file, TypeManager typeManager)
            throws ExecutionException
    {
        if (unusableFiles.getIfPresent(key) != null) {
            return Optional.empty();
        }
        try {
            // single flight per key; a file that is too large or cannot be read is not cached here
            return Optional.of(cache.get(key, () -> readPartitionStatistics(table, file, key.partitionType(), typeManager)
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
    private Optional<CachedPartitionStatistics> readPartitionStatistics(Table table, PartitionStatisticsFile file, Types.StructType partitionType, TypeManager typeManager)
    {
        ImmutableList.Builder<PartitionStatisticsRow> rows = ImmutableList.builder();
        long estimatedBytes = 0;
        try (CloseableIterable<StructLike> records = readRecords(table.io().newInputFile(file.path(), file.fileSizeInBytes()), readSchema(partitionType), typeManager)) {
            for (StructLike record : records) {
                // A partition without rows adds nothing to a row count. Incremental computation keeps a partition whose
                // data files were all removed, with no rows, so a file can hold many of them.
                Long dataRecordCount = record.get(READ_DATA_RECORD_COUNT, Long.class);
                if (dataRecordCount == null || dataRecordCount == 0) {
                    continue;
                }
                // copy, so that no reader-owned containers are retained
                StructLike recordPartition = record.get(READ_PARTITION, StructLike.class);
                GenericRecord partition = GenericRecord.create(partitionType);
                for (int i = 0; i < partitionType.fields().size(); i++) {
                    partition.set(i, recordPartition.get(i, Object.class));
                }
                Long positionDeleteRecordCount = record.get(READ_POSITION_DELETE_RECORD_COUNT, Long.class);
                Integer positionDeleteFileCount = record.get(READ_POSITION_DELETE_FILE_COUNT, Integer.class);
                PartitionStatisticsRow row = new PartitionStatisticsRow(
                        record.get(READ_SPEC_ID, Integer.class),
                        partition,
                        dataRecordCount,
                        positionDeleteRecordCount == null ? 0 : positionDeleteRecordCount,
                        // an unknown count could include position delete files
                        positionDeleteFileCount == null ? 1 : positionDeleteFileCount);
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

    static CloseableIterable<StructLike> readRecords(InputFile inputFile, Schema schema, TypeManager typeManager)
    {
        return switch (FileFormat.fromFileName(inputFile.location())) {
            case AVRO -> Avro.read(inputFile).project(schema).createResolvingReader(InternalReader::create).build();
            case PARQUET -> readParquet(inputFile, schema, typeManager);
            default -> throw new IllegalArgumentException("Unsupported partition statistics file: " + inputFile.location());
        };
    }

    /**
     * Reads a Parquet file with Trino's Parquet reader into records holding Iceberg's internal representation of values,
     * like the records the Avro reader returns. The Iceberg library's own Parquet reader needs {@code parquet-hadoop},
     * which is not on the plugin class path.
     */
    private static CloseableIterable<StructLike> readParquet(InputFile inputFile, Schema schema, TypeManager typeManager)
    {
        if (!(inputFile instanceof ForwardingInputFile forwardingInputFile)) {
            throw new IllegalArgumentException("Unsupported input file: " + inputFile);
        }
        TrinoInputFile file = forwardingInputFile.inputFile();
        List<IcebergColumnHandle> columns = schema.columns().stream()
                .map(field -> getColumnHandle(field, typeManager))
                .collect(toImmutableList());
        ConnectorPageSource pageSource;
        try {
            long length = file.length();
            // the record count, partition and sequence number arguments describe a data file, and only metadata columns,
            // which are not read, use them
            pageSource = createParquetPageSource(
                    file,
                    0,
                    length,
                    length,
                    0,
                    0,
                    "",
                    schema,
                    columns,
                    ParquetReaderOptions.defaultOptions(),
                    TupleDomain.all(),
                    1,
                    new FileFormatDataSourceStats(),
                    ParquetFooterCache.noop(),
                    Optional.empty(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    newSimpleAggregatedMemoryContext(),
                    Optional.empty()).pageSource();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Iterator<StructLike> records = new AbstractIterator<>()
        {
            private SourcePage page;
            private int position;

            @Override
            protected StructLike computeNext()
            {
                while (page == null || position == page.getPositionCount()) {
                    if (pageSource.isFinished()) {
                        return endOfData();
                    }
                    page = pageSource.getNextSourcePage();
                    position = 0;
                }
                GenericRecord record = GenericRecord.create(schema);
                for (int field = 0; field < columns.size(); field++) {
                    record.set(field, icebergValue(schema.columns().get(field).type(), columns.get(field).getType(), page.getBlock(field), position));
                }
                position++;
                return record;
            }
        };
        // iterated once, by readPartitionStatistics
        return CloseableIterable.combine(() -> records, pageSource);
    }

    private static Object icebergValue(org.apache.iceberg.types.Type icebergType, Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (type instanceof RowType rowType) {
            Types.StructType struct = icebergType.asStructType();
            SqlRow row = rowType.getObject(block, position);
            GenericRecord record = GenericRecord.create(struct);
            for (int field = 0; field < struct.fields().size(); field++) {
                record.set(field, icebergValue(struct.fields().get(field).type(), rowType.getFieldTypes().get(field), row.getRawFieldBlock(field), row.getRawIndex()));
            }
            return record;
        }
        if (type.equals(VARBINARY)) {
            // a copy, so that the value does not retain the page; fixed values are ByteBuffers too
            return ByteBuffer.wrap(VARBINARY.getSlice(block, position).getBytes());
        }
        return getIcebergValue(block, position, type);
    }

    /**
     * The fields of a partition statistics file that the row count uses, read by field ID.
     */
    private static Schema readSchema(Types.StructType partitionType)
    {
        return new Schema(
                Types.NestedField.required(PartitionStatistics.EMPTY_PARTITION_FIELD.fieldId(), PartitionStatistics.EMPTY_PARTITION_FIELD.name(), partitionType),
                PartitionStatistics.SPEC_ID,
                PartitionStatistics.DATA_RECORD_COUNT,
                PartitionStatistics.POSITION_DELETE_RECORD_COUNT,
                PartitionStatistics.POSITION_DELETE_FILE_COUNT);
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
            PartitionFieldPredicate partitionFieldPredicate,
            Domain pathDomain,
            List<Types.NestedField> columns,
            List<Type> columnTypes,
            TypeManager typeManager)
    {
        return sampleManifest(table, filteredManifests, filter, columnIds, partitionFieldPredicate, pathDomain, columns, columnTypes, typeManager, SAMPLE_DATA_FILE_LIMIT);
    }

    @VisibleForTesting
    static Optional<IcebergStatistics> sampleManifest(
            Table table,
            List<ManifestFile> filteredManifests,
            Expression filter,
            Set<Integer> columnIds,
            PartitionFieldPredicate partitionFieldPredicate,
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
        collectManifestStatistics(builder, table, smallest.get(), filter, columnIds, partitionFieldPredicate, pathDomain, dataFileLimit);
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
