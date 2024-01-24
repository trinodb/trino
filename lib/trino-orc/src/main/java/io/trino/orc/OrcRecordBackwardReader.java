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
package io.trino.orc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.google.errorprone.annotations.FormatMethod;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.orc.OrcReader.FieldMapperFactory;
import io.trino.orc.OrcWriteValidation.StatisticsValidation;
import io.trino.orc.OrcWriteValidation.WriteChecksum;
import io.trino.orc.OrcWriteValidation.WriteChecksumBuilder;
import io.trino.orc.metadata.ColumnEncoding;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.MetadataReader;
import io.trino.orc.metadata.OrcType;
import io.trino.orc.metadata.PostScript.HiveWriterVersion;
import io.trino.orc.metadata.StripeInformation;
import io.trino.orc.metadata.statistics.ColumnStatistics;
import io.trino.orc.metadata.statistics.StripeStatistics;
import io.trino.orc.reader.ColumnReader;
import io.trino.orc.stream.InputStreamSources;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.orc.OrcReader.BATCH_SIZE_GROWTH_FACTOR;
import static io.trino.orc.OrcReader.MAX_BATCH_SIZE;
import static io.trino.orc.OrcRecordReader.LinearProbeRangeFinder.createTinyStripesRangeFinder;
import static io.trino.orc.OrcWriteValidation.WriteChecksumBuilder.createWriteChecksumBuilder;
import static io.trino.spi.block.LazyBlock.listenForLoads;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

public class OrcRecordBackwardReader
        implements RecordReader
{
    private final OrcDataSource orcDataSource;

    private final ColumnReader[] columnReaders;
    private final long[] currentBytesPerCell;
    private final long[] maxBytesPerCell;
    private long maxCombinedBytesPerRow;

    private final long totalRowCount;
    private final long splitLength;
    private final long totalDataLength;
    private final long maxBlockBytes;
    private final ColumnMetadata<OrcType> orcTypes;
    private int currentBatchSize;
    private int nextBatchSize;
    private int maxBatchSize = MAX_BATCH_SIZE;

    private final List<StripeInformation> stripes;
    private final StripeReader stripeReader;
    private AggregatedMemoryContext currentStripeMemoryContext;

    private final long fileRowCount;
    private final List<Long> stripeFilePositions;
    private int stripeSize;
    private long filePosition;

    private ListIterator<RowGroup> backwardRowGroups = ImmutableList.<RowGroup>of().listIterator();
    private int currentRowGroup = -1;
    private long currentGroupRowCount;
    private long nextRowInGroup;

    private final Map<String, Slice> userMetadata;

    private final AggregatedMemoryContext memoryUsage;
    private final LocalMemoryContext orcDataSourceMemoryUsage;

    private final OrcBlockFactory blockFactory;

    private final Optional<OrcWriteValidation> writeValidation;
    private final Optional<WriteChecksumBuilder> writeChecksumBuilder;
    private final Optional<StatisticsValidation> rowGroupStatisticsValidation;
    private final Optional<StatisticsValidation> stripeStatisticsValidation;
    private final Optional<StatisticsValidation> fileStatisticsValidation;

    private final Optional<Long> startRowPosition;
    private final Optional<Long> endRowPosition;

    public OrcRecordBackwardReader(
            List<OrcColumn> readColumns,
            List<Type> readTypes,
            List<OrcReader.ProjectedLayout> readLayouts,
            OrcPredicate predicate,
            long numberOfRows,
            List<StripeInformation> fileStripes,
            Optional<ColumnMetadata<ColumnStatistics>> fileStats,
            List<Optional<StripeStatistics>> stripeStats,
            OrcDataSource orcDataSource,
            long splitOffset,
            long splitLength,
            ColumnMetadata<OrcType> orcTypes,
            Optional<OrcDecompressor> decompressor,
            OptionalInt rowsInRowGroup,
            DateTimeZone legacyFileTimeZone,
            HiveWriterVersion hiveWriterVersion,
            MetadataReader metadataReader,
            OrcReaderOptions options,
            Map<String, Slice> userMetadata,
            AggregatedMemoryContext memoryUsage,
            Optional<OrcWriteValidation> writeValidation,
            int initialBatchSize,
            Function<Exception, RuntimeException> exceptionTransform,
            FieldMapperFactory fieldMapperFactory)
            throws OrcCorruptionException
    {
        requireNonNull(readColumns, "readColumns is null");
        checkArgument(readColumns.stream().distinct().count() == readColumns.size(), "readColumns contains duplicate entries");
        requireNonNull(readTypes, "readTypes is null");
        checkArgument(readColumns.size() == readTypes.size(), "readColumns and readTypes must have the same size");
        requireNonNull(readLayouts, "readLayouts is null");
        checkArgument(readColumns.size() == readLayouts.size(), "readColumns and readLayouts must have the same size");
        requireNonNull(predicate, "predicate is null");
        requireNonNull(fileStripes, "fileStripes is null");
        requireNonNull(stripeStats, "stripeStats is null");
        requireNonNull(orcDataSource, "orcDataSource is null");
        this.orcTypes = requireNonNull(orcTypes, "orcTypes is null");
        requireNonNull(decompressor, "decompressor is null");
        requireNonNull(legacyFileTimeZone, "legacyFileTimeZone is null");
        requireNonNull(userMetadata, "userMetadata is null");
        requireNonNull(memoryUsage, "memoryUsage is null");
        requireNonNull(exceptionTransform, "exceptionTransform is null");

        this.writeValidation = requireNonNull(writeValidation, "writeValidation is null");
        this.writeChecksumBuilder = writeValidation.map(validation -> createWriteChecksumBuilder(orcTypes, readTypes));
        this.rowGroupStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(orcTypes, readTypes));
        this.stripeStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(orcTypes, readTypes));
        this.fileStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(orcTypes, readTypes));
        this.memoryUsage = memoryUsage.newAggregatedMemoryContext();
        this.blockFactory = new OrcBlockFactory(exceptionTransform, options.isNestedLazy());

        requireNonNull(options, "options is null");
        this.maxBlockBytes = options.getMaxBlockSize().toBytes();

        // sort stripes by file position
        List<StripeInfo> stripeInfos = new ArrayList<>();
        for (int i = 0; i < fileStripes.size(); i++) {
            Optional<StripeStatistics> stats = Optional.empty();
            // ignore all stripe stats if too few or too many
            if (stripeStats.size() == fileStripes.size()) {
                stats = stripeStats.get(i);
            }
            stripeInfos.add(new StripeInfo(fileStripes.get(i), stats));
        }
        stripeInfos.sort(comparingLong(info -> info.getStripe().getOffset()));

        long totalRowCount = 0;
        long fileRowCount = 0;
        long totalDataLength = 0;
        Optional<Long> startRowPosition = Optional.empty();
        Optional<Long> endRowPosition = Optional.empty();
        ImmutableList.Builder<StripeInformation> stripes = ImmutableList.builder();
        ImmutableList.Builder<Long> stripeFilePositions = ImmutableList.builder();
        if (fileStats.isEmpty() || predicate.matches(numberOfRows, fileStats.get())) {
            // select stripes that start within the specified split
            for (StripeInfo info : stripeInfos) {
                StripeInformation stripe = info.getStripe();
                if (splitContainsStripe(splitOffset, splitLength, stripe) && isStripeIncluded(stripe, info.getStats(), predicate)) {
                    stripes.add(stripe);
                    stripeFilePositions.add(fileRowCount);
                    totalRowCount += stripe.getNumberOfRows();
                    totalDataLength += stripe.getDataLength();

                    if (startRowPosition.isEmpty()) {
                        startRowPosition = Optional.of(fileRowCount);
                    }
                    endRowPosition = Optional.of(fileRowCount + stripe.getNumberOfRows());
                }
                fileRowCount += stripe.getNumberOfRows();
            }
        }

        this.startRowPosition = startRowPosition;
        this.endRowPosition = endRowPosition;

        this.totalRowCount = totalRowCount;
        this.totalDataLength = totalDataLength;
        this.stripes = stripes.build();
        this.stripeFilePositions = stripeFilePositions.build();
        this.stripeSize = this.stripes.size();

        orcDataSource = wrapWithCacheIfTinyStripes(orcDataSource, this.stripes, options.getMaxMergeDistance(), options.getTinyStripeThreshold());
        this.orcDataSource = orcDataSource;
        this.orcDataSourceMemoryUsage = memoryUsage.newLocalMemoryContext(OrcDataSource.class.getSimpleName());
        this.orcDataSourceMemoryUsage.setBytes(orcDataSource.getRetainedSize());
        this.splitLength = splitLength;

        this.fileRowCount = stripeInfos.stream()
                .map(StripeInfo::getStripe)
                .mapToLong(StripeInformation::getNumberOfRows)
                .sum();

        this.userMetadata = ImmutableMap.copyOf(Maps.transformValues(userMetadata, Slice::copy));

        this.currentStripeMemoryContext = this.memoryUsage.newAggregatedMemoryContext();
        // The streamReadersMemoryContext covers the StreamReader local buffer sizes, plus leaf node StreamReaders'
        // instance sizes who use local buffers. SliceDirectStreamReader's instance size is not counted, because it
        // doesn't have a local buffer. All non-leaf level StreamReaders' (e.g. MapStreamReader, LongStreamReader,
        // ListStreamReader and StructStreamReader) instance sizes were not counted, because calling setBytes() in
        // their constructors is confusing.
        AggregatedMemoryContext streamReadersMemoryContext = this.memoryUsage.newAggregatedMemoryContext();

        stripeReader = new StripeReader(
                orcDataSource,
                ZoneId.of(legacyFileTimeZone.getID()),
                decompressor,
                orcTypes,
                ImmutableSet.copyOf(readColumns),
                rowsInRowGroup,
                predicate,
                hiveWriterVersion,
                metadataReader,
                writeValidation);

        columnReaders = createColumnReaders(
                readColumns,
                readTypes,
                readLayouts,
                streamReadersMemoryContext,
                blockFactory,
                fieldMapperFactory);

        currentBytesPerCell = new long[columnReaders.length];
        maxBytesPerCell = new long[columnReaders.length];
        nextBatchSize = initialBatchSize;
    }

    @VisibleForTesting
    static OrcDataSource wrapWithCacheIfTinyStripes(OrcDataSource dataSource, List<StripeInformation> stripes, DataSize maxMergeDistance, DataSize tinyStripeThreshold)
    {
        if (dataSource instanceof MemoryOrcDataSource || dataSource instanceof CachingOrcDataSource) {
            return dataSource;
        }
        for (StripeInformation stripe : stripes) {
            if (stripe.getTotalLength() > tinyStripeThreshold.toBytes()) {
                return dataSource;
            }
        }
        return new CachingOrcDataSource(dataSource, createTinyStripesRangeFinder(stripes, maxMergeDistance, tinyStripeThreshold));
    }

    /**
     * Return the row position relative to the start of the file.
     */
    @Override
    public long getFilePosition()
    {
        return filePosition;
    }

    /**
     * Returns the total number of rows in the file. This count includes rows
     * for stripes that were completely excluded due to stripe statistics.
     */
    public long getFileRowCount()
    {
        return fileRowCount;
    }

    /**
     * Returns the total number of rows that can possibly be read by this reader.
     * This count may be fewer than the number of rows in the file if some
     * stripes were excluded due to stripe statistics, but may be more than
     * the number of rows read if some row groups are excluded due to statistics.
     */
    public long getReaderRowCount()
    {
        return totalRowCount;
    }

    public long getSplitLength()
    {
        return splitLength;
    }

    @Override
    public long getTotalDataLength()
    {
        return totalDataLength;
    }

    /**
     * Returns the sum of the largest cells in size from each column
     */
    @Override
    public long getMaxCombinedBytesPerRow()
    {
        return maxCombinedBytesPerRow;
    }

    @Override
    public ColumnMetadata<OrcType> getColumnTypes()
    {
        return orcTypes;
    }

    @Override
    public Optional<Long> getStartRowPosition()
    {
        return startRowPosition;
    }

    @Override
    public Optional<Long> getEndRowPosition()
    {
        return endRowPosition;
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(orcDataSource);
            for (ColumnReader column : columnReaders) {
                if (column != null) {
                    closer.register(column::close);
                }
            }
        }

        if (writeChecksumBuilder.isPresent()) {
            WriteChecksum actualChecksum = writeChecksumBuilder.get().build();
            validateWrite(validation -> validation.getChecksum().getTotalRowCount() == actualChecksum.getTotalRowCount(), "Invalid row count");
            List<Long> columnHashes = actualChecksum.getColumnHashes();
            for (int i = 0; i < columnHashes.size(); i++) {
                int columnIndex = i;
                validateWrite(validation -> validation.getChecksum().getColumnHashes().get(columnIndex).equals(columnHashes.get(columnIndex)),
                        "Invalid checksum for column %s", columnIndex);
            }
            validateWrite(validation -> validation.getChecksum().getStripeHash() == actualChecksum.getStripeHash(), "Invalid stripes checksum");
        }
        if (fileStatisticsValidation.isPresent()) {
            Optional<ColumnMetadata<ColumnStatistics>> columnStatistics = fileStatisticsValidation.get().build();
            writeValidation.get().validateFileStatistics(orcDataSource.getId(), columnStatistics);
        }
    }

    @Override
    public Page nextPage()
            throws IOException
    {
        // update position for current row group (advancing resets them)
        filePosition += currentBatchSize;
        currentBatchSize = 0;

        // if next row is within the current group return
        if (nextRowInGroup >= currentGroupRowCount) {
            // attempt to advance to next row group
            if (!advanceToNextRowGroup()) {
                filePosition = fileRowCount;
                return null;
            }
        }

        // We will grow currentBatchSize by BATCH_SIZE_GROWTH_FACTOR starting from initialBatchSize to maxBatchSize or
        // the number of rows left in this rowgroup, whichever is smaller. maxBatchSize is adjusted according to the
        // block size for every batch and never exceed MAX_BATCH_SIZE. But when the number of rows in the last batch in
        // the current rowgroup is smaller than min(nextBatchSize, maxBatchSize), the nextBatchSize for next batch in
        // the new rowgroup should be grown based on min(nextBatchSize, maxBatchSize) but not by the number of rows in
        // the last batch, i.e. currentGroupRowCount - nextRowInGroup. For example, if the number of rows read for
        // single fixed width column are: 1, 16, 256, 1024, 1024,..., 1024, 256 and the 256 was because there is only
        // 256 rows left in this row group, then the nextBatchSize should be 1024 instead of 512. So we need to grow the
        // nextBatchSize before limiting the currentBatchSize by currentGroupRowCount - nextRowInGroup.
        currentBatchSize = min(nextBatchSize, maxBatchSize);
        nextBatchSize = min(currentBatchSize * BATCH_SIZE_GROWTH_FACTOR, MAX_BATCH_SIZE);
        currentBatchSize = toIntExact(min(currentBatchSize, currentGroupRowCount - nextRowInGroup));

        for (ColumnReader column : columnReaders) {
            if (column != null) {
                column.prepareNextRead(currentBatchSize);
            }
        }
        nextRowInGroup += currentBatchSize;

        // create a lazy page
        blockFactory.nextPage();
        Arrays.fill(currentBytesPerCell, 0);
        Block[] blocks = new Block[columnReaders.length];
        for (int i = 0; i < columnReaders.length; i++) {
            int columnIndex = i;
            blocks[columnIndex] = blockFactory.createBlock(
                    currentBatchSize,
                    columnReaders[columnIndex]::readBlock,
                    false);
            listenForLoads(blocks[columnIndex], block -> blockLoaded(columnIndex, block));
        }

        Page page = new Page(currentBatchSize, blocks);
        long[] values = new long[page.getPositionCount()];
        Arrays.fill(values, (long) stripeSize << 32 | currentRowGroup);
        LongArrayBlock rowGroupId = new LongArrayBlock(page.getPositionCount(), Optional.empty(), values);
        page = page.appendColumn(rowGroupId);
        validateWritePageChecksum(page);
        return page;
    }

    private void blockLoaded(int columnIndex, Block block)
    {
        if (block.getPositionCount() <= 0) {
            return;
        }

        currentBytesPerCell[columnIndex] += block.getSizeInBytes() / currentBatchSize;
        if (maxBytesPerCell[columnIndex] < currentBytesPerCell[columnIndex]) {
            long delta = currentBytesPerCell[columnIndex] - maxBytesPerCell[columnIndex];
            maxCombinedBytesPerRow += delta;
            maxBytesPerCell[columnIndex] = currentBytesPerCell[columnIndex];
            maxBatchSize = toIntExact(min(maxBatchSize, max(1, maxBlockBytes / maxCombinedBytesPerRow)));
        }
    }

    public Map<String, Slice> getUserMetadata()
    {
        return ImmutableMap.copyOf(Maps.transformValues(userMetadata, Slice::copy));
    }

    private boolean advanceToNextRowGroup()
            throws IOException
    {
        nextRowInGroup = 0;

        if (currentRowGroup >= 0) {
            if (rowGroupStatisticsValidation.isPresent()) {
                StatisticsValidation statisticsValidation = rowGroupStatisticsValidation.get();
                long offset = stripes.get(stripeSize).getOffset();
                writeValidation.get().validateRowGroupStatistics(orcDataSource.getId(), offset, currentRowGroup, statisticsValidation.build().get());
                statisticsValidation.reset();
            }
        }
        while (!backwardRowGroups.hasPrevious() && stripeSize >= 0) {
            advanceToNextStripe();
            currentRowGroup = -1;
        }

        if (!backwardRowGroups.hasPrevious()) {
            currentGroupRowCount = 0;
            return false;
        }

        currentRowGroup++;
        RowGroup currentRowGroup = backwardRowGroups.previous();
        currentGroupRowCount = currentRowGroup.getRowCount();
        if (currentRowGroup.getMinAverageRowBytes() > 0) {
            maxBatchSize = toIntExact(min(maxBatchSize, max(1, maxBlockBytes / currentRowGroup.getMinAverageRowBytes())));
        }

        filePosition = stripeFilePositions.get(stripeSize) + currentRowGroup.getRowOffset();

        // give reader data streams from row group
        InputStreamSources rowGroupStreamSources = currentRowGroup.getStreamSources();
        for (ColumnReader column : columnReaders) {
            if (column != null) {
                column.startRowGroup(rowGroupStreamSources);
            }
        }

        return true;
    }

    private void advanceToNextStripe()
            throws IOException
    {
        currentStripeMemoryContext.close();
        currentStripeMemoryContext = memoryUsage.newAggregatedMemoryContext();
        backwardRowGroups = ImmutableList.<RowGroup>of().listIterator();

        stripeSize--;
        if (stripeSize >= 0) {
            if (stripeStatisticsValidation.isPresent()) {
                StatisticsValidation statisticsValidation = stripeStatisticsValidation.get();
                long offset = stripes.get(stripeSize).getOffset();
                writeValidation.get().validateStripeStatistics(orcDataSource.getId(), offset, statisticsValidation.build().get());
                statisticsValidation.reset();
            }
        }

        if (stripeSize < 0) {
            return;
        }

        StripeInformation stripeInformation = stripes.get(stripeSize);
        validateWriteStripe(stripeInformation.getNumberOfRows());

        Stripe stripe = stripeReader.readStripe(stripeInformation, currentStripeMemoryContext);
        if (stripe != null) {
            // Give readers access to dictionary streams
            InputStreamSources dictionaryStreamSources = stripe.getDictionaryStreamSources();
            ColumnMetadata<ColumnEncoding> columnEncodings = stripe.getColumnEncodings();
            ZoneId fileTimeZone = stripe.getFileTimeZone();
            for (ColumnReader column : columnReaders) {
                if (column != null) {
                    column.startStripe(fileTimeZone, dictionaryStreamSources, columnEncodings);
                }
            }

            backwardRowGroups = stripe.getRowGroups().listIterator(stripe.getRowGroups().size());
        }
        orcDataSourceMemoryUsage.setBytes(orcDataSource.getRetainedSize());
    }

    @SuppressWarnings("FormatStringAnnotation")
    @FormatMethod
    private void validateWrite(Predicate<OrcWriteValidation> test, String messageFormat, Object... args)
            throws OrcCorruptionException
    {
        if (writeValidation.isPresent() && !test.test(writeValidation.get())) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Write validation failed: " + messageFormat, args);
        }
    }

    private void validateWriteStripe(int rowCount)
    {
        writeChecksumBuilder.ifPresent(builder -> builder.addStripe(rowCount));
    }

    private void validateWritePageChecksum(Page page)
    {
        if (writeChecksumBuilder.isPresent()) {
            page = page.getLoadedPage();
            writeChecksumBuilder.get().addPage(page);
            rowGroupStatisticsValidation.get().addPage(page);
            stripeStatisticsValidation.get().addPage(page);
            fileStatisticsValidation.get().addPage(page);
        }
    }
}
