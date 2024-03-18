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
package io.trino.plugin.varada.dispatcher;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Provider;
import io.airlift.log.Logger;
import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.varada.dispatcher.query.data.QueryColumn;
import io.trino.plugin.varada.storage.read.PrefilledPageSource;
import io.trino.plugin.varada.storage.read.VaradaStoragePageSource;
import io.trino.plugin.warp.gen.stats.VaradaStatsDispatcherPageSource;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;
import io.varada.log.ShapingLogger;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.StringJoiner;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DispatcherPageSource
        implements ConnectorPageSource
{
    private static final int MINIMUM_OUTPUT_ROW_COUNT = 256;
    private static final int START_INDEX_OF_PROXIED_CONNECTOR_COLUMNS = 0;
    private static final Logger logger = Logger.get(DispatcherPageSource.class);
    private static final int pageSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;

    private final ShapingLogger shapingLogger;
    protected final VaradaStoragePageSource varadaPageSource;
    private final PrefilledPageSource prefilledPageSource;
    private final QueryContext queryContext;
    private final RowGroupData rowGroupData;
    private final PageSourceDecision pageSourceDecision;
    private final VaradaStatsDispatcherPageSource stats;
    private final QueryClassifier queryClassifier;
    private final Provider<ConnectorPageSource> proxiedConnectorPageSourceProvider;
    private final ReadErrorHandler readErrorHandler;
    private final RowGroupCloseHandler closeHandler;
    private final long startTime;
    private final List<Type> varadaWithoutPrefilledAndProxiedCollectTypes;
    private ConnectorPageSource proxiedConnectorPageSource;
    private Page currentProxiedPage;
    private int currentProxiedPagePosition; // Position upto which currentProxiedPage has been consumed
    private final Deque<RowRange> proxiedPageRanges;
    private long proxiedPagePositionsRead;
    private Optional<Boolean> proxiedConnectorProvidesRowRanges;
    private boolean wasProxiedPagedLoaded;
    private Page currentVaradaPage;
    private int currentVaradaPagePosition; // Position upto which currentVaradaPage has been consumed
    private Deque<RowRange> varadaPageRanges;

    public DispatcherPageSource(Provider<ConnectorPageSource> proxiedConnectorPageSourceProvider,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            QueryClassifier queryClassifier,
            VaradaStoragePageSource varadaPageSource,
            QueryContext queryContext,
            RowGroupData rowGroupData,
            PageSourceDecision pageSourceDecision,
            VaradaStatsDispatcherPageSource stats,
            RowGroupCloseHandler closeHandler,
            ReadErrorHandler readErrorHandler,
            GlobalConfiguration globalConfiguration)
    {
        this.proxiedConnectorPageSourceProvider = proxiedConnectorPageSourceProvider;
        this.queryClassifier = queryClassifier;
        this.varadaPageSource = varadaPageSource;
        this.prefilledPageSource = new PrefilledPageSource(
                queryContext.getPrefilledQueryCollectDataByBlockIndex(),
                stats,
                rowGroupData,
                queryContext.getTotalRecords(),
                Optional.of(closeHandler));
        this.queryContext = queryContext;
        this.rowGroupData = rowGroupData;
        this.pageSourceDecision = pageSourceDecision;
        this.stats = stats;
        this.closeHandler = requireNonNull(closeHandler);
        this.readErrorHandler = requireNonNull(readErrorHandler);
        this.proxiedPageRanges = new ArrayDeque<>();
        this.varadaPageRanges = new ArrayDeque<>();
        this.proxiedConnectorProvidesRowRanges = Optional.empty();
        this.startTime = System.currentTimeMillis();
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfiguration.getShapingLoggerThreshold(),
                globalConfiguration.getShapingLoggerDuration(),
                globalConfiguration.getShapingLoggerNumberOfSamples());
        this.varadaWithoutPrefilledAndProxiedCollectTypes = Stream.concat(
                        queryContext.getRemainingCollectColumns().stream().map(dispatcherProxiedConnectorTransformer::getColumnType),
                        queryContext.getNativeQueryCollectDataList().stream().map(QueryColumn::getType))
                .collect(toImmutableList());
    }

    @Override
    public Page getNextPage()
    {
        try {
            if (PageSourceDecision.VARADA.equals(pageSourceDecision)) {
                Page result = varadaPageSource.getNextPage();
                return mergeVaradaPrefilled(result, prefilledPageSource);
            }

            PageBuilder resultPageBuilder = PageBuilder.withMaxPageSize(pageSizeInBytes, varadaWithoutPrefilledAndProxiedCollectTypes);
            if (varadaPageRanges.isEmpty()) {
                // Calling getNextPage at least once is necessary to make varada page source populate row ranges
                getNextVaradaPage();
                if (varadaPageRanges.isEmpty()) {
                    return buildResultPage(resultPageBuilder);
                }
            }

            // Calling getNextPage at least once is necessary to make proxied page source populate row ranges
            if (currentProxiedPage == null) {
                getNextProxiedPage();
            }

            while (!resultPageBuilder.isFull() && !varadaPageRanges.isEmpty() && !proxiedPageRanges.isEmpty()) {
                RowRange varadaCurrentRange = varadaPageRanges.peek();
                RowRange proxiedCurrentRange = proxiedPageRanges.peek();
                logger.debug(
                        "Going to merge results for a mixed query with predicate. varadaCurrentRange=%s, proxiedCurrentRange=%s, currentVaradaPagePosition=%d, currentProxiedPagePositions=%d, currentVaradaPagePositions=%d, currentProxiedPagePositions=%d",
                        varadaCurrentRange, proxiedCurrentRange, currentVaradaPagePosition, currentProxiedPagePosition, currentVaradaPage.getPositionCount(), currentProxiedPage.getPositionCount());

                if (varadaCurrentRange.isFullyBefore(proxiedCurrentRange)) {
                    varadaPageRanges.removeFirst();
                    seekVaradaPageSource(toIntExact(varadaCurrentRange.getRowCount()));
                }
                else if (proxiedCurrentRange.isFullyBefore(varadaCurrentRange)) {
                    proxiedPageRanges.removeFirst();
                    seekProxiedPageSource(proxiedCurrentRange.getRowCount());
                }
                else { // There is some overlap in varadaCurrentRange and proxiedCurrentRange
                    varadaPageRanges.removeFirst();
                    proxiedPageRanges.removeFirst();
                    long overlapStartInclusive = Math.max(varadaCurrentRange.minInclusive(), proxiedCurrentRange.minInclusive());
                    long overlapEndExclusive = Math.min(varadaCurrentRange.maxExclusive(), proxiedCurrentRange.maxExclusive());
                    // Seek to overlap start in both varada and proxied page
                    currentVaradaPagePosition += toIntExact(Math.max(overlapStartInclusive - varadaCurrentRange.minInclusive(), 0));
                    seekProxiedPageSource(Math.max(overlapStartInclusive - proxiedCurrentRange.minInclusive(), 0));
                    int overlapRowCount = toIntExact(overlapEndExclusive - overlapStartInclusive);
                    // Collect overlappingRows from varada and proxied pages
                    if (canMergeFull(resultPageBuilder, overlapRowCount)) {
                        Page resultPage = buildFullResultPage(overlapRowCount);
                        long resultEndExclusive = overlapStartInclusive + resultPage.getPositionCount();
                        // Add back any remaining part of row range onto the deque for the next iteration
                        if (varadaCurrentRange.maxExclusive() > resultEndExclusive) {
                            varadaPageRanges.addFirst(new RowRange(resultEndExclusive, varadaCurrentRange.maxExclusive()));
                        }
                        if (proxiedCurrentRange.maxExclusive() > resultEndExclusive) {
                            proxiedPageRanges.addFirst(new RowRange(resultEndExclusive, proxiedCurrentRange.maxExclusive()));
                        }
                        return resultPage;
                    }
                    fillOverlappingPage(resultPageBuilder, overlapRowCount);
                    // Add back any remaining part of row range onto the deque for the next iteration
                    if (varadaCurrentRange.maxExclusive() > overlapEndExclusive) {
                        varadaPageRanges.addFirst(new RowRange(overlapEndExclusive, varadaCurrentRange.maxExclusive()));
                    }
                    else if (proxiedCurrentRange.maxExclusive() > overlapEndExclusive) {
                        proxiedPageRanges.addFirst(new RowRange(overlapEndExclusive, proxiedCurrentRange.maxExclusive()));
                    }
                }
            }
            return buildResultPage(resultPageBuilder);
        }
        catch (Throwable e) {
            stats.inccached_varada_failed_pages();
            shapingLogger.error(e, "failed to read cache file %s from varada.", rowGroupData.getRowGroupKey());
            readErrorHandler.handle(e, rowGroupData, queryContext);
            throw e;
        }
    }

    private Page mergeVaradaPrefilled(Page currentVaradaPage,
            PrefilledPageSource prefilledPageSource)
    {
        Block[] orderedBlocks = new Block[queryContext.getTotalCollectCount()];
        int startPointVarada = 0;

        if (currentVaradaPage.getPositionCount() == 0 || queryContext.getTotalCollectCount() == 0) {
            return new Page(currentVaradaPage.getPositionCount());
        }

        for (int i = 0; i < queryContext.getTotalCollectCount(); i++) {
            if (prefilledPageSource.hasBlock(i)) {
                orderedBlocks[i] = prefilledPageSource.createBlock(i, currentVaradaPage.getPositionCount());
            }
            else {
                final int varadaBlockIndex = queryContext.getNativeQueryCollectDataList()
                        .get(startPointVarada)
                        .getBlockIndex();
                orderedBlocks[varadaBlockIndex] = currentVaradaPage.getBlock(startPointVarada);
                startPointVarada++;
            }
        }
        return new Page(currentVaradaPage.getPositionCount(), orderedBlocks);
    }

    private void seekVaradaPageSource(int rowsToSkip)
    {
        currentVaradaPagePosition += rowsToSkip;
        checkState(
                currentVaradaPagePosition <= currentVaradaPage.getPositionCount(),
                "currentVaradaPagePosition %s, currentVaradaPage positions %s, varadaPageRanges %s",
                currentVaradaPagePosition, currentVaradaPage.getPositionCount(), varadaPageRanges);
        if (currentVaradaPagePosition == currentVaradaPage.getPositionCount()) {
            checkState(
                    varadaPageRanges.isEmpty(),
                    "currentVaradaPagePosition %s, currentVaradaPage positions %s, varadaPageRanges %s",
                    currentVaradaPagePosition, currentVaradaPage.getPositionCount(), varadaPageRanges);
            getNextVaradaPage();
        }
    }

    private void seekProxiedPageSource(long rowsToSkip)
    {
        int pagePositionsLeft = currentProxiedPage.getPositionCount() - currentProxiedPagePosition;
        while (rowsToSkip >= pagePositionsLeft && rowsToSkip > 0) {
            rowsToSkip -= pagePositionsLeft;
            getNextProxiedPage();
            pagePositionsLeft = currentProxiedPage.getPositionCount();
        }
        currentProxiedPagePosition += toIntExact(rowsToSkip);
    }

    /**
     * Proxied page source produces LazyBlocks, but using PageBuilder forces loading of lazy blocks.
     * Therefore, we attempt to use currentProxiedPage directly when it does not lead to too small output page
     */
    private boolean canMergeFull(PageBuilder resultPageBuilder, int overlapRowCount)
    {
        if (!resultPageBuilder.isEmpty()) {
            return false; // We're already building result using PageBuilder
        }
        int pagePositionsLeft = currentProxiedPage.getPositionCount() - currentProxiedPagePosition;
        if (currentProxiedPagePosition == 0 && overlapRowCount >= pagePositionsLeft) {
            return true; // Full proxied page is selected
        }
        return Math.min(pagePositionsLeft, overlapRowCount) >= MINIMUM_OUTPUT_ROW_COUNT;
    }

    private Page buildFullResultPage(int overlapRowCount)
    {
        if (queryContext.getTotalCollectCount() != (currentProxiedPage.getChannelCount() + currentVaradaPage.getChannelCount() + prefilledPageSource.getChannelCount())) {
            throw new TrinoException(VaradaErrorCode.VARADA_FAILED_TO_BUILD_MIXED_PAGE, "wrong number of columns");
        }
        Block[] orderedBlocks = new Block[queryContext.getTotalCollectCount()];
        int startPointVarada = 0;
        int startPointProxied = START_INDEX_OF_PROXIED_CONNECTOR_COLUMNS;
        int positionCount = Math.min(currentProxiedPage.getPositionCount() - currentProxiedPagePosition, overlapRowCount);
        Page overlapPoxiedPage = currentProxiedPage.getRegion(currentProxiedPagePosition, positionCount);
        recordProxiedPageLoad(); // Technically the proxied page is not "loaded" here, but we track it for metrics anyway
        Page overlapVaradaPage = currentVaradaPage.getRegion(currentVaradaPagePosition, positionCount);
        for (int i = 0; i < queryContext.getTotalCollectCount(); i++) {
            if (queryContext.getRemainingCollectColumnByBlockIndex().containsKey(i)) {
                orderedBlocks[i] = overlapPoxiedPage.getBlock(startPointProxied);
                startPointProxied++;
            }
            else {
                if (prefilledPageSource.hasBlock(i)) {
                    orderedBlocks[i] = prefilledPageSource.createBlock(i, positionCount);
                }
                else {
                    final int varadaBlockIndex = queryContext.getNativeQueryCollectDataList().get(startPointVarada).getBlockIndex();
                    orderedBlocks[varadaBlockIndex] = overlapVaradaPage.getBlock(startPointVarada);
                    startPointVarada++;
                }
            }
        }

        seekVaradaPageSource(positionCount);
        seekProxiedPageSource(positionCount);
        return new Page(positionCount, orderedBlocks);
    }

    private void fillOverlappingPage(PageBuilder resultPageBuilder, int numberOfRowsToAdd)
    {
        resultPageBuilder.declarePositions(numberOfRowsToAdd);
        long start = System.nanoTime();
        // Add overlapping rows from proxied page source
        int pagePositionsLeft = currentProxiedPage.getPositionCount() - currentProxiedPagePosition;
        int overlapRowsRemaining = numberOfRowsToAdd;
        while (overlapRowsRemaining >= pagePositionsLeft && overlapRowsRemaining > 0) {
            addColumnsToBuilder(resultPageBuilder, pagePositionsLeft, currentProxiedPage, currentProxiedPagePosition, 0);
            recordProxiedPageLoad();
            overlapRowsRemaining -= pagePositionsLeft;
            getNextProxiedPage();
            pagePositionsLeft = currentProxiedPage.getPositionCount();
            if (proxiedConnectorPageSource.isFinished()) {
                // Cannot reach end of proxied page source before overlap rows are produced
                checkState(
                        overlapRowsRemaining <= pagePositionsLeft,
                        "Reached end of proxied page source with overlapRowsRemaining %s, pagePositionsLeft %s",
                        overlapRowsRemaining,
                        pagePositionsLeft);
            }
        }
        if (overlapRowsRemaining > 0) {
            addColumnsToBuilder(resultPageBuilder, overlapRowsRemaining, currentProxiedPage, currentProxiedPagePosition, 0);
            recordProxiedPageLoad();
            currentProxiedPagePosition += overlapRowsRemaining;
        }
        stats.addproxied_loaded_pages_time(System.nanoTime() - start);

        // Add overlapping rows from currentVaradaPage
        addColumnsToBuilder(resultPageBuilder, numberOfRowsToAdd, currentVaradaPage, currentVaradaPagePosition, queryContext.getRemainingCollectColumnByBlockIndex().size());
        seekVaradaPageSource(numberOfRowsToAdd);
    }

    private void recordProxiedPageLoad()
    {
        if (!wasProxiedPagedLoaded) {
            wasProxiedPagedLoaded = true;
            stats.incproxied_loaded_pages();
            stats.addproxied_loaded_pages_bytes(currentProxiedPage.getSizeInBytes());
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return varadaPageSource.getCompletedBytes() + (proxiedConnectorPageSource == null ? 0 : proxiedConnectorPageSource.getCompletedBytes());
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        if (proxiedConnectorPageSource != null) {
            return proxiedConnectorPageSource.getCompletedPositions();
        }
        return OptionalLong.of(varadaPageSource.getCompletedPositions());
    }

    @Override
    public long getReadTimeNanos()
    {
        return proxiedConnectorPageSource == null ? 0 : proxiedConnectorPageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return (proxiedPageRanges.isEmpty() || varadaPageRanges.isEmpty())
                && (varadaPageSource.isFinished() || (proxiedConnectorPageSource != null && proxiedConnectorPageSource.isFinished()));
    }

    @Override
    public long getMemoryUsage()
    {
        return varadaPageSource.getMemoryUsage() + (proxiedConnectorPageSource == null ? 0 : proxiedConnectorPageSource.getMemoryUsage());
    }

    @Override
    public void close()
            throws IOException
    {
        //don't call prefilledPageSource.close since it is not used as a ConnectorPageSource here
        try {
            boolean success = true;
            StringJoiner errorMsg = new StringJoiner(",");
            try {
                varadaPageRanges.clear();
                currentVaradaPage = null;
                varadaPageSource.close();
            }
            catch (Exception e) {
                errorMsg.add(e.getMessage());
                success = false;
            }
            try {
                if (proxiedConnectorPageSource != null) {
                    proxiedPageRanges.clear();
                    currentProxiedPage = null;
                    proxiedConnectorPageSource.close();
                }
            }
            catch (Exception e) {
                errorMsg.add(e.getMessage());
                success = false;
            }
            if (success) {
                stats.inccached_varada_success_files();
            }
            else {
                stats.inccached_varada_failed_files();
                errorMsg.add(format("failed to close file %s", rowGroupData.getRowGroupKey()));
                throw new IOException(errorMsg.toString());
            }
        }
        finally {
            queryClassifier.close(queryContext);
            this.stats.addexecution_time(System.currentTimeMillis() - this.startTime);
            closeHandler.accept(rowGroupData);
        }
    }

    private void getNextVaradaPage()
    {
        currentVaradaPage = requireNonNull(varadaPageSource.getNextPage(), "varadaPageSource returned a null Page");
        currentVaradaPagePosition = 0;
        // VaradaPageSource#getSortedRowRanges always returns row ranges for the Page returned from previous call of VaradaPageSource#getNextPage
        // It may return a smaller Page than the row ranges when LIMIT is reached
        ConnectorPageSource.RowRanges varadaRowRanges = varadaPageSource.getSortedRowRanges();
        if (varadaPageSource.isRowsLimitReached()) {
            validateRanges(
                    varadaRowRanges.getRowCount() >= currentVaradaPage.getPositionCount(),
                    "Row ranges %s are smaller than page positions count %s",
                    varadaRowRanges,
                    currentVaradaPage.getPositionCount());
        }
        else {
            validateRanges(
                    varadaRowRanges.getRowCount() == currentVaradaPage.getPositionCount(),
                    "Mismatch in row ranges %s and page positions count %s",
                    varadaRowRanges,
                    currentVaradaPage.getPositionCount());
        }
        Deque<RowRange> sortedRowRangesWithLimit = new ArrayDeque<>();
        int rowRangeToCollect = currentVaradaPage.getPositionCount();
        for (int rangeIndex = 0; rangeIndex < varadaRowRanges.getRangesCount() && rowRangeToCollect > 0; rangeIndex++) {
            long lowerInclusive = varadaRowRanges.getLowerInclusive(rangeIndex);
            long upperExclusive = varadaRowRanges.getUpperExclusive(rangeIndex);
            int rangeRowCount = toIntExact(upperExclusive - lowerInclusive);
            if (rangeRowCount <= rowRangeToCollect) {
                sortedRowRangesWithLimit.addLast(new RowRange(lowerInclusive, upperExclusive));
                rowRangeToCollect -= rangeRowCount;
            }
            else {
                sortedRowRangesWithLimit.addLast(new RowRange(lowerInclusive, lowerInclusive + rowRangeToCollect));
                rowRangeToCollect = 0;
            }
        }
        varadaPageRanges = sortedRowRangesWithLimit;
    }

    private void getNextProxiedPage()
    {
        long start = System.nanoTime();
        if (proxiedConnectorPageSource == null) {
            proxiedConnectorPageSource = proxiedConnectorPageSourceProvider.get();
        }
        currentProxiedPage = null;
        wasProxiedPagedLoaded = false;
        while (currentProxiedPage == null && !proxiedConnectorPageSource.isFinished()) {
            currentProxiedPage = proxiedConnectorPageSource.getNextPage();
            // ConnectorPage#getNextFilteredRowRanges always returns row ranges for at least the Page returned from previous call of ConnectorPage#getNextPage
            // and additionally for any number of Pages to come after that
            Optional<ConnectorPageSource.RowRanges> rowRanges = proxiedConnectorPageSource.getNextFilteredRowRanges();
            if (rowRanges.isEmpty()) {
                // Filtering is not supported by this connector page source (e.g. TEXT, CSV, AVRO etc.)
                checkState(proxiedConnectorProvidesRowRanges.isEmpty() || !proxiedConnectorProvidesRowRanges.get(), "proxied connector was expected to provide row ranges");
                proxiedConnectorProvidesRowRanges = Optional.of(false);
                if (currentProxiedPage != null && currentProxiedPage.getPositionCount() > 0) {
                    proxiedPageRanges.add(new RowRange(proxiedPagePositionsRead, proxiedPagePositionsRead + currentProxiedPage.getPositionCount()));
                }
            }
            else {
                checkState(proxiedConnectorProvidesRowRanges.isEmpty() || proxiedConnectorProvidesRowRanges.get(), "proxied connector was not expected to provide row ranges");
                proxiedConnectorProvidesRowRanges = Optional.of(true);
                int positionsCount = currentProxiedPage == null ? 0 : currentProxiedPage.getPositionCount();
                validateRanges(
                        rowRanges.get().getRangesCount() == 0 || rowRanges.get().getRowCount() >= positionsCount,
                        "proxied connector returned fewer row ranges %s than page positions %s",
                        rowRanges,
                        positionsCount);
                for (int index = 0; index < rowRanges.get().getRangesCount(); index++) {
                    proxiedPageRanges.add(new RowRange(rowRanges.get().getLowerInclusive(index), rowRanges.get().getUpperExclusive(index)));
                }
            }
        }
        currentProxiedPagePosition = 0;
        if (currentProxiedPage == null) {
            // proxiedConnectorPageSource is finished, return an empty page
            currentProxiedPage = new Page(0);
        }
        else {
            proxiedPagePositionsRead += currentProxiedPage.getPositionCount();
            stats.incproxied_pages();
        }
        stats.addproxied_time(System.nanoTime() - start);
    }

    private Page buildResultPage(PageBuilder resultPageBuilder)
    {
        Page resultPage = resultPageBuilder.build();
        int blocksCount = queryContext.getTotalCollectCount();

        if (blocksCount != resultPage.getChannelCount() + prefilledPageSource.getChannelCount()) {
            throw new TrinoException(VaradaErrorCode.VARADA_FAILED_TO_BUILD_MIXED_PAGE, "wrong number of columns");
        }
        Block[] orderedBlocks = new Block[blocksCount];
        final int varadaStartIndex = queryContext.getRemainingCollectColumnByBlockIndex().size();
        int varadaColumnIndex = varadaStartIndex;
        int proxiedConnectorColumnIndex = START_INDEX_OF_PROXIED_CONNECTOR_COLUMNS;
        for (int i = 0; i < queryContext.getTotalCollectCount(); i++) {
            if (queryContext.getRemainingCollectColumnByBlockIndex().containsKey(i)) {
                orderedBlocks[i] = resultPage.getBlock(proxiedConnectorColumnIndex);
                proxiedConnectorColumnIndex++;
            }
            else {
                if (prefilledPageSource.hasBlock(i)) {
                    orderedBlocks[i] = prefilledPageSource.createBlock(i, resultPage.getPositionCount());
                }
                else {
                    final int varadaBlockIndex = queryContext.getNativeQueryCollectDataList().get(varadaColumnIndex - varadaStartIndex).getBlockIndex();
                    orderedBlocks[varadaBlockIndex] = resultPage.getBlock(varadaColumnIndex);
                    varadaColumnIndex++;
                }
            }
        }
        resultPageBuilder.reset();
        return new Page(orderedBlocks);
    }

    private void fillBlock(Type type, Block block, BlockBuilder blockBuilder, int start, int end)
    {
        for (int position = start; position < end; position++) {
            type.appendTo(block, position, blockBuilder);
        }
    }

    private void addColumnsToBuilder(PageBuilder resultPageBuilder,
            int numberOfRowsToAdd,
            Page page,
            int currentRowInPage,
            int columnInBuilder)
    {
        for (int column = 0; column < page.getChannelCount(); column++) {
            Block block = page.getBlock(column).getLoadedBlock();
            BlockBuilder blockBuilder = resultPageBuilder.getBlockBuilder(columnInBuilder);
            fillBlock(resultPageBuilder.getType(columnInBuilder), block, blockBuilder, currentRowInPage, currentRowInPage + numberOfRowsToAdd);
            columnInBuilder++;
        }
    }

    @VisibleForTesting
    PageSourceDecision getPageSourceDecision()
    {
        return pageSourceDecision;
    }

    private record RowRange(long minInclusive, long maxExclusive)
    {
        public RowRange
        {
            checkArgument(
                    minInclusive < maxExclusive && minInclusive >= 0,
                    "minInclusive %s must be smaller than maxExclusive %s",
                    minInclusive,
                    maxExclusive);
        }

        public boolean isFullyBefore(RowRange other)
        {
            return maxExclusive <= other.minInclusive();
        }

        public long getRowCount()
        {
            return maxExclusive - minInclusive;
        }
    }

    // TODO add @FormatMethod after implementing ConnectorPageSource.RowRanges#toString
    @SuppressWarnings("AnnotateFormatMethod")
    //@FormatMethod
    private static void validateRanges(boolean condition, String formatString, Object... args)
    {
        if (!condition) {
            throw new TrinoException(VaradaErrorCode.VARADA_MATCH_RANGES_ERROR, format(formatString, args));
        }
    }
}
