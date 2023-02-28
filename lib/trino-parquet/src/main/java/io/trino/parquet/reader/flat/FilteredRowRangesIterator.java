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
package io.trino.parquet.reader.flat;

import io.trino.parquet.reader.FilteredRowRanges;

import java.util.Iterator;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.parquet.reader.FilteredRowRanges.RowRange;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * When filtering using column indexes we might skip reading some pages for different columns. Because the rows are
 * not aligned between the pages of the different columns it might be required to skip some values. The values (and the
 * related rl and dl) are skipped based on the iterator of the required row indexes and the first row index of each
 * page.
 * For example:
 *
 * <pre>
 * rows   col1   col2   col3
 *      ┌──────┬──────┬──────┐
 *   0  │  p0  │      │      │
 *      ╞══════╡  p0  │  p0  │
 *  20  │ p1(X)│------│------│
 *      ╞══════╪══════╡      │
 *  40  │ p2(X)│      │------│
 *      ╞══════╡ p1(X)╞══════╡
 *  60  │ p3(X)│      │------│
 *      ╞══════╪══════╡      │
 *  80  │  p4  │      │  p1  │
 *      ╞══════╡  p2  │      │
 * 100  │  p5  │      │      │
 *      └──────┴──────┴──────┘
 * </pre>
 *
 * The pages 1, 2, 3 in col1 are skipped, so we have to skip the rows [20, 79]. Because page 1 in col2 contains values
 * only for the rows [40, 79] we skip this entire page as well. To synchronize the row reading we have to skip the
 * values (and the related rl and dl) for the rows [20, 39] in the end of the page 0 for col2. Similarly, we have to
 * skip values while reading page0 and page1 for col3.
 */
public class FilteredRowRangesIterator
        implements RowRangesIterator
{
    private final Iterator<RowRange> rowRangeIterator;

    // The current row range
    private RowRange currentRange;

    private long pageFirstRowIndex = -1;
    private int pageValuesConsumed;

    public FilteredRowRangesIterator(FilteredRowRanges rowRanges)
    {
        requireNonNull(rowRanges, "rowRanges is null");
        this.rowRangeIterator = rowRanges.getRowRanges().iterator();
        // We don't handle the empty rowRanges case because that should result in all pages getting eliminated
        // and nothing should be read from file for that particular row group
        checkArgument(this.rowRangeIterator.hasNext(), "rowRanges is empty");
        nextRange();
    }

    /**
     * @return Size of the next read within current range, bounded by chunkSize.
     */
    @Override
    public int getRowsLeftInCurrentRange()
    {
        return toIntExact(currentRange.end() - pageFirstRowIndex) - pageValuesConsumed + 1;
    }

    /**
     * @return Size of the next read within current range, bounded by chunkSize.
     * When all the rows of the current range have been read, advance to the next range.
     */
    @Override
    public int advanceRange(int chunkSize)
    {
        checkState(pageFirstRowIndex >= 0, "pageFirstRowIndex %s cannot be negative", pageFirstRowIndex);
        long rangeEnd = currentRange.end();
        int rowsLeftInRange = toIntExact(rangeEnd - pageFirstRowIndex) - pageValuesConsumed + 1;
        if (rowsLeftInRange > chunkSize) {
            pageValuesConsumed += chunkSize;
            return chunkSize;
        }
        pageValuesConsumed += rowsLeftInRange;
        if (rowRangeIterator.hasNext()) {
            nextRange();
        }
        else {
            checkState(
                    rowsLeftInRange == chunkSize,
                    "Reached end of filtered rowRanges with chunkSize %s, rowsLeftInRange %s, pageFirstRowIndex %s, pageValuesConsumed %s",
                    chunkSize,
                    rowsLeftInRange,
                    pageFirstRowIndex,
                    pageValuesConsumed);
        }
        return rowsLeftInRange;
    }

    /**
     * Seek forward in the page by chunkSize.
     * Advance rowRanges if we seek beyond currentRange.
     *
     * @return number of values skipped within rowRanges
     */
    @Override
    public int seekForward(int chunkSize)
    {
        checkState(pageFirstRowIndex >= 0, "pageFirstRowIndex %s cannot be negative", pageFirstRowIndex);
        long currentIndex = pageFirstRowIndex + pageValuesConsumed;
        int skippedInRange = 0;
        while (chunkSize > 0) {
            // Before currentRange
            if (currentIndex < currentRange.start()) {
                int stepSize = min(chunkSize, toIntExact(currentRange.start() - currentIndex));
                currentIndex += stepSize;
                pageValuesConsumed += stepSize;
                chunkSize -= stepSize;
            }
            // Within currentRange
            else if (currentIndex <= currentRange.end()) {
                int stepSize = min(chunkSize, toIntExact(currentRange.end() - currentIndex) + 1);
                currentIndex += stepSize;
                skippedInRange += stepSize;
                pageValuesConsumed += stepSize;
                chunkSize -= stepSize;
            }
            // After currentRange
            else {
                // chunkSize can never go beyond rowRanges end
                checkState(
                        rowRangeIterator.hasNext(),
                        "Reached end of filtered rowRanges with chunkSize %s, currentIndex %s, pageFirstRowIndex %s, pageValuesConsumed %s",
                        chunkSize,
                        currentIndex,
                        pageFirstRowIndex,
                        pageValuesConsumed);
                nextRange();
            }
        }
        return skippedInRange;
    }

    /**
     * @return Count of values to be skipped when current range start
     * is after current position in the page
     */
    @Override
    public long skipToRangeStart()
    {
        checkState(pageFirstRowIndex >= 0, "pageFirstRowIndex %s cannot be negative", pageFirstRowIndex);
        long rangeStart = currentRange.start();
        long currentIndex = pageFirstRowIndex + pageValuesConsumed;
        if (rangeStart <= currentIndex) {
            return 0;
        }
        long skipCount = rangeStart - currentIndex;
        pageValuesConsumed += skipCount;
        return skipCount;
    }

    /**
     * Must be called at the beginning of reading a new page.
     * Advances rowRanges if current range has no overlap with the new page.
     */
    @Override
    public void resetForNewPage(OptionalLong firstRowIndex)
    {
        checkArgument(firstRowIndex.isPresent(), "Missing firstRowIndex for selecting rowRanges");
        checkArgument(firstRowIndex.getAsLong() >= 0, "firstRowIndex %s cannot be negative", firstRowIndex.getAsLong());
        checkArgument(
                firstRowIndex.getAsLong() >= pageFirstRowIndex,
                "firstRowIndex %s cannot be less than current pageFirstRowIndex %s",
                firstRowIndex.getAsLong(),
                pageFirstRowIndex);

        pageFirstRowIndex = firstRowIndex.getAsLong();
        pageValuesConsumed = 0;
        long rangeEnd = currentRange.end();
        if (pageFirstRowIndex > rangeEnd) {
            nextRange();
            rangeEnd = currentRange.end();
        }
        // We only read pages which contain some rows matched by rowRanges.
        // At the end of reading previous page, one of 2 cases can happen:
        // 1. Current range was not fully read, so firstRowIndex must be <= rangeEnd.
        // 2. Current range was fully read, the next range must have some overlap with new page.
        verify(pageFirstRowIndex <= rangeEnd);
    }

    /**
     * Returns whether the current page with the provided value count
     * is fully contained within the current row range.
     */
    @Override
    public boolean isPageFullyConsumed(int pageValueCount)
    {
        return pageFirstRowIndex >= currentRange.start()
                && (pageFirstRowIndex + pageValueCount) <= currentRange.end() + 1;
    }

    private void nextRange()
    {
        currentRange = rowRangeIterator.next();
    }
}
