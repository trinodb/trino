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
package io.trino.parquet.reader;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;

class FilteredOffsetIndex
        implements OffsetIndex
{
    /*
     * Returns the filtered offset index containing only the pages which are overlapping with rowRanges.
     */
    public static FilteredOffsetIndex filterOffsetIndex(OffsetIndex offsetIndex, RowRanges rowRanges, long totalRowCount)
    {
        IntList indexMap = new IntArrayList();
        for (int i = 0, n = offsetIndex.getPageCount(); i < n; ++i) {
            long from = offsetIndex.getFirstRowIndex(i);
            if (rowRanges.isOverlapping(from, offsetIndex.getLastRowIndex(i, totalRowCount))) {
                indexMap.add(i);
            }
        }
        return new FilteredOffsetIndex(offsetIndex, indexMap.toIntArray());
    }

    private final OffsetIndex offsetIndex;
    private final int[] indexMap;

    private FilteredOffsetIndex(OffsetIndex offsetIndex, int[] indexMap)
    {
        this.offsetIndex = offsetIndex;
        this.indexMap = indexMap;
    }

    @Override
    public int getPageCount()
    {
        return indexMap.length;
    }

    @Override
    public long getOffset(int pageIndex)
    {
        return offsetIndex.getOffset(indexMap[pageIndex]);
    }

    @Override
    public int getCompressedPageSize(int pageIndex)
    {
        return offsetIndex.getCompressedPageSize(indexMap[pageIndex]);
    }

    @Override
    public long getFirstRowIndex(int pageIndex)
    {
        return offsetIndex.getFirstRowIndex(indexMap[pageIndex]);
    }

    @Override
    public long getLastRowIndex(int pageIndex, long totalRowCount)
    {
        int nextIndex = indexMap[pageIndex] + 1;
        return (nextIndex >= offsetIndex.getPageCount() ? totalRowCount : offsetIndex.getFirstRowIndex(nextIndex)) - 1;
    }

    @Override
    public String toString()
    {
        try (Formatter formatter = new Formatter()) {
            formatter.format("%-12s  %20s  %16s  %20s\n", "", "offset", "compressed size", "first row index");
            for (int i = 0, n = offsetIndex.getPageCount(); i < n; ++i) {
                int index = Arrays.binarySearch(indexMap, i);
                boolean isHidden = index < 0;
                formatter.format("%spage-%-5d  %20d  %16d  %20d\n",
                        isHidden ? "- " : "  ",
                        isHidden ? i : index,
                        offsetIndex.getOffset(i),
                        offsetIndex.getCompressedPageSize(i),
                        offsetIndex.getFirstRowIndex(i));
            }
            return formatter.toString();
        }
    }

    public List<OffsetRange> calculateOffsetRanges(long rowGroupOffset)
    {
        List<OffsetRange> ranges = new ArrayList<>();
        int pageCount = getPageCount();
        if (pageCount > 0) {
            long firstPageOffset = offsetIndex.getOffset(0);
            // Add a range for the dictionary page if required
            if (rowGroupOffset < firstPageOffset) {
                // We need to adjust the offset by startingPosition for Trino because dataSource.readFully() started at startingPosition
                ranges.add(new OffsetRange(rowGroupOffset, firstPageOffset - rowGroupOffset));
            }

            long currentOffset = getOffset(0);
            long currentLength = getCompressedPageSize(0);
            for (int i = 1; i < pageCount; ++i) {
                long offset = getOffset(i);
                int length = getCompressedPageSize(i);

                if (currentOffset + currentLength == offset) {
                    currentLength += length;
                }
                else {
                    ranges.add(new OffsetRange(currentOffset, currentLength));
                    currentOffset = offset;
                    currentLength = length;
                }
            }
            ranges.add(new OffsetRange(currentOffset, currentLength));
        }
        return ranges;
    }

    public static class OffsetRange
    {
        private final long offset;
        private final long length;

        public OffsetRange(long offset, long length)
        {
            this.offset = offset;
            this.length = length;
        }

        long getOffset()
        {
            return offset;
        }

        long getLength()
        {
            return length;
        }
    }
}
