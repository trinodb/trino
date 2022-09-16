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
package io.trino.parquet.batchreader;

import org.apache.parquet.internal.filter2.columnindex.RowRanges;

import java.util.Iterator;
import java.util.List;

public class RowRangeIterator
{
    private List<RowRanges.Range> rangeList;
    RowRange currentRange;
    Iterator<RowRanges.Range> iterator;

    public static class RowRange
    {
        private long from;
        private long toExclusive;

        public RowRange(long from, long toExclusive)
        {
            this.from = from;
            this.toExclusive = toExclusive;
        }

        public long start()
        {
            return from;
        }

        public long count()
        {
            return toExclusive - from;
        }

        public void advance(long n)
        {
            if (terminal()) {
                throw new RuntimeException("Can't consume from invalid range");
            }

            from += n;
            if (from > toExclusive) {
                throw new RuntimeException("Can't consume beyond end of range");
            }
        }

        public boolean isEmpty()
        {
            return from >= toExclusive;
        }

        public boolean terminal()
        {
            return from == Long.MAX_VALUE;
        }
    }

    private static final RowRange TERMINAL = new RowRange(Long.MAX_VALUE, Long.MAX_VALUE);

    private void getNextRange()
    {
        if (iterator.hasNext()) {
            RowRanges.Range range = iterator.next();
            currentRange = new RowRange(range.from, range.to + 1);
        }
        else {
            currentRange = TERMINAL;
        }
    }

    public RowRangeIterator(RowRanges ranges)
    {
        rangeList = ranges.getRanges();
        iterator = rangeList.iterator();
        getNextRange();
    }

    public RowRange getCurrentRange()
    {
        if (currentRange.isEmpty()) {
            getNextRange();
        }
        return currentRange;
    }

    public void advance(int nrows)
    {
        currentRange.advance(nrows);
    }

    public boolean terminated()
    {
        return currentRange.terminal();
    }
}
