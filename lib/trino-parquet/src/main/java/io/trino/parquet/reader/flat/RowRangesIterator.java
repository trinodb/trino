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

import java.util.Optional;
import java.util.OptionalLong;

public interface RowRangesIterator
{
    RowRangesIterator ALL_ROW_RANGES_ITERATOR = new AllRowRangesIterator();

    int getRowsLeftInCurrentRange();

    int advanceRange(int chunkSize);

    int seekForward(int chunkSize);

    long skipToRangeStart();

    void resetForNewPage(OptionalLong firstRowIndex);

    boolean isPageFullyConsumed(int pageValueCount);

    class AllRowRangesIterator
            implements RowRangesIterator
    {
        @Override
        public int getRowsLeftInCurrentRange()
        {
            return Integer.MAX_VALUE;
        }

        @Override
        public int advanceRange(int chunkSize)
        {
            return chunkSize;
        }

        @Override
        public int seekForward(int chunkSize)
        {
            return chunkSize;
        }

        @Override
        public long skipToRangeStart()
        {
            return 0;
        }

        @Override
        public void resetForNewPage(OptionalLong firstRowIndex) {}

        @Override
        public boolean isPageFullyConsumed(int pageValueCount)
        {
            return true;
        }
    }

    static RowRangesIterator createRowRangesIterator(Optional<FilteredRowRanges> filteredRowRanges)
    {
        if (filteredRowRanges.isEmpty()) {
            return ALL_ROW_RANGES_ITERATOR;
        }
        return new FilteredRowRangesIterator(filteredRowRanges.get());
    }
}
