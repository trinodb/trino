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
package org.apache.parquet.internal.filter2.columnindex;

import org.apache.parquet.internal.column.columnindex.OffsetIndex;

import java.util.stream.IntStream;

import static io.trino.parquet.reader.FilteredRowRanges.RowRange;
import static java.util.Objects.requireNonNull;

public class TestingRowRanges
{
    private TestingRowRanges() {}

    public static RowRanges toRowRange(long rowCount)
    {
        return RowRanges.createSingle(rowCount);
    }

    public static RowRanges toRowRanges(RowRange... ranges)
    {
        return RowRanges.create(-1, IntStream.range(0, ranges.length).iterator(), new MockOffsetIndex(ranges));
    }

    private static class MockOffsetIndex
            implements OffsetIndex
    {
        private final RowRange[] rowRanges;

        private MockOffsetIndex(RowRange[] rowRanges)
        {
            this.rowRanges = requireNonNull(rowRanges, "rowRanges is null");
        }

        @Override
        public int getPageCount()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getOffset(int pageIndex)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getCompressedPageSize(int pageIndex)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getFirstRowIndex(int pageIndex)
        {
            return rowRanges[pageIndex].start();
        }

        @Override
        public long getLastRowIndex(int pageIndex, long rowGroupRowCount)
        {
            return rowRanges[pageIndex].end();
        }
    }
}
