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

import org.apache.parquet.internal.filter2.columnindex.RowRanges;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

// PrimitiveColumnReader iterates over RowRanges value-by-value
// FlatColumnReader iterates over range of values using FilteredRowRangesIterator
public class FilteredRowRanges
{
    private final RowRanges parquetRowRanges;
    private final List<RowRange> rowRanges;
    private final long rowCount;

    public FilteredRowRanges(RowRanges parquetRowRanges)
    {
        this.parquetRowRanges = requireNonNull(parquetRowRanges, "parquetRowRanges is null");
        this.rowRanges = constructRanges(parquetRowRanges);
        this.rowCount = parquetRowRanges.rowCount();
    }

    public RowRanges getParquetRowRanges()
    {
        return parquetRowRanges;
    }

    public List<RowRange> getRowRanges()
    {
        return rowRanges;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("parquetRowRanges", parquetRowRanges)
                .add("rowRanges", rowRanges)
                .add("rowCount", rowCount)
                .toString();
    }

    /**
     * Construct a list of row ranges from the given `rowRanges`. For example, suppose the
     * `rowRanges` are `[0, 1, 2, 4, 5, 7, 8, 9]`, it will be converted into 3 row ranges:
     * `[0-2], [4-5], [7-9]`.
     */
    private static List<RowRange> constructRanges(RowRanges rowRanges)
    {
        return rowRanges.getRanges().stream()
                .map(range -> new RowRange(range.from, range.to))
                .collect(toImmutableList());
    }

    /**
     * Helper struct to represent a range of row indexes `[start, end]`.
     */
    public record RowRange(long start, long end) {}
}
