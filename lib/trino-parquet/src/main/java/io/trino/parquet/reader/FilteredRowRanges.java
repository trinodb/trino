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

import static java.util.Objects.requireNonNull;

public class FilteredRowRanges
{
    private final RowRanges parquetRowRanges;
    private final long rowCount;

    public FilteredRowRanges(RowRanges parquetRowRanges)
    {
        this.parquetRowRanges = requireNonNull(parquetRowRanges, "parquetRowRanges is null");
        this.rowCount = parquetRowRanges.rowCount();
    }

    public RowRanges getParquetRowRanges()
    {
        return parquetRowRanges;
    }

    public long getRowCount()
    {
        return rowCount;
    }
}
