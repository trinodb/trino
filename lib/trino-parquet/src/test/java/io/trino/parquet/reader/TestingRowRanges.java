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

import io.trino.parquet.reader.FilteredRowRanges.RowRange;
import org.apache.parquet.filter2.columnindex.RowRanges;

public class TestingRowRanges
{
    private TestingRowRanges() {}

    public static RowRanges toRowRange(long rowCount)
    {
        return RowRanges.createSingle(rowCount);
    }

    public static RowRanges toRowRanges(RowRange... ranges)
    {
        RowRanges.Builder builder = RowRanges.builder();
        for (RowRange range : ranges) {
            builder.addSelectedRange(range.start(), range.end());
        }
        return builder.build();
    }
}
