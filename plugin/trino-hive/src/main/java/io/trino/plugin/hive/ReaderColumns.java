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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Stores a mapping between
 * - the projected columns required by a connector level pagesource and
 * -  the columns supplied by format-specific page source
 * <p>
 * Currently used in {@link HivePageSource}, {@code io.trino.plugin.iceberg.IcebergPageSource},
 * and {@code io.trino.plugin.deltalake.DeltaLakePageSource}.
 */
public class ReaderColumns
{
    // columns to be read by the reader (ordered)
    private final List<ColumnHandle> readerColumns;
    // indices for mapping expected column handles to the reader's column handles
    private final List<Integer> readerBlockIndices;

    public ReaderColumns(List<? extends ColumnHandle> readerColumns, List<Integer> readerBlockIndices)
    {
        this.readerColumns = ImmutableList.copyOf(requireNonNull(readerColumns, "readerColumns is null"));

        readerBlockIndices.forEach(value -> checkArgument(value >= 0 && value < readerColumns.size(), "block index out of bounds"));
        this.readerBlockIndices = ImmutableList.copyOf(requireNonNull(readerBlockIndices, "readerBlockIndices is null"));
    }

    /**
     * For a column required by the wrapper page source, returns the column read by the delegate page source or record cursor.
     */
    public ColumnHandle getForColumnAt(int index)
    {
        checkArgument(index >= 0 && index < readerBlockIndices.size(), "index is not valid");
        int readerIndex = readerBlockIndices.get(index);
        return readerColumns.get(readerIndex);
    }

    /**
     * For a channel expected by wrapper page source, returns the channel index in the underlying page source or record cursor.
     */
    public int getPositionForColumnAt(int index)
    {
        checkArgument(index >= 0 && index < readerBlockIndices.size(), "index is invalid");
        return readerBlockIndices.get(index);
    }

    /**
     * returns the actual list of columns being read by underlying page source or record cursor in order.
     */
    public List<ColumnHandle> get()
    {
        return readerColumns;
    }
}
