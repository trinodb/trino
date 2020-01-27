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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Stores a mapping of the projected columns required by {@link HivePageSource} to the columns supplied by format-specific
 * page sources or record cursors.
 */
public class ReaderProjections
{
    // columns to be read by the reader (ordered)
    private final List<HiveColumnHandle> readerColumns;
    // indices for mapping expected hive column handles to the reader's column handles
    private final List<Integer> readerBlockIndices;

    private ReaderProjections(List<HiveColumnHandle> readerColumns, List<Integer> readerBlockIndices)
    {
        this.readerColumns = ImmutableList.copyOf(requireNonNull(readerColumns, "readerColumns is null"));

        readerBlockIndices.forEach(value -> checkArgument(value >= 0 && value < readerColumns.size(), "block index out of bounds"));
        this.readerBlockIndices = ImmutableList.copyOf(requireNonNull(readerBlockIndices, "readerBlockIndices is null"));
    }

    /**
     * For a column required by the {@link HivePageSource}, returns the column read by the delegate page source or record cursor.
     */
    public HiveColumnHandle readerColumnForHiveColumnAt(int index)
    {
        checkArgument(index >= 0 && index < readerBlockIndices.size(), "index is not valid");
        int readerIndex = readerBlockIndices.get(index);
        return readerColumns.get(readerIndex);
    }

    /**
     * For a channel expected by {@link HivePageSource}, returns the channel index in the underlying page source or record cursor.
     */
    public int readerColumnPositionForHiveColumnAt(int index)
    {
        checkArgument(index >= 0 && index < readerBlockIndices.size(), "index is invalid");
        return readerBlockIndices.get(index);
    }

    /**
     * returns the actual list of columns being read by underlying page source or record cursor in order.
     */
    public List<HiveColumnHandle> getReaderColumns()
    {
        return readerColumns;
    }

    /**
     * Creates a mapping between the input {@param columns} and base columns if required.
     */
    public static Optional<ReaderProjections> projectBaseColumns(List<HiveColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");

        // No projection is required if all columns are base columns
        if (columns.stream().allMatch(HiveColumnHandle::isBaseColumn)) {
            return Optional.empty();
        }

        ImmutableList.Builder<HiveColumnHandle> projectedColumns = ImmutableList.builder();
        ImmutableList.Builder<Integer> outputColumnMapping = ImmutableList.builder();
        Map<Integer, Integer> mappedHiveColumnIndices = new HashMap<>();
        int projectedColumnCount = 0;

        for (HiveColumnHandle column : columns) {
            int hiveColumnIndex = column.getBaseHiveColumnIndex();
            Integer mapped = mappedHiveColumnIndices.get(hiveColumnIndex);

            if (mapped == null) {
                projectedColumns.add(column.getBaseColumn());
                mappedHiveColumnIndices.put(hiveColumnIndex, projectedColumnCount);
                outputColumnMapping.add(projectedColumnCount);
                projectedColumnCount++;
            }
            else {
                outputColumnMapping.add(mapped);
            }
        }

        return Optional.of(new ReaderProjections(projectedColumns.build(), outputColumnMapping.build()));
    }
}
