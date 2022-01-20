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
package io.trino.operator.hash.fixedwidth;

import io.trino.operator.hash.ColumnValueExtractor;

import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Value object that defines column types and offset for the values stored in the fixed width hash table row.
 */
public class FixedWidthEntryStructure
{
    private final int hashChannelsCount;
    private final ColumnValueExtractor[] columnValueExtractors;
    private final int[] valuesOffsets;
    private final int valuesLength;

    public FixedWidthEntryStructure(ColumnValueExtractor[] columnValueExtractors)
    {
        this.columnValueExtractors = requireNonNull(columnValueExtractors, "columnValueExtractors is null");
        this.hashChannelsCount = columnValueExtractors.length;

        valuesOffsets = new int[hashChannelsCount];
        int mainBufferOffset = 0;
        for (int i = 0; i < columnValueExtractors.length; i++) {
            valuesOffsets[i] = mainBufferOffset;
            checkArgument(columnValueExtractors[i].isFixedSize());
            mainBufferOffset += columnValueExtractors[i].getSize();
        }
        this.valuesLength = mainBufferOffset;
    }

    public int getHashChannelsCount()
    {
        return hashChannelsCount;
    }

    public ColumnValueExtractor[] getColumnValueExtractors()
    {
        return columnValueExtractors;
    }

    public int[] getValuesOffsets()
    {
        return valuesOffsets;
    }

    public int getValuesLength()
    {
        return valuesLength;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FixedWidthEntryStructure that = (FixedWidthEntryStructure) o;
        return hashChannelsCount == that.hashChannelsCount && valuesLength == that.valuesLength && Arrays.equals(columnValueExtractors, that.columnValueExtractors) && Arrays.equals(valuesOffsets, that.valuesOffsets);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(hashChannelsCount, valuesLength);
        result = 31 * result + Arrays.hashCode(columnValueExtractors);
        result = 31 * result + Arrays.hashCode(valuesOffsets);
        return result;
    }
}
