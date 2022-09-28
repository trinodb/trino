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
package io.trino.parquet;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.ColumnarMap.toColumnarMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class ColumnStatisticsValidation
{
    private final Type type;
    private final List<ColumnStatisticsValidation> fieldBuilders;

    private long valuesCount;
    private long nonLeafValuesCount;

    public ColumnStatisticsValidation(Type type)
    {
        this.type = requireNonNull(type, "type is null");
        this.fieldBuilders = type.getTypeParameters().stream()
                .map(ColumnStatisticsValidation::new)
                .collect(toImmutableList());
    }

    public void addBlock(Block block)
    {
        addBlock(block, new ColumnStatistics(0, 0));
    }

    public List<ColumnStatistics> build()
    {
        if (fieldBuilders.isEmpty()) {
            return ImmutableList.of(new ColumnStatistics(valuesCount, nonLeafValuesCount));
        }
        return fieldBuilders.stream()
                .flatMap(builder -> builder.build().stream())
                .collect(toImmutableList());
    }

    private void addBlock(Block block, ColumnStatistics columnStatistics)
    {
        if (fieldBuilders.isEmpty()) {
            addPrimitiveBlock(block);
            valuesCount += columnStatistics.valuesCount();
            nonLeafValuesCount += columnStatistics.nonLeafValuesCount();
            return;
        }

        List<Block> fields;
        ColumnStatistics mergedColumnStatistics;
        if (type instanceof ArrayType) {
            ColumnarArray columnarArray = toColumnarArray(block);
            fields = ImmutableList.of(columnarArray.getElementsBlock());
            mergedColumnStatistics = columnStatistics.merge(addArrayBlock(columnarArray));
        }
        else if (type instanceof MapType) {
            ColumnarMap columnarMap = toColumnarMap(block);
            fields = ImmutableList.of(columnarMap.getKeysBlock(), columnarMap.getValuesBlock());
            mergedColumnStatistics = columnStatistics.merge(addMapBlock(columnarMap));
        }
        else if (type instanceof RowType) {
            ColumnarRow columnarRow = ColumnarRow.toColumnarRow(block);
            ImmutableList.Builder<Block> fieldsBuilder = ImmutableList.builder();
            for (int index = 0; index < columnarRow.getFieldCount(); index++) {
                fieldsBuilder.add(columnarRow.getField(index));
            }
            fields = fieldsBuilder.build();
            mergedColumnStatistics = columnStatistics.merge(addRowBlock(columnarRow));
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported type: %s", type));
        }

        for (int i = 0; i < fieldBuilders.size(); i++) {
            fieldBuilders.get(i).addBlock(fields.get(i), mergedColumnStatistics);
        }
    }

    private void addPrimitiveBlock(Block block)
    {
        valuesCount += block.getPositionCount();
        if (!block.mayHaveNull()) {
            return;
        }
        int nullsCount = 0;
        for (int position = 0; position < block.getPositionCount(); position++) {
            nullsCount += block.isNull(position) ? 1 : 0;
        }
        nonLeafValuesCount += nullsCount;
    }

    private static ColumnStatistics addMapBlock(ColumnarMap block)
    {
        if (!block.mayHaveNull()) {
            int emptyEntriesCount = 0;
            for (int position = 0; position < block.getPositionCount(); position++) {
                emptyEntriesCount += block.getEntryCount(position) == 0 ? 1 : 0;
            }
            return new ColumnStatistics(emptyEntriesCount, emptyEntriesCount);
        }
        int nonLeafValuesCount = 0;
        for (int position = 0; position < block.getPositionCount(); position++) {
            nonLeafValuesCount += block.isNull(position) || block.getEntryCount(position) == 0 ? 1 : 0;
        }
        return new ColumnStatistics(nonLeafValuesCount, nonLeafValuesCount);
    }

    private static ColumnStatistics addArrayBlock(ColumnarArray block)
    {
        if (!block.mayHaveNull()) {
            int emptyEntriesCount = 0;
            for (int position = 0; position < block.getPositionCount(); position++) {
                emptyEntriesCount += block.getLength(position) == 0 ? 1 : 0;
            }
            return new ColumnStatistics(emptyEntriesCount, emptyEntriesCount);
        }
        int nonLeafValuesCount = 0;
        for (int position = 0; position < block.getPositionCount(); position++) {
            nonLeafValuesCount += block.isNull(position) || block.getLength(position) == 0 ? 1 : 0;
        }
        return new ColumnStatistics(nonLeafValuesCount, nonLeafValuesCount);
    }

    private static ColumnStatistics addRowBlock(ColumnarRow block)
    {
        if (!block.mayHaveNull()) {
            return new ColumnStatistics(0, 0);
        }
        int nullsCount = 0;
        for (int position = 0; position < block.getPositionCount(); position++) {
            nullsCount += block.isNull(position) ? 1 : 0;
        }
        return new ColumnStatistics(nullsCount, nullsCount);
    }

    /**
     * @param valuesCount Count of values for a column field, including nulls, empty and defined values.
     * @param nonLeafValuesCount Count of non-leaf values for a column field, this is nulls count for primitives
     * and count of values below the max definition level for nested types
     */
    record ColumnStatistics(long valuesCount, long nonLeafValuesCount)
    {
        ColumnStatistics merge(ColumnStatistics other)
        {
            return new ColumnStatistics(
                    valuesCount + other.valuesCount(),
                    nonLeafValuesCount + other.nonLeafValuesCount());
        }
    }
}
