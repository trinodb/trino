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
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HivePageSourceProvider.projectColumnDereferences;
import static io.trino.plugin.hive.TestHivePageSourceProvider.RowData.rowData;
import static io.trino.plugin.hive.TestHiveReaderProjectionsUtil.ROWTYPE_OF_ROW_AND_PRIMITIVES;
import static io.trino.plugin.hive.TestHiveReaderProjectionsUtil.createProjectedColumnHandle;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

class TestHivePageSourceProvider
{
    private static final HiveColumnHandle BASE_COLUMN = createBaseColumn("col", 0, toHiveType(ROWTYPE_OF_ROW_AND_PRIMITIVES), ROWTYPE_OF_ROW_AND_PRIMITIVES, REGULAR, Optional.empty());

    @Test
    void testProjectColumnDereferences()
            throws Exception
    {
        List<HiveColumnHandle> columns = ImmutableList.of(
                createProjectedColumnHandle(BASE_COLUMN, ImmutableList.of(0, 0)),
                createProjectedColumnHandle(BASE_COLUMN, ImmutableList.of(0)));

        Page outputPage;
        try (ConnectorPageSource connectorPageSource = projectColumnDereferences(columns, TestHivePageSourceProvider::createPageSource)) {
            outputPage = connectorPageSource
                    .getNextSourcePage()
                    .getPage();
        }
        // Verify output block values
        Block baseInputBlock = createInputPage().getBlock(0);
        for (int i = 0, columnsSize = columns.size(); i < columnsSize; i++) {
            HiveColumnHandle column = columns.get(i);
            verifyBlock(
                    outputPage.getBlock(i),
                    column.getType(),
                    baseInputBlock,
                    BASE_COLUMN.getType(),
                    HivePageSourceProvider.getProjection(column, BASE_COLUMN));
        }
    }

    private static FixedPageSource createPageSource(List<HiveColumnHandle> columns)
    {
        assertThat(columns).containsOnly(BASE_COLUMN);
        return new FixedPageSource(ImmutableList.of(createInputPage()));
    }

    private static Page createInputPage()
    {
        List<Object> inputBlockData = new ArrayList<>();
        inputBlockData.add(rowData(rowData(11L, 12L, 13L), 1L));
        inputBlockData.add(rowData(null, 2L));
        inputBlockData.add(null);
        inputBlockData.add(rowData(rowData(31L, 32L, 33L), 3L));

        return new Page(createInputBlock(inputBlockData, BASE_COLUMN.getType()));
    }

    private static Block createInputBlock(List<Object> data, Type type)
    {
        if (type instanceof RowType) {
            return createRowBlock(data, (RowType) type);
        }
        if (BIGINT.equals(type)) {
            return createLongArrayBlock(data);
        }
        throw new UnsupportedOperationException();
    }

    private static Block createRowBlock(List<Object> data, RowType rowType)
    {
        int positionCount = data.size();

        boolean[] isNull = new boolean[positionCount];
        int fieldCount = rowType.getFields().size();

        List<List<Object>> fieldsData = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            fieldsData.add(new ArrayList<>());
        }

        // Extract data to generate fieldBlocks
        for (int position = 0; position < data.size(); position++) {
            RowData row = (RowData) data.get(position);
            if (row == null) {
                isNull[position] = true;
                for (int field = 0; field < fieldCount; field++) {
                    fieldsData.get(field).add(null);
                }
            }
            else {
                for (int field = 0; field < fieldCount; field++) {
                    fieldsData.get(field).add(row.getField(field));
                }
            }
        }

        Block[] fieldBlocks = new Block[fieldCount];
        for (int field = 0; field < fieldCount; field++) {
            fieldBlocks[field] = createInputBlock(fieldsData.get(field), rowType.getFields().get(field).getType());
        }

        return RowBlock.fromNotNullSuppressedFieldBlocks(positionCount, Optional.of(isNull), fieldBlocks);
    }

    private static Block createLongArrayBlock(List<Object> data)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(data.size());
        for (Object datum : data) {
            if (datum == null) {
                builder.appendNull();
            }
            else {
                BIGINT.writeLong(builder, (Long) datum);
            }
        }
        return builder.build();
    }

    private static void verifyBlock(Block actualBlock, Type outputType, Block input, Type inputType, List<Integer> dereferences)
    {
        assertThat(inputType).isInstanceOf(RowType.class);
        Block expectedOutputBlock = createProjectedColumnBlock(input, outputType, (RowType) inputType, dereferences);
        assertBlockEquals(outputType, actualBlock, expectedOutputBlock);
    }

    private static Block createProjectedColumnBlock(Block data, Type finalType, RowType blockType, List<Integer> dereferences)
    {
        if (dereferences.isEmpty()) {
            return data;
        }

        BlockBuilder builder = finalType.createBlockBuilder(null, data.getPositionCount());

        for (int i = 0; i < data.getPositionCount(); i++) {
            RowType sourceType = blockType;

            SqlRow currentData = null;
            boolean isNull = data.isNull(i);

            if (!isNull) {
                // Get SqlRow corresponding to element at position i
                currentData = sourceType.getObject(data, i);
            }

            // Apply all dereferences except for the last one, because the type can be different
            for (int j = 0; j < dereferences.size() - 1; j++) {
                if (isNull) {
                    // If a null element is discovered at any dereferencing step, break
                    break;
                }

                int fieldIndex = dereferences.get(j);
                Block fieldBlock = currentData.getRawFieldBlock(fieldIndex);

                RowType rowType = sourceType;
                int rawIndex = currentData.getRawIndex();
                if (fieldBlock.isNull(rawIndex)) {
                    currentData = null;
                }
                else {
                    sourceType = (RowType) rowType.getFields().get(fieldIndex).getType();
                    currentData = sourceType.getObject(fieldBlock, rawIndex);
                }

                isNull = currentData == null;
            }

            if (isNull) {
                // Append null if any of the elements in the dereference chain were null
                builder.appendNull();
            }
            else {
                int lastDereference = dereferences.getLast();

                finalType.appendTo(currentData.getRawFieldBlock(lastDereference), currentData.getRawIndex(), builder);
            }
        }

        return builder.build();
    }

    static class RowData
    {
        private final List<?> data;

        private RowData(Object... data)
        {
            this.data = Arrays.asList(requireNonNull(data, "data is null"));
        }

        static RowData rowData(Object... data)
        {
            return new RowData(data);
        }

        Object getField(int field)
        {
            checkArgument(field >= 0 && field < data.size());
            return data.get(field);
        }
    }
}
