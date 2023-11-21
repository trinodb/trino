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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static io.trino.plugin.hive.TestHiveReaderProjectionsUtil.ROWTYPE_OF_ROW_AND_PRIMITIVES;
import static io.trino.plugin.hive.TestHiveReaderProjectionsUtil.createProjectedColumnHandle;
import static io.trino.plugin.hive.TestHiveReaderProjectionsUtil.createTestFullColumns;
import static io.trino.plugin.hive.TestReaderProjectionsAdapter.RowData.rowData;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestReaderProjectionsAdapter
{
    private static final String TEST_COLUMN_NAME = "col";
    private static final Type TEST_COLUMN_TYPE = ROWTYPE_OF_ROW_AND_PRIMITIVES;

    private static final Map<String, HiveColumnHandle> TEST_FULL_COLUMNS = createTestFullColumns(
            ImmutableList.of(TEST_COLUMN_NAME),
            ImmutableMap.of(TEST_COLUMN_NAME, TEST_COLUMN_TYPE));

    @Test
    public void testAdaptPage()
    {
        List<HiveColumnHandle> columns = ImmutableList.of(
                createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col"), ImmutableList.of(0, 0)),
                createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col"), ImmutableList.of(0)));

        Optional<ReaderColumns> readerProjections = projectBaseColumns(columns);

        List<Object> inputBlockData = new ArrayList<>();
        inputBlockData.add(rowData(rowData(11L, 12L, 13L), 1L));
        inputBlockData.add(rowData(null, 2L));
        inputBlockData.add(null);
        inputBlockData.add(rowData(rowData(31L, 32L, 33L), 3L));

        ReaderProjectionsAdapter adapter = new ReaderProjectionsAdapter(
                columns.stream().map(ColumnHandle.class::cast).collect(toImmutableList()),
                readerProjections.get(),
                column -> ((HiveColumnHandle) column).getType(),
                HivePageSourceProvider::getProjection);
        verifyPageAdaptation(adapter, ImmutableList.of(inputBlockData));
    }

    @Test
    public void testLazyDereferenceProjectionLoading()
    {
        List<HiveColumnHandle> columns = ImmutableList.of(createProjectedColumnHandle(TEST_FULL_COLUMNS.get("col"), ImmutableList.of(0, 0)));

        List<Object> inputBlockData = new ArrayList<>();
        inputBlockData.add(rowData(rowData(11L, 12L, 13L), 1L));
        inputBlockData.add(rowData(null, 2L));
        inputBlockData.add(null);
        inputBlockData.add(rowData(rowData(31L, 32L, 33L), 3L));

        // Produce an output page by applying adaptation
        Optional<ReaderColumns> readerProjections = projectBaseColumns(columns);
        ReaderProjectionsAdapter adapter = new ReaderProjectionsAdapter(
                columns.stream().map(ColumnHandle.class::cast).collect(toImmutableList()),
                readerProjections.get(),
                column -> ((HiveColumnHandle) column).getType(),
                HivePageSourceProvider::getProjection);
        Page inputPage = createPage(ImmutableList.of(inputBlockData), adapter.getInputTypes());
        adapter.adaptPage(inputPage).getLoadedPage();

        // Verify that only the block corresponding to subfield "col.f_row_0.f_bigint_0" should be completely loaded, others are not.

        // Assertion for "col"
        Block lazyBlockLevel1 = inputPage.getBlock(0);
        assertThat(lazyBlockLevel1 instanceof LazyBlock).isTrue();
        assertThat(lazyBlockLevel1.isLoaded()).isFalse();
        RowBlock rowBlockLevel1 = (RowBlock) ((LazyBlock) lazyBlockLevel1).getBlock();
        assertThat(rowBlockLevel1.isLoaded()).isFalse();

        // Assertion for "col.f_row_0" and col.f_bigint_0"
        assertThat(rowBlockLevel1.getFieldBlock(0).isLoaded()).isFalse();
        assertThat(rowBlockLevel1.getFieldBlock(1).isLoaded()).isFalse();

        Block lazyBlockLevel2 = rowBlockLevel1.getFieldBlock(0);
        assertThat(lazyBlockLevel2 instanceof LazyBlock).isTrue();
        RowBlock rowBlockLevel2 = ((RowBlock) ((LazyBlock) lazyBlockLevel2).getBlock());
        assertThat(rowBlockLevel2.isLoaded()).isFalse();
        // Assertion for "col.f_row_0.f_bigint_0" and "col.f_row_0.f_bigint_1"
        assertThat(rowBlockLevel2.getFieldBlock(0).isLoaded()).isTrue();
        assertThat(rowBlockLevel2.getFieldBlock(1).isLoaded()).isFalse();
    }

    private void verifyPageAdaptation(ReaderProjectionsAdapter adapter, List<List<Object>> inputPageData)
    {
        List<ReaderProjectionsAdapter.ChannelMapping> columnMapping = adapter.getOutputToInputMapping();
        List<Type> outputTypes = adapter.getOutputTypes();
        List<Type> inputTypes = adapter.getInputTypes();

        Page inputPage = createPage(inputPageData, inputTypes);
        Page outputPage = adapter.adaptPage(inputPage).getLoadedPage();

        // Verify output block values
        for (int i = 0; i < columnMapping.size(); i++) {
            ReaderProjectionsAdapter.ChannelMapping mapping = columnMapping.get(i);
            int inputBlockIndex = mapping.getInputChannelIndex();
            verifyBlock(
                    outputPage.getBlock(i),
                    outputTypes.get(i),
                    inputPage.getBlock(inputBlockIndex),
                    inputTypes.get(inputBlockIndex),
                    mapping.getDereferenceSequence());
        }
    }

    private static Page createPage(List<List<Object>> pageData, List<Type> types)
    {
        Block[] inputPageBlocks = new Block[pageData.size()];
        for (int i = 0; i < inputPageBlocks.length; i++) {
            inputPageBlocks[i] = createInputBlock(pageData.get(i), types.get(i));
        }

        return new Page(inputPageBlocks);
    }

    private static Block createInputBlock(List<Object> data, Type type)
    {
        int positionCount = data.size();

        if (type instanceof RowType) {
            return new LazyBlock(data.size(), () -> createRowBlockWithLazyNestedBlocks(data, (RowType) type));
        }
        if (BIGINT.equals(type)) {
            return new LazyBlock(positionCount, () -> createLongArrayBlock(data));
        }
        throw new UnsupportedOperationException();
    }

    private static Block createRowBlockWithLazyNestedBlocks(List<Object> data, RowType rowType)
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
        BlockBuilder builder = BIGINT.createBlockBuilder(null, data.size());
        for (int i = 0; i < data.size(); i++) {
            Long value = (Long) data.get(i);
            if (value == null) {
                builder.appendNull();
            }
            else {
                BIGINT.writeLong(builder, value);
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
        if (dereferences.size() == 0) {
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
                    // If null element is discovered at any dereferencing step, break
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

                isNull = isNull || (currentData == null);
            }

            if (isNull) {
                // Append null if any of the elements in the dereference chain were null
                builder.appendNull();
            }
            else {
                int lastDereference = dereferences.get(dereferences.size() - 1);

                finalType.appendTo(currentData.getRawFieldBlock(lastDereference), currentData.getRawIndex(), builder);
            }
        }

        return builder.build();
    }

    static class RowData
    {
        private final List<? extends Object> data;

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
