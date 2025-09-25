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

package io.trino.block;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.block.RowBlock.fromFieldBlocks;
import static io.trino.spi.block.RowBlock.fromNotNullSuppressedFieldBlocks;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRowBlock
        extends AbstractTestBlock
{
    @Test
    public void testWithVarcharBigint()
    {
        List<Type> fieldTypes = ImmutableList.of(VARCHAR, BIGINT);
        List<Object>[] testRows = generateTestRows(fieldTypes, 100);

        testWith(fieldTypes, testRows);
        testWith(fieldTypes, alternatingNullValues(testRows));
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        List<Type> fieldTypes = ImmutableList.of(VARCHAR, BIGINT);
        List<Object>[] expectedValues = alternatingNullValues(generateTestRows(fieldTypes, 100));
        Block block = createBlockBuilderWithValues(fieldTypes, expectedValues).build();
        assertThat(block.getPositionCount()).isEqualTo(expectedValues.length);
        for (int i = 0; i < block.getPositionCount(); i++) {
            int expectedSize = getExpectedEstimatedDataSize(expectedValues[i]);
            assertThat(block.getEstimatedDataSizeForStats(i)).isEqualTo(expectedSize);
        }
    }

    @Test
    public void testFromFieldBlocksNoNullsDetection()
    {
        // Blocks does not discard the null mask during creation if no values are null
        boolean[] rowIsNull = new boolean[5];
        assertThat(fromNotNullSuppressedFieldBlocks(5, Optional.of(rowIsNull), new Block[] {
                new ByteArrayBlock(5, Optional.empty(), createExpectedValue(5).getBytes())}).mayHaveNull()).isTrue();
        rowIsNull[rowIsNull.length - 1] = true;
        assertThat(fromNotNullSuppressedFieldBlocks(5, Optional.of(rowIsNull), new Block[] {
                new ByteArrayBlock(5, Optional.of(rowIsNull), createExpectedValue(5).getBytes())}).mayHaveNull()).isTrue();

        // Empty blocks have no nulls and can also discard their null mask
        assertThat(fromNotNullSuppressedFieldBlocks(0, Optional.of(new boolean[0]), new Block[] {new ByteArrayBlock(0, Optional.empty(), new byte[0])}).mayHaveNull()).isFalse();

        // Normal blocks should have null masks preserved
        List<Type> fieldTypes = ImmutableList.of(VARCHAR, BIGINT);
        Block hasNullsBlock = createBlockBuilderWithValues(fieldTypes, alternatingNullValues(generateTestRows(fieldTypes, 100))).build();
        assertThat(hasNullsBlock.mayHaveNull()).isTrue();
    }

    private int getExpectedEstimatedDataSize(List<Object> row)
    {
        if (row == null) {
            return 0;
        }
        int size = 0;
        size += row.get(0) == null ? 0 : ((String) row.get(0)).length();
        size += row.get(1) == null ? 0 : Long.BYTES;
        return size;
    }

    @Test
    public void testCompactBlock()
    {
        Block emptyBlock = new ByteArrayBlock(0, Optional.empty(), new byte[0]);
        boolean[] rowIsNull = {false, true, false, false, false, false};

        // NOTE: nested row blocks are required to have the exact same size so they are always compact
        assertCompact(fromFieldBlocks(0, new Block[] {emptyBlock, emptyBlock}));
        assertCompact(fromNotNullSuppressedFieldBlocks(rowIsNull.length, Optional.of(rowIsNull), new Block[] {
                new ByteArrayBlock(6, Optional.of(rowIsNull), createExpectedValue(6).getBytes()),
                new ByteArrayBlock(6, Optional.of(rowIsNull), createExpectedValue(6).getBytes())}));
    }

    @Test
    public void testCopyWithAppendedNull()
    {
        List<Type> fieldTypes = ImmutableList.of(VARCHAR, BIGINT);

        // Test without startOffset
        List<Object>[] testRows = generateTestRows(fieldTypes, 3);
        RowBlock rowBlock = (RowBlock) createBlockBuilderWithValues(fieldTypes, testRows).build();
        RowBlock appendedBlock = rowBlock.copyWithAppendedNull();
        assertThat(appendedBlock.getPositionCount()).isEqualTo(testRows.length + 1);
        for (int i = 0; i < testRows.length; i++) {
            assertPositionValue(appendedBlock, i, testRows[i]);
        }
        assertThat(appendedBlock.isNull(appendedBlock.getPositionCount() - 1)).isTrue();

        // Test with existing nulls - create block with nulls manually
        RowBlockBuilder builderWithNulls = new RowBlockBuilder(fieldTypes, null, 3);
        builderWithNulls.buildEntry(fieldBuilders -> {
            VARCHAR.writeString(fieldBuilders.get(0), "test1");
            BIGINT.writeLong(fieldBuilders.get(1), 42);
        });
        builderWithNulls.appendNull();
        builderWithNulls.buildEntry(fieldBuilders -> {
            VARCHAR.writeString(fieldBuilders.get(0), "test3");
            BIGINT.writeLong(fieldBuilders.get(1), 44);
        });
        RowBlock rowBlockWithNulls = builderWithNulls.buildValueBlock();
        RowBlock appendedBlockWithNulls = rowBlockWithNulls.copyWithAppendedNull();
        assertThat(appendedBlockWithNulls.getPositionCount()).isEqualTo(4);
        assertThat(appendedBlockWithNulls.isNull(3)).isTrue();

        // Test without existing nulls
        List<Object>[] testRowsNoNulls = generateTestRows(fieldTypes, 2);
        RowBlock rowBlockNoNulls = (RowBlock) createBlockBuilderWithValues(fieldTypes, testRowsNoNulls).build();
        RowBlock appendedBlockNoNulls = rowBlockNoNulls.copyWithAppendedNull();
        assertThat(appendedBlockNoNulls.getPositionCount()).isEqualTo(3);
        assertThat(appendedBlockNoNulls.isNull(2)).isTrue();
        assertThat(appendedBlockNoNulls.isNull(0)).isFalse();
        assertThat(appendedBlockNoNulls.isNull(1)).isFalse();

        List<Object>[] largerTestRows = generateTestRows(fieldTypes, 5);
        RowBlock largerBlock = (RowBlock) createBlockBuilderWithValues(fieldTypes, largerTestRows).build();
        RowBlock regionBlock = largerBlock.getRegion(1, 2);
        RowBlock offsetAppendedBlock = regionBlock.copyWithAppendedNull();
        assertThat(offsetAppendedBlock.getPositionCount()).isEqualTo(3);

        assertPositionValue(offsetAppendedBlock, 0, largerTestRows[1]);
        assertPositionValue(offsetAppendedBlock, 1, largerTestRows[2]);
        assertThat(offsetAppendedBlock.isNull(2)).isTrue();
    }

    @Test
    public void testGetFieldBlocks()
    {
        List<Type> fieldTypes = ImmutableList.of(VARCHAR, BIGINT);
        List<Object>[] testRows = generateTestRows(fieldTypes, 3);
        RowBlock rowBlock = (RowBlock) createBlockBuilderWithValues(fieldTypes, testRows).build();

        List<Block> fieldBlocks = rowBlock.getFieldBlocks();
        assertThat(fieldBlocks.size()).isEqualTo(2);

        // Verify field values match original data
        for (int pos = 0; pos < 3; pos++) {
            List<Object> expectedRow = testRows[pos];
            if (expectedRow.get(0) != null) {
                assertThat(VARCHAR.getSlice(fieldBlocks.get(0), pos)).isEqualTo(utf8Slice((String) expectedRow.get(0)));
            }
            if (expectedRow.get(1) != null) {
                assertThat(BIGINT.getLong(fieldBlocks.get(1), pos)).isEqualTo((Long) expectedRow.get(1));
            }
        }

        // Test with offset
        List<Object>[] largerTestRows = generateTestRows(fieldTypes, 5);
        RowBlock originalBlock = (RowBlock) createBlockBuilderWithValues(fieldTypes, largerTestRows).build();
        RowBlock regionBlock = originalBlock.getRegion(1, 3);
        List<Block> fieldBlocksWithOffset = regionBlock.getFieldBlocks();
        assertThat(fieldBlocksWithOffset.size()).isEqualTo(2);
        assertThat(fieldBlocksWithOffset.get(0).getPositionCount()).isEqualTo(3);

        // Verify values are from the correct region
        for (int pos = 0; pos < 3; pos++) {
            List<Object> expectedRow = largerTestRows[pos + 1];
            if (expectedRow.get(0) != null) {
                assertThat(VARCHAR.getSlice(fieldBlocksWithOffset.get(0), pos)).isEqualTo(utf8Slice((String) expectedRow.get(0)));
            }
        }
    }

    @Test
    public void testCopyPositions()
    {
        List<Type> fieldTypes = ImmutableList.of(VARCHAR, BIGINT);
        List<Object>[] testRows = generateTestRows(fieldTypes, 5);
        RowBlock rowBlock = (RowBlock) createBlockBuilderWithValues(fieldTypes, testRows).build();

        RowBlock copiedBlock = rowBlock.copyPositions(new int[] {0, 2, 4}, 0, 3);
        assertThat(copiedBlock.getPositionCount()).isEqualTo(3);
        assertPositionValue(copiedBlock, 0, testRows[0]);
        assertPositionValue(copiedBlock, 1, testRows[2]);
        assertPositionValue(copiedBlock, 2, testRows[4]);

        // Test with nulls
        List<Object>[] testRowsWithNulls = alternatingNullValues(generateTestRows(fieldTypes, 3));
        RowBlock rowBlockWithNulls = (RowBlock) createBlockBuilderWithValues(fieldTypes, testRowsWithNulls).build().getRegion(1, testRowsWithNulls.length - 1);

        RowBlock copiedBlockWithNulls = rowBlockWithNulls.copyPositions(new int[] {0, 2}, 0, 2);
        assertThat(copiedBlockWithNulls.getPositionCount()).isEqualTo(2);
        assertPositionValue(copiedBlockWithNulls, 0, testRowsWithNulls[1]);
        assertPositionValue(copiedBlockWithNulls, 1, testRowsWithNulls[3]);

        // Test with offset
        RowBlock regionBlock = rowBlock.getRegion(1, 3);
        RowBlock copiedBlockWithOffset = regionBlock.copyPositions(new int[] {0, 2}, 0, 2);
        assertThat(copiedBlockWithOffset.getPositionCount()).isEqualTo(2);
        assertPositionValue(copiedBlockWithOffset, 0, testRows[1]);
        assertPositionValue(copiedBlockWithOffset, 1, testRows[3]);

        // Test empty positions
        RowBlock emptyBlock = rowBlock.copyPositions(new int[0], 0, 0);
        assertThat(emptyBlock.getPositionCount()).isEqualTo(0);
    }

    @Test
    public void testGetRegion()
    {
        List<Type> fieldTypes = ImmutableList.of(VARCHAR, BIGINT);
        List<Object>[] testRows = generateTestRows(fieldTypes, 5);
        RowBlock rowBlock = (RowBlock) createBlockBuilderWithValues(fieldTypes, testRows).build();

        RowBlock regionBlock = rowBlock.getRegion(1, 3);
        assertThat(regionBlock.getPositionCount()).isEqualTo(3);
        assertPositionValue(regionBlock, 0, testRows[1]);
        assertPositionValue(regionBlock, 1, testRows[2]);
        assertPositionValue(regionBlock, 2, testRows[3]);

        // Test zero length
        RowBlock zeroLengthRegion = rowBlock.getRegion(1, 0);
        assertThat(zeroLengthRegion.getPositionCount()).isEqualTo(0);
    }

    @Test
    public void testCopyRegion()
    {
        List<Type> fieldTypes = ImmutableList.of(VARCHAR, BIGINT);
        List<Object>[] testRows = generateTestRows(fieldTypes, 5);
        RowBlock rowBlock = (RowBlock) createBlockBuilderWithValues(fieldTypes, testRows).build();

        RowBlock copiedRegion = rowBlock.copyRegion(1, 3);
        assertThat(copiedRegion.getPositionCount()).isEqualTo(3);
        assertPositionValue(copiedRegion, 0, testRows[1]);
        assertPositionValue(copiedRegion, 1, testRows[2]);
        assertPositionValue(copiedRegion, 2, testRows[3]);

        // Test with nulls and offset
        List<Object>[] testRowsWithNulls = alternatingNullValues(generateTestRows(fieldTypes, 3));
        RowBlock rowBlockWithNulls = (RowBlock) createBlockBuilderWithValues(fieldTypes, testRowsWithNulls).build().getRegion(1, testRowsWithNulls.length - 2);
        RowBlock copiedRegionWithNulls = rowBlockWithNulls.copyRegion(0, 2);
        assertThat(copiedRegionWithNulls.getPositionCount()).isEqualTo(2);
        assertPositionValue(copiedRegionWithNulls, 0, testRowsWithNulls[1]);
        assertPositionValue(copiedRegionWithNulls, 1, testRowsWithNulls[2]);

        // Test zero length
        RowBlock zeroLengthCopy = rowBlock.copyRegion(1, 0);
        assertThat(zeroLengthCopy.getPositionCount()).isEqualTo(0);
    }

    private void testWith(List<Type> fieldTypes, List<Object>[] expectedValues)
    {
        Block block = createBlockBuilderWithValues(fieldTypes, expectedValues).build();
        assertBlock(block, expectedValues);

        IntArrayList positionList = generatePositionList(expectedValues.length, expectedValues.length / 2);
        assertBlockFilteredPositions(expectedValues, block, positionList.toIntArray());
    }

    private BlockBuilder createBlockBuilderWithValues(List<Type> fieldTypes, List<Object>[] rows)
    {
        RowBlockBuilder rowBlockBuilder = new RowBlockBuilder(fieldTypes, null, 1);
        for (List<Object> row : rows) {
            if (row == null) {
                rowBlockBuilder.appendNull();
            }
            else {
                rowBlockBuilder.buildEntry(fieldBuilders -> {
                    for (int i = 0; i < row.size(); i++) {
                        Object fieldValue = row.get(i);
                        if (fieldValue == null) {
                            fieldBuilders.get(i).appendNull();
                        }
                        else {
                            if (fieldValue instanceof Long longValue) {
                                BIGINT.writeLong(fieldBuilders.get(i), longValue.longValue());
                            }
                            else if (fieldValue instanceof String string) {
                                VARCHAR.writeSlice(fieldBuilders.get(i), utf8Slice(string));
                            }
                            else {
                                throw new IllegalArgumentException();
                            }
                        }
                    }
                });
            }
        }

        return rowBlockBuilder;
    }

    @Override
    protected <T> void assertPositionValue(Block block, int position, T expectedValue)
    {
        if (expectedValue == null) {
            assertThat(block.isNull(position)).isTrue();
            return;
        }

        RowBlock rowBlock = (RowBlock) block;
        List<?> row = (List<?>) expectedValue;

        assertThat(rowBlock.isNull(position)).isFalse();
        SqlRow sqlRow = rowBlock.getRow(position);
        assertThat(sqlRow.getFieldCount()).isEqualTo(row.size());

        int rawIndex = sqlRow.getRawIndex();
        for (int i = 0; i < row.size(); i++) {
            Object fieldValue = row.get(i);
            Block rawFieldBlock = sqlRow.getRawFieldBlock(i);
            if (fieldValue == null) {
                assertThat(rawFieldBlock.isNull(rawIndex)).isTrue();
            }
            else {
                if (fieldValue instanceof Long longValue) {
                    assertThat(BIGINT.getLong(rawFieldBlock, rawIndex)).isEqualTo(longValue.longValue());
                }
                else if (fieldValue instanceof String stringValue) {
                    assertThat(VARCHAR.getSlice(rawFieldBlock, rawIndex)).isEqualTo(utf8Slice(stringValue));
                }
                else {
                    throw new IllegalArgumentException();
                }
            }
        }
    }

    private List<Object>[] generateTestRows(List<Type> fieldTypes, int numRows)
    {
        @SuppressWarnings("unchecked")
        List<Object>[] testRows = new List[numRows];
        for (int i = 0; i < numRows; i++) {
            List<Object> testRow = new ArrayList<>(fieldTypes.size());
            for (int j = 0; j < fieldTypes.size(); j++) {
                int cellId = i * fieldTypes.size() + j;
                if (cellId % 7 == 3) {
                    // Put null value for every 7 cells
                    testRow.add(null);
                }
                else {
                    if (fieldTypes.get(j) == BIGINT) {
                        testRow.add(i * 100L + j);
                    }
                    else if (fieldTypes.get(j) == VARCHAR) {
                        testRow.add(format("field(%s, %s)", i, j));
                    }
                    else {
                        throw new IllegalArgumentException();
                    }
                }
            }
            testRows[i] = testRow;
        }
        return testRows;
    }

    private IntArrayList generatePositionList(int numRows, int numPositions)
    {
        IntArrayList positions = new IntArrayList(numPositions);
        for (int i = 0; i < numPositions; i++) {
            positions.add((7 * i + 3) % numRows);
        }
        Collections.sort(positions);
        return positions;
    }
}
