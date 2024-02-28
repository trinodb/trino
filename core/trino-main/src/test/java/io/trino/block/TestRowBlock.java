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
                            if (fieldValue instanceof Long) {
                                BIGINT.writeLong(fieldBuilders.get(i), ((Long) fieldValue).longValue());
                            }
                            else if (fieldValue instanceof String) {
                                VARCHAR.writeSlice(fieldBuilders.get(i), utf8Slice((String) fieldValue));
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
                if (fieldValue instanceof Long) {
                    assertThat(BIGINT.getLong(rawFieldBlock, rawIndex)).isEqualTo(((Long) fieldValue).longValue());
                }
                else if (fieldValue instanceof String) {
                    assertThat(VARCHAR.getSlice(rawFieldBlock, rawIndex)).isEqualTo(utf8Slice((String) fieldValue));
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
