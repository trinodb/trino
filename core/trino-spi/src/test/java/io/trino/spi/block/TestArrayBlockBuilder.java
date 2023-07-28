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
package io.trino.spi.block;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Long.BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestArrayBlockBuilder
        extends AbstractTestBlockBuilder<List<String>>
{
    // ArrayBlockBuilder: isNull, offset, 3 * value (FixedWidthBlockBuilder: isNull, value)
    private static final int THREE_INTS_ENTRY_SIZE = Byte.BYTES + Integer.BYTES + 3 * (Byte.BYTES + Long.BYTES);
    private static final int EXPECTED_ENTRY_COUNT = 100;

    @Test
    public void testArrayBlockIsFull()
    {
        testIsFull(new PageBuilderStatus(THREE_INTS_ENTRY_SIZE * EXPECTED_ENTRY_COUNT));
    }

    private void testIsFull(PageBuilderStatus pageBuilderStatus)
    {
        ArrayBlockBuilder blockBuilder = new ArrayBlockBuilder(BIGINT, pageBuilderStatus.createBlockBuilderStatus(), EXPECTED_ENTRY_COUNT);
        assertThat(pageBuilderStatus.isEmpty()).isTrue();
        while (!pageBuilderStatus.isFull()) {
            blockBuilder.buildEntry(elementBuilder -> {
                BIGINT.writeLong(elementBuilder, 12);
                elementBuilder.appendNull();
                BIGINT.writeLong(elementBuilder, 34);
            });
        }
        assertThat(blockBuilder.getPositionCount()).isEqualTo(EXPECTED_ENTRY_COUNT);
        assertThat(pageBuilderStatus.isFull()).isEqualTo(true);
    }

    //TODO we should systematically test Block::getRetainedSizeInBytes()
    @Test
    public void testRetainedSizeInBytes()
    {
        int expectedEntries = 1000;
        ArrayBlockBuilder arrayBlockBuilder = new ArrayBlockBuilder(BIGINT, null, expectedEntries);
        long initialRetainedSize = arrayBlockBuilder.getRetainedSizeInBytes();
        for (int i = 0; i < expectedEntries; i++) {
            int value = i;
            arrayBlockBuilder.buildEntry(elementBuilder -> BIGINT.writeLong(elementBuilder, value));
        }
        assertThat(arrayBlockBuilder.getRetainedSizeInBytes())
                .isGreaterThanOrEqualTo(expectedEntries * BYTES + instanceSize(LongArrayBlockBuilder.class) + initialRetainedSize);
    }

    @Test
    public void testConcurrentWriting()
    {
        ArrayBlockBuilder blockBuilder = new ArrayBlockBuilder(BIGINT, null, EXPECTED_ENTRY_COUNT);
        blockBuilder.buildEntry(elementBuilder -> {
            BIGINT.writeLong(elementBuilder, 45);
            assertThatThrownBy(() -> blockBuilder.buildEntry(ignore -> {}))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Expected current entry to be closed but was opened");
        });
    }

    @Test
    public void testBuilderProducesNullRleForNullRows()
    {
        // empty block
        assertIsAllNulls(blockBuilder().build(), 0);

        // single null
        assertIsAllNulls(blockBuilder().appendNull().build(), 1);

        // multiple nulls
        assertIsAllNulls(blockBuilder().appendNull().appendNull().build(), 2);
    }

    private static BlockBuilder blockBuilder()
    {
        return new ArrayBlockBuilder(BIGINT, null, 10);
    }

    private static void assertIsAllNulls(Block block, int expectedPositionCount)
    {
        assertThat(block.getPositionCount()).isEqualTo(expectedPositionCount);
        if (expectedPositionCount <= 1) {
            assertThat(block.getClass()).isEqualTo(ArrayBlock.class);
        }
        else {
            assertThat(block.getClass()).isEqualTo(RunLengthEncodedBlock.class);
            assertThat(((RunLengthEncodedBlock) block).getValue().getClass()).isEqualTo(ArrayBlock.class);
        }
        if (expectedPositionCount > 0) {
            assertThat(block.isNull(0)).isTrue();
        }
    }

    @Override
    protected BlockBuilder createBlockBuilder()
    {
        return new ArrayBlockBuilder(new VariableWidthBlockBuilder(null, 1, 100), null, 1);
    }

    @Override
    protected List<List<String>> getTestValues()
    {
        return List.of(
                List.of("a", "apple", "ape"),
                Arrays.asList("b", null, "bear", "break"),
                List.of("c", "cherry"),
                Arrays.asList("d", "date", "dinosaur", null, "dirt"),
                List.of("e", "eggplant", "empty", ""));
    }

    @Override
    protected List<String> getUnusedTestValue()
    {
        return List.of("unused", "ignore me");
    }

    @Override
    protected ValueBlock blockFromValues(Iterable<List<String>> values)
    {
        ArrayBlockBuilder blockBuilder = new ArrayBlockBuilder(new VariableWidthBlockBuilder(null, 1, 100), null, 1);
        for (List<String> array : values) {
            if (array == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.buildEntry(elementBuilder -> {
                    for (String entry : array) {
                        if (entry == null) {
                            elementBuilder.appendNull();
                        }
                        else {
                            VARCHAR.writeString(elementBuilder, entry);
                        }
                    }
                });
            }
        }
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected List<List<String>> blockToValues(ValueBlock valueBlock)
    {
        ArrayBlock block = (ArrayBlock) valueBlock;
        List<List<String>> actualValues = new ArrayList<>(block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                actualValues.add(null);
            }
            else {
                Block array = block.getArray(i);
                ArrayList<String> arrayBuilder = new ArrayList<>();
                for (int j = 0; j < array.getPositionCount(); j++) {
                    if (array.isNull(j)) {
                        arrayBuilder.add(null);
                    }
                    else {
                        arrayBuilder.add(VARCHAR.getSlice(array, j).toStringUtf8());
                    }
                }
                actualValues.add(arrayBuilder);
            }
        }
        return actualValues;
    }
}
