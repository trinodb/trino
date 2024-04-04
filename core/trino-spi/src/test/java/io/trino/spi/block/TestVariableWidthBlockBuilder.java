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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.ceil;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;

public class TestVariableWidthBlockBuilder
        extends AbstractTestBlockBuilder<String>
{
    private static final int BLOCK_BUILDER_INSTANCE_SIZE = instanceSize(VariableWidthBlockBuilder.class);
    private static final int VARCHAR_VALUE_SIZE = 7;
    private static final int VARCHAR_ENTRY_SIZE = SIZE_OF_INT + VARCHAR_VALUE_SIZE;
    private static final int EXPECTED_ENTRY_COUNT = 3;

    @Test
    public void testFixedBlockIsFull()
    {
        testIsFull(new PageBuilderStatus(VARCHAR_ENTRY_SIZE * EXPECTED_ENTRY_COUNT));
    }

    @Test
    public void testNewBlockBuilderLike()
    {
        int entries = 12345;
        double resetSkew = 1.25;
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, entries, entries);
        for (int i = 0; i < entries; i++) {
            blockBuilder.writeEntry(Slices.wrappedBuffer((byte) i));
        }
        blockBuilder = (VariableWidthBlockBuilder) blockBuilder.newBlockBuilderLike(null);
        // force to initialize capacity
        blockBuilder.writeEntry(Slices.wrappedBuffer((byte) 1));

        long actualArraySize = sizeOf(new int[(int) ceil(resetSkew * (entries + 1))]) + sizeOf(new boolean[(int) ceil(resetSkew * entries)]);
        long actualBytesSize = sizeOf(new byte[(int) ceil(resetSkew * entries)]);
        assertThat(blockBuilder.getRetainedSizeInBytes()).isEqualTo(BLOCK_BUILDER_INSTANCE_SIZE + actualBytesSize + actualArraySize);
    }

    private void testIsFull(PageBuilderStatus pageBuilderStatus)
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(pageBuilderStatus.createBlockBuilderStatus(), 32, 1024);
        assertThat(pageBuilderStatus.isEmpty()).isTrue();
        while (!pageBuilderStatus.isFull()) {
            VARCHAR.writeSlice(blockBuilder, Slices.allocate(VARCHAR_VALUE_SIZE));
        }
        assertThat(blockBuilder.getPositionCount()).isEqualTo(EXPECTED_ENTRY_COUNT);
        assertThat(pageBuilderStatus.isFull()).isEqualTo(true);
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

    @Test
    public void testAppendRepeatedEmpty()
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 100);
        ValueBlock value = VARCHAR.createBlockBuilder(null, 1)
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(Slices.EMPTY_SLICE)
                .writeEntry(utf8Slice("ignored"))
                .buildValueBlock();
        blockBuilder.appendRepeated(value, 1, 10);

        List<String> strings = toStrings(blockBuilder.buildValueBlock());
        assertThat(strings).isEqualTo(nCopies(10, ""));
    }

    @Test
    public void testAppendRepeatedSingle()
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 100);
        VariableWidthBlock value = VARCHAR.createBlockBuilder(null, 1)
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(utf8Slice("ab"))
                .writeEntry(utf8Slice("ignored"))
                .buildValueBlock();
        blockBuilder.appendRepeated(value, 1, 1);

        List<String> strings = toStrings(blockBuilder.buildValueBlock());
        assertThat(strings).isEqualTo(ImmutableList.of("ab"));
    }

    @Test
    public void testAppendRepeated1Byte()
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 100);
        VariableWidthBlock value = VARCHAR.createBlockBuilder(null, 1)
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(utf8Slice("X"))
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(utf8Slice("Y"))
                .writeEntry(utf8Slice("ignored"))
                .buildValueBlock();
        blockBuilder.appendRepeated(value, 1, 3);
        blockBuilder.appendRepeated(value, 3, 2);

        List<String> strings = toStrings(blockBuilder.buildValueBlock());
        assertThat(strings).isEqualTo(ImmutableList.of("X", "X", "X", "Y", "Y"));
    }

    @Test
    public void testAppendRepeated2Bytes()
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 100);
        VariableWidthBlock value = VARCHAR.createBlockBuilder(null, 1)
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(utf8Slice("ab"))
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(utf8Slice("Y"))
                .writeEntry(utf8Slice("ignored"))
                .buildValueBlock();
        blockBuilder.appendRepeated(value, 1, 3);

        List<String> strings = toStrings(blockBuilder.buildValueBlock());
        assertThat(strings).isEqualTo(ImmutableList.of("ab", "ab", "ab"));
    }

    @Test
    public void testAppendRepeatedMultipleBytesOddNumberOfTimes()
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 100);
        VariableWidthBlock value = VARCHAR.createBlockBuilder(null, 1)
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(utf8Slice("abc"))
                .writeEntry(utf8Slice("ignored"))
                .buildValueBlock();
        blockBuilder.appendRepeated(value, 1, 5);

        List<String> strings = toStrings(blockBuilder.buildValueBlock());
        assertThat(strings).isEqualTo(nCopies(5, "abc"));
    }

    @Test
    public void testAppendRepeatedMultipleBytesEvenNumberOfTimes()
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 100);
        VariableWidthBlock value = VARCHAR.createBlockBuilder(null, 1)
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(utf8Slice("abc"))
                .writeEntry(utf8Slice("ignored"))
                .buildValueBlock();
        blockBuilder.appendRepeated(value, 1, 6);

        List<String> strings = toStrings(blockBuilder.buildValueBlock());
        assertThat(strings).isEqualTo(nCopies(6, "abc"));
    }

    @Test
    public void testAppendRepeatedMultipleBytesPowerOf2NumberOfTimes()
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 100);
        VariableWidthBlock value = VARCHAR.createBlockBuilder(null, 1)
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(utf8Slice("abc"))
                .writeEntry(utf8Slice("ignored"))
                .buildValueBlock();
        blockBuilder.appendRepeated(value, 1, 8);

        List<String> strings = toStrings(blockBuilder.buildValueBlock());
        assertThat(strings).isEqualTo(nCopies(8, "abc"));
    }

    private static BlockBuilder blockBuilder()
    {
        return new VariableWidthBlockBuilder(null, 10, 0);
    }

    private static void assertIsAllNulls(Block block, int expectedPositionCount)
    {
        assertThat(block.getPositionCount()).isEqualTo(expectedPositionCount);
        if (expectedPositionCount <= 1) {
            assertThat(block.getClass()).isEqualTo(VariableWidthBlock.class);
        }
        else {
            assertThat(block.getClass()).isEqualTo(RunLengthEncodedBlock.class);
            assertThat(((RunLengthEncodedBlock) block).getValue().getClass()).isEqualTo(VariableWidthBlock.class);
        }
        if (expectedPositionCount > 0) {
            assertThat(block.isNull(0)).isTrue();
        }
    }

    private static List<String> toStrings(VariableWidthBlock block)
    {
        ImmutableList.Builder<String> list = ImmutableList.builder();
        for (int i = 0; i < block.getPositionCount(); i++) {
            list.add(VARCHAR.getSlice(block, i).toStringUtf8());
        }
        return list.build();
    }

    @Override
    protected BlockBuilder createBlockBuilder()
    {
        return new VariableWidthBlockBuilder(null, 1, 100);
    }

    @Override
    protected List<String> getTestValues()
    {
        return List.of("a", "bb", "cCc", "dddd", "eeEee");
    }

    @Override
    protected String getUnusedTestValue()
    {
        return "unused";
    }

    @Override
    protected ValueBlock blockFromValues(Iterable<String> values)
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 100);
        for (String value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeEntry(utf8Slice(value));
            }
        }
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected List<String> blockToValues(ValueBlock valueBlock)
    {
        VariableWidthBlock block = (VariableWidthBlock) valueBlock;
        List<String> actualValues = new ArrayList<>(block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                actualValues.add(null);
            }
            else {
                actualValues.add(block.getSlice(i).toStringUtf8());
            }
        }
        return actualValues;
    }
}
