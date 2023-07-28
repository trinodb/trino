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

import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.ceil;
import static org.assertj.core.api.Assertions.assertThat;

public class TestVariableWidthBlockBuilder
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
}
