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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.type.BlockTypeOperators;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.block.BlockAssertions.createEmptyLongsBlock;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.spi.StandardErrorCode.EXCEEDED_FUNCTION_MEMORY_LIMIT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBlockSet
{
    private static final BlockTypeOperators BLOCK_TYPE_OPERATORS = new BlockTypeOperators(new TypeOperators());
    private static final String FUNCTION_NAME = "typed_set_test";

    @Test
    public void testConstructor()
    {
        for (int i = -2; i <= -1; i++) {
            int expectedSize = i;
            assertThatThrownBy(() -> createBlockSet(BIGINT, expectedSize))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("maximumSize must not be negative");
        }

        assertThatThrownBy(() -> new BlockSet(null, null, null, 1))
                .isInstanceOfAny(NullPointerException.class, IllegalArgumentException.class);
    }

    @Test
    public void testGetElementPosition()
    {
        int elementCount = 100;
        BlockSet blockSet = createBlockSet(BIGINT, elementCount);
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(elementCount);
        for (int i = 0; i < elementCount; i++) {
            BIGINT.writeLong(blockBuilder, i);
            blockSet.add(blockBuilder, i);
        }

        assertEquals(blockSet.size(), elementCount);

        for (int j = 0; j < blockBuilder.getPositionCount(); j++) {
            assertEquals(blockSet.positionOf(blockBuilder, j), j);
        }
    }

    @Test
    public void testGetElementPositionWithNull()
    {
        int elementCount = 100;
        BlockSet blockSet = createBlockSet(BIGINT, elementCount);
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(elementCount);
        for (int i = 0; i < elementCount; i++) {
            if (i % 10 == 0) {
                blockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(blockBuilder, i);
            }
            blockSet.add(blockBuilder, i);
        }

        // The internal elementBlock and hashtable of the blockSet should contain
        // all distinct non-null elements plus one null
        assertEquals(blockSet.size(), elementCount - elementCount / 10 + 1);

        int nullCount = 0;
        for (int j = 0; j < blockBuilder.getPositionCount(); j++) {
            // The null is only added to blockSet once, so the internal elementBlock subscript is shifted by nullCountMinusOne
            if (!blockBuilder.isNull(j)) {
                assertEquals(blockSet.positionOf(blockBuilder, j), j - nullCount + 1);
            }
            else {
                // The first null added to blockSet is at position 0
                assertEquals(blockSet.positionOf(blockBuilder, j), 0);
                nullCount++;
            }
        }
    }

    @Test
    public void testMaxSize()
    {
        for (int maxSize : ImmutableList.of(0, 1, 10, 100, 1000)) {
            BlockSet blockSet = createBlockSet(BIGINT, maxSize);
            for (int i = 0; i < maxSize; i++) {
                assertThat(blockSet.add(toBlock(i == 20 ? null : (long) i), 0)).isTrue();
                assertThat(blockSet.size()).isEqualTo(i + 1);
            }

            assertThatThrownBy(() -> blockSet.add(toBlock((long) maxSize), 0))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("BlockSet is full");
            assertThat(blockSet.size()).isEqualTo(maxSize);

            if (maxSize < 20) {
                assertThatThrownBy(() -> blockSet.add(toBlock(null), 0))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("BlockSet is full");
                assertThat(blockSet.size()).isEqualTo(maxSize);
            }

            for (int i = 0; i < maxSize; i++) {
                assertThat(blockSet.add(toBlock(i == 20 ? null : (long) i), 0)).isFalse();
            }
        }
    }

    private static Block toBlock(Long value)
    {
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(1);
        if (value == null) {
            blockBuilder.appendNull();
        }
        else {
            BIGINT.writeLong(blockBuilder, value);
        }
        return blockBuilder.build();
    }

    @Test
    public void testGetElementPositionRandom()
    {
        BlockBuilder keys = VARCHAR.createBlockBuilder(null, 5);
        VARCHAR.writeSlice(keys, utf8Slice("hello"));
        VARCHAR.writeSlice(keys, utf8Slice("bye"));
        VARCHAR.writeSlice(keys, utf8Slice("abc"));

        BlockSet set = createBlockSet(VARCHAR, 4);
        for (int i = 0; i < keys.getPositionCount(); i++) {
            set.add(keys, i);
        }

        BlockBuilder values = VARCHAR.createBlockBuilder(null, 5);
        VARCHAR.writeSlice(values, utf8Slice("bye"));
        VARCHAR.writeSlice(values, utf8Slice("abc"));
        VARCHAR.writeSlice(values, utf8Slice("hello"));
        VARCHAR.writeSlice(values, utf8Slice("bad"));
        values.appendNull();

        assertEquals(set.positionOf(values, 4), -1);
        assertEquals(set.positionOf(values, 2), 0);
        assertEquals(set.positionOf(values, 1), 2);
        assertEquals(set.positionOf(values, 0), 1);
        assertFalse(set.contains(values, 3));

        set.add(values, 4);
        assertTrue(set.contains(values, 4));
    }

    @Test
    public void testBigintSimpleBlockSet()
    {
        testBigint(createEmptyLongsBlock());
        testBigint(createLongsBlock(1L));
        testBigint(createLongsBlock(1L, 2L, 3L));
        testBigint(createLongsBlock(1L, 2L, 3L, 1L, 2L, 3L));
        testBigint(createLongsBlock(1L, null, 3L));
        testBigint(createLongsBlock(null, null, null));
        testBigint(createLongSequenceBlock(0, 100));
        testBigint(createLongSequenceBlock(-100, 100));
        testBigint(createLongsBlock(nCopies(1, null)));
        testBigint(createLongsBlock(nCopies(100, null)));
        testBigint(createLongsBlock(nCopies(2000, null)));
        testBigint(createLongsBlock(nCopies(2000, 0L)));
    }

    private static void testBigint(Block longBlock)
    {
        BlockSet blockSet = createBlockSet(BIGINT, longBlock.getPositionCount());
        Set<Long> set = new HashSet<>();
        for (int blockPosition = 0; blockPosition < longBlock.getPositionCount(); blockPosition++) {
            long number = BIGINT.getLong(longBlock, blockPosition);
            assertEquals(blockSet.contains(longBlock, blockPosition), set.contains(number));
            assertEquals(blockSet.size(), set.size());

            set.add(number);
            blockSet.add(longBlock, blockPosition);

            assertEquals(blockSet.contains(longBlock, blockPosition), set.contains(number));
            assertEquals(blockSet.size(), set.size());
        }
    }

    @Test
    public void testMemoryExceeded()
    {
        DataSize maxSize = DataSize.of(20, KILOBYTE);
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(1024);
        for (int i = 0; blockBuilder.getSizeInBytes() < maxSize.toBytes() + 8; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();

        BlockSet blockSet = createBlockSet(BIGINT, block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); i++) {
            blockSet.add(block, i);
        }
        // blockSet should contain all positions
        assertThat(blockSet.size()).isEqualTo(block.getPositionCount());

        // getting all blocks should fail
        BlockBuilder testOutput = BIGINT.createFixedSizeBlockBuilder(1024);
        assertTrinoExceptionThrownBy(() -> blockSet.getAllWithSizeLimit(testOutput, FUNCTION_NAME, maxSize))
                .hasErrorCode(EXCEEDED_FUNCTION_MEMORY_LIMIT)
                .hasMessageContaining(FUNCTION_NAME);

        // blockBuilder should not contain all positions
        int actualPositionsWritten = testOutput.getPositionCount();
        assertThat(actualPositionsWritten).isLessThan(block.getPositionCount());

        // writing to the same block builder, should fail with the same count
        assertTrinoExceptionThrownBy(() -> blockSet.getAllWithSizeLimit(testOutput, FUNCTION_NAME, maxSize))
                .hasErrorCode(EXCEEDED_FUNCTION_MEMORY_LIMIT)
                .hasMessageContaining(FUNCTION_NAME);
        assertThat(testOutput.getPositionCount()).isEqualTo(actualPositionsWritten * 2);

        // writing with a higher limit should work
        blockSet.getAllWithSizeLimit(testOutput, FUNCTION_NAME, DataSize.of(30, KILOBYTE));
        assertThat(testOutput.getPositionCount()).isEqualTo(actualPositionsWritten * 2 + blockSet.size());
    }

    private static BlockSet createBlockSet(Type type, int expectedSize)
    {
        return new BlockSet(
                type,
                BLOCK_TYPE_OPERATORS.getDistinctFromOperator(type),
                BLOCK_TYPE_OPERATORS.getHashCodeOperator(type),
                expectedSize);
    }
}
