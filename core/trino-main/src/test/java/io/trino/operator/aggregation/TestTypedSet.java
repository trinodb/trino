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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.type.BlockTypeOperators;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
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

public class TestTypedSet
{
    private static final BlockTypeOperators BLOCK_TYPE_OPERATORS = new BlockTypeOperators(new TypeOperators());
    private static final String FUNCTION_NAME = "typed_set_test";

    @Test
    public void testConstructor()
    {
        for (int i = -2; i <= -1; i++) {
            int expectedSize = i;
            assertThatThrownBy(() -> createEqualityTypedSet(BIGINT, expectedSize))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("expectedSize must not be negative");
        }

        assertThatThrownBy(() -> TypedSet.createEqualityTypedSet(null, null, null, 1, FUNCTION_NAME))
                .isInstanceOfAny(NullPointerException.class, IllegalArgumentException.class);
    }

    @Test
    public void testGetElementPosition()
    {
        int elementCount = 100;
        // Set initialTypedSetEntryCount to a small number to trigger rehash()
        int initialTypedSetEntryCount = 10;
        TypedSet typedSet = createEqualityTypedSet(BIGINT, initialTypedSetEntryCount);
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(elementCount);
        for (int i = 0; i < elementCount; i++) {
            BIGINT.writeLong(blockBuilder, i);
            typedSet.add(blockBuilder, i);
        }

        assertThat(typedSet.size()).isEqualTo(elementCount);

        for (int j = 0; j < blockBuilder.getPositionCount(); j++) {
            assertThat(typedSet.positionOf(blockBuilder, j)).isEqualTo(j);
        }
    }

    @Test
    public void testGetElementPositionWithNull()
    {
        int elementCount = 100;
        // Set initialTypedSetEntryCount to a small number to trigger rehash()
        int initialTypedSetEntryCount = 10;
        TypedSet typedSet = createEqualityTypedSet(BIGINT, initialTypedSetEntryCount);
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(elementCount);
        for (int i = 0; i < elementCount; i++) {
            if (i % 10 == 0) {
                blockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(blockBuilder, i);
            }
            typedSet.add(blockBuilder, i);
        }

        // The internal elementBlock and hashtable of the typedSet should contain
        // all distinct non-null elements plus one null
        assertThat(typedSet.size()).isEqualTo(elementCount - elementCount / 10 + 1);

        int nullCount = 0;
        for (int j = 0; j < blockBuilder.getPositionCount(); j++) {
            // The null is only added to typedSet once, so the internal elementBlock subscript is shifted by nullCountMinusOne
            if (!blockBuilder.isNull(j)) {
                assertThat(typedSet.positionOf(blockBuilder, j)).isEqualTo(j - nullCount + 1);
            }
            else {
                // The first null added to typedSet is at position 0
                assertThat(typedSet.positionOf(blockBuilder, j)).isEqualTo(0);
                nullCount++;
            }
        }
    }

    @Test
    public void testGetElementPositionWithProvidedEmptyBlockBuilder()
    {
        int elementCount = 100;
        // Set initialTypedSetEntryCount to a small number to trigger rehash()
        int initialTypedSetEntryCount = 10;

        BlockBuilder emptyBlockBuilder = BIGINT.createFixedSizeBlockBuilder(elementCount);
        TypedSet typedSet = createDistinctTypedSet(BIGINT, initialTypedSetEntryCount, emptyBlockBuilder);
        BlockBuilder externalBlockBuilder = BIGINT.createFixedSizeBlockBuilder(elementCount);
        for (int i = 0; i < elementCount; i++) {
            if (i % 10 == 0) {
                externalBlockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(externalBlockBuilder, i);
            }
            typedSet.add(externalBlockBuilder, i);
        }

        assertThat(typedSet.size()).isEqualTo(emptyBlockBuilder.getPositionCount());
        assertThat(typedSet.size()).isEqualTo(elementCount - elementCount / 10 + 1);

        for (int j = 0; j < typedSet.size(); j++) {
            assertThat(typedSet.positionOf(emptyBlockBuilder, j)).isEqualTo(j);
        }
    }

    @Test
    public void testGetElementPositionWithProvidedNonEmptyBlockBuilder()
    {
        int elementCount = 100;
        // Set initialTypedSetEntryCount to a small number to trigger rehash()
        int initialTypedSetEntryCount = 10;

        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT));
        BlockBuilder firstBlockBuilder = pageBuilder.getBlockBuilder(0);

        for (int i = 0; i < elementCount; i++) {
            BIGINT.writeLong(firstBlockBuilder, i);
        }
        pageBuilder.declarePositions(elementCount);

        // The secondBlockBuilder should already have elementCount rows.
        BlockBuilder secondBlockBuilder = pageBuilder.getBlockBuilder(0);

        TypedSet typedSet = createDistinctTypedSet(BIGINT, initialTypedSetEntryCount, secondBlockBuilder);
        BlockBuilder externalBlockBuilder = BIGINT.createFixedSizeBlockBuilder(elementCount);
        for (int i = 0; i < elementCount; i++) {
            if (i % 10 == 0) {
                externalBlockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(externalBlockBuilder, i);
            }
            typedSet.add(externalBlockBuilder, i);
        }

        assertThat(typedSet.size()).isEqualTo(secondBlockBuilder.getPositionCount() - elementCount);
        assertThat(typedSet.size()).isEqualTo(elementCount - elementCount / 10 + 1);

        for (int i = 0; i < typedSet.size(); i++) {
            int expectedPositionInSecondBlockBuilder = i + elementCount;
            assertThat(typedSet.positionOf(secondBlockBuilder, expectedPositionInSecondBlockBuilder)).isEqualTo(expectedPositionInSecondBlockBuilder);
        }
    }

    @Test
    public void testGetElementPositionRandom()
    {
        TypedSet set = createEqualityTypedSet(VARCHAR, 1);
        testGetElementPositionRandomFor(set);

        BlockBuilder emptyBlockBuilder = VARCHAR.createBlockBuilder(null, 3);
        TypedSet setWithPassedInBuilder = createDistinctTypedSet(VARCHAR, 1, emptyBlockBuilder);
        testGetElementPositionRandomFor(setWithPassedInBuilder);
    }

    @Test
    public void testBigintSimpleTypedSet()
    {
        List<Integer> expectedSetSizes = ImmutableList.of(1, 10, 100, 1000);
        List<Block> longBlocks =
                ImmutableList.of(
                        createEmptyLongsBlock(),
                        createLongsBlock(1L),
                        createLongsBlock(1L, 2L, 3L),
                        createLongsBlock(1L, 2L, 3L, 1L, 2L, 3L),
                        createLongsBlock(1L, null, 3L),
                        createLongsBlock(null, null, null),
                        createLongSequenceBlock(0, 100),
                        createLongSequenceBlock(-100, 100),
                        createLongsBlock(nCopies(1, null)),
                        createLongsBlock(nCopies(100, null)),
                        createLongsBlock(nCopies(expectedSetSizes.get(expectedSetSizes.size() - 1) * 2, null)),
                        createLongsBlock(nCopies(expectedSetSizes.get(expectedSetSizes.size() - 1) * 2, 0L)));

        for (int expectedSetSize : expectedSetSizes) {
            for (Block block : longBlocks) {
                testBigint(block, expectedSetSize);
            }
        }
    }

    @Test
    public void testMemoryExceeded()
    {
        assertTrinoExceptionThrownBy(() -> {
            TypedSet typedSet = createEqualityTypedSet(BIGINT, 10);
            for (int i = 0; i <= TypedSet.MAX_FUNCTION_MEMORY.toBytes() + 1; i++) {
                Block block = createLongsBlock(nCopies(1, (long) i));
                typedSet.add(block, 0);
            }
        }).hasErrorCode(EXCEEDED_FUNCTION_MEMORY_LIMIT);
    }

    private void testGetElementPositionRandomFor(TypedSet set)
    {
        BlockBuilder keys = VARCHAR.createBlockBuilder(null, 5);
        VARCHAR.writeSlice(keys, utf8Slice("hello"));
        VARCHAR.writeSlice(keys, utf8Slice("bye"));
        VARCHAR.writeSlice(keys, utf8Slice("abc"));

        for (int i = 0; i < keys.getPositionCount(); i++) {
            set.add(keys, i);
        }

        BlockBuilder values = VARCHAR.createBlockBuilder(null, 5);
        VARCHAR.writeSlice(values, utf8Slice("bye"));
        VARCHAR.writeSlice(values, utf8Slice("abc"));
        VARCHAR.writeSlice(values, utf8Slice("hello"));
        VARCHAR.writeSlice(values, utf8Slice("bad"));
        values.appendNull();

        assertThat(set.positionOf(values, 4)).isEqualTo(-1);
        assertThat(set.positionOf(values, 2)).isEqualTo(0);
        assertThat(set.positionOf(values, 1)).isEqualTo(2);
        assertThat(set.positionOf(values, 0)).isEqualTo(1);
        assertThat(set.contains(values, 3)).isFalse();

        set.add(values, 4);
        assertThat(set.contains(values, 4)).isTrue();
    }

    private static void testBigint(Block longBlock, int expectedSetSize)
    {
        TypedSet typedSet = createEqualityTypedSet(BIGINT, expectedSetSize);
        testBigintFor(typedSet, longBlock);

        BlockBuilder emptyBlockBuilder = BIGINT.createBlockBuilder(null, expectedSetSize);
        TypedSet typedSetWithPassedInBuilder = createDistinctTypedSet(BIGINT, expectedSetSize, emptyBlockBuilder);
        testBigintFor(typedSetWithPassedInBuilder, longBlock);
    }

    private static TypedSet createEqualityTypedSet(Type type, int expectedSize)
    {
        return TypedSet.createEqualityTypedSet(
                type,
                BLOCK_TYPE_OPERATORS.getEqualOperator(type),
                BLOCK_TYPE_OPERATORS.getHashCodeOperator(type),
                expectedSize,
                FUNCTION_NAME);
    }

    private static TypedSet createDistinctTypedSet(Type type, int expectedSize, BlockBuilder blockBuilder)
    {
        return TypedSet.createDistinctTypedSet(
                type,
                BLOCK_TYPE_OPERATORS.getDistinctFromOperator(type),
                BLOCK_TYPE_OPERATORS.getHashCodeOperator(type),
                blockBuilder,
                expectedSize,
                FUNCTION_NAME);
    }

    private static void testBigintFor(TypedSet typedSet, Block longBlock)
    {
        Set<Long> set = new HashSet<>();
        for (int blockPosition = 0; blockPosition < longBlock.getPositionCount(); blockPosition++) {
            long number = BIGINT.getLong(longBlock, blockPosition);
            assertThat(typedSet.contains(longBlock, blockPosition)).isEqualTo(set.contains(number));
            assertThat(typedSet.size()).isEqualTo(set.size());

            set.add(number);
            typedSet.add(longBlock, blockPosition);

            assertThat(typedSet.contains(longBlock, blockPosition)).isEqualTo(set.contains(number));
            assertThat(typedSet.size()).isEqualTo(set.size());
        }
    }
}
