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
import io.airlift.slice.Slices;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestBlockBuilder
{
    @Test
    public void testMultipleValuesWithNull()
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, 10);
        blockBuilder.appendNull();
        BIGINT.writeLong(blockBuilder, 42);
        blockBuilder.appendNull();
        BIGINT.writeLong(blockBuilder, 42);
        Block block = blockBuilder.build();

        assertThat(block.isNull(0)).isTrue();
        assertThat(BIGINT.getLong(block, 1)).isEqualTo(42L);
        assertThat(block.isNull(2)).isTrue();
        assertThat(BIGINT.getLong(block, 3)).isEqualTo(42L);
    }

    @Test
    public void testNewBlockBuilderLike()
    {
        List<Type> channels = ImmutableList.of(BIGINT, VARCHAR, new ArrayType(new ArrayType(BIGINT)));
        PageBuilder pageBuilder = new PageBuilder(channels);
        BlockBuilder bigintBlockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder varcharBlockBuilder = pageBuilder.getBlockBuilder(1);
        ArrayBlockBuilder arrayBlockBuilder = (ArrayBlockBuilder) pageBuilder.getBlockBuilder(2);

        for (int i = 0; i < 100; i++) {
            int value = i;
            BIGINT.writeLong(bigintBlockBuilder, value);
            VARCHAR.writeSlice(varcharBlockBuilder, Slices.utf8Slice("test" + value));
            arrayBlockBuilder.buildEntry(elementBuilder -> {
                ArrayBlockBuilder nestedArrayBuilder = (ArrayBlockBuilder) elementBuilder;
                nestedArrayBuilder.buildEntry(valueBuilder -> BIGINT.writeLong(valueBuilder, value));
                nestedArrayBuilder.buildEntry(valueBuilder -> BIGINT.writeLong(valueBuilder, value * 2L));
            });
            pageBuilder.declarePosition();
        }

        PageBuilder newPageBuilder = pageBuilder.newPageBuilderLike();
        for (int i = 0; i < channels.size(); i++) {
            assertThat(newPageBuilder.getType(i)).isEqualTo(pageBuilder.getType(i));
            // we should get new block builder instances
            assertThat(pageBuilder.getBlockBuilder(i))
                    .isNotEqualTo(newPageBuilder.getBlockBuilder(i));
            assertThat(newPageBuilder.getBlockBuilder(i).getPositionCount()).isEqualTo(0);
            assertThat(newPageBuilder.getBlockBuilder(i).getRetainedSizeInBytes() < pageBuilder.getBlockBuilder(i).getRetainedSizeInBytes()).isTrue();
        }
    }

    @Test
    public void testGetPositions()
    {
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(5);
        blockBuilder.appendNull();
        BIGINT.writeLong(blockBuilder, 42L);
        blockBuilder.appendNull();
        BIGINT.writeLong(blockBuilder, 43L);
        blockBuilder.appendNull();
        int[] positions = new int[] {0, 1, 1, 1, 4};

        // test getPositions for block
        Block block = blockBuilder.build();
        assertBlockEquals(BIGINT, block.getPositions(positions, 0, positions.length), buildBigintBlock(null, 42, 42, 42, null));
        assertBlockEquals(BIGINT, block.getPositions(positions, 1, 4), buildBigintBlock(42, 42, 42, null));
        assertBlockEquals(BIGINT, block.getPositions(positions, 2, 1), buildBigintBlock(42));
        assertBlockEquals(BIGINT, block.getPositions(positions, 0, 0), buildBigintBlock());
        assertBlockEquals(BIGINT, block.getPositions(positions, 1, 0), buildBigintBlock());

        // out of range
        assertInvalidPosition(block, new int[] {-1}, 0, 1);
        assertInvalidPosition(block, new int[] {6}, 0, 1);
        assertInvalidOffset(block, new int[] {6}, 1, 1);
        assertInvalidOffset(block, new int[] {6}, -1, 1);
        assertInvalidOffset(block, new int[] {6}, 2, -1);

        // assert we should not copy ids
        AtomicBoolean isIdentical = new AtomicBoolean(false);
        block.getPositions(positions, 0, positions.length - 1).retainedBytesForEachPart((part, size) -> {
            if (part == positions) {
                isIdentical.set(true);
            }
        });
        assertThat(isIdentical.get()).isTrue();
    }

    private static Block buildBigintBlock(Integer... values)
    {
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(5);
        for (Integer value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(blockBuilder, value);
            }
        }
        return blockBuilder.build();
    }

    private static void assertInvalidPosition(Block block, int[] positions, int offset, int length)
    {
        assertThatThrownBy(() -> block.getPositions(positions, offset, length).getLong(0, 0))
                .isInstanceOfAny(IllegalArgumentException.class, IndexOutOfBoundsException.class)
                .hasMessage("Invalid position %d and length 1 in block with %d positions", positions[0], block.getPositionCount());
    }

    private static void assertInvalidOffset(Block block, int[] positions, int offset, int length)
    {
        assertThatThrownBy(() -> block.getPositions(positions, offset, length).getLong(0, 0))
                .isInstanceOfAny(IllegalArgumentException.class, IndexOutOfBoundsException.class)
                .hasMessage(format("Invalid offset %d and length %d in array with %d elements", offset, length, positions.length));
    }
}
