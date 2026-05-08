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
package io.trino.operator.join.nonspilling;

import io.trino.spi.block.Block;
import io.trino.spi.block.ValueBlock;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;

import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.block.BlockAssertions.createRandomDictionaryBlock;
import static io.trino.block.BlockAssertions.createRandomRleBlock;
import static io.trino.operator.join.NullablePositions.getNonNullPositions;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

final class TestJoinProbe
{
    @Test
    void testGetNonNullPositions()
    {
        Block[] blocks = new Block[100];
        int positionCount = 8192;
        for (int i = 0; i < blocks.length; i++) {
            ValueBlock block = createRandomBlockForType(BIGINT, positionCount, 0.1f);
            if (i % 3 == 0) {
                blocks[i] = createRandomRleBlock(block, positionCount);
            }
            else if (i % 3 == 1) {
                blocks[i] = createRandomDictionaryBlock(block, positionCount);
            }
            else {
                blocks[i] = block;
            }
        }

        Optional<int[]> nonNullPositions = getNonNullPositions(blocks, 90, positionCount);
        int[] expectedNonNullPositions = getExpectedNonNullPositions(Arrays.copyOf(blocks, 90), positionCount);
        assertThat(nonNullPositions).hasValueSatisfying(positions -> assertThat(positions).containsExactly(expectedNonNullPositions));

        Block[] blocks1 = Arrays.copyOfRange(blocks, 13, 46);
        Optional<int[]> nonNullPositions1 = getNonNullPositions(blocks1, blocks1.length, positionCount);
        int[] expected1 = getExpectedNonNullPositions(blocks1, positionCount);
        assertThat(nonNullPositions1).hasValueSatisfying(positions -> assertThat(positions).containsExactly(expected1));

        Block[] blocks2 = Arrays.copyOfRange(blocks1, 0, 25);
        Optional<int[]> nonNullPositions2 = getNonNullPositions(blocks2, blocks2.length, positionCount);
        int[] expected2 = getExpectedNonNullPositions(blocks2, positionCount);
        assertThat(nonNullPositions2).hasValueSatisfying(positions -> assertThat(positions).containsExactly(expected2));
    }

    @Test
    void testGetNonNullPositionsReturnsEmptyWhenAllNonNull()
    {
        int positionCount = 1024;
        // No nullable blocks at all
        assertThat(getNonNullPositions(new Block[0], 0, positionCount)).isEmpty();

        // Nullable blocks present but containing no actual nulls
        Block[] blocks = new Block[3];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = createRandomBlockForType(BIGINT, positionCount, 0f);
        }
        assertThat(getNonNullPositions(blocks, blocks.length, positionCount)).isEmpty();
    }

    private static int[] getExpectedNonNullPositions(Block[] blocks, int positionCount)
    {
        int[] nonNullPositions = new int[positionCount];
        int nonNullCount = 0;
        for (int position = 0; position < positionCount; position++) {
            boolean isNull = false;
            for (Block block : blocks) {
                if (block.isNull(position)) {
                    isNull = true;
                    break;
                }
            }
            if (!isNull) {
                nonNullPositions[nonNullCount++] = position;
            }
        }
        return Arrays.copyOf(nonNullPositions, nonNullCount);
    }
}
