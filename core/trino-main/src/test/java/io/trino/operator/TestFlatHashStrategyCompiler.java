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
package io.trino.operator;

import io.trino.spi.block.Block;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;

class TestFlatHashStrategyCompiler
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Test
    void test()
    {
        // this will work with 100K columns, but that uses too much memory for the CI
        FlatHashStrategyCompiler.compileFlatHashStrategy(nCopies(2_001, BIGINT), TYPE_OPERATORS);
        FlatHashStrategyCompiler.compileFlatHashStrategy(nCopies(2_001, VARCHAR), TYPE_OPERATORS);
    }

    @Test
    public void testMultiChunkIdenticalAndHash()
    {
        int[] testColumns = new int[] {
                FlatHashStrategyCompiler.COLUMNS_PER_CHUNK - 1,
                FlatHashStrategyCompiler.COLUMNS_PER_CHUNK,
                FlatHashStrategyCompiler.COLUMNS_PER_CHUNK + 1};
        for (int columns : testColumns) {
            // All variable width, mix of nulls and non nulls
            testVariableWidthColumns(columns);
            // All fixed width
            testFixedWidthColumns(columns);
        }
    }

    private static void testVariableWidthColumns(int columns)
    {
        Block[] variableBlocks = createVariableWidthTestColumns(columns);
        FlatHashStrategy hashStrategy = FlatHashStrategyCompiler.compileFlatHashStrategy(nCopies(columns, VARCHAR), TYPE_OPERATORS);
        byte[] fixedChunk = new byte[hashStrategy.getTotalFlatFixedLength()];
        int variableLength = toIntExact(Arrays.stream(variableBlocks).mapToLong(block -> VARCHAR.getFlatVariableWidthSize(block, 0)).sum());
        byte[] variableChunk = new byte[variableLength];
        hashStrategy.writeFlat(variableBlocks, 0, fixedChunk, 0, variableChunk, 0);
        assertThat(hashStrategy.valueIdentical(fixedChunk, 0, variableChunk, 0, variableBlocks, 0)).isTrue();

        long blocksHash = hashStrategy.hash(variableBlocks, 0);
        assertThat(hashStrategy.hash(fixedChunk, 0, variableChunk, 0)).isEqualTo(blocksHash);
    }

    private static void testFixedWidthColumns(int columns)
    {
        Block[] fixedBlocks = createFixedWidthTestColumns(columns);
        FlatHashStrategy hashStrategy = FlatHashStrategyCompiler.compileFlatHashStrategy(nCopies(columns, BIGINT), TYPE_OPERATORS);
        byte[] fixedChunk = new byte[hashStrategy.getTotalFlatFixedLength()];
        byte[] variableChunk = null;
        hashStrategy.writeFlat(fixedBlocks, 0, fixedChunk, 0, fixedChunk, 0);
        assertThat(hashStrategy.valueIdentical(fixedChunk, 0, variableChunk, 0, fixedBlocks, 0)).isTrue();

        long blocksHash = hashStrategy.hash(fixedBlocks, 0);
        assertThat(hashStrategy.hash(fixedChunk, 0, variableChunk, 0)).isEqualTo(blocksHash);
    }

    private static Block[] createVariableWidthTestColumns(int columns)
    {
        Block nonNullBlock = createStringsBlock("test string");
        Block nullsBlock = VARCHAR.createBlockBuilder(null, 1).appendNull().buildValueBlock();
        Block[] blocks = new Block[columns];
        for (int i = 0; i < columns; i++) {
            blocks[i] = i % 2 == 0 ? nullsBlock : nonNullBlock;
        }
        return blocks;
    }

    private static Block[] createFixedWidthTestColumns(int columns)
    {
        Block nonNullBlock = createLongsBlock((long) Integer.MAX_VALUE + 1);
        Block nullsBlock = BIGINT.createBlockBuilder(null, 1).appendNull().buildValueBlock();
        Block[] blocks = new Block[columns];
        for (int i = 0; i < columns; i++) {
            blocks[i] = i % 2 == 0 ? nullsBlock : nonNullBlock;
        }
        return blocks;
    }
}
