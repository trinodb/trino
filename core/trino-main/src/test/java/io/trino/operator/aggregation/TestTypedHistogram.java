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

import io.trino.block.BlockAssertions;
import io.trino.operator.aggregation.histogram.SingleTypedHistogram;
import io.trino.operator.aggregation.histogram.TypedHistogram;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import io.trino.type.BlockTypeOperators;
import org.testng.annotations.Test;

import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.util.StructuralTestUtil.mapType;
import static org.testng.Assert.assertEquals;

public class TestTypedHistogram
{
    private static final BlockTypeOperators BLOCK_TYPE_OPERATORS = new BlockTypeOperators(new TypeOperators());

    @Test
    public void testMassive()
    {
        BlockBuilder inputBlockBuilder = BIGINT.createBlockBuilder(null, 5000);

        TypedHistogram typedHistogram = new SingleTypedHistogram(
                BIGINT,
                BLOCK_TYPE_OPERATORS.getEqualOperator(BIGINT),
                BLOCK_TYPE_OPERATORS.getHashCodeOperator(BIGINT),
                1000);
        IntStream.range(1, 2000)
                .flatMap(i -> IntStream.iterate(i, IntUnaryOperator.identity()).limit(i))
                .forEach(j -> BIGINT.writeLong(inputBlockBuilder, j));

        Block inputBlock = inputBlockBuilder.build();
        addInputBlockToTypedHistogram(typedHistogram, inputBlock);

        MapType mapType = mapType(BIGINT, BIGINT);
        BlockBuilder out = mapType.createBlockBuilder(null, 1);
        typedHistogram.serialize(out);
        Block outputBlock = mapType.getObject(out, 0);
        for (int i = 0; i < outputBlock.getPositionCount(); i += 2) {
            assertEquals(BIGINT.getLong(outputBlock, i + 1), BIGINT.getLong(outputBlock, i));
        }
    }

    @Test
    public void testGetPositionCountBasic()
    {
        Block inputBlock = BlockAssertions.createLongsBlock(1, 2, 3);

        SingleTypedHistogram typedHistogram = new SingleTypedHistogram(
                BIGINT,
                BLOCK_TYPE_OPERATORS.getEqualOperator(BIGINT),
                BLOCK_TYPE_OPERATORS.getHashCodeOperator(BIGINT),
                inputBlock.getPositionCount());

        addInputBlockToTypedHistogram(typedHistogram, inputBlock);
        assertEquals(typedHistogram.getPositionCount(), 3);
    }

    @Test
    public void testGetPositionCountDuplicates()
    {
        Block inputBlock = BlockAssertions.createLongsBlock(1, 2, 1);

        SingleTypedHistogram typedHistogram = new SingleTypedHistogram(
                BIGINT,
                BLOCK_TYPE_OPERATORS.getEqualOperator(BIGINT),
                BLOCK_TYPE_OPERATORS.getHashCodeOperator(BIGINT),
                inputBlock.getPositionCount());

        addInputBlockToTypedHistogram(typedHistogram, inputBlock);
        assertEquals(typedHistogram.getPositionCount(), 2);
    }

    @Test
    public void testGetPositionCountLargeExpected()
    {
        Block inputBlock = BlockAssertions.createLongsBlock(1, 2, 3);

        SingleTypedHistogram typedHistogram = new SingleTypedHistogram(
                BIGINT,
                BLOCK_TYPE_OPERATORS.getEqualOperator(BIGINT),
                BLOCK_TYPE_OPERATORS.getHashCodeOperator(BIGINT),
                99);

        addInputBlockToTypedHistogram(typedHistogram, inputBlock);
        assertEquals(typedHistogram.getPositionCount(), 3);
    }

    @Test
    public void testGetPositionCountEmpty()
    {
        Block inputBlock = BIGINT.createBlockBuilder(null, 0).build();
        SingleTypedHistogram typedHistogram = new SingleTypedHistogram(
                BIGINT,
                BLOCK_TYPE_OPERATORS.getEqualOperator(BIGINT),
                BLOCK_TYPE_OPERATORS.getHashCodeOperator(BIGINT),
                1);

        addInputBlockToTypedHistogram(typedHistogram, inputBlock);
        assertEquals(typedHistogram.getPositionCount(), 0);
    }

    private void addInputBlockToTypedHistogram(TypedHistogram typedHistogram, Block inputBlock)
    {
        for (int i = 0; i < inputBlock.getPositionCount(); i++) {
            typedHistogram.add(i, inputBlock, 1);
        }
    }
}
