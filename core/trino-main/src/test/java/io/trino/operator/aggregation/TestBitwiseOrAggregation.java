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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.LongStream;

import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.spi.type.BigintType.BIGINT;

public class TestBitwiseOrAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    protected Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, length);

        for (int i = start; i < start + length; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    protected Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        return LongStream.range(start, start + length).reduce(start, (x, y) -> x | y);
    }

    @Override
    protected String getFunctionName()
    {
        return "bitwise_or_agg";
    }

    @Override
    protected List<Type> getFunctionParameterTypes()
    {
        return ImmutableList.of(BIGINT);
    }

    @Test
    public void testNulls()
    {
        testAggregation(1L, createLongsBlock(1L, null));
        testAggregation(1L, createLongsBlock(null, 1L));
    }
}
