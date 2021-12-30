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
package io.trino.operator.aggregation.minmaxn;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.trino.operator.aggregation.AbstractTestAggregationFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.stream.LongStream;

import static io.trino.block.BlockAssertions.createLongRepeatBlock;
import static io.trino.spi.type.BigintType.BIGINT;

public class TestArrayMaxNAggregation
        extends AbstractTestAggregationFunction
{
    public static Block createLongArraysBlock(Long[] values)
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, values.length);
        for (Long value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder elementBlockBuilder = blockBuilder.beginBlockEntry();
                BIGINT.writeLong(elementBlockBuilder, value);
                blockBuilder.closeEntry();
            }
        }
        return blockBuilder.build();
    }

    public static Block createLongArraySequenceBlock(int start, int length)
    {
        return createLongArraysBlock(LongStream.range(start, length).boxed().toArray(Long[]::new));
    }

    @Override
    protected Block[] getSequenceBlocks(int start, int length)
    {
        return new Block[] {createLongArraySequenceBlock(start, start + length), createLongRepeatBlock(2, length)};
    }

    @Override
    protected String getFunctionName()
    {
        return "max";
    }

    @Override
    protected List<Type> getFunctionParameterTypes()
    {
        return ImmutableList.of(new ArrayType(BIGINT), BIGINT);
    }

    @Override
    protected Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        if (length == 1) {
            return ImmutableList.of(ImmutableList.of((long) start));
        }
        return ImmutableList.of(ImmutableList.of((long) start + length - 1), ImmutableList.of((long) start + length - 2));
    }

    @Test
    public void testMoreCornerCases()
    {
        testCustomAggregation(new Long[] {1L, 2L, null, 3L}, 5);
        testInvalidAggregation(new Long[] {1L, 2L, 3L}, 0);
        testInvalidAggregation(new Long[] {1L, 2L, 3L}, -1);
        testInvalidAggregation(new Long[] {1L, 2L, 3L}, 10001);
    }

    private void testInvalidAggregation(Long[] x, int n)
    {
        assertInvalidAggregation(() ->
                testAggregation(null, createLongArraysBlock(x), createLongRepeatBlock(n, x.length)));
    }

    private void testCustomAggregation(Long[] values, int n)
    {
        PriorityQueue<Long> heap = new PriorityQueue<>(n);
        Arrays.stream(values).filter(Objects::nonNull).forEach(heap::add);
        ImmutableList.Builder<List<Long>> expected = ImmutableList.builder();
        for (int i = heap.size() - 1; i >= 0; i--) {
            expected.add(ImmutableList.of(heap.remove()));
        }
        testAggregation(Lists.reverse(expected.build()), createLongArraysBlock(values), createLongRepeatBlock(n, values.length));
    }
}
