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
import io.trino.operator.aggregation.AbstractTestAggregationFunction;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;

import static io.trino.block.BlockAssertions.createDoubleSequenceBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.block.BlockAssertions.createLongRepeatBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;

public class TestDoubleMinNAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    protected Block[] getSequenceBlocks(int start, int length)
    {
        return new Block[] {createDoubleSequenceBlock(start, start + length), createLongRepeatBlock(2, length)};
    }

    @Override
    protected String getFunctionName()
    {
        return "min";
    }

    @Override
    protected List<Type> getFunctionParameterTypes()
    {
        return ImmutableList.of(DOUBLE, BIGINT);
    }

    @Override
    protected Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        if (length == 1) {
            return ImmutableList.of((double) start);
        }
        return ImmutableList.of((double) start, (double) start + 1);
    }

    @Test
    public void testMoreCornerCases()
    {
        testCustomAggregation(new Double[] {1.0, 2.0, null, 3.0}, 5);
        testInvalidAggregation(new Double[] {1.0, 2.0, 3.0}, 0);
        testInvalidAggregation(new Double[] {1.0, 2.0, 3.0}, -1);
    }

    private void testInvalidAggregation(Double[] x, int n)
    {
        assertInvalidAggregation(() ->
                testAggregation(new long[] {}, createDoublesBlock(x), createLongRepeatBlock(n, x.length)));
    }

    private void testCustomAggregation(Double[] values, int n)
    {
        PriorityQueue<Double> heap = new PriorityQueue<>(n, (x, y) -> -Double.compare(x, y));
        Arrays.stream(values).filter(Objects::nonNull).forEach(heap::add);
        Double[] expected = new Double[heap.size()];
        for (int i = heap.size() - 1; i >= 0; i--) {
            expected[i] = heap.remove();
        }
        testAggregation(Arrays.asList(expected), createDoublesBlock(values), createLongRepeatBlock(n, values.length));
    }
}
