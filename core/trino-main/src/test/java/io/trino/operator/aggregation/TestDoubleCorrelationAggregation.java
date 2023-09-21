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
import io.trino.spi.type.Type;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.DoubleStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.block.BlockAssertions.createDoubleSequenceBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.operator.aggregation.AggregationTestUtils.constructDoublePrimitiveArray;
import static io.trino.spi.type.DoubleType.DOUBLE;

public class TestDoubleCorrelationAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    protected Block[] getSequenceBlocks(int start, int length)
    {
        return new Block[] {createDoubleSequenceBlock(start, start + length), createDoubleSequenceBlock(start + 2, start + 2 + length)};
    }

    @Override
    protected String getFunctionName()
    {
        return "corr";
    }

    @Override
    protected List<Type> getFunctionParameterTypes()
    {
        return ImmutableList.of(DOUBLE, DOUBLE);
    }

    @Override
    protected Object getExpectedValue(int start, int length)
    {
        if (length <= 1) {
            return null;
        }
        PearsonsCorrelation corr = new PearsonsCorrelation();
        return corr.correlation(constructDoublePrimitiveArray(start + 2, length), constructDoublePrimitiveArray(start, length));
    }

    @Test
    public void testDivisionByZero()
    {
        testAggregation(null, createDoublesBlock(2.0, 2.0, 2.0, 2.0, 2.0), createDoublesBlock(1.0, 4.0, 9.0, 16.0, 25.0));
        testAggregation(null, createDoublesBlock(1.0, 4.0, 9.0, 16.0, 25.0), createDoublesBlock(2.0, 2.0, 2.0, 2.0, 2.0));
    }

    @Test
    public void testNonTrivialResult()
    {
        // All other test produce 0, 1, or null only
        testNonTrivialAggregation(new double[] {1, 2, 3, 4, 5}, new double[] {1, 4, 9, 16, 25});
    }

    @Test
    public void testInverseCorrelation()
    {
        testNonTrivialAggregation(new double[] {1, 2, 3, 4, 5}, new double[] {5, 4, 3, 2, 1});
    }

    private void testNonTrivialAggregation(double[] y, double[] x)
    {
        PearsonsCorrelation corr = new PearsonsCorrelation();
        double expected = corr.correlation(x, y);
        checkArgument(Double.isFinite(expected) && expected != 0.0 && expected != 1.0, "Expected result is trivial");
        testAggregation(expected, createDoublesBlock(box(y)), createDoublesBlock(box(x)));
    }

    private Double[] box(double[] values)
    {
        return DoubleStream.of(values).boxed().toArray(Double[]::new);
    }
}
