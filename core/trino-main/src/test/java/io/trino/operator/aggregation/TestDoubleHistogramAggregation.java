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
import com.google.common.collect.Maps;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.AggregationMetrics;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.AggregationNode.Step;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.OptionalInt;

import static io.trino.operator.aggregation.AggregationTestUtils.getFinalBlock;
import static io.trino.operator.aggregation.AggregationTestUtils.getIntermediateBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.util.StructuralTestUtil.mapType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDoubleHistogramAggregation
{
    private final TestingAggregationFunction function;
    private final Type intermediateType;
    private final Type finalType;
    private final Page input;

    public TestDoubleHistogramAggregation()
    {
        function = new TestingFunctionResolution().getAggregateFunction("numeric_histogram", fromTypes(BIGINT, DOUBLE, DOUBLE));
        intermediateType = function.getIntermediateType();
        finalType = function.getFinalType();
        input = makeInput(10);
    }

    @Test
    public void test()
    {
        Aggregator singleStep = getAggregator(SINGLE);
        singleStep.processPage(input);
        Block expected = getFinalBlock(finalType, singleStep);

        Aggregator partialStep = getAggregator(PARTIAL);
        partialStep.processPage(input);
        Block partialBlock = getIntermediateBlock(intermediateType, partialStep);

        Aggregator finalStep = getAggregator(FINAL);
        finalStep.processPage(new Page(partialBlock));
        Block actual = getFinalBlock(finalType, finalStep);

        assertThat(extractSingleValue(actual)).isEqualTo(extractSingleValue(expected));
    }

    @Test
    public void testMerge()
    {
        Aggregator singleStep = getAggregator(SINGLE);
        singleStep.processPage(input);
        Block singleStepResult = getFinalBlock(finalType, singleStep);

        Aggregator partialStep = getAggregator(PARTIAL);
        partialStep.processPage(input);
        Block intermediate = getIntermediateBlock(intermediateType, partialStep);

        Aggregator finalStep = getAggregator(FINAL);

        finalStep.processPage(new Page(intermediate));
        finalStep.processPage(new Page(intermediate));
        Block actual = getFinalBlock(finalType, finalStep);

        Map<Double, Double> expected = Maps.transformValues(extractSingleValue(singleStepResult), value -> value * 2);

        assertThat(extractSingleValue(actual)).isEqualTo(expected);
    }

    @Test
    public void testNull()
    {
        Aggregator aggregator = getAggregator(SINGLE);
        Block result = getFinalBlock(finalType, aggregator);

        assertThat(result.getPositionCount() == 1).isTrue();
        assertThat(result.isNull(0)).isTrue();
    }

    @Test
    public void testBadNumberOfBuckets()
    {
        Aggregator singleStep = getAggregator(SINGLE);
        assertThatThrownBy(() -> singleStep.processPage(makeInput(0)))
                .isInstanceOf(TrinoException.class)
                .hasMessage("numeric_histogram bucket count must be greater than one");
        getFinalBlock(finalType, singleStep);
    }

    private Aggregator getAggregator(Step step)
    {
        return function.createAggregatorFactory(step, step.isInputRaw() ? ImmutableList.of(0, 1, 2) : ImmutableList.of(0), OptionalInt.empty())
                .createAggregator(new AggregationMetrics());
    }

    private static Map<Double, Double> extractSingleValue(Block block)
    {
        MapType mapType = mapType(DOUBLE, DOUBLE);
        return (Map<Double, Double>) mapType.getObjectValue(null, block, 0);
    }

    private static Page makeInput(int numberOfBuckets)
    {
        PageBuilder builder = new PageBuilder(ImmutableList.of(BIGINT, DOUBLE, DOUBLE));

        for (int i = 0; i < 100; i++) {
            builder.declarePosition();

            BIGINT.writeLong(builder.getBlockBuilder(0), numberOfBuckets);
            DOUBLE.writeDouble(builder.getBlockBuilder(1), i); // value
            DOUBLE.writeDouble(builder.getBlockBuilder(2), 1); // weight
        }

        return builder.build();
    }
}
