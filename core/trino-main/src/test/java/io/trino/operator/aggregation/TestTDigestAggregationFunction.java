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
import io.airlift.stats.TDigest;
import io.trino.block.BlockAssertions;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.SqlVarbinary;
import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.block.BlockAssertions.createDoubleSequenceBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.operator.scalar.TDigestFunctions.DEFAULT_WEIGHT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.lang.Math.abs;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestTDigestAggregationFunction
{
    private static final BiFunction<Object, Object, Boolean> TDIGEST_EQUALITY = (actualBinary, expectedBinary) -> {
        if (actualBinary == null && expectedBinary == null) {
            return true;
        }
        requireNonNull(actualBinary, "actual value was null");
        requireNonNull(expectedBinary, "expected value was null");

        TDigest actual = TDigest.deserialize(wrappedBuffer(((SqlVarbinary) actualBinary).getBytes()));
        TDigest expected = TDigest.deserialize(wrappedBuffer(((SqlVarbinary) expectedBinary).getBytes()));
        return actual.getMin() == expected.getMin() &&
                actual.getMax() == expected.getMax() &&
                returnSimilarResults(actual, expected, (actual.getMax() - actual.getMin()) / 1000);
    };

    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    @Test
    public void testTdigestAggregationFunction()
    {
        List<Double> weights = ImmutableList.of(1.5, 2.0, 1.1, 1.111, 3.5, 4.4, 4.4, 1.0, 9.9, 9.0);
        testAggregation(
                createDoublesBlock(1.0, null, 2.0, null, 3.0, null, 4.0, null, 5.0, null),
                createDoublesBlock(weights),
                ImmutableList.of(1.5, 1.1, 3.5, 4.4, 9.9),
                1.0, 2.0, 3.0, 4.0, 5.0);
        testAggregation(
                createDoublesBlock(null, null, null, null, null),
                BlockAssertions.createRepeatedValuesBlock(1.0, 5),
                ImmutableList.of());
        testAggregation(
                createDoublesBlock(-1.0, -2.0, -3.0, -4.0, -5.0, -6.0, -7.0, -8.0, -9.0, -10.0),
                createDoublesBlock(weights),
                ImmutableList.copyOf(weights),
                -1.0, -2.0, -3.0, -4.0, -5.0, -6.0, -7.0, -8.0, -9.0, -10.0);
        testAggregation(
                createDoublesBlock(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0),
                createDoublesBlock(weights),
                ImmutableList.copyOf(weights),
                1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);
        testAggregation(
                createDoublesBlock(),
                BlockAssertions.createRepeatedValuesBlock(1.0, 0),
                ImmutableList.of());
        testAggregation(
                createDoublesBlock(1.0),
                BlockAssertions.createRepeatedValuesBlock(1.1, 1),
                ImmutableList.of(1.1),
                1.0);

        // This test case uses a custom equality assertion. Otherwise it fails on partial aggregation tests because T-digests differ in shape depending on the order of merge operations.
        weights = LongStream.range(-1000, 1000).asDoubleStream().map(number -> 2 - number / 1000.0).boxed().collect(toImmutableList());
        testAggregation(
                TDIGEST_EQUALITY,
                createDoubleSequenceBlock(-1000, 1000),
                createDoublesBlock(weights),
                ImmutableList.copyOf(weights),
                LongStream.range(-1000, 1000).asDoubleStream().toArray());
    }

    private void testAggregation(Block doublesBlock, Block weightsBlock, List<Double> weights, double... inputs)
    {
        // Test without weights
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("tdigest_agg"),
                fromTypes(DOUBLE),
                getExpectedValue(nCopies(inputs.length, DEFAULT_WEIGHT), inputs),
                new Page(doublesBlock));
        // Test with weights
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("tdigest_agg"),
                fromTypes(DOUBLE, DOUBLE),
                getExpectedValue(weights, inputs),
                new Page(doublesBlock, weightsBlock));
    }

    private void testAggregation(BiFunction<Object, Object, Boolean> equalAssertion, Block doublesBlock, Block weightsBlock, List<Double> weights, double... inputs)
    {
        // Test without weights
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("tdigest_agg"),
                fromTypes(DOUBLE),
                equalAssertion, "Test multiple values",
                new Page(doublesBlock), getExpectedValue(nCopies(inputs.length, DEFAULT_WEIGHT), inputs));
        // Test with weights
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("tdigest_agg"),
                fromTypes(DOUBLE, DOUBLE),
                equalAssertion,
                "Test multiple values",
                new Page(doublesBlock, weightsBlock),
                getExpectedValue(weights, inputs));
    }

    private Object getExpectedValue(List<Double> weights, double... values)
    {
        assertEquals(weights.size(), values.length, "mismatched weights and values");
        if (values.length == 0) {
            return null;
        }
        TDigest tdigest = new TDigest();
        for (int i = 0; i < weights.size(); i++) {
            tdigest.add(values[i], weights.get(i));
        }
        return new SqlVarbinary(tdigest.serialize().getBytes());
    }

    private static boolean returnSimilarResults(TDigest first, TDigest second, double maxError)
    {
        double[] quantiles = {0.0001, 0.001, 0.01, 0.1, 0.5, 0.567, 0.89, 0.999};
        for (double quantile : quantiles) {
            if (abs(first.valueAt(quantile) - second.valueAt(quantile)) > maxError) {
                return false;
            }
        }
        return true;
    }
}
