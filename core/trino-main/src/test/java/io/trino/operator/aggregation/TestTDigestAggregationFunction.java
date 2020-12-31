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
package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.airlift.stats.TDigest;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.SqlVarbinary;
import io.prestosql.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.block.BlockAssertions.createDoubleSequenceBlock;
import static io.prestosql.block.BlockAssertions.createDoublesBlock;
import static io.prestosql.block.BlockAssertions.createRLEBlock;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.prestosql.operator.scalar.TDigestFunctions.DEFAULT_WEIGHT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.lang.Math.abs;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestTDigestAggregationFunction
        extends AbstractTestFunctions
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

    private static final Metadata METADATA = createTestMetadataManager();

    private final InternalAggregationFunction tdigestAggregation = METADATA.getAggregateFunctionImplementation(METADATA.resolveFunction(QualifiedName.of("tdigest_agg"), fromTypes(DOUBLE)));
    private final InternalAggregationFunction tdigestWeigthedAggregation = METADATA.getAggregateFunctionImplementation(METADATA.resolveFunction(QualifiedName.of("tdigest_agg"), fromTypes(DOUBLE, DOUBLE)));

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
                createRLEBlock(1.0, 5),
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
                createRLEBlock(1.0, 0),
                ImmutableList.of());
        testAggregation(
                createDoublesBlock(1.0),
                createRLEBlock(1.1, 1),
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
        testAggregation(
                tdigestAggregation,
                new Page(doublesBlock),
                nCopies(inputs.length, DEFAULT_WEIGHT),
                inputs);
        // Test with weights
        testAggregation(
                tdigestWeigthedAggregation,
                new Page(doublesBlock, weightsBlock),
                weights,
                inputs);
    }

    private void testAggregation(InternalAggregationFunction function, Page page, List<Double> weights, double... inputs)
    {
        assertAggregation(function, getExpectedValue(weights, inputs), page);
    }

    private void testAggregation(BiFunction<Object, Object, Boolean> equalAssertion, Block doublesBlock, Block weightsBlock, List<Double> weights, double... inputs)
    {
        // Test without weights
        testAggregation(
                equalAssertion,
                tdigestAggregation,
                new Page(doublesBlock),
                nCopies(inputs.length, DEFAULT_WEIGHT),
                inputs);
        // Test with weights
        testAggregation(
                equalAssertion,
                tdigestWeigthedAggregation,
                new Page(doublesBlock, weightsBlock),
                weights,
                inputs);
    }

    private void testAggregation(BiFunction<Object, Object, Boolean> equalAssertion, InternalAggregationFunction function, Page page, List<Double> weights, double... inputs)
    {
        assertAggregation(function, equalAssertion, "Test multiple values", page, getExpectedValue(weights, inputs));
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
