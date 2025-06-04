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
import io.airlift.slice.Slice;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestApproximateCountDistinct
{
    protected abstract Type getValueType();

    protected abstract Object randomValue();

    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    protected int getUniqueValuesCount()
    {
        return 20000;
    }

    @Test
    public void testNoPositions()
    {
        assertCount(ImmutableList.of(), 0.0230, 0);
        assertCount(ImmutableList.of(), 0.0115, 0);
    }

    @Test
    public void testSinglePosition()
    {
        assertCount(ImmutableList.of(randomValue()), 0.0230, 1);
        assertCount(ImmutableList.of(randomValue()), 0.0115, 1);
    }

    @Test
    public void testAllPositionsNull()
    {
        assertCount(Collections.nCopies(100, null), 0.0230, 0);
        assertCount(Collections.nCopies(100, null), 0.0115, 0);
    }

    @Test
    public void testMixedNullsAndNonNulls()
    {
        testMixedNullsAndNonNulls(0.0230);
        testMixedNullsAndNonNulls(0.0115);
    }

    private void testMixedNullsAndNonNulls(double maxStandardError)
    {
        int uniques = getUniqueValuesCount();
        List<Object> baseline = createRandomSample(uniques, (int) (uniques * 1.5));

        // Randomly insert nulls
        // We need to retain the preexisting order to ensure that the HLL can generate the same estimates.
        Iterator<Object> iterator = baseline.iterator();
        List<Object> mixed = new ArrayList<>();
        while (iterator.hasNext()) {
            mixed.add(ThreadLocalRandom.current().nextBoolean() ? null : iterator.next());
        }

        assertCount(mixed, maxStandardError, estimateGroupByCount(baseline, maxStandardError));
    }

    @Test
    public void testMultiplePositions()
    {
        testMultiplePositions(0.0230);
        testMultiplePositions(0.0115);
    }

    private void testMultiplePositions(double maxStandardError)
    {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        for (int i = 0; i < 500; ++i) {
            int uniques = ThreadLocalRandom.current().nextInt(getUniqueValuesCount()) + 1;

            List<Object> values = createRandomSample(uniques, (int) (uniques * 1.5));

            long actual = estimateGroupByCount(values, maxStandardError);
            double error = (actual - uniques) * 1.0 / uniques;

            stats.addValue(error);
        }

        assertThat(stats.getMean()).isLessThan(1.0e-2);
        assertThat(stats.getStandardDeviation()).isLessThan(1.0e-2 + maxStandardError);
    }

    @Test
    public void testMultiplePositionsPartial()
    {
        testMultiplePositionsPartial(0.0230);
        testMultiplePositionsPartial(0.0115);
    }

    private void testMultiplePositionsPartial(double maxStandardError)
    {
        for (int i = 0; i < 100; ++i) {
            int uniques = ThreadLocalRandom.current().nextInt(getUniqueValuesCount()) + 1;
            List<Object> values = createRandomSample(uniques, (int) (uniques * 1.5));
            assertThat(estimateCountPartial(values, maxStandardError)).isEqualTo(estimateGroupByCount(values, maxStandardError));
        }
    }

    protected void assertCount(List<?> values, double maxStandardError, long expectedCount)
    {
        if (!values.isEmpty()) {
            assertThat(estimateGroupByCount(values, maxStandardError)).isEqualTo(expectedCount);
        }
        assertThat(estimateCount(values, maxStandardError)).isEqualTo(expectedCount);
        assertThat(estimateCountPartial(values, maxStandardError)).isEqualTo(expectedCount);
    }

    private long estimateGroupByCount(List<?> values, double maxStandardError)
    {
        Object result = AggregationTestUtils.groupedAggregation(getAggregationFunction(), createPage(values, maxStandardError));
        return (long) result;
    }

    private long estimateCount(List<?> values, double maxStandardError)
    {
        Object result = AggregationTestUtils.aggregation(getAggregationFunction(), createPage(values, maxStandardError));
        return (long) result;
    }

    private long estimateCountPartial(List<?> values, double maxStandardError)
    {
        Object result = AggregationTestUtils.partialAggregation(getAggregationFunction(), createPage(values, maxStandardError));
        return (long) result;
    }

    private TestingAggregationFunction getAggregationFunction()
    {
        return FUNCTION_RESOLUTION.getAggregateFunction("approx_distinct", fromTypes(getValueType(), DOUBLE));
    }

    private Page createPage(List<?> values, double maxStandardError)
    {
        if (values.isEmpty()) {
            return new Page(0);
        }
        return new Page(values.size(),
                createBlock(getValueType(), values),
                createBlock(DOUBLE, ImmutableList.copyOf(Collections.nCopies(values.size(), maxStandardError))));
    }

    /**
     * Produce a block with the given values in the last field.
     */
    private static Block createBlock(Type type, List<?> values)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, values.size());

        for (Object value : values) {
            Class<?> javaType = type.getJavaType();
            if (value == null) {
                blockBuilder.appendNull();
            }
            else if (javaType == boolean.class) {
                type.writeBoolean(blockBuilder, (Boolean) value);
            }
            else if (javaType == long.class) {
                type.writeLong(blockBuilder, (Long) value);
            }
            else if (javaType == double.class) {
                type.writeDouble(blockBuilder, (Double) value);
            }
            else if (javaType == Slice.class) {
                Slice slice = (Slice) value;
                type.writeSlice(blockBuilder, slice, 0, slice.length());
            }
            else {
                type.writeObject(blockBuilder, value);
            }
        }

        return blockBuilder.build();
    }

    private List<Object> createRandomSample(int uniques, int total)
    {
        checkArgument(uniques <= total, "uniques (%s) must be <= total (%s)", uniques, total);

        List<Object> result = new ArrayList<>(total);
        result.addAll(makeRandomSet(uniques));

        Random random = ThreadLocalRandom.current();
        while (result.size() < total) {
            int index = random.nextInt(result.size());
            result.add(result.get(index));
        }

        return result;
    }

    private Set<Object> makeRandomSet(int count)
    {
        Set<Object> result = new HashSet<>();
        while (result.size() < count) {
            result.add(randomValue());
        }

        return result;
    }
}
