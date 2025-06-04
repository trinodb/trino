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
import io.airlift.slice.Slices;
import io.airlift.stats.cardinality.HyperLogLog;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.SqlVarbinary;
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
import static com.google.common.io.BaseEncoding.base16;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.util.Collections.shuffle;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestApproximateSetGeneric
{
    private static final double STD_ERROR = 0.023;

    protected abstract Type getValueType();

    protected abstract Object randomValue();

    protected static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    protected int getUniqueValuesCount()
    {
        return 20000;
    }

    @Test
    public void testNoPositions()
    {
        assertThat(estimateSet(ImmutableList.of())).isNull();
        assertThat(estimateSetPartial(ImmutableList.of())).isNull();
    }

    @Test
    public void testSinglePosition()
    {
        assertCount(ImmutableList.of(randomValue()), 1);
    }

    @Test
    public void testAllPositionsNull()
    {
        List<Object> justNulls = Collections.nCopies(100, null);
        assertThat(estimateSet(justNulls)).isNull();
        assertThat(estimateSetPartial(justNulls)).isNull();
        assertThat(estimateSetGrouped(justNulls)).isNull();
    }

    @Test
    public void testMixedNullsAndNonNulls()
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

        assertCount(mixed, estimateSetGrouped(baseline).cardinality());
    }

    @Test
    public void testMultiplePositions()
    {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        for (int i = 0; i < 500; ++i) {
            int uniques = ThreadLocalRandom.current().nextInt(getUniqueValuesCount()) + 1;

            List<Object> values = createRandomSample(uniques, (int) (uniques * 1.5));
            long actualCount = estimateSetGrouped(values).cardinality();
            double error = (actualCount - uniques) * 1.0 / uniques;

            stats.addValue(error);
        }

        assertThat(stats.getMean()).isLessThan(1.0e-2);
        assertThat(stats.getStandardDeviation()).isLessThan(1.0e-2 + STD_ERROR);
    }

    @Test
    public void testMultiplePositionsPartial()
    {
        for (int i = 0; i < 100; ++i) {
            int uniques = ThreadLocalRandom.current().nextInt(getUniqueValuesCount()) + 1;
            List<Object> values = createRandomSample(uniques, (int) (uniques * 1.5));
            assertThat(estimateSetPartial(values).cardinality()).isEqualTo(estimateSetGrouped(values).cardinality());
        }
    }

    @Test
    public void testResultStability()
    {
        for (int i = 0; i < 10; ++i) {
            List<Object> sample = new ArrayList<>(getResultStabilityTestSample());
            shuffle(sample);
            assertThat(base16().encode(estimateSet(sample).serialize().getBytes())).isEqualTo(getResultStabilityExpected());
            assertThat(base16().encode(estimateSetPartial(sample).serialize().getBytes())).isEqualTo(getResultStabilityExpected());
            assertThat(base16().encode(estimateSetGrouped(sample).serialize().getBytes())).isEqualTo(getResultStabilityExpected());
        }
    }

    protected abstract List<Object> getResultStabilityTestSample();

    protected abstract String getResultStabilityExpected();

    protected void assertCount(List<?> values, long expectedCount)
    {
        if (!values.isEmpty()) {
            HyperLogLog actualSet = estimateSetGrouped(values);
            assertThat(actualSet.cardinality()).isEqualTo(expectedCount);
        }
        assertThat(estimateSet(values).cardinality()).isEqualTo(expectedCount);
        assertThat(estimateSetPartial(values).cardinality()).isEqualTo(expectedCount);
    }

    private HyperLogLog estimateSetGrouped(List<?> values)
    {
        SqlVarbinary hllSerialized = (SqlVarbinary) AggregationTestUtils.groupedAggregation(getAggregationFunction(), createPage(values));
        if (hllSerialized == null) {
            return null;
        }
        return HyperLogLog.newInstance(Slices.wrappedBuffer(hllSerialized.getBytes()));
    }

    private HyperLogLog estimateSet(List<?> values)
    {
        SqlVarbinary hllSerialized = (SqlVarbinary) AggregationTestUtils.aggregation(getAggregationFunction(), createPage(values));
        if (hllSerialized == null) {
            return null;
        }
        return HyperLogLog.newInstance(Slices.wrappedBuffer(hllSerialized.getBytes()));
    }

    private HyperLogLog estimateSetPartial(List<?> values)
    {
        SqlVarbinary hllSerialized = (SqlVarbinary) AggregationTestUtils.partialAggregation(getAggregationFunction(), createPage(values));
        if (hllSerialized == null) {
            return null;
        }
        return HyperLogLog.newInstance(Slices.wrappedBuffer(hllSerialized.getBytes()));
    }

    private TestingAggregationFunction getAggregationFunction()
    {
        return FUNCTION_RESOLUTION.getAggregateFunction("$approx_set", fromTypes(getValueType()));
    }

    private Page createPage(List<?> values)
    {
        if (values.isEmpty()) {
            return new Page(0);
        }
        return new Page(values.size(), createBlock(getValueType(), values));
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
