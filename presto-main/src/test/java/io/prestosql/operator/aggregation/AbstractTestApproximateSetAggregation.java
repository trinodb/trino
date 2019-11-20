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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.stats.cardinality.HyperLogLog;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.SqlVarbinary;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.QualifiedName;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static io.airlift.testing.Assertions.assertLessThan;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestApproximateSetAggregation
{
    protected abstract Type getValueType();

    protected abstract Object randomValue();

    protected static final Metadata metadata = createTestMetadataManager();

    protected int getUniqueValuesCount()
    {
        return 20000;
    }

    @Test
    public void testNoPositions()
    {
        assertCount(ImmutableList.of(), 0);
    }

    @Test
    public void testSinglePosition()
    {
        assertCount(ImmutableList.of(randomValue()), 1);
    }

    @Test
    public void testAllPositionsNull()
    {
        assertCount(Collections.nCopies(100, null), 0);
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

        assertCount(mixed, estimateGroupByCount(baseline));
    }

    @Test
    public void testMultiplePositions()
    {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        for (int i = 0; i < 500; ++i) {
            int uniques = ThreadLocalRandom.current().nextInt(getUniqueValuesCount()) + 1;

            List<Object> values = createRandomSample(uniques, (int) (uniques * 1.5));

            long actual = estimateGroupByCount(values);
            double error = (actual - uniques) * 1.0 / uniques;

            stats.addValue(error);
        }

        assertLessThan(stats.getMean(), 1.5e-2);
        assertLessThan(stats.getStandardDeviation(), 1.5e-2);
    }

    @Test
    public void testMultiplePositionsPartial()
    {
        for (int i = 0; i < 100; ++i) {
            int uniques = ThreadLocalRandom.current().nextInt(getUniqueValuesCount()) + 1;
            List<Object> values = createRandomSample(uniques, (int) (uniques * 1.5));
            assertEquals(estimateCountPartial(values), estimateGroupByCount(values));
        }
    }

    protected void assertCount(List<?> values, long expectedCount)
    {
        if (!values.isEmpty()) {
            assertEquals(estimateGroupByCount(values), expectedCount);
        }
        assertEquals(estimateCount(values), expectedCount);
        assertEquals(estimateCountPartial(values), expectedCount);
    }

    private long estimateGroupByCount(List<?> values)
    {
        Object result = AggregationTestUtils.groupedAggregation(getAggregationFunction(), createPage(values));
        return convertHllToCardinality(result);
    }

    private long estimateCount(List<?> values)
    {
        Object result = AggregationTestUtils.aggregation(getAggregationFunction(), createPage(values));
        return convertHllToCardinality(result);
    }

    private long estimateCountPartial(List<?> values)
    {
        Object result = AggregationTestUtils.partialAggregation(getAggregationFunction(), createPage(values));
        return convertHllToCardinality(result);
    }

    private InternalAggregationFunction getAggregationFunction()
    {
        return metadata.getAggregateFunctionImplementation(
                metadata.resolveFunction(QualifiedName.of("approx_set"), fromTypes(getValueType())));
    }

    private long convertHllToCardinality(Object result)
    {
        if (result == null) {
            return 0;
        }

        return HyperLogLog.newInstance(Slices.wrappedBuffer(((SqlVarbinary) result).getBytes())).cardinality();
    }

    private Page createPage(List<?> values)
    {
        if (values.isEmpty()) {
            return new Page(0);
        }
        else {
            return new Page(values.size(), createBlock(getValueType(), values));
        }
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
                throw new UnsupportedOperationException("not yet implemented: " + javaType);
            }
        }

        return blockBuilder.build();
    }

    private List<Object> createRandomSample(int uniques, int total)
    {
        Preconditions.checkArgument(uniques <= total, "uniques (%s) must be <= total (%s)", uniques, total);

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
