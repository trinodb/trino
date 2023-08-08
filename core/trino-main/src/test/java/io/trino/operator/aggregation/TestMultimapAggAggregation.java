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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.primitives.Ints;
import io.trino.RowPageBuilder;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.aggregation.groupby.AggregationTestInput;
import io.trino.operator.aggregation.groupby.AggregationTestInputBuilder;
import io.trino.operator.aggregation.groupby.AggregationTestOutput;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Random;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.util.StructuralTestUtil.mapType;
import static org.testng.Assert.assertTrue;

public class TestMultimapAggAggregation
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    @Test
    public void testSingleValueMap()
    {
        testMultimapAgg(DOUBLE, ImmutableList.of(1.0), VARCHAR, ImmutableList.of("a"));
        testMultimapAgg(VARCHAR, ImmutableList.of("a"), BIGINT, ImmutableList.of(1L));
    }

    @Test
    public void testMultiValueMap()
    {
        testMultimapAgg(DOUBLE, ImmutableList.of(1.0, 1.0, 1.0), VARCHAR, ImmutableList.of("a", "b", "c"));
        testMultimapAgg(DOUBLE, ImmutableList.of(1.0, 1.0, 2.0), VARCHAR, ImmutableList.of("a", "b", "c"));
    }

    @Test
    public void testOrderValueMap()
    {
        testMultimapAgg(VARCHAR, ImmutableList.of("a", "a", "a"), BIGINT, ImmutableList.of(1L, 2L, 3L));
        testMultimapAgg(VARCHAR, ImmutableList.of("a", "a", "a"), BIGINT, ImmutableList.of(2L, 1L, 3L));
        testMultimapAgg(VARCHAR, ImmutableList.of("a", "a", "a"), BIGINT, ImmutableList.of(3L, 2L, 1L));
    }

    @Test
    public void testDuplicateValueMap()
    {
        testMultimapAgg(VARCHAR, ImmutableList.of("a", "a", "a"), BIGINT, ImmutableList.of(1L, 1L, 1L));
        testMultimapAgg(VARCHAR, ImmutableList.of("a", "b", "a", "b", "c"), BIGINT, ImmutableList.of(1L, 1L, 1L, 1L, 1L));
    }

    @Test
    public void testNullMap()
    {
        testMultimapAgg(DOUBLE, ImmutableList.<Double>of(), VARCHAR, ImmutableList.<String>of());
    }

    @Test
    public void testKeysUseIsDistinctSemantics()
    {
        testMultimapAgg(DOUBLE, ImmutableList.of(Double.NaN, Double.NaN), BIGINT, ImmutableList.of(1L, 1L));
        testMultimapAgg(DOUBLE, ImmutableList.of(Double.NaN, Double.NaN, Double.NaN), BIGINT, ImmutableList.of(2L, 1L, 2L));
    }

    @Test
    public void testDoubleMapMultimap()
    {
        Type mapType = mapType(VARCHAR, BIGINT);
        List<Double> expectedKeys = ImmutableList.of(1.0, 2.0, 3.0);
        List<Map<String, Long>> expectedValues = ImmutableList.of(ImmutableMap.of("a", 1L), ImmutableMap.of("b", 2L, "c", 3L, "d", 4L), ImmutableMap.of("a", 1L));

        testMultimapAgg(DOUBLE, expectedKeys, mapType, expectedValues);
    }

    @Test
    public void testDoubleArrayMultimap()
    {
        Type arrayType = new ArrayType(VARCHAR);
        List<Double> expectedKeys = ImmutableList.of(1.0, 2.0, 3.0);
        List<List<String>> expectedValues = ImmutableList.of(ImmutableList.of("a", "b"), ImmutableList.of("c"), ImmutableList.of("d", "e", "f"));

        testMultimapAgg(DOUBLE, expectedKeys, arrayType, expectedValues);
    }

    @Test
    public void testDoubleRowMap()
    {
        RowType innerRowType = RowType.from(ImmutableList.of(
                RowType.field("f1", BIGINT),
                RowType.field("f2", DOUBLE)));
        testMultimapAgg(DOUBLE, ImmutableList.of(1.0, 2.0, 3.0), innerRowType, ImmutableList.of(ImmutableList.of(1L, 1.0), ImmutableList.of(2L, 2.0), ImmutableList.of(3L, 3.0)));
    }

    @Test
    public void testMultiplePages()
    {
        TestingAggregationFunction aggFunction = getAggregationFunction(BIGINT, BIGINT);
        GroupedAggregator groupedAggregator = aggFunction.createAggregatorFactory(SINGLE, ImmutableList.of(0, 1), OptionalInt.empty()).createGroupedAggregator();

        testMultimapAggWithGroupBy(aggFunction, groupedAggregator, 0, BIGINT, ImmutableList.of(1L, 1L), BIGINT, ImmutableList.of(2L, 3L));
    }

    @Test
    public void testMultiplePagesAndGroups()
    {
        TestingAggregationFunction aggFunction = getAggregationFunction(BIGINT, BIGINT);
        GroupedAggregator groupedAggregator = aggFunction.createAggregatorFactory(SINGLE, ImmutableList.of(0, 1), OptionalInt.empty()).createGroupedAggregator();

        testMultimapAggWithGroupBy(aggFunction, groupedAggregator, 0, BIGINT, ImmutableList.of(1L, 1L), BIGINT, ImmutableList.of(2L, 3L));
        testMultimapAggWithGroupBy(aggFunction, groupedAggregator, 300, BIGINT, ImmutableList.of(7L, 7L), BIGINT, ImmutableList.of(8L, 9L));
    }

    @Test
    public void testManyValues()
    {
        TestingAggregationFunction aggFunction = getAggregationFunction(BIGINT, BIGINT);
        GroupedAggregator groupedAggregator = aggFunction.createAggregatorFactory(SINGLE, ImmutableList.of(0, 1), OptionalInt.empty()).createGroupedAggregator();

        int numGroups = 30000;
        int numKeys = 10;
        int numValueArraySize = 2;
        Random random = new Random();

        for (int group = 0; group < numGroups; group++) {
            ImmutableList.Builder<Long> keyBuilder = ImmutableList.builder();
            ImmutableList.Builder<Long> valueBuilder = ImmutableList.builder();
            for (int i = 0; i < numKeys; i++) {
                long key = random.nextLong();
                for (int j = 0; j < numValueArraySize; j++) {
                    long value = random.nextLong();
                    keyBuilder.add(key);
                    valueBuilder.add(value);
                }
            }
            testMultimapAggWithGroupBy(aggFunction, groupedAggregator, group, BIGINT, keyBuilder.build(), BIGINT, valueBuilder.build());
        }
    }

    @Test
    public void testEmptyStateOutputIsNull()
    {
        TestingAggregationFunction aggregationFunction = getAggregationFunction(BIGINT, BIGINT);
        GroupedAggregator groupedAggregator = aggregationFunction.createAggregatorFactory(SINGLE, Ints.asList(), OptionalInt.empty()).createGroupedAggregator();
        BlockBuilder blockBuilder = aggregationFunction.getFinalType().createBlockBuilder(null, 1);
        groupedAggregator.evaluate(0, blockBuilder);
        assertTrue(blockBuilder.isNull(0));
    }

    private static TestingAggregationFunction getAggregationFunction(Type keyType, Type valueType)
    {
        return FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("multimap_agg"), fromTypes(keyType, valueType));
    }

    /**
     * Given a list of keys and a list of corresponding values, manually
     * aggregate them into a map of list and check that Trino's aggregation has
     * the same results.
     */
    private static <K, V> void testMultimapAgg(Type keyType, List<K> expectedKeys, Type valueType, List<V> expectedValues)
    {
        checkState(expectedKeys.size() == expectedValues.size(), "expectedKeys and expectedValues should have equal size");
        Map<K, List<V>> map = new HashMap<>();
        for (int i = 0; i < expectedKeys.size(); i++) {
            if (!map.containsKey(expectedKeys.get(i))) {
                map.put(expectedKeys.get(i), new ArrayList<>());
            }
            map.get(expectedKeys.get(i)).add(expectedValues.get(i));
        }

        RowPageBuilder builder = RowPageBuilder.rowPageBuilder(keyType, valueType);
        for (int i = 0; i < expectedKeys.size(); i++) {
            builder.row(expectedKeys.get(i), expectedValues.get(i));
        }

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("multimap_agg"),
                fromTypes(keyType, valueType),
                map.isEmpty() ? null : map,
                builder.build());
    }

    private static <K, V> void testMultimapAggWithGroupBy(
            TestingAggregationFunction aggregationFunction,
            GroupedAggregator groupedAggregator,
            int groupId,
            Type keyType,
            List<K> expectedKeys,
            Type valueType,
            List<V> expectedValues)
    {
        RowPageBuilder pageBuilder = RowPageBuilder.rowPageBuilder(keyType, valueType);
        ImmutableMultimap.Builder<K, V> outputBuilder = ImmutableMultimap.builder();
        for (int i = 0; i < expectedValues.size(); i++) {
            pageBuilder.row(expectedKeys.get(i), expectedValues.get(i));
            outputBuilder.put(expectedKeys.get(i), expectedValues.get(i));
        }
        Page page = pageBuilder.build();

        AggregationTestInput input = new AggregationTestInputBuilder(
                new Block[] {page.getBlock(0), page.getBlock(1)},
                aggregationFunction).build();

        AggregationTestOutput testOutput = new AggregationTestOutput(outputBuilder.build().asMap());
        input.runPagesOnAggregatorWithAssertion(groupId, aggregationFunction.getFinalType(), groupedAggregator, testOutput);
    }
}
