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
import com.google.common.primitives.Ints;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.aggregation.groupby.AggregationTestInput;
import io.trino.operator.aggregation.groupby.AggregationTestInputBuilder;
import io.trino.operator.aggregation.groupby.AggregationTestOutput;
import io.trino.operator.aggregation.histogram.Histogram;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.tree.QualifiedName;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createStringArraysBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.operator.OperatorAssertion.toRow;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.util.DateTimeZoneIndex.getDateTimeZone;
import static io.trino.util.StructuralTestUtil.mapBlockOf;
import static io.trino.util.StructuralTestUtil.mapType;
import static org.testng.Assert.assertTrue;

public class TestHistogram
{
    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("UTC");
    private static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    @Test
    public void testSimpleHistograms()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of(Histogram.NAME),
                fromTypes(VARCHAR),
                ImmutableMap.of("a", 1L, "b", 1L, "c", 1L),
                createStringsBlock("a", "b", "c"));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of(Histogram.NAME),
                fromTypes(BIGINT),
                ImmutableMap.of(100L, 1L, 200L, 1L, 300L, 1L),
                createLongsBlock(100L, 200L, 300L));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of(Histogram.NAME),
                fromTypes(DOUBLE),
                ImmutableMap.of(0.1, 1L, 0.3, 1L, 0.2, 1L),
                createDoublesBlock(0.1, 0.3, 0.2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of(Histogram.NAME),
                fromTypes(BOOLEAN),
                ImmutableMap.of(true, 1L, false, 1L),
                createBooleansBlock(true, false));
    }

    @Test
    public void testSharedGroupBy()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of(Histogram.NAME),
                fromTypes(VARCHAR),
                ImmutableMap.of("a", 1L, "b", 1L, "c", 1L),
                createStringsBlock("a", "b", "c"));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of(Histogram.NAME),
                fromTypes(BIGINT),
                ImmutableMap.of(100L, 1L, 200L, 1L, 300L, 1L),
                createLongsBlock(100L, 200L, 300L));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of(Histogram.NAME), fromTypes(DOUBLE),
                ImmutableMap.of(0.1, 1L, 0.3, 1L, 0.2, 1L),
                createDoublesBlock(0.1, 0.3, 0.2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of(Histogram.NAME),
                fromTypes(BOOLEAN),
                ImmutableMap.of(true, 1L, false, 1L),
                createBooleansBlock(true, false));
    }

    @Test
    public void testDuplicateKeysValues()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of(Histogram.NAME),
                fromTypes(VARCHAR),
                ImmutableMap.of("a", 2L, "b", 1L),
                createStringsBlock("a", "b", "a"));

        long timestampWithTimeZone1 = packDateTimeWithZone(new DateTime(1970, 1, 1, 0, 0, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY);
        long timestampWithTimeZone2 = packDateTimeWithZone(new DateTime(2015, 1, 1, 0, 0, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of(Histogram.NAME),
                fromTypes(TIMESTAMP_WITH_TIME_ZONE),
                ImmutableMap.of(SqlTimestampWithTimeZone.newInstance(3, unpackMillisUtc(timestampWithTimeZone1), 0, unpackZoneKey(timestampWithTimeZone1)), 2L, SqlTimestampWithTimeZone.newInstance(3, unpackMillisUtc(timestampWithTimeZone2), 0, unpackZoneKey(timestampWithTimeZone2)), 1L),
                createLongsBlock(timestampWithTimeZone1, timestampWithTimeZone1, timestampWithTimeZone2));
    }

    @Test
    public void testWithNulls()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of(Histogram.NAME),
                fromTypes(BIGINT),
                ImmutableMap.of(1L, 1L, 2L, 1L),
                createLongsBlock(2L, null, 1L));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of(Histogram.NAME),
                fromTypes(BIGINT),
                null,
                createLongsBlock((Long) null));
    }

    @Test
    public void testArrayHistograms()
    {
        ArrayType arrayType = new ArrayType(VARCHAR);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of(Histogram.NAME),
                fromTypes(arrayType),
                ImmutableMap.of(ImmutableList.of("a", "b", "c"), 1L, ImmutableList.of("d", "e", "f"), 1L, ImmutableList.of("c", "b", "a"), 1L),
                createStringArraysBlock(ImmutableList.of(ImmutableList.of("a", "b", "c"), ImmutableList.of("d", "e", "f"), ImmutableList.of("c", "b", "a"))));
    }

    @Test
    public void testMapHistograms()
    {
        MapType innerMapType = mapType(VARCHAR, VARCHAR);

        BlockBuilder builder = innerMapType.createBlockBuilder(null, 3);
        innerMapType.writeObject(builder, mapBlockOf(VARCHAR, VARCHAR, ImmutableMap.of("a", "b")));
        innerMapType.writeObject(builder, mapBlockOf(VARCHAR, VARCHAR, ImmutableMap.of("c", "d")));
        innerMapType.writeObject(builder, mapBlockOf(VARCHAR, VARCHAR, ImmutableMap.of("e", "f")));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of(Histogram.NAME),
                fromTypes(innerMapType),
                ImmutableMap.of(ImmutableMap.of("a", "b"), 1L, ImmutableMap.of("c", "d"), 1L, ImmutableMap.of("e", "f"), 1L),
                builder.build());
    }

    @Test
    public void testRowHistograms()
    {
        RowType innerRowType = RowType.from(ImmutableList.of(
                RowType.field("f1", BIGINT),
                RowType.field("f2", DOUBLE)));
        BlockBuilder builder = innerRowType.createBlockBuilder(null, 3);
        innerRowType.writeObject(builder, toRow(ImmutableList.of(BIGINT, DOUBLE), 1L, 1.0));
        innerRowType.writeObject(builder, toRow(ImmutableList.of(BIGINT, DOUBLE), 2L, 2.0));
        innerRowType.writeObject(builder, toRow(ImmutableList.of(BIGINT, DOUBLE), 3L, 3.0));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of(Histogram.NAME),
                fromTypes(innerRowType),
                ImmutableMap.of(ImmutableList.of(1L, 1.0), 1L, ImmutableList.of(2L, 2.0), 1L, ImmutableList.of(3L, 3.0), 1L),
                builder.build());
    }

    @Test
    public void testLargerHistograms()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of(Histogram.NAME),
                fromTypes(VARCHAR),
                ImmutableMap.of("a", 25L, "b", 10L, "c", 12L, "d", 1L, "e", 2L),
                createStringsBlock("a", "b", "c", "d", "e", "e", "c", "a", "a", "a", "b", "a", "a", "a", "a", "b", "a", "a", "a", "a", "b", "a", "a", "a", "a", "b", "a", "a", "a", "a", "b", "a", "c", "c", "b", "a", "c", "c", "b", "a", "c", "c", "b", "a", "c", "c", "b", "a", "c", "c"));
    }

    @Test
    public void testEmptyHistogramOutputsNull()
    {
        TestingAggregationFunction function = getInternalDefaultVarCharAggregation();
        GroupedAggregator groupedAggregator = function.createAggregatorFactory(SINGLE, Ints.asList(new int[] {}), OptionalInt.empty())
                .createGroupedAggregator();
        BlockBuilder blockBuilder = function.getFinalType().createBlockBuilder(null, 1000);

        groupedAggregator.evaluate(0, blockBuilder);
        assertTrue(blockBuilder.isNull(0));
    }

    @Test
    public void testSharedGroupByWithOverlappingValuesRunner()
    {
        TestingAggregationFunction classicFunction = getInternalDefaultVarCharAggregation();
        TestingAggregationFunction singleInstanceFunction = getInternalDefaultVarCharAggregation();

        testSharedGroupByWithOverlappingValuesRunner(classicFunction);
        testSharedGroupByWithOverlappingValuesRunner(singleInstanceFunction);
    }

    @Test
    public void testSharedGroupByWithDistinctValuesPerGroup()
    {
        // test that two groups don't affect one another
        TestingAggregationFunction classicFunction = getInternalDefaultVarCharAggregation();
        TestingAggregationFunction singleInstanceFunction = getInternalDefaultVarCharAggregation();
        testSharedGroupByWithDistinctValuesPerGroupRunner(classicFunction);
        testSharedGroupByWithDistinctValuesPerGroupRunner(singleInstanceFunction);
    }

    @Test
    public void testSharedGroupByWithOverlappingValuesPerGroup()
    {
        // test that two groups don't affect one another
        TestingAggregationFunction classicFunction = getInternalDefaultVarCharAggregation();
        TestingAggregationFunction singleInstanceFunction = getInternalDefaultVarCharAggregation();
        testSharedGroupByWithOverlappingValuesPerGroupRunner(classicFunction);
        testSharedGroupByWithOverlappingValuesPerGroupRunner(singleInstanceFunction);
    }

    @Test
    public void testSharedGroupByWithManyGroups()
    {
        // uses a large enough data set to induce rehashing and test correctness
        TestingAggregationFunction classicFunction = getInternalDefaultVarCharAggregation();
        TestingAggregationFunction singleInstanceFunction = getInternalDefaultVarCharAggregation();

        // this is to validate the test as there have been test-bugs that looked like code bugs--if both fail, likely a test bug
        testManyValuesInducingRehash(classicFunction);
        testManyValuesInducingRehash(singleInstanceFunction);
    }

    private static void testManyValuesInducingRehash(TestingAggregationFunction aggregationFunction)
    {
        double distinctFraction = 0.1f;
        int numGroups = 50000;
        int itemCount = 30;
        Random random = new Random();
        GroupedAggregator groupedAggregator = aggregationFunction.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty())
                .createGroupedAggregator();

        for (int j = 0; j < numGroups; j++) {
            Map<String, Long> expectedValues = new HashMap<>();
            List<String> valueList = new ArrayList<>();

            for (int i = 0; i < itemCount; i++) {
                String str = String.valueOf(i % 10);
                String item = IntStream.range(0, itemCount).mapToObj(x -> str).collect(Collectors.joining());
                boolean distinctValue = random.nextDouble() < distinctFraction;
                if (distinctValue) {
                    // produce a unique value for the histogram
                    item = j + "-" + item;
                    valueList.add(item);
                }
                else {
                    valueList.add(item);
                }
                expectedValues.compute(item, (k, v) -> v == null ? 1L : ++v);
            }

            Block block = createStringsBlock(valueList);
            AggregationTestInputBuilder testInputBuilder = new AggregationTestInputBuilder(
                    new Block[] {block},
                    aggregationFunction);
            AggregationTestInput test1 = testInputBuilder.build();

            test1.runPagesOnAggregatorWithAssertion(j, aggregationFunction.getFinalType(), groupedAggregator, new AggregationTestOutput(expectedValues));
        }
    }

    private static void testSharedGroupByWithOverlappingValuesPerGroupRunner(TestingAggregationFunction aggregationFunction)
    {
        Block block1 = createStringsBlock("a", "b", "c");
        Block block2 = createStringsBlock("b", "c", "d");
        AggregationTestOutput aggregationTestOutput1 = new AggregationTestOutput(ImmutableMap.of("a", 1L, "b", 1L, "c", 1L));
        AggregationTestInputBuilder testBuilder1 = new AggregationTestInputBuilder(
                new Block[] {block1},
                aggregationFunction);
        AggregationTestInput test1 = testBuilder1.build();
        GroupedAggregator groupedAggregator = test1.createGroupedAggregator();

        test1.runPagesOnAggregatorWithAssertion(0L, aggregationFunction.getFinalType(), groupedAggregator, aggregationTestOutput1);

        AggregationTestOutput aggregationTestOutput2 = new AggregationTestOutput(ImmutableMap.of("b", 1L, "c", 1L, "d", 1L));
        AggregationTestInputBuilder testbuilder2 = new AggregationTestInputBuilder(
                new Block[] {block2},
                aggregationFunction);
        AggregationTestInput test2 = testbuilder2.build();
        test2.runPagesOnAggregatorWithAssertion(255L, aggregationFunction.getFinalType(), groupedAggregator, aggregationTestOutput2);
    }

    private static void testSharedGroupByWithDistinctValuesPerGroupRunner(TestingAggregationFunction aggregationFunction)
    {
        Block block1 = createStringsBlock("a", "b", "c");
        Block block2 = createStringsBlock("d", "e", "f");
        AggregationTestOutput aggregationTestOutput1 = new AggregationTestOutput(ImmutableMap.of("a", 1L, "b", 1L, "c", 1L));
        AggregationTestInputBuilder testInputBuilder1 = new AggregationTestInputBuilder(
                new Block[] {block1},
                aggregationFunction);
        AggregationTestInput test1 = testInputBuilder1.build();
        GroupedAggregator groupedAggregator = test1.createGroupedAggregator();

        test1.runPagesOnAggregatorWithAssertion(0L, aggregationFunction.getFinalType(), groupedAggregator, aggregationTestOutput1);

        AggregationTestOutput aggregationTestOutput2 = new AggregationTestOutput(ImmutableMap.of("d", 1L, "e", 1L, "f", 1L));
        AggregationTestInputBuilder testBuilder2 = new AggregationTestInputBuilder(
                new Block[] {block2},
                aggregationFunction);
        AggregationTestInput test2 = testBuilder2.build();
        test2.runPagesOnAggregatorWithAssertion(255L, aggregationFunction.getFinalType(), groupedAggregator, aggregationTestOutput2);
    }

    private static void testSharedGroupByWithOverlappingValuesRunner(TestingAggregationFunction aggregationFunction)
    {
        Block block1 = createStringsBlock("a", "b", "c", "d", "a1", "b2", "c3", "d4", "a", "b2", "c", "d4", "a3", "b3", "c3", "b2");
        AggregationTestInputBuilder testInputBuilder1 = new AggregationTestInputBuilder(
                new Block[] {block1},
                aggregationFunction);
        AggregationTestOutput aggregationTestOutput1 = new AggregationTestOutput(ImmutableMap.<String, Long>builder()
                .put("a", 2L)
                .put("b", 1L)
                .put("c", 2L)
                .put("d", 1L)
                .put("a1", 1L)
                .put("b2", 3L)
                .put("c3", 2L)
                .put("d4", 2L)
                .put("a3", 1L)
                .put("b3", 1L)
                .buildOrThrow());
        AggregationTestInput test1 = testInputBuilder1.build();

        test1.runPagesOnAggregatorWithAssertion(0L, aggregationFunction.getFinalType(), test1.createGroupedAggregator(), aggregationTestOutput1);
    }

    private static TestingAggregationFunction getInternalDefaultVarCharAggregation()
    {
        return FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of(Histogram.NAME), fromTypes(VARCHAR));
    }
}
