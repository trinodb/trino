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
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.aggregation.groupby.AggregationTestInput;
import io.trino.operator.aggregation.groupby.AggregationTestInputBuilder;
import io.trino.operator.aggregation.groupby.AggregationTestOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.SqlDate;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.Random;

import static io.trino.block.BlockAssertions.createArrayBigintBlock;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.block.BlockAssertions.createTypedLongsBlock;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static org.testng.Assert.assertTrue;

public class TestArrayAggregation
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    @Test
    public void testEmpty()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("array_agg"),
                fromTypes(BIGINT),
                null,
                createLongsBlock(new Long[] {}));
    }

    @Test
    public void testNullOnly()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("array_agg"),
                fromTypes(BIGINT),
                Arrays.asList(null, null, null),
                createLongsBlock(new Long[] {null, null, null}));
    }

    @Test
    public void testNullPartial()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("array_agg"),
                fromTypes(BIGINT),
                Arrays.asList(null, 2L, null, 3L, null),
                createLongsBlock(new Long[] {null, 2L, null, 3L, null}));
    }

    @Test
    public void testBoolean()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("array_agg"),
                fromTypes(BOOLEAN),
                Arrays.asList(true, false),
                createBooleansBlock(new Boolean[] {true, false}));
    }

    @Test
    public void testBigInt()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("array_agg"),
                fromTypes(BIGINT),
                Arrays.asList(2L, 1L, 2L),
                createLongsBlock(new Long[] {2L, 1L, 2L}));
    }

    @Test
    public void testVarchar()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("array_agg"),
                fromTypes(VARCHAR),
                Arrays.asList("hello", "world"),
                createStringsBlock(new String[] {"hello", "world"}));
    }

    @Test
    public void testDate()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("array_agg"),
                fromTypes(DATE),
                Arrays.asList(new SqlDate(1), new SqlDate(2), new SqlDate(4)),
                createTypedLongsBlock(DATE, ImmutableList.of(1L, 2L, 4L)));
    }

    @Test
    public void testArray()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("array_agg"),
                fromTypes(new ArrayType(BIGINT)),
                Arrays.asList(Arrays.asList(1L), Arrays.asList(1L, 2L), Arrays.asList(1L, 2L, 3L)),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(1L, 2L), ImmutableList.of(1L, 2L, 3L))));
    }

    @Test
    public void testEmptyStateOutputsNull()
    {
        TestingAggregationFunction bigIntAgg = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("array_agg"), fromTypes(BIGINT));
        GroupedAggregator groupedAggregator = bigIntAgg.createAggregatorFactory(SINGLE, ImmutableList.of(), OptionalInt.empty())
                .createGroupedAggregator();
        BlockBuilder blockBuilder = bigIntAgg.getFinalType().createBlockBuilder(null, 1000);

        groupedAggregator.evaluate(0, blockBuilder);
        assertTrue(blockBuilder.isNull(0));
    }

    @Test
    public void testWithMultiplePages()
    {
        TestingAggregationFunction varcharAgg = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("array_agg"), fromTypes(VARCHAR));

        AggregationTestInputBuilder testInputBuilder = new AggregationTestInputBuilder(
                new Block[] {
                        createStringsBlock("hello", "world", "hello2", "world2", "hello3", "world3", "goodbye")},
                varcharAgg);
        AggregationTestOutput testOutput = new AggregationTestOutput(ImmutableList.of("hello", "world", "hello2", "world2", "hello3", "world3", "goodbye"));
        AggregationTestInput testInput = testInputBuilder.build();

        testInput.runPagesOnAggregatorWithAssertion(0, varcharAgg.getFinalType(), testInput.createGroupedAggregator(), testOutput);
    }

    @Test
    public void testMultipleGroupsWithMultiplePages()
    {
        TestingAggregationFunction varcharAgg = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("array_agg"), fromTypes(VARCHAR));

        Block block1 = createStringsBlock("a", "b", "c", "d", "e");
        Block block2 = createStringsBlock("f", "g", "h", "i", "j");
        AggregationTestOutput aggregationTestOutput1 = new AggregationTestOutput(ImmutableList.of("a", "b", "c", "d", "e"));
        AggregationTestInputBuilder testInputBuilder1 = new AggregationTestInputBuilder(
                new Block[] {block1},
                varcharAgg);
        AggregationTestInput test1 = testInputBuilder1.build();
        GroupedAggregator groupedAggregator = test1.createGroupedAggregator();

        test1.runPagesOnAggregatorWithAssertion(0, varcharAgg.getFinalType(), groupedAggregator, aggregationTestOutput1);

        AggregationTestOutput aggregationTestOutput2 = new AggregationTestOutput(ImmutableList.of("f", "g", "h", "i", "j"));
        AggregationTestInputBuilder testBuilder2 = new AggregationTestInputBuilder(
                new Block[] {block2},
                varcharAgg);
        AggregationTestInput test2 = testBuilder2.build();
        test2.runPagesOnAggregatorWithAssertion(255, varcharAgg.getFinalType(), groupedAggregator, aggregationTestOutput2);
    }

    @Test
    public void testManyValues()
    {
        // Test many values so multiple BlockBuilders will be used to store group state.
        TestingAggregationFunction varcharAgg = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("array_agg"), fromTypes(VARCHAR));

        int numGroups = 50000;
        int arraySize = 30;
        Random random = new Random();
        GroupedAggregator groupedAggregator = varcharAgg.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty())
                .createGroupedAggregator();

        for (int j = 0; j < numGroups; j++) {
            List<String> expectedValues = new ArrayList<>();
            List<String> valueList = new ArrayList<>();

            for (int i = 0; i < arraySize; i++) {
                String str = String.valueOf(random.nextInt());
                valueList.add(str);
                expectedValues.add(str);
            }

            Block block = createStringsBlock(valueList);
            AggregationTestInputBuilder testInputBuilder = new AggregationTestInputBuilder(
                    new Block[] {block},
                    varcharAgg);
            AggregationTestInput test1 = testInputBuilder.build();

            test1.runPagesOnAggregatorWithAssertion(j, varcharAgg.getFinalType(), groupedAggregator, new AggregationTestOutput(expectedValues));
        }
    }
}
