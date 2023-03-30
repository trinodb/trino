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
package io.trino.operator.unnest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PageTestUtils;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.operator.PageTestUtils.Wrapping.DICTIONARY;
import static io.trino.operator.PageTestUtils.Wrapping.RUN_LENGTH;
import static io.trino.operator.PageTestUtils.createRandomPage;
import static io.trino.operator.unnest.TestingUnnesterUtil.UnnestedLengths;
import static io.trino.operator.unnest.TestingUnnesterUtil.buildExpectedPage;
import static io.trino.operator.unnest.TestingUnnesterUtil.buildOutputTypes;
import static io.trino.operator.unnest.TestingUnnesterUtil.calculateMaxCardinalities;
import static io.trino.operator.unnest.TestingUnnesterUtil.mergePages;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.anonymous;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static io.trino.util.StructuralTestUtil.arrayBlockOf;
import static io.trino.util.StructuralTestUtil.mapBlockOf;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestUnnestOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;

    private static final int PAGE_COUNT = 2;
    private static final int POSITION_COUNT = 500;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testUnnest()
    {
        Type arrayType = new ArrayType(BIGINT);
        Type mapType = TESTING_TYPE_MANAGER.getType(mapType(BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));

        List<Page> input = rowPagesBuilder(BIGINT, arrayType, mapType)
                .row(1L, arrayBlockOf(BIGINT, 2, 3), mapBlockOf(BIGINT, BIGINT, ImmutableMap.of(4, 5)))
                .row(2L, arrayBlockOf(BIGINT, 99), null)
                .row(3L, null, null)
                .pageBreak()
                .row(6L, arrayBlockOf(BIGINT, 7, 8), mapBlockOf(BIGINT, BIGINT, ImmutableMap.of(9, 10, 11, 12)))
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                0, new PlanNodeId("test"), ImmutableList.of(0), ImmutableList.of(BIGINT), ImmutableList.of(1, 2), ImmutableList.of(arrayType, mapType), false, false);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, BIGINT, BIGINT)
                .row(1L, 2L, 4L, 5L)
                .row(1L, 3L, null, null)
                .row(2L, 99L, null, null)
                .row(6L, 7L, 9L, 10L)
                .row(6L, 8L, 11L, 12L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testUnnestWithArray()
    {
        Type arrayType = new ArrayType(new ArrayType(BIGINT));
        Type mapType = TESTING_TYPE_MANAGER.getType(mapType(new ArrayType(BIGINT).getTypeSignature(), new ArrayType(BIGINT).getTypeSignature()));

        List<Page> input = rowPagesBuilder(BIGINT, arrayType, mapType)
                .row(
                        1L,
                        arrayBlockOf(new ArrayType(BIGINT), ImmutableList.of(2, 4), ImmutableList.of(3, 6)),
                        mapBlockOf(new ArrayType(BIGINT), new ArrayType(BIGINT), ImmutableMap.of(ImmutableList.of(4, 8), ImmutableList.of(5, 10))))
                .row(2L, arrayBlockOf(new ArrayType(BIGINT), ImmutableList.of(99, 198)), null)
                .row(3L, null, null)
                .pageBreak()
                .row(
                        6,
                        arrayBlockOf(new ArrayType(BIGINT), ImmutableList.of(7, 14), ImmutableList.of(8, 16)),
                        mapBlockOf(new ArrayType(BIGINT), new ArrayType(BIGINT), ImmutableMap.of(ImmutableList.of(9, 18), ImmutableList.of(10, 20), ImmutableList.of(11, 22), ImmutableList.of(12, 24))))
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                0, new PlanNodeId("test"), ImmutableList.of(0), ImmutableList.of(BIGINT), ImmutableList.of(1, 2), ImmutableList.of(arrayType, mapType), false, false);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, new ArrayType(BIGINT), new ArrayType(BIGINT), new ArrayType(BIGINT))
                .row(1L, ImmutableList.of(2L, 4L), ImmutableList.of(4L, 8L), ImmutableList.of(5L, 10L))
                .row(1L, ImmutableList.of(3L, 6L), null, null)
                .row(2L, ImmutableList.of(99L, 198L), null, null)
                .row(6L, ImmutableList.of(7L, 14L), ImmutableList.of(9L, 18L), ImmutableList.of(10L, 20L))
                .row(6L, ImmutableList.of(8L, 16L), ImmutableList.of(11L, 22L), ImmutableList.of(12L, 24L))
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testUnnestWithOrdinality()
    {
        Type arrayType = new ArrayType(BIGINT);
        Type mapType = TESTING_TYPE_MANAGER.getType(mapType(BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));

        List<Page> input = rowPagesBuilder(BIGINT, arrayType, mapType)
                .row(1L, arrayBlockOf(BIGINT, 2, 3), mapBlockOf(BIGINT, BIGINT, ImmutableMap.of(4, 5)))
                .row(2L, arrayBlockOf(BIGINT, 99), null)
                .row(3L, null, null)
                .pageBreak()
                .row(6L, arrayBlockOf(BIGINT, 7, 8), mapBlockOf(BIGINT, BIGINT, ImmutableMap.of(9, 10, 11, 12)))
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                0, new PlanNodeId("test"), ImmutableList.of(0), ImmutableList.of(BIGINT), ImmutableList.of(1, 2), ImmutableList.of(arrayType, mapType), true, false);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, BIGINT, BIGINT, BIGINT)
                .row(1L, 2L, 4L, 5L, 1L)
                .row(1L, 3L, null, null, 2L)
                .row(2L, 99L, null, null, 1L)
                .row(6L, 7L, 9L, 10L, 1L)
                .row(6L, 8L, 11L, 12L, 2L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testUnnestNonNumericDoubles()
    {
        Type arrayType = new ArrayType(DOUBLE);
        Type mapType = TESTING_TYPE_MANAGER.getType(mapType(BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));

        List<Page> input = rowPagesBuilder(BIGINT, arrayType, mapType)
                .row(1L, arrayBlockOf(DOUBLE, NEGATIVE_INFINITY, POSITIVE_INFINITY, NaN),
                        mapBlockOf(BIGINT, DOUBLE, ImmutableMap.of(1, NEGATIVE_INFINITY, 2, POSITIVE_INFINITY, 3, NaN)))
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                0, new PlanNodeId("test"), ImmutableList.of(0), ImmutableList.of(BIGINT), ImmutableList.of(1, 2), ImmutableList.of(arrayType, mapType), false, false);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE, BIGINT, DOUBLE)
                .row(1L, NEGATIVE_INFINITY, 1L, NEGATIVE_INFINITY)
                .row(1L, POSITIVE_INFINITY, 2L, POSITIVE_INFINITY)
                .row(1L, NaN, 3L, NaN)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testUnnestWithArrayOfRows()
    {
        Type elementType = anonymous(ImmutableList.of(BIGINT, DOUBLE, VARCHAR));
        Type arrayOfRowType = new ArrayType(elementType);

        List<Page> input = rowPagesBuilder(BIGINT, arrayOfRowType)
                .row(1, arrayBlockOf(elementType, ImmutableList.of(2, 4.2, "abc"), ImmutableList.of(3, 6.6, "def")))
                .row(2, arrayBlockOf(elementType, ImmutableList.of(99, 3.14, "pi"), null))
                .row(3, null)
                .pageBreak()
                .row(6, arrayBlockOf(elementType, null, ImmutableList.of(8, 1.111, "tt")))
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                0, new PlanNodeId("test"), ImmutableList.of(0), ImmutableList.of(BIGINT), ImmutableList.of(1), ImmutableList.of(arrayOfRowType), false, false);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, DOUBLE, VARCHAR)
                .row(1L, 2L, 4.2, "abc")
                .row(1L, 3L, 6.6, "def")
                .row(2L, 99L, 3.14, "pi")
                .row(2L, null, null, null)
                .row(6L, null, null, null)
                .row(6L, 8L, 1.111, "tt")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testOuterUnnest()
    {
        Type mapType = TESTING_TYPE_MANAGER.getType(mapType(BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
        Type arrayType = new ArrayType(BIGINT);
        Type elementType = anonymous(ImmutableList.of(BIGINT, DOUBLE, VARCHAR));
        Type arrayOfRowType = new ArrayType(elementType);

        List<Page> input = rowPagesBuilder(BIGINT, mapType, arrayType, arrayOfRowType)
                .row(
                        1,
                        mapBlockOf(BIGINT, BIGINT, ImmutableMap.of(1, 2)),
                        arrayBlockOf(BIGINT, 3),
                        arrayBlockOf(elementType, ImmutableList.of(4, 5.5, "a"), ImmutableList.of(6, 7.7, "b")))
                .row(2, null, null, null)
                .pageBreak()
                .row(3, null, null, null)
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                0, new PlanNodeId("test"), ImmutableList.of(0), ImmutableList.of(BIGINT), ImmutableList.of(1, 2, 3), ImmutableList.of(mapType, arrayType, arrayOfRowType), false, true);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, DOUBLE, VARCHAR)
                .row(1L, 1L, 2L, 3L, 4L, 5.5, "a")
                .row(1L, null, null, null, 6L, 7.7, "b")
                .row(2L, null, null, null, null, null, null)
                .row(3L, null, null, null, null, null, null)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testOuterUnnestWithOrdinality()
    {
        Type mapType = TESTING_TYPE_MANAGER.getType(mapType(BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
        Type arrayType = new ArrayType(BIGINT);
        Type elementType = anonymous(ImmutableList.of(BIGINT, DOUBLE, VARCHAR));
        Type arrayOfRowType = new ArrayType(elementType);

        List<Page> input = rowPagesBuilder(BIGINT, mapType, arrayType, arrayOfRowType)
                .row(
                        1,
                        mapBlockOf(BIGINT, BIGINT, ImmutableMap.of(1, 2, 6, 7)),
                        arrayBlockOf(BIGINT, 3),
                        arrayBlockOf(elementType, ImmutableList.of(4, 5.5, "a")))
                .row(2, null, null, null)
                .pageBreak()
                .row(3, null, null, null)
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                0, new PlanNodeId("test"), ImmutableList.of(0), ImmutableList.of(BIGINT), ImmutableList.of(1, 2, 3), ImmutableList.of(mapType, arrayType, arrayOfRowType), true, true);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT)
                .row(1L, 1L, 2L, 3L, 4L, 5.5, "a", 1L)
                .row(1L, 6L, 7L, null, null, null, null, 2L)
                .row(2L, null, null, null, null, null, null, null)
                .row(3L, null, null, null, null, null, null, null)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testUnnestSingleArray()
    {
        testUnnest(
                ImmutableList.of(BIGINT),
                ImmutableList.of(new ArrayType(BIGINT)));

        testUnnest(
                ImmutableList.of(VARCHAR),
                ImmutableList.of(new ArrayType(VARCHAR)));

        testUnnest(
                ImmutableList.of(VARCHAR),
                ImmutableList.of(new ArrayType(BIGINT)));
    }

    @Test
    public void testUnnestSingleMap()
    {
        testUnnest(
                ImmutableList.of(BIGINT),
                ImmutableList.of(createMapType(BIGINT, BIGINT)));

        testUnnest(
                ImmutableList.of(VARCHAR),
                ImmutableList.of(createMapType(VARCHAR, VARCHAR)));

        testUnnest(
                ImmutableList.of(VARCHAR),
                ImmutableList.of(createMapType(VARCHAR, BIGINT)));
    }

    @Test
    public void testUnnestSingleArrayWithEmptyInput()
    {
        Page emptyPage = new PageBuilder(ImmutableList.of(BIGINT, new ArrayType(BIGINT))).build();
        testUnnest(
                ImmutableList.of(emptyPage),
                ImmutableList.of(BIGINT),
                ImmutableList.of(new ArrayType(BIGINT)),
                false,
                false);
    }

    @Test
    public void testUnnestSingleArrayOfRow()
    {
        testUnnest(
                ImmutableList.of(BIGINT),
                ImmutableList.of(new ArrayType(anonymousRow(BIGINT, BIGINT, BIGINT))));

        testUnnest(
                ImmutableList.of(VARCHAR),
                ImmutableList.of(new ArrayType(anonymousRow(VARCHAR, VARCHAR, VARCHAR))));

        testUnnest(
                ImmutableList.of(VARCHAR),
                ImmutableList.of(new ArrayType(anonymousRow(
                        VARCHAR,
                        new ArrayType(BIGINT),
                        createMapType(VARCHAR, VARCHAR)))));
    }

    @Test
    public void testUnnestTwoArrays()
    {
        testUnnest(
                ImmutableList.of(BOOLEAN),
                ImmutableList.of(new ArrayType(BOOLEAN), new ArrayType(BOOLEAN)));

        testUnnest(
                ImmutableList.of(SMALLINT),
                ImmutableList.of(new ArrayType(SMALLINT), new ArrayType(SMALLINT)));

        testUnnest(
                ImmutableList.of(INTEGER),
                ImmutableList.of(new ArrayType(INTEGER), new ArrayType(INTEGER)));

        testUnnest(
                ImmutableList.of(BIGINT),
                ImmutableList.of(new ArrayType(BIGINT), new ArrayType(BIGINT)));

        Type decimalType = createDecimalType(MAX_SHORT_PRECISION + 1);
        testUnnest(
                ImmutableList.of(decimalType),
                ImmutableList.of(new ArrayType(decimalType), new ArrayType(decimalType)));

        testUnnest(
                ImmutableList.of(VARCHAR),
                ImmutableList.of(new ArrayType(VARCHAR), new ArrayType(VARCHAR)));

        testUnnest(
                ImmutableList.of(BIGINT),
                ImmutableList.of(new ArrayType(BIGINT), new ArrayType(VARCHAR)));

        Type bigintMapType = createMapType(BIGINT, BIGINT);
        testUnnest(
                ImmutableList.of(bigintMapType),
                ImmutableList.of(new ArrayType(bigintMapType), new ArrayType(bigintMapType)));
    }

    @Test
    public void testUnnestTwoMaps()
    {
        testUnnest(
                ImmutableList.of(BIGINT),
                ImmutableList.of(createMapType(BIGINT, BIGINT), createMapType(VARCHAR, VARCHAR)));
    }

    @Test
    public void testUnnestTwoArraysOfRow()
    {
        Type rowOfIntegers = anonymousRow(INTEGER, INTEGER);
        testUnnest(
                ImmutableList.of(BIGINT),
                ImmutableList.of(new ArrayType(rowOfIntegers), new ArrayType(rowOfIntegers)));
    }

    @Test
    public void testUnnestMultipleMixed()
    {
        testUnnest(
                ImmutableList.of(BIGINT),
                ImmutableList.of(
                        new ArrayType(BIGINT),
                        createMapType(VARCHAR, VARCHAR),
                        new ArrayType(anonymousRow(BIGINT, BIGINT, BIGINT)),
                        new ArrayType(anonymousRow(VARCHAR, VARCHAR, VARCHAR))));
    }

    @Test
    public void testUnnestArrayOfRowsWithNulls()
    {
        Type replicatedType = VARCHAR;
        // Unnest type
        Type elementType = anonymousRow(VARCHAR, VARCHAR);
        Type unnestType = new ArrayType(elementType);

        List<Object> nonNullRowValue = ImmutableList.of("lalala", "lalala");
        List<Page> input = rowPagesBuilder(VARCHAR, unnestType)
                .row(
                        "abc",
                        arrayBlockOf(elementType, Collections.nCopies(999, nonNullRowValue).toArray()))
                .row(
                        "def",
                        arrayBlockOf(elementType, null, null))
                .build();

        testUnnest(input, ImmutableList.of(replicatedType), ImmutableList.of(unnestType), false, false);
    }

    protected void testUnnest(List<Type> replicatedTypes, List<Type> unnestTypes)
    {
        testUnnest(replicatedTypes, unnestTypes, 0.0f, ImmutableList.of());
        testUnnest(replicatedTypes, unnestTypes, 0.2f, ImmutableList.of());

        testUnnest(replicatedTypes, unnestTypes, 0.0f, ImmutableList.of(DICTIONARY));
        testUnnest(replicatedTypes, unnestTypes, 0.0f, ImmutableList.of(RUN_LENGTH));
        testUnnest(replicatedTypes, unnestTypes, 0.0f, ImmutableList.of(DICTIONARY, DICTIONARY));
        testUnnest(replicatedTypes, unnestTypes, 0.0f, ImmutableList.of(RUN_LENGTH, DICTIONARY));

        testUnnest(replicatedTypes, unnestTypes, 0.2f, ImmutableList.of(DICTIONARY));
        testUnnest(replicatedTypes, unnestTypes, 0.2f, ImmutableList.of(RUN_LENGTH));
        testUnnest(replicatedTypes, unnestTypes, 0.2f, ImmutableList.of(DICTIONARY, DICTIONARY));
        testUnnest(replicatedTypes, unnestTypes, 0.2f, ImmutableList.of(RUN_LENGTH, DICTIONARY));
    }

    private void testUnnest(
            List<Type> replicatedTypes,
            List<Type> unnestTypes,
            float nullRate,
            List<PageTestUtils.Wrapping> wrappings)
    {
        List<Type> types = ImmutableList.<Type>builder()
                .addAll(replicatedTypes)
                .addAll(unnestTypes)
                .build();

        List<Page> inputPages = new ArrayList<>();
        for (int i = 0; i < PAGE_COUNT; i++) {
            Page inputPage = createRandomPage(types, POSITION_COUNT, Optional.empty(), nullRate, wrappings);
            inputPages.add(inputPage);
        }

        testUnnest(inputPages, replicatedTypes, unnestTypes, false, false);
        testUnnest(inputPages, replicatedTypes, unnestTypes, true, false);
        testUnnest(inputPages, replicatedTypes, unnestTypes, false, true);
        testUnnest(inputPages, replicatedTypes, unnestTypes, true, true);
    }

    private void testUnnest(List<Page> inputPages, List<Type> replicatedTypes, List<Type> unnestTypes, boolean withOrdinality, boolean outer)
    {
        List<Integer> replicatedChannels = IntStream.range(0, replicatedTypes.size()).boxed().collect(toImmutableList());
        List<Integer> unnestChannels = IntStream.range(replicatedTypes.size(), replicatedTypes.size() + unnestTypes.size()).boxed().collect(toImmutableList());

        Operator unnestOperator = new UnnestOperator.UnnestOperatorFactory(
                0,
                new PlanNodeId("test"),
                replicatedChannels,
                replicatedTypes,
                unnestChannels,
                unnestTypes,
                withOrdinality,
                outer)
                .createOperator(createDriverContext());

        for (Page inputPage : inputPages) {
            UnnestedLengths unnestedLengths = calculateMaxCardinalities(inputPage, replicatedTypes, unnestTypes, outer);
            List<Type> outputTypes = buildOutputTypes(replicatedTypes, unnestTypes, withOrdinality);

            Page expectedPage = buildExpectedPage(inputPage, replicatedTypes, unnestTypes, outputTypes, unnestedLengths, withOrdinality);

            unnestOperator.addInput(inputPage);

            List<Page> outputPages = new ArrayList<>();
            while (true) {
                Page outputPage = unnestOperator.getOutput();

                if (outputPage == null) {
                    break;
                }

                assertTrue(outputPage.getPositionCount() <= 1000);

                outputPages.add(outputPage);
            }

            Page mergedOutputPage = mergePages(outputTypes, outputPages);
            try {
                assertPageEquals(outputTypes, mergedOutputPage, expectedPage);
            }
            catch (Throwable e) {
                System.out.println("withOrdinality: " + withOrdinality + ", outer: " + outer);
                System.out.println("Last index: " + (outputTypes.size() - 1));
                throw e;
            }
        }
    }

    private DriverContext createDriverContext()
    {
        Session testSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        return createTaskContext(executor, scheduledExecutor, testSession)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    private static Type createMapType(Type keyType, Type valueType)
    {
        return TESTING_TYPE_MANAGER.getType(mapType(keyType.getTypeSignature(), valueType.getTypeSignature()));
    }
}
