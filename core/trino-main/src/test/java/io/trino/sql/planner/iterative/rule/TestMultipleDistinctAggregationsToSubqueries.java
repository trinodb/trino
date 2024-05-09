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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.assertions.SetOperationOutputMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.PlanTester;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.symbol;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.union;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.AggregationNode.groupingSets;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestMultipleDistinctAggregationsToSubqueries
        extends BaseRuleTest
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";

    private static final Session MOCK_SESSION = testSessionBuilder().setCatalog(MOCK_CATALOG).setSchema(TEST_SCHEMA).build();

    private static final String COLUMN_1 = "orderkey";
    private static final ColumnHandle COLUMN_1_HANDLE = new MockConnectorColumnHandle(COLUMN_1, BIGINT);
    private static final String COLUMN_2 = "partkey";
    private static final ColumnHandle COLUMN_2_HANDLE = new MockConnectorColumnHandle(COLUMN_2, BIGINT);
    private static final String COLUMN_3 = "linenumber";
    private static final ColumnHandle COLUMN_3_HANDLE = new MockConnectorColumnHandle(COLUMN_3, BIGINT);

    private static final String COLUMN_4 = "shipdate";
    private static final ColumnHandle COLUMN_4_HANDLE = new MockConnectorColumnHandle(COLUMN_4, DATE);
    private static final String GROUPING_KEY_COLUMN = "suppkey";
    private static final ColumnHandle GROUPING_KEY_COLUMN_HANDLE = new MockConnectorColumnHandle(GROUPING_KEY_COLUMN, BIGINT);
    private static final SchemaTableName TABLE_SCHEMA = new SchemaTableName(TEST_SCHEMA, TEST_TABLE);
    private static final List<ColumnMetadata> ALL_COLUMNS = Stream.of(COLUMN_1_HANDLE, COLUMN_2_HANDLE, COLUMN_3_HANDLE, COLUMN_4_HANDLE, GROUPING_KEY_COLUMN_HANDLE)
            .map(columnHandle -> (MockConnectorColumnHandle) columnHandle)
            .map(column -> new ColumnMetadata(column.getName(), column.getType()))
            .collect(toImmutableList());

    private RuleTester ruleTester = tester(true);

    @AfterAll
    public final void tearDownTester()
    {
        closeAllRuntimeException(ruleTester);
        ruleTester = null;
    }

    @Test
    public void testDoesNotFire()
    {
        // no distinct aggregation
        ruleTester.assertThat(new MultipleDistinctAggregationsToSubqueries(ruleTester.getMetadata()))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol inputSymbol = p.symbol("inputSymbol");
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(inputSymbol)
                            .source(
                                    p.tableScan(
                                            testTableHandle(ruleTester),
                                            ImmutableList.of(inputSymbol),
                                            ImmutableMap.of(inputSymbol, COLUMN_1_HANDLE))));
                })
                .doesNotFire();

        // single distinct
        ruleTester.assertThat(new MultipleDistinctAggregationsToSubqueries(ruleTester.getMetadata()))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol inputSymbol = p.symbol("inputSymbol", BIGINT);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "inputSymbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(
                                            testTableHandle(ruleTester),
                                            ImmutableList.of(inputSymbol),
                                            ImmutableMap.of(inputSymbol, COLUMN_1_HANDLE))));
                })
                .doesNotFire();

        // two distinct on the same input
        ruleTester.assertThat(new MultipleDistinctAggregationsToSubqueries(ruleTester.getMetadata()))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol", BIGINT);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2", BIGINT), PlanBuilder.aggregation("sum", true, ImmutableList.of(new Reference(BIGINT, "input1Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(
                                            testTableHandle(ruleTester),
                                            ImmutableList.of(input1Symbol),
                                            ImmutableMap.of(
                                                    input1Symbol, COLUMN_1_HANDLE))));
                })
                .doesNotFire();

        // hash symbol
        ruleTester.assertThat(new MultipleDistinctAggregationsToSubqueries(ruleTester.getMetadata()))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol", BIGINT);
                    Symbol input2Symbol = p.symbol("input2Symbol", BIGINT);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2", BIGINT), PlanBuilder.aggregation("sum", true, ImmutableList.of(new Reference(BIGINT, "input2Symbol"))), ImmutableList.of(BIGINT))
                            .hashSymbol(p.symbol("hashSymbol", BIGINT))
                            .source(
                                    p.tableScan(
                                            testTableHandle(ruleTester),
                                            ImmutableList.of(input1Symbol, input2Symbol),
                                            ImmutableMap.of(
                                                    input1Symbol, COLUMN_1_HANDLE,
                                                    input2Symbol, COLUMN_2_HANDLE))));
                })
                .doesNotFire();

        // non-distinct
        ruleTester.assertThat(new MultipleDistinctAggregationsToSubqueries(ruleTester.getMetadata()))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol", BIGINT);
                    Symbol input2Symbol = p.symbol("input2Symbol", BIGINT);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2", BIGINT), PlanBuilder.aggregation("sum", true, ImmutableList.of(new Reference(BIGINT, "input2Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output3", BIGINT), PlanBuilder.aggregation("count", ImmutableList.of(new Reference(BIGINT, "input1Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(
                                            testTableHandle(ruleTester),
                                            ImmutableList.of(input1Symbol, input2Symbol),
                                            ImmutableMap.of(
                                                    input1Symbol, COLUMN_1_HANDLE,
                                                    input2Symbol, COLUMN_2_HANDLE))));
                })
                .doesNotFire();

        // groupingSetCount > 1
        ruleTester.assertThat(new MultipleDistinctAggregationsToSubqueries(ruleTester.getMetadata()))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol", BIGINT);
                    Symbol input2Symbol = p.symbol("input2Symbol", BIGINT);
                    return p.aggregation(builder -> builder
                            .groupingSets(groupingSets(ImmutableList.of(), 2, ImmutableSet.of(0, 1)))
                            .addAggregation(p.symbol("output1", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2", BIGINT), PlanBuilder.aggregation("sum", true, ImmutableList.of(new Reference(BIGINT, "input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(
                                            testTableHandle(ruleTester),
                                            ImmutableList.of(input1Symbol, input2Symbol),
                                            ImmutableMap.of(
                                                    input1Symbol, COLUMN_1_HANDLE,
                                                    input2Symbol, COLUMN_2_HANDLE))));
                })
                .doesNotFire();

        // complex subquery (join)
        ruleTester.assertThat(new MultipleDistinctAggregationsToSubqueries(ruleTester.getMetadata()))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol", BIGINT);
                    Symbol input2Symbol = p.symbol("input2Symbol", BIGINT);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2", BIGINT), PlanBuilder.aggregation("sum", true, ImmutableList.of(new Reference(BIGINT, "input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.join(
                                            INNER,
                                            p.tableScan(
                                                    testTableHandle(ruleTester),
                                                    ImmutableList.of(),
                                                    ImmutableMap.of()),
                                            p.tableScan(
                                                    testTableHandle(ruleTester),
                                                    ImmutableList.of(input1Symbol, input2Symbol),
                                                    ImmutableMap.of(
                                                            input1Symbol, COLUMN_1_HANDLE,
                                                            input2Symbol, COLUMN_2_HANDLE)))));
                })
                .doesNotFire();

        // complex subquery (filter on top of join to test recursion)
        ruleTester.assertThat(new MultipleDistinctAggregationsToSubqueries(ruleTester.getMetadata()))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol", BIGINT);
                    Symbol input2Symbol = p.symbol("input2Symbol", BIGINT);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2", BIGINT), PlanBuilder.aggregation("sum", true, ImmutableList.of(new Reference(BIGINT, "input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.filter(
                                            TRUE,
                                            p.join(
                                                    INNER,
                                                    p.tableScan(
                                                            testTableHandle(ruleTester),
                                                            ImmutableList.of(),
                                                            ImmutableMap.of()),
                                                    p.tableScan(
                                                            testTableHandle(ruleTester),
                                                            ImmutableList.of(input1Symbol, input2Symbol),
                                                            ImmutableMap.of(
                                                                    input1Symbol, COLUMN_1_HANDLE,
                                                                    input2Symbol, COLUMN_2_HANDLE))))));
                })
                .doesNotFire();

        // connector does not support efficient single column reads
        RuleTester ruleTesterNotObjectStore = tester(false);

        ruleTesterNotObjectStore.assertThat(new MultipleDistinctAggregationsToSubqueries(ruleTesterNotObjectStore.getMetadata()))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol", BIGINT);
                    Symbol input2Symbol = p.symbol("input2Symbol", BIGINT);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2", BIGINT), PlanBuilder.aggregation("sum", true, ImmutableList.of(new Reference(BIGINT, "input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(
                                            testTableHandle(ruleTesterNotObjectStore),
                                            ImmutableList.of(input1Symbol, input2Symbol),
                                            ImmutableMap.of(
                                                    input1Symbol, COLUMN_1_HANDLE,
                                                    input2Symbol, COLUMN_2_HANDLE))));
                })
                .doesNotFire();

        // rule not enabled
        ruleTester.assertThat(new MultipleDistinctAggregationsToSubqueries(ruleTester.getMetadata()))
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol", BIGINT);
                    Symbol input2Symbol = p.symbol("input2Symbol", BIGINT);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2", BIGINT), PlanBuilder.aggregation("sum", true, ImmutableList.of(new Reference(BIGINT, "input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(
                                            testTableHandle(ruleTester),
                                            ImmutableList.of(input1Symbol, input2Symbol),
                                            ImmutableMap.of(
                                                    input1Symbol, COLUMN_1_HANDLE,
                                                    input2Symbol, COLUMN_2_HANDLE))));
                })
                .doesNotFire();
    }

    @Test
    public void testGlobalDistinctToSubqueries()
    {
        ruleTester.assertThat(new MultipleDistinctAggregationsToSubqueries(ruleTester.getMetadata()))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol", BIGINT);
                    Symbol input2Symbol = p.symbol("input2Symbol", BIGINT);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2", BIGINT), PlanBuilder.aggregation("sum", true, ImmutableList.of(new Reference(BIGINT, "input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(
                                            testTableHandle(ruleTester),
                                            ImmutableList.of(input1Symbol, input2Symbol),
                                            ImmutableMap.of(
                                                    input1Symbol, COLUMN_1_HANDLE,
                                                    input2Symbol, COLUMN_2_HANDLE))));
                })
                .matches(project(
                        ImmutableMap.of(
                                "final_output1", PlanMatchPattern.expression(new Reference(BIGINT, "output1")),
                                "final_output2", PlanMatchPattern.expression(new Reference(BIGINT, "output2"))),
                        join(
                                INNER,
                                builder -> builder
                                        .left(aggregation(
                                                ImmutableMap.of("output1", aggregationFunction("count", true, ImmutableList.of(symbol("input1Symbol")))),
                                                tableScan(TABLE_SCHEMA.getTableName(), ImmutableMap.of("input1Symbol", COLUMN_1))))
                                        .right(aggregation(
                                                ImmutableMap.of("output2", aggregationFunction("sum", true, ImmutableList.of(symbol("input2Symbol")))),
                                                tableScan(TABLE_SCHEMA.getTableName(), ImmutableMap.of("input2Symbol", COLUMN_2)))))));
    }

    @Test
    public void testGlobalWith3DistinctToSubqueries()
    {
        ruleTester.assertThat(new MultipleDistinctAggregationsToSubqueries(ruleTester.getMetadata()))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol", BIGINT);
                    Symbol input2Symbol = p.symbol("input2Symbol", BIGINT);
                    Symbol input3Symbol = p.symbol("input3Symbol", BIGINT);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2", BIGINT), PlanBuilder.aggregation("sum", true, ImmutableList.of(new Reference(BIGINT, "input2Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output3", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input3Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(
                                            testTableHandle(ruleTester),
                                            ImmutableList.of(input1Symbol, input2Symbol, input3Symbol),
                                            ImmutableMap.of(
                                                    input1Symbol, COLUMN_1_HANDLE,
                                                    input2Symbol, COLUMN_2_HANDLE,
                                                    input3Symbol, COLUMN_3_HANDLE))));
                })
                .matches(project(
                        ImmutableMap.of(
                                "final_output1", PlanMatchPattern.expression(new Reference(BIGINT, "output1")),
                                "final_output2", PlanMatchPattern.expression(new Reference(BIGINT, "output2")),
                                "final_output3", PlanMatchPattern.expression(new Reference(BIGINT, "output3"))),
                        join(
                                INNER,
                                join -> join
                                        .left(aggregation(
                                                ImmutableMap.of("output1", aggregationFunction("count", true, ImmutableList.of(symbol("input1Symbol")))),
                                                tableScan(TABLE_SCHEMA.getTableName(), ImmutableMap.of("input1Symbol", COLUMN_1))))
                                        .right(join(
                                                INNER,
                                                subJoin -> subJoin
                                                        .left(aggregation(
                                                                ImmutableMap.of("output2", aggregationFunction("sum", true, ImmutableList.of(symbol("input2Symbol")))),
                                                                tableScan(TABLE_SCHEMA.getTableName(), ImmutableMap.of("input2Symbol", COLUMN_2))))
                                                        .right(aggregation(
                                                                ImmutableMap.of("output3", aggregationFunction("count", true, ImmutableList.of(symbol("input3Symbol")))),
                                                                tableScan(TABLE_SCHEMA.getTableName(), ImmutableMap.of("input3Symbol", COLUMN_3)))))))));
    }

    // tests right deep join hierarchy
    @Test
    public void testGlobalWith4DistinctToSubqueries()
    {
        ruleTester.assertThat(new MultipleDistinctAggregationsToSubqueries(ruleTester.getMetadata()))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol", BIGINT);
                    Symbol input2Symbol = p.symbol("input2Symbol", BIGINT);
                    Symbol input3Symbol = p.symbol("input3Symbol", BIGINT);
                    Symbol input4Symbol = p.symbol("input4Symbol", BIGINT);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input2Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output3", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input3Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output4", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input4Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(
                                            testTableHandle(ruleTester),
                                            ImmutableList.of(input1Symbol, input2Symbol, input3Symbol, input4Symbol),
                                            ImmutableMap.of(
                                                    input1Symbol, COLUMN_1_HANDLE,
                                                    input2Symbol, COLUMN_2_HANDLE,
                                                    input3Symbol, COLUMN_3_HANDLE,
                                                    input4Symbol, COLUMN_4_HANDLE))));
                })
                .matches(project(
                        ImmutableMap.of(
                                "final_output1", PlanMatchPattern.expression(new Reference(BIGINT, "output1")),
                                "final_output2", PlanMatchPattern.expression(new Reference(BIGINT, "output2")),
                                "final_output3", PlanMatchPattern.expression(new Reference(BIGINT, "output3")),
                                "final_output4", PlanMatchPattern.expression(new Reference(BIGINT, "output4"))),
                        join(
                                INNER,
                                join -> join
                                        .left(aggregation(
                                                ImmutableMap.of("output1", aggregationFunction("count", true, ImmutableList.of(symbol("input1Symbol")))),
                                                tableScan(TABLE_SCHEMA.getTableName(), ImmutableMap.of("input1Symbol", COLUMN_1))))
                                        .right(join(
                                                INNER,
                                                subJoin -> subJoin
                                                        .left(aggregation(
                                                                ImmutableMap.of("output2", aggregationFunction("count", true, ImmutableList.of(symbol("input2Symbol")))),
                                                                tableScan(TABLE_SCHEMA.getTableName(), ImmutableMap.of("input2Symbol", COLUMN_2))))
                                                        .right(join(
                                                                INNER,
                                                                subJoin2 -> subJoin2
                                                                        .left(aggregation(
                                                                                ImmutableMap.of("output3", aggregationFunction("count", true, ImmutableList.of(symbol("input3Symbol")))),
                                                                                tableScan(TABLE_SCHEMA.getTableName(), ImmutableMap.of("input3Symbol", COLUMN_3))))
                                                                        .right(aggregation(
                                                                                ImmutableMap.of("output4", aggregationFunction("count", true, ImmutableList.of(symbol("input4Symbol")))),
                                                                                tableScan(TABLE_SCHEMA.getTableName(), ImmutableMap.of("input4Symbol", COLUMN_4)))))))))));
    }

    @Test
    public void testGlobal2DistinctOnTheSameInputToSubqueries()
    {
        ruleTester.assertThat(new MultipleDistinctAggregationsToSubqueries(ruleTester.getMetadata()))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol", BIGINT);
                    Symbol input2Symbol = p.symbol("input2Symbol", BIGINT);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2", BIGINT), PlanBuilder.aggregation("sum", true, ImmutableList.of(new Reference(BIGINT, "input2Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output3", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(
                                            testTableHandle(ruleTester),
                                            ImmutableList.of(input1Symbol, input2Symbol),
                                            ImmutableMap.of(
                                                    input1Symbol, COLUMN_1_HANDLE,
                                                    input2Symbol, COLUMN_2_HANDLE))));
                })
                .matches(project(
                        ImmutableMap.of(
                                "final_output1", PlanMatchPattern.expression(new Reference(BIGINT, "output1")),
                                "final_output2", PlanMatchPattern.expression(new Reference(BIGINT, "output2")),
                                "final_output3", PlanMatchPattern.expression(new Reference(BIGINT, "output3"))),
                        join(
                                INNER,
                                builder -> builder
                                        .left(aggregation(
                                                ImmutableMap.of("output1", aggregationFunction("count", true, ImmutableList.of(symbol("input1Symbol")))),
                                                tableScan(TABLE_SCHEMA.getTableName(), ImmutableMap.of("input1Symbol", COLUMN_1))))
                                        .right(aggregation(
                                                ImmutableMap.of(
                                                        "output2", aggregationFunction("sum", true, ImmutableList.of(symbol("input2Symbol"))),
                                                        "output3", aggregationFunction("count", true, ImmutableList.of(symbol("input2Symbol")))),
                                                tableScan(TABLE_SCHEMA.getTableName(), ImmutableMap.of("input2Symbol", COLUMN_2)))))));
    }

    @Test
    public void testGroupByWithDistinctToSubqueries()
    {
        String aggregationNodeId = "aggregationNodeId";
        ruleTester.assertThat(new MultipleDistinctAggregationsToSubqueries(ruleTester.getMetadata()))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .overrideStats(aggregationNodeId, PlanNodeStatsEstimate.builder().setOutputRowCount(100_000).build())
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol", BIGINT);
                    Symbol input2Symbol = p.symbol("input2Symbol", BIGINT);
                    Symbol groupingKey = p.symbol("groupingKey", BIGINT);
                    return p.aggregation(builder -> builder
                            .nodeId(new PlanNodeId(aggregationNodeId))
                            .singleGroupingSet(groupingKey)
                            .addAggregation(p.symbol("output1", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2", BIGINT), PlanBuilder.aggregation("sum", true, ImmutableList.of(new Reference(BIGINT, "input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(
                                            testTableHandle(ruleTester),
                                            ImmutableList.of(input1Symbol, input2Symbol),
                                            ImmutableMap.of(
                                                    input1Symbol, COLUMN_1_HANDLE,
                                                    input2Symbol, COLUMN_2_HANDLE,
                                                    groupingKey, GROUPING_KEY_COLUMN_HANDLE))));
                })
                .matches(project(
                        ImmutableMap.of(
                                "final_output1", PlanMatchPattern.expression(new Reference(BIGINT, "output1")),
                                "final_output2", PlanMatchPattern.expression(new Reference(BIGINT, "output2")),
                                "group_by_key", PlanMatchPattern.expression(new Reference(BIGINT, "left_groupingKey"))),
                        join(
                                INNER,
                                builder -> builder
                                        .equiCriteria("left_groupingKey", "right_groupingKey")
                                        .left(aggregation(
                                                singleGroupingSet("left_groupingKey"),
                                                ImmutableMap.of(Optional.of("output1"), aggregationFunction("count", true, ImmutableList.of(symbol("input1Symbol")))),
                                                Optional.empty(),
                                                SINGLE,
                                                tableScan(
                                                        TABLE_SCHEMA.getTableName(),
                                                        ImmutableMap.of(
                                                                "input1Symbol", COLUMN_1,
                                                                "left_groupingKey", GROUPING_KEY_COLUMN))))
                                        .right(aggregation(
                                                singleGroupingSet("right_groupingKey"),
                                                ImmutableMap.of(Optional.of("output2"), aggregationFunction("sum", true, ImmutableList.of(symbol("input2Symbol")))),
                                                Optional.empty(),
                                                SINGLE,
                                                tableScan(
                                                        TABLE_SCHEMA.getTableName(),
                                                        ImmutableMap.of(
                                                                "input2Symbol", COLUMN_2,
                                                                "right_groupingKey", GROUPING_KEY_COLUMN)))))));
    }

    @Test
    public void testGroupByWithDistinctOverUnionToSubqueries()
    {
        String aggregationNodeId = "aggregationNodeId";
        ruleTester.assertThat(new MultipleDistinctAggregationsToSubqueries(ruleTester.getMetadata()))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .overrideStats(aggregationNodeId, PlanNodeStatsEstimate.builder().setOutputRowCount(100_000).build())
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol", BIGINT);
                    Symbol input11Symbol = p.symbol("input1_1Symbol", BIGINT);
                    Symbol input12Symbol = p.symbol("input1_2Symbol", BIGINT);
                    Symbol input2Symbol = p.symbol("input2Symbol", BIGINT);
                    Symbol input21Symbol = p.symbol("input2_1Symbol", BIGINT);
                    Symbol input22Symbol = p.symbol("input2_2Symbol", BIGINT);
                    Symbol groupingKey = p.symbol("groupingKey", BIGINT);
                    Symbol groupingKey1 = p.symbol("groupingKey1", BIGINT);
                    Symbol groupingKey2 = p.symbol("groupingKey2", BIGINT);

                    return p.aggregation(builder -> builder
                            .nodeId(new PlanNodeId(aggregationNodeId))
                            .singleGroupingSet(groupingKey)
                            .addAggregation(p.symbol("output1", BIGINT), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2", BIGINT), PlanBuilder.aggregation("sum", true, ImmutableList.of(new Reference(BIGINT, "input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.union(
                                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                                    .put(input1Symbol, input11Symbol)
                                                    .put(input1Symbol, input12Symbol)
                                                    .put(input2Symbol, input21Symbol)
                                                    .put(input2Symbol, input22Symbol)
                                                    .put(groupingKey, groupingKey1)
                                                    .put(groupingKey, groupingKey2)
                                                    .build(),
                                            ImmutableList.of(
                                                    p.filter(
                                                            new Comparison(GREATER_THAN, new Reference(BIGINT, "input1_1Symbol"), new Constant(BIGINT, 0L)),
                                                            p.tableScan(
                                                                    testTableHandle(ruleTester),
                                                                    ImmutableList.of(input11Symbol, input21Symbol, groupingKey1),
                                                                    ImmutableMap.of(
                                                                            input11Symbol, COLUMN_1_HANDLE,
                                                                            input21Symbol, COLUMN_2_HANDLE,
                                                                            groupingKey1, GROUPING_KEY_COLUMN_HANDLE))),
                                                    p.filter(
                                                            new Comparison(GREATER_THAN, new Reference(BIGINT, "input2_2Symbol"), new Constant(BIGINT, 2L)),
                                                            p.tableScan(
                                                                    testTableHandle(ruleTester),
                                                                    ImmutableList.of(input12Symbol, input22Symbol, groupingKey2),
                                                                    ImmutableMap.of(
                                                                            input12Symbol, COLUMN_1_HANDLE,
                                                                            input22Symbol, COLUMN_2_HANDLE,
                                                                            groupingKey2, GROUPING_KEY_COLUMN_HANDLE)))))));
                })
                .matches(project(
                        ImmutableMap.of(
                                "final_output1", PlanMatchPattern.expression(new Reference(BIGINT, "output1")),
                                "final_output2", PlanMatchPattern.expression(new Reference(BIGINT, "output2")),
                                "group_by_key", PlanMatchPattern.expression(new Reference(BIGINT, "left_groupingKey"))),
                        join(
                                INNER,
                                builder -> builder
                                        .equiCriteria("left_groupingKey", "right_groupingKey")
                                        .left(aggregation(
                                                singleGroupingSet("left_groupingKey"),
                                                ImmutableMap.of(Optional.of("output1"), aggregationFunction("count", true, ImmutableList.of(symbol("input1Symbol1")))),
                                                Optional.empty(),
                                                SINGLE,
                                                union(
                                                        filter(
                                                                new Comparison(GREATER_THAN, new Reference(BIGINT, "input1_1_1Symbol"), new Constant(BIGINT, 0L)),
                                                                tableScan(
                                                                        TABLE_SCHEMA.getTableName(),
                                                                        ImmutableMap.of(
                                                                                "input1_1_1Symbol", COLUMN_1,
                                                                                "input2_1_1Symbol", COLUMN_2,
                                                                                "left_groupingKey1", GROUPING_KEY_COLUMN))),
                                                        filter(
                                                                new Comparison(GREATER_THAN, new Reference(BIGINT, "input2_2_1Symbol"), new Constant(BIGINT, 2L)),
                                                                tableScan(
                                                                        TABLE_SCHEMA.getTableName(),
                                                                        ImmutableMap.of(
                                                                                "input1_2_1Symbol", COLUMN_1,
                                                                                "input2_2_1Symbol", COLUMN_2,
                                                                                "left_groupingKey2", GROUPING_KEY_COLUMN))))
                                                        .withAlias("input1Symbol1", new SetOperationOutputMatcher(0))
                                                        .withAlias("input2Symbol1", new SetOperationOutputMatcher(1))
                                                        .withAlias("left_groupingKey", new SetOperationOutputMatcher(2))))
                                        .right(aggregation(
                                                singleGroupingSet("right_groupingKey"),
                                                ImmutableMap.of(Optional.of("output2"), aggregationFunction("sum", true, ImmutableList.of(symbol("input2Symbol2")))),
                                                Optional.empty(),
                                                SINGLE,
                                                union(
                                                        filter(
                                                                new Comparison(GREATER_THAN, new Reference(BIGINT, "input1_1_2Symbol"), new Constant(BIGINT, 0L)),
                                                                tableScan(
                                                                        TABLE_SCHEMA.getTableName(),
                                                                        ImmutableMap.of(
                                                                                "input1_1_2Symbol", COLUMN_1,
                                                                                "input2_1_2Symbol", COLUMN_2,
                                                                                "right_groupingKey1", GROUPING_KEY_COLUMN))),
                                                        filter(
                                                                new Comparison(GREATER_THAN, new Reference(BIGINT, "input2_2_2Symbol"), new Constant(BIGINT, 2L)),
                                                                tableScan(
                                                                        TABLE_SCHEMA.getTableName(),
                                                                        ImmutableMap.of(
                                                                                "input1_2_2Symbol", COLUMN_1,
                                                                                "input2_2_2Symbol", COLUMN_2,
                                                                                "right_groupingKey2", GROUPING_KEY_COLUMN))))
                                                        .withAlias("input1Symbol2", new SetOperationOutputMatcher(0))
                                                        .withAlias("input2Symbol2", new SetOperationOutputMatcher(1))
                                                        .withAlias("right_groupingKey", new SetOperationOutputMatcher(2)))))));
    }

    private static TableHandle testTableHandle(RuleTester ruleTester)
    {
        return new TableHandle(ruleTester.getCurrentCatalogHandle(), new MockConnectorTableHandle(TABLE_SCHEMA, TupleDomain.all(), Optional.empty()), TestingTransactionHandle.create());
    }

    private static RuleTester tester(boolean allowSplittingReadIntoMultipleSubQueries)
    {
        PlanTester planTester = PlanTester.create(MOCK_SESSION);
        MockConnectorFactory.Builder builder = MockConnectorFactory.builder()
                .withAllowSplittingReadIntoMultipleSubQueries(allowSplittingReadIntoMultipleSubQueries)
                .withGetTableHandle((_, schemaTableName) -> new MockConnectorTableHandle(schemaTableName))
                .withGetColumns(_ -> ALL_COLUMNS);
        planTester.createCatalog(
                MOCK_CATALOG,
                builder.build(),
                ImmutableMap.of());
        return new RuleTester(planTester);
    }
}
