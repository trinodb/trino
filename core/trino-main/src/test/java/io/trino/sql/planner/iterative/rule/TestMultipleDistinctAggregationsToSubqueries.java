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
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.cost.TaskCountEstimator;
import io.trino.metadata.AnalyzeMetadata;
import io.trino.metadata.AnalyzeTableHandle;
import io.trino.metadata.CatalogFunctionMetadata;
import io.trino.metadata.CatalogInfo;
import io.trino.metadata.InsertTableHandle;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.MergeHandle;
import io.trino.metadata.Metadata;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.OutputTableHandle;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.ResolvedIndex;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableFunctionHandle;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableLayout;
import io.trino.metadata.TableMetadata;
import io.trino.metadata.TableProperties;
import io.trino.metadata.TableSchema;
import io.trino.metadata.TableVersion;
import io.trino.metadata.ViewDefinition;
import io.trino.metadata.ViewInfo;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.assertions.SetOperationOutputMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.PlanTester;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.UnaryOperator;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.symbol;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.union;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.AggregationNode.groupingSets;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.PlanTesterBuilder.planTesterBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public class TestMultipleDistinctAggregationsToSubqueries
        extends BaseRuleTest
{
    private static final String COLUMN_1 = "orderkey";
    private static final ColumnHandle COLUMN_1_HANDLE = new TpchColumnHandle(COLUMN_1, BIGINT);
    private static final String COLUMN_2 = "partkey";
    private static final ColumnHandle COLUMN_2_HANDLE = new TpchColumnHandle(COLUMN_2, BIGINT);
    private static final String COLUMN_3 = "linenumber";
    private static final ColumnHandle COLUMN_3_HANDLE = new TpchColumnHandle(COLUMN_3, BIGINT);

    private static final String COLUMN_4 = "shipdate";
    private static final ColumnHandle COLUMN_4_HANDLE = new TpchColumnHandle(COLUMN_4, DATE);
    private static final String GROUPING_KEY_COLUMN = "suppkey";
    private static final ColumnHandle GROUPING_KEY_COLUMN_HANDLE = new TpchColumnHandle(GROUPING_KEY_COLUMN, BIGINT);
    private static final String GROUPING_KEY2_COLUMN = "comment";
    private static final ColumnHandle GROUPING_KEY2_COLUMN_HANDLE = new TpchColumnHandle(GROUPING_KEY2_COLUMN, VARCHAR);
    private static final String TABLE_NAME = "lineitem";

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
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
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
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol inputSymbol = p.symbol("inputSymbol");
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("inputSymbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(
                                            testTableHandle(ruleTester),
                                            ImmutableList.of(inputSymbol),
                                            ImmutableMap.of(inputSymbol, COLUMN_1_HANDLE))));
                })
                .doesNotFire();

        // two distinct on the same input
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(
                                            testTableHandle(ruleTester),
                                            ImmutableList.of(input1Symbol),
                                            ImmutableMap.of(
                                                    input1Symbol, COLUMN_1_HANDLE))));
                })
                .doesNotFire();

        // hash symbol
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
                            .hashSymbol(p.symbol("hashSymbol"))
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
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output3"), PlanBuilder.aggregation("count", ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
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
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    return p.aggregation(builder -> builder
                            .groupingSets(groupingSets(ImmutableList.of(), 2, ImmutableSet.of(0, 1)))
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
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
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
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
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.filter(
                                            expression("true"),
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

        ruleTesterNotObjectStore.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTesterNotObjectStore))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
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
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "single_step")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(
                                            testTableHandle(ruleTester),
                                            ImmutableList.of(input1Symbol, input2Symbol),
                                            ImmutableMap.of(
                                                    input1Symbol, COLUMN_1_HANDLE,
                                                    input2Symbol, COLUMN_2_HANDLE))));
                })
                .doesNotFire();

        // automatic but single_step is preferred
        String aggregationSourceId = "aggregationSourceId";
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "automatic")
                .overrideStats(aggregationSourceId, PlanNodeStatsEstimate.builder().addSymbolStatistics(
                        new Symbol("groupingKey"), SymbolStatsEstimate.builder().setDistinctValuesCount(1_000_000).build()).build())
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(p.symbol("groupingKey"))
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(tableScan -> tableScan
                                            .setNodeId(new PlanNodeId(aggregationSourceId))
                                            .setTableHandle(testTableHandle(ruleTester))
                                            .setSymbols(ImmutableList.of(input1Symbol, input2Symbol))
                                            .setAssignments(ImmutableMap.of(
                                                    input1Symbol, COLUMN_1_HANDLE,
                                                    input2Symbol, COLUMN_2_HANDLE)))));
                })
                .doesNotFire();
    }

    @Test
    public void testAutomaticDecisionForAggregationOnTableScan()
    {
        // automatic but single_step is preferred
        String aggregationSourceId = "aggregationSourceId";
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "automatic")
                .overrideStats(aggregationSourceId, PlanNodeStatsEstimate.builder().addSymbolStatistics(
                        new Symbol("groupingKey"), SymbolStatsEstimate.builder().setDistinctValuesCount(1_000_000).build()).build())
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(p.symbol("groupingKey"))
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(tableScan -> tableScan
                                            .setNodeId(new PlanNodeId(aggregationSourceId))
                                            .setTableHandle(testTableHandle(ruleTester))
                                            .setSymbols(ImmutableList.of(input1Symbol, input2Symbol))
                                            .setAssignments(ImmutableMap.of(
                                                    input1Symbol, COLUMN_1_HANDLE,
                                                    input2Symbol, COLUMN_2_HANDLE)))));
                })
                .doesNotFire();

        // single_step is not preferred, the overhead of groupingKey is not big
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "automatic")
                .overrideStats(aggregationSourceId, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(new Symbol("groupingKey"), SymbolStatsEstimate.builder().setDistinctValuesCount(10).build()).build())
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    Symbol groupingKey = p.symbol("groupingKey");
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(groupingKey)
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(tableScan -> tableScan
                                            .setNodeId(new PlanNodeId(aggregationSourceId))
                                            .setTableHandle(testTableHandle(ruleTester))
                                            .setSymbols(ImmutableList.of(input1Symbol, input2Symbol, groupingKey))
                                            .setAssignments(ImmutableMap.of(
                                                    input1Symbol, COLUMN_1_HANDLE,
                                                    input2Symbol, COLUMN_2_HANDLE,
                                                    groupingKey, GROUPING_KEY_COLUMN_HANDLE)))));
                })
                .matches(project(
                        ImmutableMap.of(
                                "final_output1", PlanMatchPattern.expression("output1"),
                                "final_output2", PlanMatchPattern.expression("output2"),
                                "group_by_key", PlanMatchPattern.expression("left_groupingKey")),
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
                                                        TABLE_NAME,
                                                        ImmutableMap.of(
                                                                "input1Symbol", COLUMN_1,
                                                                "left_groupingKey", GROUPING_KEY_COLUMN))))
                                        .right(aggregation(
                                                singleGroupingSet("right_groupingKey"),
                                                ImmutableMap.of(Optional.of("output2"), aggregationFunction("sum", true, ImmutableList.of(symbol("input2Symbol")))),
                                                Optional.empty(),
                                                SINGLE,
                                                tableScan(
                                                        TABLE_NAME,
                                                        ImmutableMap.of(
                                                                "input2Symbol", COLUMN_2,
                                                                "right_groupingKey", GROUPING_KEY_COLUMN)))))));

        // single_step is not preferred, the overhead of groupingKeys is bigger than 50%
        String aggregationId = "aggregationId";
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "automatic")
                .overrideStats(aggregationSourceId, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(new Symbol("groupingKey"), SymbolStatsEstimate.builder().setDistinctValuesCount(10).build())
                        .addSymbolStatistics(new Symbol("groupingKey2"), SymbolStatsEstimate.builder().setAverageRowSize(1_000_000).build())
                        .build())
                .overrideStats(aggregationId, PlanNodeStatsEstimate.builder().setOutputRowCount(10).build())
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    Symbol groupingKey = p.symbol("groupingKey");
                    Symbol groupingKey2 = p.symbol("groupingKey2", VARCHAR);
                    return p.aggregation(builder -> builder
                            .nodeId(new PlanNodeId(aggregationId))
                            .singleGroupingSet(groupingKey, groupingKey2)
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.tableScan(tableScan -> tableScan
                                            .setNodeId(new PlanNodeId(aggregationSourceId))
                                            .setTableHandle(testTableHandle(ruleTester))
                                            .setSymbols(ImmutableList.of(input1Symbol, input2Symbol, groupingKey, groupingKey2))
                                            .setAssignments(ImmutableMap.of(
                                                    input1Symbol, COLUMN_1_HANDLE,
                                                    input2Symbol, COLUMN_2_HANDLE,
                                                    groupingKey, GROUPING_KEY_COLUMN_HANDLE,
                                                    groupingKey2, GROUPING_KEY2_COLUMN_HANDLE)))));
                })
                .doesNotFire();
    }

    @Test
    public void testAutomaticDecisionForAggregationOnProjectedTableScan()
    {
        String aggregationSourceId = "aggregationSourceId";
        String aggregationId = "aggregationId";
        // the overhead of the projection is bigger than 50%
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "automatic")
                .overrideStats(aggregationSourceId, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(new Symbol("projectionInput1"), SymbolStatsEstimate.builder().setDistinctValuesCount(10).build())
                        .addSymbolStatistics(new Symbol("projectionInput2"), SymbolStatsEstimate.builder().setAverageRowSize(1_000_000).build())
                        .build())
                .overrideStats(aggregationId, PlanNodeStatsEstimate.builder().setOutputRowCount(10).build())
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    Symbol groupingKey = p.symbol("groupingKey");
                    Symbol projectionInput1 = p.symbol("projectionInput1");
                    Symbol projectionInput2 = p.symbol("projectionInput2", VARCHAR);
                    return p.aggregation(builder -> builder
                            .nodeId(new PlanNodeId(aggregationId))
                            .singleGroupingSet(groupingKey)
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.project(
                                            Assignments.builder()
                                                    .putIdentity(input1Symbol)
                                                    .putIdentity(input2Symbol)
                                                    .put(groupingKey, expression("projectionInput1 + CAST(projectionInput2 as BIGINT)"))
                                                    .build(),
                                            p.tableScan(tableScan -> tableScan
                                                    .setNodeId(new PlanNodeId(aggregationSourceId))
                                                    .setTableHandle(testTableHandle(ruleTester))
                                                    .setSymbols(ImmutableList.of(input1Symbol, input2Symbol, projectionInput1, projectionInput2))
                                                    .setAssignments(ImmutableMap.of(
                                                            input1Symbol, COLUMN_1_HANDLE,
                                                            input2Symbol, COLUMN_2_HANDLE,
                                                            projectionInput1, GROUPING_KEY_COLUMN_HANDLE,
                                                            projectionInput2, GROUPING_KEY2_COLUMN_HANDLE))))));
                })
                .doesNotFire();

        // the big projection is used as distinct input. we could handle this case, but for simplicity sake, the rule won't fire here
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "automatic")
                .overrideStats(aggregationSourceId, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(new Symbol("projectionInput1"), SymbolStatsEstimate.builder().setDistinctValuesCount(10).build())
                        .addSymbolStatistics(new Symbol("projectionInput2"), SymbolStatsEstimate.builder().setAverageRowSize(1_000_000).build())
                        .build())
                .overrideStats(aggregationId, PlanNodeStatsEstimate.builder().setOutputRowCount(10).build())
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    Symbol groupingKey = p.symbol("groupingKey");
                    Symbol projectionInput1 = p.symbol("projectionInput1");
                    Symbol projectionInput2 = p.symbol("projectionInput2", VARCHAR);
                    return p.aggregation(builder -> builder
                            .nodeId(new PlanNodeId(aggregationId))
                            .singleGroupingSet(groupingKey)
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.project(
                                            Assignments.builder()
                                                    .put(input1Symbol, expression("projectionInput1 + CAST(projectionInput2 as BIGINT)"))
                                                    .putIdentity(input2Symbol)
                                                    .putIdentity(groupingKey)
                                                    .build(),
                                            p.tableScan(tableScan -> tableScan
                                                    .setNodeId(new PlanNodeId(aggregationSourceId))
                                                    .setTableHandle(testTableHandle(ruleTester))
                                                    .setSymbols(ImmutableList.of(groupingKey, input2Symbol, projectionInput1, projectionInput2))
                                                    .setAssignments(ImmutableMap.of(
                                                            groupingKey, COLUMN_1_HANDLE,
                                                            input2Symbol, COLUMN_2_HANDLE,
                                                            projectionInput1, GROUPING_KEY_COLUMN_HANDLE,
                                                            projectionInput2, GROUPING_KEY2_COLUMN_HANDLE))))));
                })
                .doesNotFire();
    }

    @Test
    public void testAutomaticDecisionForAggregationOnFilteredTableScan()
    {
        String aggregationSourceId = "aggregationSourceId";
        String aggregationId = "aggregationId";
        String filterId = "filterId";
        // selective filter
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "automatic")
                .overrideStats(aggregationSourceId, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(new Symbol("filterInput"), SymbolStatsEstimate.builder().setAverageRowSize(1).build())
                        .build())
                .overrideStats(filterId, PlanNodeStatsEstimate.builder().setOutputRowCount(1).build())
                .overrideStats(aggregationId, PlanNodeStatsEstimate.builder().setOutputRowCount(1).build())
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    Symbol groupingKey = p.symbol("groupingKey");
                    Symbol filterInput = p.symbol("filterInput", VARCHAR);

                    return p.aggregation(builder -> builder
                            .nodeId(new PlanNodeId(aggregationId))
                            .singleGroupingSet(groupingKey)
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.filter(
                                            new PlanNodeId(filterId),
                                            expression("filterInput IS NOT NULL"),
                                            p.tableScan(tableScan -> tableScan
                                                    .setNodeId(new PlanNodeId(aggregationSourceId))
                                                    .setTableHandle(testTableHandle(ruleTester))
                                                    .setSymbols(ImmutableList.of(input1Symbol, input2Symbol, groupingKey, filterInput))
                                                    .setAssignments(ImmutableMap.of(
                                                            input1Symbol, COLUMN_1_HANDLE,
                                                            input2Symbol, COLUMN_2_HANDLE,
                                                            groupingKey, GROUPING_KEY_COLUMN_HANDLE,
                                                            filterInput, GROUPING_KEY2_COLUMN_HANDLE))))));
                })
                .doesNotFire();

        // non-selective filter
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "automatic")
                .overrideStats(aggregationSourceId, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(new Symbol("filterInput"), SymbolStatsEstimate.builder().setAverageRowSize(1).build())
                        .build())
                .overrideStats(filterId, PlanNodeStatsEstimate.builder().setOutputRowCount(100).build())
                .overrideStats(aggregationId, PlanNodeStatsEstimate.builder().setOutputRowCount(100).build())
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    Symbol groupingKey = p.symbol("groupingKey");
                    Symbol filterInput = p.symbol("filterInput", VARCHAR);

                    return p.aggregation(builder -> builder
                            .nodeId(new PlanNodeId(aggregationId))
                            .singleGroupingSet(groupingKey)
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.filter(
                                            new PlanNodeId(filterId),
                                            expression("filterInput IS NOT NULL"),
                                            p.tableScan(tableScan -> tableScan
                                                    .setNodeId(new PlanNodeId(aggregationSourceId))
                                                    .setTableHandle(testTableHandle(ruleTester))
                                                    .setSymbols(ImmutableList.of(input1Symbol, input2Symbol, groupingKey, filterInput))
                                                    .setAssignments(ImmutableMap.of(
                                                            input1Symbol, COLUMN_1_HANDLE,
                                                            input2Symbol, COLUMN_2_HANDLE,
                                                            groupingKey, GROUPING_KEY_COLUMN_HANDLE,
                                                            filterInput, GROUPING_KEY2_COLUMN_HANDLE))))));
                })
                .matches(project(
                        ImmutableMap.of(
                                "final_output1", PlanMatchPattern.expression("output1"),
                                "final_output2", PlanMatchPattern.expression("output2"),
                                "group_by_key", PlanMatchPattern.expression("left_groupingKey")),
                        join(
                                INNER,
                                builder -> builder
                                        .equiCriteria("left_groupingKey", "right_groupingKey")
                                        .left(aggregation(
                                                singleGroupingSet("left_groupingKey"),
                                                ImmutableMap.of(Optional.of("output1"), aggregationFunction("count", true, ImmutableList.of(symbol("input1Symbol")))),
                                                Optional.empty(),
                                                SINGLE,
                                                filter(
                                                        "left_filterInput IS NOT NULL",
                                                        tableScan(
                                                                TABLE_NAME,
                                                                ImmutableMap.of(
                                                                        "input1Symbol", COLUMN_1,
                                                                        "left_groupingKey", GROUPING_KEY_COLUMN,
                                                                        "left_filterInput", GROUPING_KEY2_COLUMN)))))
                                        .right(aggregation(
                                                singleGroupingSet("right_groupingKey"),
                                                ImmutableMap.of(Optional.of("output2"), aggregationFunction("sum", true, ImmutableList.of(symbol("input2Symbol")))),
                                                Optional.empty(),
                                                SINGLE,
                                                filter(
                                                        "right_filterInput IS NOT NULL",
                                                        tableScan(
                                                                TABLE_NAME,
                                                                ImmutableMap.of(
                                                                        "input2Symbol", COLUMN_2,
                                                                        "right_groupingKey", GROUPING_KEY_COLUMN,
                                                                        "right_filterInput", GROUPING_KEY2_COLUMN))))))));
    }

    @Test
    public void testAutomaticDecisionForAggregationOnFilteredUnion()
    {
        String aggregationSourceId = "aggregationSourceId";
        String aggregationId = "aggregationId";
        String filterId = "filterId";
        // union with additional columns to read
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "automatic")
                .overrideStats(aggregationSourceId, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(new Symbol("filterInput"), SymbolStatsEstimate.builder().setAverageRowSize(1).build())
                        .build())
                .overrideStats(filterId, PlanNodeStatsEstimate.builder().setOutputRowCount(100).build())
                .overrideStats(aggregationId, PlanNodeStatsEstimate.builder().setOutputRowCount(100).build())
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input11Symbol = p.symbol("input1_1Symbol");
                    Symbol input12Symbol = p.symbol("input1_2Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    Symbol input21Symbol = p.symbol("input2_1Symbol");
                    Symbol input22Symbol = p.symbol("input2_2Symbol");
                    Symbol groupingKey = p.symbol("groupingKey");
                    Symbol groupingKey1 = p.symbol("groupingKey1");
                    Symbol groupingKey2 = p.symbol("groupingKey2");

                    return p.aggregation(builder -> builder
                            .nodeId(new PlanNodeId(aggregationId))
                            .singleGroupingSet(groupingKey)
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
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
                                                            expression("input1_1Symbol > 0"),
                                                            p.tableScan(
                                                                    testTableHandle(ruleTester),
                                                                    ImmutableList.of(input11Symbol, input21Symbol, groupingKey1),
                                                                    ImmutableMap.of(
                                                                            input11Symbol, COLUMN_1_HANDLE,
                                                                            input21Symbol, COLUMN_2_HANDLE,
                                                                            groupingKey1, GROUPING_KEY_COLUMN_HANDLE))),
                                                    p.filter(
                                                            expression("input2_2Symbol > 2"),
                                                            p.tableScan(
                                                                    testTableHandle(ruleTester),
                                                                    ImmutableList.of(input12Symbol, input22Symbol, groupingKey2),
                                                                    ImmutableMap.of(
                                                                            input12Symbol, COLUMN_1_HANDLE,
                                                                            input22Symbol, COLUMN_2_HANDLE,
                                                                            groupingKey2, GROUPING_KEY_COLUMN_HANDLE)))))));
                })
                .doesNotFire();
    }

    @Test
    public void testGlobalDistinctToSubqueries()
    {
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
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
                                "final_output1", PlanMatchPattern.expression("output1"),
                                "final_output2", PlanMatchPattern.expression("output2")),
                        join(
                                INNER,
                                builder -> builder
                                        .left(aggregation(
                                                ImmutableMap.of("output1", aggregationFunction("count", true, ImmutableList.of(symbol("input1Symbol")))),
                                                tableScan(TABLE_NAME, ImmutableMap.of("input1Symbol", COLUMN_1))))
                                        .right(aggregation(
                                                ImmutableMap.of("output2", aggregationFunction("sum", true, ImmutableList.of(symbol("input2Symbol")))),
                                                tableScan(TABLE_NAME, ImmutableMap.of("input2Symbol", COLUMN_2)))))));
    }

    @Test
    public void testGlobalWith3DistinctToSubqueries()
    {
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    Symbol input3Symbol = p.symbol("input3Symbol");
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output3"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input3Symbol"))), ImmutableList.of(BIGINT))
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
                                "final_output1", PlanMatchPattern.expression("output1"),
                                "final_output2", PlanMatchPattern.expression("output2"),
                                "final_output3", PlanMatchPattern.expression("output3")),
                        join(
                                INNER,
                                join -> join
                                        .left(aggregation(
                                                ImmutableMap.of("output1", aggregationFunction("count", true, ImmutableList.of(symbol("input1Symbol")))),
                                                tableScan(TABLE_NAME, ImmutableMap.of("input1Symbol", COLUMN_1))))
                                        .right(join(
                                                INNER,
                                                subJoin -> subJoin
                                                        .left(aggregation(
                                                                ImmutableMap.of("output2", aggregationFunction("sum", true, ImmutableList.of(symbol("input2Symbol")))),
                                                                tableScan(TABLE_NAME, ImmutableMap.of("input2Symbol", COLUMN_2))))
                                                        .right(aggregation(
                                                                ImmutableMap.of("output3", aggregationFunction("count", true, ImmutableList.of(symbol("input3Symbol")))),
                                                                tableScan(TABLE_NAME, ImmutableMap.of("input3Symbol", COLUMN_3)))))))));
    }

    // tests right deep join hierarchy
    @Test
    public void testGlobalWith4DistinctToSubqueries()
    {
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    Symbol input3Symbol = p.symbol("input3Symbol");
                    Symbol input4Symbol = p.symbol("input4Symbol");
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output3"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input3Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output4"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input4Symbol"))), ImmutableList.of(BIGINT))
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
                                "final_output1", PlanMatchPattern.expression("output1"),
                                "final_output2", PlanMatchPattern.expression("output2"),
                                "final_output3", PlanMatchPattern.expression("output3"),
                                "final_output4", PlanMatchPattern.expression("output4")),
                        join(
                                INNER,
                                join -> join
                                        .left(aggregation(
                                                ImmutableMap.of("output1", aggregationFunction("count", true, ImmutableList.of(symbol("input1Symbol")))),
                                                tableScan(TABLE_NAME, ImmutableMap.of("input1Symbol", COLUMN_1))))
                                        .right(join(
                                                INNER,
                                                subJoin -> subJoin
                                                        .left(aggregation(
                                                                ImmutableMap.of("output2", aggregationFunction("count", true, ImmutableList.of(symbol("input2Symbol")))),
                                                                tableScan(TABLE_NAME, ImmutableMap.of("input2Symbol", COLUMN_2))))
                                                        .right(join(
                                                                INNER,
                                                                subJoin2 -> subJoin2
                                                                        .left(aggregation(
                                                                                ImmutableMap.of("output3", aggregationFunction("count", true, ImmutableList.of(symbol("input3Symbol")))),
                                                                                tableScan(TABLE_NAME, ImmutableMap.of("input3Symbol", COLUMN_3))))
                                                                        .right(aggregation(
                                                                                ImmutableMap.of("output4", aggregationFunction("count", true, ImmutableList.of(symbol("input4Symbol")))),
                                                                                tableScan(TABLE_NAME, ImmutableMap.of("input4Symbol", COLUMN_4)))))))))));
    }

    @Test
    public void testGlobal2DistinctOnTheSameInputtoSubqueries()
    {
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output3"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
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
                                "final_output1", PlanMatchPattern.expression("output1"),
                                "final_output2", PlanMatchPattern.expression("output2"),
                                "final_output3", PlanMatchPattern.expression("output3")),
                        join(
                                INNER,
                                builder -> builder
                                        .left(aggregation(
                                                ImmutableMap.of("output1", aggregationFunction("count", true, ImmutableList.of(symbol("input1Symbol")))),
                                                tableScan(TABLE_NAME, ImmutableMap.of("input1Symbol", COLUMN_1))))
                                        .right(aggregation(
                                                ImmutableMap.of(
                                                        "output2", aggregationFunction("sum", true, ImmutableList.of(symbol("input2Symbol"))),
                                                        "output3", aggregationFunction("count", true, ImmutableList.of(symbol("input2Symbol")))),
                                                tableScan(TABLE_NAME, ImmutableMap.of("input2Symbol", COLUMN_2)))))));
    }

    @Test
    public void testGroupByWithDistinctToSubqueries()
    {
        String aggregationNodeId = "aggregationNodeId";
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .overrideStats(aggregationNodeId, PlanNodeStatsEstimate.builder().setOutputRowCount(100_000).build())
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    Symbol groupingKey = p.symbol("groupingKey");
                    return p.aggregation(builder -> builder
                            .nodeId(new PlanNodeId(aggregationNodeId))
                            .singleGroupingSet(groupingKey)
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
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
                                "final_output1", PlanMatchPattern.expression("output1"),
                                "final_output2", PlanMatchPattern.expression("output2"),
                                "group_by_key", PlanMatchPattern.expression("left_groupingKey")),
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
                                                        TABLE_NAME,
                                                        ImmutableMap.of(
                                                                "input1Symbol", COLUMN_1,
                                                                "left_groupingKey", GROUPING_KEY_COLUMN))))
                                        .right(aggregation(
                                                singleGroupingSet("right_groupingKey"),
                                                ImmutableMap.of(Optional.of("output2"), aggregationFunction("sum", true, ImmutableList.of(symbol("input2Symbol")))),
                                                Optional.empty(),
                                                SINGLE,
                                                tableScan(
                                                        TABLE_NAME,
                                                        ImmutableMap.of(
                                                                "input2Symbol", COLUMN_2,
                                                                "right_groupingKey", GROUPING_KEY_COLUMN)))))));
    }

    @Test
    public void testGroupByWithDistinctOverUnionToSubqueries()
    {
        String aggregationNodeId = "aggregationNodeId";
        ruleTester.assertThat(newMultipleDistinctAggregationsToSubqueries(ruleTester))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .overrideStats(aggregationNodeId, PlanNodeStatsEstimate.builder().setOutputRowCount(100_000).build())
                .on(p -> {
                    Symbol input1Symbol = p.symbol("input1Symbol");
                    Symbol input11Symbol = p.symbol("input1_1Symbol");
                    Symbol input12Symbol = p.symbol("input1_2Symbol");
                    Symbol input2Symbol = p.symbol("input2Symbol");
                    Symbol input21Symbol = p.symbol("input2_1Symbol");
                    Symbol input22Symbol = p.symbol("input2_2Symbol");
                    Symbol groupingKey = p.symbol("groupingKey");
                    Symbol groupingKey1 = p.symbol("groupingKey1");
                    Symbol groupingKey2 = p.symbol("groupingKey2");

                    return p.aggregation(builder -> builder
                            .nodeId(new PlanNodeId(aggregationNodeId))
                            .singleGroupingSet(groupingKey)
                            .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1Symbol"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input2Symbol"))), ImmutableList.of(BIGINT))
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
                                                            expression("input1_1Symbol > 0"),
                                                            p.tableScan(
                                                                    testTableHandle(ruleTester),
                                                                    ImmutableList.of(input11Symbol, input21Symbol, groupingKey1),
                                                                    ImmutableMap.of(
                                                                            input11Symbol, COLUMN_1_HANDLE,
                                                                            input21Symbol, COLUMN_2_HANDLE,
                                                                            groupingKey1, GROUPING_KEY_COLUMN_HANDLE))),
                                                    p.filter(
                                                            expression("input2_2Symbol > 2"),
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
                                "final_output1", PlanMatchPattern.expression("output1"),
                                "final_output2", PlanMatchPattern.expression("output2"),
                                "group_by_key", PlanMatchPattern.expression("left_groupingKey")),
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
                                                                "input1_1_1Symbol > 0",
                                                                tableScan(
                                                                        TABLE_NAME,
                                                                        ImmutableMap.of(
                                                                                "input1_1_1Symbol", COLUMN_1,
                                                                                "input2_1_1Symbol", COLUMN_2,
                                                                                "left_groupingKey1", GROUPING_KEY_COLUMN))),
                                                        filter(
                                                                "input2_2_1Symbol > 2",
                                                                tableScan(
                                                                        TABLE_NAME,
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
                                                                "input1_1_2Symbol > 0",
                                                                tableScan(
                                                                        TABLE_NAME,
                                                                        ImmutableMap.of(
                                                                                "input1_1_2Symbol", COLUMN_1,
                                                                                "input2_1_2Symbol", COLUMN_2,
                                                                                "right_groupingKey1", GROUPING_KEY_COLUMN))),
                                                        filter(
                                                                "input2_2_2Symbol > 2",
                                                                tableScan(
                                                                        TABLE_NAME,
                                                                        ImmutableMap.of(
                                                                                "input1_2_2Symbol", COLUMN_1,
                                                                                "input2_2_2Symbol", COLUMN_2,
                                                                                "right_groupingKey2", GROUPING_KEY_COLUMN))))
                                                        .withAlias("input1Symbol2", new SetOperationOutputMatcher(0))
                                                        .withAlias("input2Symbol2", new SetOperationOutputMatcher(1))
                                                        .withAlias("right_groupingKey", new SetOperationOutputMatcher(2)))))));
    }

    private static MultipleDistinctAggregationsToSubqueries newMultipleDistinctAggregationsToSubqueries(RuleTester ruleTester)
    {
        return new MultipleDistinctAggregationsToSubqueries(ruleTester.getMetadata(), new DistinctAggregationController(
                new TaskCountEstimator(() -> Integer.MAX_VALUE),
                ruleTester.getMetadata()));
    }

    private static TableHandle testTableHandle(RuleTester ruleTester)
    {
        return new TableHandle(ruleTester.getCurrentCatalogHandle(), new TpchTableHandle("sf1", TABLE_NAME, 1.0), TestingTransactionHandle.create());
    }

    private static RuleTester tester(boolean connectorSupportsSingleColumnReads)
    {
        Session session = testSessionBuilder().build();
        PlanTester planTester = planTesterBuilder(session)
                .withMetadataDecorator(metadata -> new DelegatingMetadata(metadata)
                {
                    @Override
                    public boolean isColumnarTableScan(Session session, TableHandle tableHandle)
                    {
                        return connectorSupportsSingleColumnReads;
                    }
                })
                .build();
        planTester.createCatalog(
                session.getCatalog().orElseThrow(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
        return new RuleTester(planTester);
    }

    public static class DelegatingMetadata
            implements Metadata
    {
        private final Metadata metadata;

        public DelegatingMetadata(Metadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, CatalogHandle catalogHandle)
        {
            return metadata.getConnectorCapabilities(session, catalogHandle);
        }

        @Override
        public boolean catalogExists(Session session, String catalogName)
        {
            return metadata.catalogExists(session, catalogName);
        }

        @Override
        public boolean schemaExists(Session session, CatalogSchemaName schema)
        {
            return metadata.schemaExists(session, schema);
        }

        @Override
        public List<String> listSchemaNames(Session session, String catalogName)
        {
            return metadata.listSchemaNames(session, catalogName);
        }

        @Override
        public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName)
        {
            return metadata.getTableHandle(session, tableName);
        }

        @Override
        public Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName)
        {
            return metadata.getSystemTable(session, tableName);
        }

        @Override
        public Optional<TableExecuteHandle> getTableHandleForExecute(Session session, TableHandle tableHandle, String procedureName, Map<String, Object> executeProperties)
        {
            return metadata.getTableHandleForExecute(session, tableHandle, procedureName, executeProperties);
        }

        @Override
        public Optional<TableLayout> getLayoutForTableExecute(Session session, TableExecuteHandle tableExecuteHandle)
        {
            return metadata.getLayoutForTableExecute(session, tableExecuteHandle);
        }

        @Override
        public BeginTableExecuteResult<TableExecuteHandle, TableHandle> beginTableExecute(Session session, TableExecuteHandle handle, TableHandle updatedSourceTableHandle)
        {
            return metadata.beginTableExecute(session, handle, updatedSourceTableHandle);
        }

        @Override
        public void finishTableExecute(Session session, TableExecuteHandle handle, Collection<Slice> fragments, List<Object> tableExecuteState)
        {
            metadata.finishTableExecute(session, handle, fragments, tableExecuteState);
        }

        @Override
        public void executeTableExecute(Session session, TableExecuteHandle handle)
        {
            metadata.executeTableExecute(session, handle);
        }

        @Override
        public TableProperties getTableProperties(Session session, TableHandle handle)
        {
            return metadata.getTableProperties(session, handle);
        }

        @Override
        public TableHandle makeCompatiblePartitioning(Session session, TableHandle table, PartitioningHandle partitioningHandle)
        {
            return metadata.makeCompatiblePartitioning(session, table, partitioningHandle);
        }

        @Override
        public Optional<PartitioningHandle> getCommonPartitioning(Session session, PartitioningHandle left, PartitioningHandle right)
        {
            return metadata.getCommonPartitioning(session, left, right);
        }

        @Override
        public Optional<Object> getInfo(Session session, TableHandle handle)
        {
            return metadata.getInfo(session, handle);
        }

        @Override
        public CatalogSchemaTableName getTableName(Session session, TableHandle tableHandle)
        {
            return metadata.getTableName(session, tableHandle);
        }

        @Override
        public TableSchema getTableSchema(Session session, TableHandle tableHandle)
        {
            return metadata.getTableSchema(session, tableHandle);
        }

        @Override
        public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
        {
            return metadata.getTableMetadata(session, tableHandle);
        }

        @Override
        public TableStatistics getTableStatistics(Session session, TableHandle tableHandle)
        {
            return metadata.getTableStatistics(session, tableHandle);
        }

        @Override
        public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
        {
            return metadata.listTables(session, prefix);
        }

        @Override
        public Map<SchemaTableName, RelationType> getRelationTypes(Session session, QualifiedTablePrefix prefix)
        {
            return metadata.getRelationTypes(session, prefix);
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
        {
            return metadata.getColumnHandles(session, tableHandle);
        }

        @Override
        public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            return metadata.getColumnMetadata(session, tableHandle, columnHandle);
        }

        @Override
        public List<TableColumnsMetadata> listTableColumns(Session session, QualifiedTablePrefix prefix, UnaryOperator<Set<SchemaTableName>> relationFilter)
        {
            return metadata.listTableColumns(session, prefix, relationFilter);
        }

        @Override
        public List<RelationCommentMetadata> listRelationComments(Session session, String catalogName, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
        {
            return metadata.listRelationComments(session, catalogName, schemaName, relationFilter);
        }

        @Override
        public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties, TrinoPrincipal principal)
        {
            metadata.createSchema(session, schema, properties, principal);
        }

        @Override
        public void dropSchema(Session session, CatalogSchemaName schema, boolean cascade)
        {
            metadata.dropSchema(session, schema, cascade);
        }

        @Override
        public void renameSchema(Session session, CatalogSchemaName source, String target)
        {
            metadata.renameSchema(session, source, target);
        }

        @Override
        public void setSchemaAuthorization(Session session, CatalogSchemaName source, TrinoPrincipal principal)
        {
            metadata.setSchemaAuthorization(session, source, principal);
        }

        @Override
        public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
        {
            metadata.createTable(session, catalogName, tableMetadata, saveMode);
        }

        @Override
        public void renameTable(Session session, TableHandle tableHandle, CatalogSchemaTableName currentTableName, QualifiedObjectName newTableName)
        {
            metadata.renameTable(session, tableHandle, currentTableName, newTableName);
        }

        @Override
        public void setTableProperties(Session session, TableHandle tableHandle, Map<String, Optional<Object>> properties)
        {
            metadata.setTableProperties(session, tableHandle, properties);
        }

        @Override
        public void setTableComment(Session session, TableHandle tableHandle, Optional<String> comment)
        {
            metadata.setTableComment(session, tableHandle, comment);
        }

        @Override
        public void setViewComment(Session session, QualifiedObjectName viewName, Optional<String> comment)
        {
            metadata.setViewComment(session, viewName, comment);
        }

        @Override
        public void setViewColumnComment(Session session, QualifiedObjectName viewName, String columnName, Optional<String> comment)
        {
            metadata.setViewColumnComment(session, viewName, columnName, comment);
        }

        @Override
        public void setColumnComment(Session session, TableHandle tableHandle, ColumnHandle column, Optional<String> comment)
        {
            metadata.setColumnComment(session, tableHandle, column, comment);
        }

        @Override
        public void renameColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnHandle source, String target)
        {
            metadata.renameColumn(session, tableHandle, table, source, target);
        }

        @Override
        public void renameField(Session session, TableHandle tableHandle, List<String> fieldPath, String target)
        {
            metadata.renameField(session, tableHandle, fieldPath, target);
        }

        @Override
        public void addColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnMetadata column)
        {
            metadata.addColumn(session, tableHandle, table, column);
        }

        @Override
        public void addField(Session session, TableHandle tableHandle, List<String> parentPath, String fieldName, Type type, boolean ignoreExisting)
        {
            metadata.addField(session, tableHandle, parentPath, fieldName, type, ignoreExisting);
        }

        @Override
        public void setColumnType(Session session, TableHandle tableHandle, ColumnHandle column, Type type)
        {
            metadata.setColumnType(session, tableHandle, column, type);
        }

        @Override
        public void setFieldType(Session session, TableHandle tableHandle, List<String> fieldPath, Type type)
        {
            metadata.setFieldType(session, tableHandle, fieldPath, type);
        }

        @Override
        public void dropNotNullConstraint(Session session, TableHandle tableHandle, ColumnHandle column)
        {
            metadata.dropNotNullConstraint(session, tableHandle, column);
        }

        @Override
        public void setTableAuthorization(Session session, CatalogSchemaTableName table, TrinoPrincipal principal)
        {
            metadata.setTableAuthorization(session, table, principal);
        }

        @Override
        public void dropColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnHandle column)
        {
            metadata.dropColumn(session, tableHandle, table, column);
        }

        @Override
        public void dropField(Session session, TableHandle tableHandle, ColumnHandle column, List<String> fieldPath)
        {
            metadata.dropField(session, tableHandle, column, fieldPath);
        }

        @Override
        public void dropTable(Session session, TableHandle tableHandle, CatalogSchemaTableName tableName)
        {
            metadata.dropTable(session, tableHandle, tableName);
        }

        @Override
        public void truncateTable(Session session, TableHandle tableHandle)
        {
            metadata.truncateTable(session, tableHandle);
        }

        @Override
        public Optional<TableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
        {
            return metadata.getNewTableLayout(session, catalogName, tableMetadata);
        }

        @Override
        public Optional<Type> getSupportedType(Session session, CatalogHandle catalogHandle, Map<String, Object> tableProperties, Type type)
        {
            return metadata.getSupportedType(session, catalogHandle, tableProperties, type);
        }

        @Override
        public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<TableLayout> layout, boolean replace)
        {
            return metadata.beginCreateTable(session, catalogName, tableMetadata, layout, replace);
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
        {
            return metadata.finishCreateTable(session, tableHandle, fragments, computedStatistics);
        }

        @Override
        public Optional<TableLayout> getInsertLayout(Session session, TableHandle target)
        {
            return metadata.getInsertLayout(session, target);
        }

        @Override
        public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, CatalogHandle catalogHandle, ConnectorTableMetadata tableMetadata)
        {
            return metadata.getStatisticsCollectionMetadataForWrite(session, catalogHandle, tableMetadata);
        }

        @Override
        public AnalyzeMetadata getStatisticsCollectionMetadata(Session session, TableHandle tableHandle, Map<String, Object> analyzeProperties)
        {
            return metadata.getStatisticsCollectionMetadata(session, tableHandle, analyzeProperties);
        }

        @Override
        public AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle)
        {
            return metadata.beginStatisticsCollection(session, tableHandle);
        }

        @Override
        public void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
        {
            metadata.finishStatisticsCollection(session, tableHandle, computedStatistics);
        }

        @Override
        public void beginQuery(Session session)
        {
            metadata.beginQuery(session);
        }

        @Override
        public void cleanupQuery(Session session)
        {
            metadata.cleanupQuery(session);
        }

        @Override
        public InsertTableHandle beginInsert(Session session, TableHandle tableHandle, List<ColumnHandle> columns)
        {
            return metadata.beginInsert(session, tableHandle, columns);
        }

        @Override
        public boolean supportsMissingColumnsOnInsert(Session session, TableHandle tableHandle)
        {
            return metadata.supportsMissingColumnsOnInsert(session, tableHandle);
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, List<TableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
        {
            return metadata.finishInsert(session, tableHandle, sourceTableHandles, fragments, computedStatistics);
        }

        @Override
        public boolean delegateMaterializedViewRefreshToConnector(Session session, QualifiedObjectName viewName)
        {
            return metadata.delegateMaterializedViewRefreshToConnector(session, viewName);
        }

        @Override
        public ListenableFuture<Void> refreshMaterializedView(Session session, QualifiedObjectName viewName)
        {
            return metadata.refreshMaterializedView(session, viewName);
        }

        @Override
        public InsertTableHandle beginRefreshMaterializedView(Session session, TableHandle tableHandle, List<TableHandle> sourceTableHandles)
        {
            return metadata.beginRefreshMaterializedView(session, tableHandle, sourceTableHandles);
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(Session session, TableHandle tableHandle, InsertTableHandle insertTableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics, List<TableHandle> sourceTableHandles, List<String> sourceTableFunctions)
        {
            return metadata.finishRefreshMaterializedView(session, tableHandle, insertTableHandle, fragments, computedStatistics, sourceTableHandles, sourceTableFunctions);
        }

        @Override
        public Optional<TableHandle> applyUpdate(Session session, TableHandle tableHandle, Map<ColumnHandle, Constant> assignments)
        {
            return metadata.applyUpdate(session, tableHandle, assignments);
        }

        @Override
        public OptionalLong executeUpdate(Session session, TableHandle tableHandle)
        {
            return metadata.executeUpdate(session, tableHandle);
        }

        @Override
        public Optional<TableHandle> applyDelete(Session session, TableHandle tableHandle)
        {
            return metadata.applyDelete(session, tableHandle);
        }

        @Override
        public OptionalLong executeDelete(Session session, TableHandle tableHandle)
        {
            return metadata.executeDelete(session, tableHandle);
        }

        @Override
        public RowChangeParadigm getRowChangeParadigm(Session session, TableHandle tableHandle)
        {
            return metadata.getRowChangeParadigm(session, tableHandle);
        }

        @Override
        public ColumnHandle getMergeRowIdColumnHandle(Session session, TableHandle tableHandle)
        {
            return metadata.getMergeRowIdColumnHandle(session, tableHandle);
        }

        @Override
        public Optional<PartitioningHandle> getUpdateLayout(Session session, TableHandle tableHandle)
        {
            return metadata.getUpdateLayout(session, tableHandle);
        }

        @Override
        public MergeHandle beginMerge(Session session, TableHandle tableHandle)
        {
            return metadata.beginMerge(session, tableHandle);
        }

        @Override
        public void finishMerge(Session session, MergeHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
        {
            metadata.finishMerge(session, tableHandle, fragments, computedStatistics);
        }

        @Override
        public Optional<CatalogHandle> getCatalogHandle(Session session, String catalogName)
        {
            return metadata.getCatalogHandle(session, catalogName);
        }

        @Override
        public List<CatalogInfo> listCatalogs(Session session)
        {
            return metadata.listCatalogs(session);
        }

        @Override
        public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
        {
            return metadata.listViews(session, prefix);
        }

        @Override
        public Map<QualifiedObjectName, ViewInfo> getViews(Session session, QualifiedTablePrefix prefix)
        {
            return metadata.getViews(session, prefix);
        }

        @Override
        public boolean isView(Session session, QualifiedObjectName viewName)
        {
            return metadata.isView(session, viewName);
        }

        @Override
        public Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName)
        {
            return metadata.getView(session, viewName);
        }

        @Override
        public Map<String, Object> getSchemaProperties(Session session, CatalogSchemaName schemaName)
        {
            return metadata.getSchemaProperties(session, schemaName);
        }

        @Override
        public Optional<TrinoPrincipal> getSchemaOwner(Session session, CatalogSchemaName schemaName)
        {
            return metadata.getSchemaOwner(session, schemaName);
        }

        @Override
        public void createView(Session session, QualifiedObjectName viewName, ViewDefinition definition, boolean replace)
        {
            metadata.createView(session, viewName, definition, replace);
        }

        @Override
        public void renameView(Session session, QualifiedObjectName existingViewName, QualifiedObjectName newViewName)
        {
            metadata.renameView(session, existingViewName, newViewName);
        }

        @Override
        public void setViewAuthorization(Session session, CatalogSchemaTableName view, TrinoPrincipal principal)
        {
            metadata.setViewAuthorization(session, view, principal);
        }

        @Override
        public void dropView(Session session, QualifiedObjectName viewName)
        {
            metadata.dropView(session, viewName);
        }

        @Override
        public Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
        {
            return metadata.resolveIndex(session, tableHandle, indexableColumns, outputColumns, tupleDomain);
        }

        @Override
        public Optional<LimitApplicationResult<TableHandle>> applyLimit(Session session, TableHandle table, long limit)
        {
            return metadata.applyLimit(session, table, limit);
        }

        @Override
        public Optional<ConstraintApplicationResult<TableHandle>> applyFilter(Session session, TableHandle table, Constraint constraint)
        {
            return metadata.applyFilter(session, table, constraint);
        }

        @Override
        public Optional<ProjectionApplicationResult<TableHandle>> applyProjection(Session session, TableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
        {
            return metadata.applyProjection(session, table, projections, assignments);
        }

        @Override
        public Optional<SampleApplicationResult<TableHandle>> applySample(Session session, TableHandle table, SampleType sampleType, double sampleRatio)
        {
            return metadata.applySample(session, table, sampleType, sampleRatio);
        }

        @Override
        public Optional<AggregationApplicationResult<TableHandle>> applyAggregation(Session session, TableHandle table, List<AggregateFunction> aggregations, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
        {
            return metadata.applyAggregation(session, table, aggregations, assignments, groupingSets);
        }

        @Override
        public Optional<JoinApplicationResult<TableHandle>> applyJoin(Session session, JoinType joinType, TableHandle left, TableHandle right, ConnectorExpression joinCondition, Map<String, ColumnHandle> leftAssignments, Map<String, ColumnHandle> rightAssignments, JoinStatistics statistics)
        {
            return metadata.applyJoin(session, joinType, left, right, joinCondition, leftAssignments, rightAssignments, statistics);
        }

        @Override
        public Optional<TopNApplicationResult<TableHandle>> applyTopN(Session session, TableHandle handle, long topNCount, List<SortItem> sortItems, Map<String, ColumnHandle> assignments)
        {
            return metadata.applyTopN(session, handle, topNCount, sortItems, assignments);
        }

        @Override
        public Optional<TableFunctionApplicationResult<TableHandle>> applyTableFunction(Session session, TableFunctionHandle handle)
        {
            return metadata.applyTableFunction(session, handle);
        }

        @Override
        public void validateScan(Session session, TableHandle table)
        {
            metadata.validateScan(session, table);
        }

        @Override
        public boolean isCatalogManagedSecurity(Session session, String catalog)
        {
            return metadata.isCatalogManagedSecurity(session, catalog);
        }

        @Override
        public boolean roleExists(Session session, String role, Optional<String> catalog)
        {
            return metadata.roleExists(session, role, catalog);
        }

        @Override
        public void createRole(Session session, String role, Optional<TrinoPrincipal> grantor, Optional<String> catalog)
        {
            metadata.createRole(session, role, grantor, catalog);
        }

        @Override
        public void dropRole(Session session, String role, Optional<String> catalog)
        {
            metadata.dropRole(session, role, catalog);
        }

        @Override
        public Set<String> listRoles(Session session, Optional<String> catalog)
        {
            return metadata.listRoles(session, catalog);
        }

        @Override
        public Set<RoleGrant> listRoleGrants(Session session, Optional<String> catalog, TrinoPrincipal principal)
        {
            return metadata.listRoleGrants(session, catalog, principal);
        }

        @Override
        public void grantRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalog)
        {
            metadata.grantRoles(session, roles, grantees, adminOption, grantor, catalog);
        }

        @Override
        public void revokeRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalog)
        {
            metadata.revokeRoles(session, roles, grantees, adminOption, grantor, catalog);
        }

        @Override
        public Set<RoleGrant> listApplicableRoles(Session session, TrinoPrincipal principal, Optional<String> catalog)
        {
            return metadata.listApplicableRoles(session, principal, catalog);
        }

        @Override
        public Set<String> listEnabledRoles(Identity identity)
        {
            return metadata.listEnabledRoles(identity);
        }

        @Override
        public Set<String> listEnabledRoles(Session session, String catalog)
        {
            return metadata.listEnabledRoles(session, catalog);
        }

        @Override
        public void grantSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
        {
            metadata.grantSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
        }

        @Override
        public void denySchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee)
        {
            metadata.denySchemaPrivileges(session, schemaName, privileges, grantee);
        }

        @Override
        public void revokeSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
        {
            metadata.revokeSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
        }

        @Override
        public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
        {
            metadata.grantTablePrivileges(session, tableName, privileges, grantee, grantOption);
        }

        @Override
        public void denyTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
        {
            metadata.denyTablePrivileges(session, tableName, privileges, grantee);
        }

        @Override
        public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
        {
            metadata.revokeTablePrivileges(session, tableName, privileges, grantee, grantOption);
        }

        @Override
        public List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
        {
            return metadata.listTablePrivileges(session, prefix);
        }

        @Override
        public Collection<FunctionMetadata> listGlobalFunctions(Session session)
        {
            return metadata.listGlobalFunctions(session);
        }

        @Override
        public Collection<FunctionMetadata> listFunctions(Session session, CatalogSchemaName schema)
        {
            return metadata.listFunctions(session, schema);
        }

        @Override
        public ResolvedFunction decodeFunction(QualifiedName name)
        {
            return metadata.decodeFunction(name);
        }

        @Override
        public Collection<CatalogFunctionMetadata> getFunctions(Session session, CatalogSchemaFunctionName catalogSchemaFunctionName)
        {
            return metadata.getFunctions(session, catalogSchemaFunctionName);
        }

        @Override
        public ResolvedFunction resolveBuiltinFunction(String name, List<TypeSignatureProvider> parameterTypes)
        {
            return metadata.resolveBuiltinFunction(name, parameterTypes);
        }

        @Override
        public ResolvedFunction resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
                throws OperatorNotFoundException
        {
            return metadata.resolveOperator(operatorType, argumentTypes);
        }

        @Override
        public ResolvedFunction getCoercion(Type fromType, Type toType)
        {
            return metadata.getCoercion(fromType, toType);
        }

        @Override
        public ResolvedFunction getCoercion(OperatorType operatorType, Type fromType, Type toType)
        {
            return metadata.getCoercion(operatorType, fromType, toType);
        }

        @Override
        public ResolvedFunction getCoercion(CatalogSchemaFunctionName name, Type fromType, Type toType)
        {
            return metadata.getCoercion(name, fromType, toType);
        }

        @Override
        public AggregationFunctionMetadata getAggregationFunctionMetadata(Session session, ResolvedFunction resolvedFunction)
        {
            return metadata.getAggregationFunctionMetadata(session, resolvedFunction);
        }

        @Override
        public FunctionDependencyDeclaration getFunctionDependencies(Session session, CatalogHandle catalogHandle, FunctionId functionId, BoundSignature boundSignature)
        {
            return metadata.getFunctionDependencies(session, catalogHandle, functionId, boundSignature);
        }

        @Override
        public boolean languageFunctionExists(Session session, QualifiedObjectName name, String signatureToken)
        {
            return metadata.languageFunctionExists(session, name, signatureToken);
        }

        @Override
        public void createLanguageFunction(Session session, QualifiedObjectName name, LanguageFunction function, boolean replace)
        {
            metadata.createLanguageFunction(session, name, function, replace);
        }

        @Override
        public void dropLanguageFunction(Session session, QualifiedObjectName name, String signatureToken)
        {
            metadata.dropLanguageFunction(session, name, signatureToken);
        }

        @Override
        public void createMaterializedView(
                Session session,
                QualifiedObjectName viewName,
                MaterializedViewDefinition definition,
                Map<String, Object> properties,
                boolean replace,
                boolean ignoreExisting)
        {
            metadata.createMaterializedView(session, viewName, definition, properties, replace, ignoreExisting);
        }

        @Override
        public void dropMaterializedView(Session session, QualifiedObjectName viewName)
        {
            metadata.dropMaterializedView(session, viewName);
        }

        @Override
        public List<QualifiedObjectName> listMaterializedViews(Session session, QualifiedTablePrefix prefix)
        {
            return metadata.listMaterializedViews(session, prefix);
        }

        @Override
        public Map<QualifiedObjectName, ViewInfo> getMaterializedViews(Session session, QualifiedTablePrefix prefix)
        {
            return metadata.getMaterializedViews(session, prefix);
        }

        @Override
        public boolean isMaterializedView(Session session, QualifiedObjectName viewName)
        {
            return metadata.isMaterializedView(session, viewName);
        }

        @Override
        public Optional<MaterializedViewDefinition> getMaterializedView(Session session, QualifiedObjectName viewName)
        {
            return metadata.getMaterializedView(session, viewName);
        }

        @Override
        public Map<String, Object> getMaterializedViewProperties(Session session, QualifiedObjectName objectName, MaterializedViewDefinition materializedViewDefinition)
        {
            return metadata.getMaterializedViewProperties(session, objectName, materializedViewDefinition);
        }

        @Override
        public MaterializedViewFreshness getMaterializedViewFreshness(Session session, QualifiedObjectName name)
        {
            return metadata.getMaterializedViewFreshness(session, name);
        }

        @Override
        public void renameMaterializedView(Session session, QualifiedObjectName existingViewName, QualifiedObjectName newViewName)
        {
            metadata.renameMaterializedView(session, existingViewName, newViewName);
        }

        @Override
        public void setMaterializedViewProperties(Session session, QualifiedObjectName viewName, Map<String, Optional<Object>> properties)
        {
            metadata.setMaterializedViewProperties(session, viewName, properties);
        }

        @Override
        public void setMaterializedViewColumnComment(Session session, QualifiedObjectName viewName, String columnName, Optional<String> comment)
        {
            metadata.setMaterializedViewColumnComment(session, viewName, columnName, comment);
        }

        @Override
        public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(Session session, TableHandle tableHandle)
        {
            return metadata.applyTableScanRedirect(session, tableHandle);
        }

        @Override
        public RedirectionAwareTableHandle getRedirectionAwareTableHandle(Session session, QualifiedObjectName tableName)
        {
            return metadata.getRedirectionAwareTableHandle(session, tableName);
        }

        @Override
        public RedirectionAwareTableHandle getRedirectionAwareTableHandle(Session session, QualifiedObjectName tableName, Optional<TableVersion> startVersion, Optional<TableVersion> endVersion)
        {
            return metadata.getRedirectionAwareTableHandle(session, tableName, startVersion, endVersion);
        }

        @Override
        public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName, Optional<TableVersion> startVersion, Optional<TableVersion> endVersion)
        {
            return metadata.getTableHandle(session, tableName, startVersion, endVersion);
        }

        @Override
        public OptionalInt getMaxWriterTasks(Session session, String catalogName)
        {
            return metadata.getMaxWriterTasks(session, catalogName);
        }

        @Override
        public boolean isColumnarTableScan(Session session, TableHandle tableHandle)
        {
            return metadata.isColumnarTableScan(session, tableHandle);
        }

        @Override
        public WriterScalingOptions getNewTableWriterScalingOptions(Session session, QualifiedObjectName tableName, Map<String, Object> tableProperties)
        {
            return metadata.getNewTableWriterScalingOptions(session, tableName, tableProperties);
        }

        @Override
        public WriterScalingOptions getInsertWriterScalingOptions(Session session, TableHandle tableHandle)
        {
            return metadata.getInsertWriterScalingOptions(session, tableHandle);
        }
    }
}
