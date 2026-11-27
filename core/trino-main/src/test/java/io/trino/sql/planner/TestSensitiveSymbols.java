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
package io.trino.sql.planner;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.AnalyzeTableHandle;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableHandle;
import io.trino.operator.RetryPolicy;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.SensitiveExpression;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AdaptivePlanNode;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SimpleTableExecuteNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.StatisticAggregationsDescriptor;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.StatisticsWriterNode.WriteStatisticsHandle;
import io.trino.sql.planner.plan.TableDeleteNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableUpdateNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TopNRankingNode.RankingType;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import io.trino.testing.TestingTableExecuteHandle;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class TestSensitiveSymbols
{
    private static final Symbol X = new Symbol(BIGINT, "x");
    private static final Symbol Y = new Symbol(BIGINT, "y");
    private static final Symbol Z = new Symbol(BIGINT, "z");
    private static final Symbol X_RENAMED = new Symbol(BIGINT, "x_renamed");
    private static final Symbol Y_MASKED = new Symbol(BIGINT, "y_masked");
    private static final ColumnHandle X_COLUMN = new TestingColumnHandle("x");
    private static final ColumnHandle Y_COLUMN = new TestingColumnHandle("y");

    @Test
    public void testReferencedIn()
    {
        Expression sensitive = new SensitiveExpression(Logical.and(equal(X, 1), greaterThan(Y, 2)));

        assertThat(SensitiveSymbols.referencedIn(Logical.and(sensitive, equal(Z, 3)))).containsExactlyInAnyOrder(X, Y);
        assertThat(SensitiveSymbols.referencedIn(equal(Z, 3))).isEmpty();
    }

    @Test
    public void testInPlanFollowsProjectedCopies()
    {
        PlanBuilder planBuilder = planBuilder();
        TableScanNode scan = scan(planBuilder);
        // the sensitive predicate references the projected name, which must map back to the scan's own symbol
        PlanNode filter = planBuilder.filter(
                new SensitiveExpression(equal(X_RENAMED, 1)),
                planBuilder.project(Assignments.of(X_RENAMED, X.toSymbolReference(), Y, Y.toSymbolReference()), scan));

        assertThat(SensitiveSymbols.inPlan(filter)).containsExactlyInAnyOrder(X_RENAMED, X);
    }

    @Test
    public void testInPlanIncludesMaskOutputsAndComputedSymbols()
    {
        PlanBuilder planBuilder = planBuilder();
        // y is replaced by a sensitive mask; z is computed from the masked value further up
        PlanNode masked = planBuilder.project(
                Assignments.of(X, X.toSymbolReference(), Y_MASKED, new SensitiveExpression(Y.toSymbolReference())),
                scan(planBuilder));
        PlanNode root = planBuilder.project(Assignments.of(Z, Y_MASKED.toSymbolReference()), masked);

        assertThat(SensitiveSymbols.inPlan(root)).containsExactlyInAnyOrder(Y_MASKED, Y, Z);
    }

    @Test
    public void testComputedSymbolDoesNotMarkItsInputs()
    {
        PlanBuilder planBuilder = planBuilder();
        PlanNode masked = planBuilder.project(
                Assignments.of(X, X.toSymbolReference(), Y_MASKED, new SensitiveExpression(Y.toSymbolReference())),
                scan(planBuilder));
        PlanNode root = planBuilder.project(Assignments.of(Z, new Coalesce(X.toSymbolReference(), Y_MASKED.toSymbolReference())), masked);

        assertThat(SensitiveSymbols.inPlan(root)).containsExactlyInAnyOrder(Y, Y_MASKED, Z);
    }

    @Test
    public void testSetOperationCopiesSymbols()
    {
        PlanBuilder planBuilder = planBuilder();
        Symbol other = new Symbol(BIGINT, "other");
        PlanNode filtered = planBuilder.filter(new SensitiveExpression(equal(X, 1)), scan(planBuilder));
        PlanNode union = planBuilder.union(ImmutableListMultimap.of(Z, X, Z, other), ImmutableList.of(filtered, planBuilder.values(other)));
        // the output carries the sensitive input's values, but the other input is not constrained
        assertThat(SensitiveSymbols.inPlan(union)).containsExactlyInAnyOrder(X, Z);

        union = planBuilder.union(ImmutableListMultimap.of(Z, X, Z, other), ImmutableList.of(scan(planBuilder), planBuilder.values(other)));
        // a constraint on the output constrains every input
        assertThat(SensitiveSymbols.inPlan(planBuilder.filter(new SensitiveExpression(equal(Z, 1)), union))).containsExactlyInAnyOrder(Z, X, other);
    }

    @Test
    public void testExchangeCopiesSymbols()
    {
        PlanBuilder planBuilder = planBuilder();
        PlanNode exchange = planBuilder.exchange(builder -> builder
                .singleDistributionPartitioningScheme(Z)
                .addInputsSet(X)
                .addSource(scan(planBuilder)));

        assertThat(SensitiveSymbols.inPlan(planBuilder.filter(new SensitiveExpression(equal(Z, 1)), exchange))).containsExactlyInAnyOrder(Z, X);
    }

    @Test
    public void testGroupIdCopiesGroupingColumns()
    {
        PlanBuilder planBuilder = planBuilder();
        Symbol groupId = new Symbol(BIGINT, "group_id");
        PlanNode groupIdNode = planBuilder.groupId(ImmutableList.of(ImmutableList.of(Z)), ImmutableMap.of(Z, X), ImmutableList.of(Y), groupId, scan(planBuilder));

        assertThat(SensitiveSymbols.inPlan(planBuilder.filter(new SensitiveExpression(equal(Z, 1)), groupIdNode))).containsExactlyInAnyOrder(Z, X);
    }

    @Test
    public void testUnnestComputesUnnestedValues()
    {
        PlanBuilder planBuilder = planBuilder();
        Symbol array = new Symbol(new ArrayType(BIGINT), "array");
        PlanNode masked = planBuilder.project(Assignments.of(array, new SensitiveExpression(new Constant(new ArrayType(BIGINT), null))), planBuilder.values(X));
        PlanNode unnest = planBuilder.unnest(ImmutableList.of(), ImmutableList.of(new UnnestNode.Mapping(array, ImmutableList.of(Z))), masked);

        assertThat(SensitiveSymbols.inPlan(unnest)).containsExactlyInAnyOrder(array, Z);
    }

    @Test
    public void testValuesMarkSensitiveItems()
    {
        PlanBuilder planBuilder = planBuilder();
        PlanNode values = planBuilder.values(ImmutableList.of(X, Y), ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 1L), new SensitiveExpression(new Constant(BIGINT, 2L)))));

        assertThat(SensitiveSymbols.inPlan(values)).containsExactly(Y);
    }

    @Test
    public void testOtherNodesComputeTheirNewOutputsFromTheirSources()
    {
        PlanBuilder planBuilder = planBuilder();
        Symbol marker = new Symbol(BOOLEAN, "marker");
        PlanNode filter = planBuilder.filter(new SensitiveExpression(equal(X, 1)), scan(planBuilder));

        assertThat(SensitiveSymbols.inPlan(planBuilder.markDistinct(marker, ImmutableList.of(Y), filter))).containsExactlyInAnyOrder(X, marker);
    }

    @Test
    public void testInPlanStopsAtGroupReferences()
    {
        PlanBuilder planBuilder = planBuilder();
        // unresolved memo nodes are printed while debugging the optimizer and must not fail the walk
        PlanNode filter = planBuilder.filter(
                new SensitiveExpression(equal(X, 1)),
                new GroupReference(new PlanNodeId("group"), 1, ImmutableList.of(X, Y)));

        assertThat(SensitiveSymbols.inPlan(filter)).containsExactly(X);
    }

    @Test
    public void testInPlanReadsUnoptimizedPlans()
    {
        PlanBuilder planBuilder = planBuilder();
        Symbol exists = new Symbol(BOOLEAN, "exists");
        // plans printed while debugging the planner still have the subquery and offset nodes
        PlanNode join = planBuilder.correlatedJoin(
                ImmutableList.of(),
                scan(planBuilder),
                INNER,
                new SensitiveExpression(equal(X, 1)),
                planBuilder.offset(1, planBuilder.values(Z)));
        PlanNode apply = planBuilder.apply(ImmutableMap.of(exists, new ApplyNode.Exists()), ImmutableList.of(), join, planBuilder.values(Y_MASKED));

        assertThat(SensitiveSymbols.inPlan(apply)).containsExactlyInAnyOrder(X, exists);
    }

    @Test
    public void testNodeExpressionsKnowsRegisteredPlanNodes()
    {
        List<PlanNode> nodes = planNodes();
        Set<Class<?>> registeredTypes = Arrays.stream(PlanNode.class.getAnnotation(JsonSubTypes.class).value())
                .map(JsonSubTypes.Type::value)
                .collect(toImmutableSet());

        // A plan node type that nodeExpressions does not know fails the queries that plan it, so a new type must
        // fail here first
        assertThat(nodes.stream().<Class<?>>map(PlanNode::getClass).collect(toImmutableSet())).containsAll(registeredTypes);
        for (PlanNode node : nodes) {
            assertThatCode(() -> SensitiveSymbols.nodeExpressions(node)).doesNotThrowAnyException();
        }
    }

    // A node of every type that PlanNode registers
    private static List<PlanNode> planNodes()
    {
        PlanBuilder planBuilder = planBuilder();
        PlanNodeId id = new PlanNodeId("node");
        Symbol array = new Symbol(new ArrayType(BIGINT), "array");
        Symbol marker = new Symbol(BOOLEAN, "marker");
        TableScanNode scan = scan(planBuilder);
        PlanNode values = planBuilder.values(Z);
        TableHandle table = scan.getTable();
        SchemaTableName tableName = new SchemaTableName("schema", "table");
        DataOrganizationSpecification specification = new DataOrganizationSpecification(
                ImmutableList.of(),
                Optional.of(new OrderingScheme(ImmutableList.of(X), ImmutableMap.of(X, ASC_NULLS_FIRST))));
        TableWriterNode writer = planBuilder.tableWriter(ImmutableList.of(X), ImmutableList.of("x"), scan);

        return ImmutableList.of(
                new AdaptivePlanNode(id, scan, ImmutableSet.of(X, Y), scan),
                planBuilder.aggregation(aggregation -> aggregation.singleGroupingSet(X).source(scan)),
                planBuilder.apply(ImmutableMap.of(marker, new ApplyNode.Exists()), ImmutableList.of(), scan, values),
                planBuilder.assignUniqueId(Z, scan),
                planBuilder.correlatedJoin(ImmutableList.of(), scan, values),
                planBuilder.distinctLimit(1, ImmutableList.of(X), scan),
                new DynamicFilterSourceNode(id, scan, ImmutableMap.of()),
                planBuilder.enforceSingleRow(scan),
                planBuilder.exchange(exchange -> exchange.singleDistributionPartitioningScheme(X, Y).addInputsSet(X, Y).addSource(scan)),
                planBuilder.explainAnalyzeNode(Z, ImmutableList.of(X), scan),
                planBuilder.filter(TRUE, scan),
                planBuilder.groupId(ImmutableList.of(ImmutableList.of(X)), ImmutableList.of(Y), Z, scan),
                planBuilder.indexJoin(IndexJoinNode.Type.INNER, scan, values),
                planBuilder.indexSource(table, ImmutableSet.of(X), ImmutableList.of(X), ImmutableMap.of(X, X_COLUMN)),
                planBuilder.intersect(ImmutableListMultimap.of(Z, X, Z, Z), ImmutableList.of(scan, values)),
                planBuilder.join(INNER, scan, values),
                planBuilder.limit(1, scan),
                planBuilder.markDistinct(marker, ImmutableList.of(X), scan),
                planBuilder.mergeProcessor(tableName, scan, X, Y, ImmutableList.of(), ImmutableList.of(), ImmutableList.of(Z)),
                planBuilder.merge(tableName, scan, X, Y, ImmutableList.of(Z)),
                planBuilder.output(ImmutableList.of("x"), ImmutableList.of(X), scan),
                planBuilder.patternRecognition(recognition -> recognition.pattern(new IrLabel("A")).addVariableDefinition(new IrLabel("A"), TRUE).source(scan)),
                planBuilder.project(Assignments.identity(X), scan),
                planBuilder.remoteSource(ImmutableList.of(new PlanFragmentId("fragment")), ImmutableList.of(X), Optional.empty(), ExchangeNode.Type.GATHER, RetryPolicy.NONE),
                planBuilder.rowNumber(ImmutableList.of(), Optional.empty(), Z, scan),
                planBuilder.sample(0.5, SampleNode.Type.BERNOULLI, scan),
                planBuilder.semiJoin(X, Z, marker, scan, values),
                new SimpleTableExecuteNode(id, ImmutableList.of(Z), new TableExecuteHandle(table.catalogHandle(), table.transaction(), table.connectorHandle(), new TestingTableExecuteHandle())),
                planBuilder.sort(ImmutableList.of(X), scan),
                planBuilder.spatialJoin(SpatialJoinNode.Type.INNER, scan, values, ImmutableList.of(X), TRUE),
                new StatisticsWriterNode(
                        id,
                        scan,
                        new WriteStatisticsHandle(new AnalyzeTableHandle(table.catalogHandle(), table.transaction(), table.connectorHandle())),
                        Z,
                        false,
                        StatisticAggregationsDescriptor.empty()),
                new TableDeleteNode(id, table, Z),
                planBuilder.tableExecute(ImmutableList.of(X), ImmutableList.of("x"), scan),
                planBuilder.tableFinish(writer, writer.getTarget(), Z),
                planBuilder.tableFunction("function", ImmutableList.of(Z), ImmutableList.of(), ImmutableList.of(), ImmutableList.of()),
                planBuilder.tableFunctionProcessor(function -> function.name("function").source(scan)),
                scan,
                new TableUpdateNode(id, table, Z),
                writer,
                planBuilder.topN(1, ImmutableList.of(X), scan),
                planBuilder.topNRanking(specification, RankingType.ROW_NUMBER, 1, Z, scan),
                planBuilder.union(ImmutableListMultimap.of(Z, X, Z, Z), ImmutableList.of(scan, values)),
                planBuilder.unnest(ImmutableList.of(), ImmutableList.of(new UnnestNode.Mapping(array, ImmutableList.of(Z))), planBuilder.values(array)),
                values,
                planBuilder.window(specification, ImmutableMap.of(), scan));
    }

    private static PlanBuilder planBuilder()
    {
        return new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, TEST_SESSION);
    }

    private static TableScanNode scan(PlanBuilder planBuilder)
    {
        return planBuilder.tableScan(ImmutableList.of(X, Y), ImmutableMap.of(X, X_COLUMN, Y, Y_COLUMN));
    }

    private static Expression equal(Symbol symbol, long value)
    {
        return comparison(EQUAL, symbol.toSymbolReference(), new Constant(BIGINT, value));
    }

    private static Expression greaterThan(Symbol symbol, long value)
    {
        return comparison(GREATER_THAN, symbol.toSymbolReference(), new Constant(BIGINT, value));
    }
}
