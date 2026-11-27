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
import io.trino.Session;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.SystemSessionProperties.PUSH_FILTER_INTO_VALUES_MAX_ROW_COUNT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static org.assertj.core.api.Assertions.assertThat;

class TestPredicateEnforcement
        extends BaseRuleTest
{
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEnforcementAcrossMemoAlternatives(boolean filterFirst)
    {
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        PlanBuilder p = new PlanBuilder(ids, tester().getPlannerContext(), tester().getSession());
        Symbol a = p.symbol("a");
        Symbol b = p.symbol("b");
        PlanNode rows = p.values(ImmutableList.of(a), ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 11L))));
        PlanNode filtered = p.filter(greaterThanTen(a), rows);
        PlanNode limited = p.limit(100, rows);
        AtomicReference<List<PlanNode>> alternatives = new AtomicReference<>(filterFirst ? ImmutableList.of(filtered, limited) : ImmutableList.of(limited, filtered));
        GroupReference group = new GroupReference(ids.getNextId(), 0, ImmutableList.of(a));
        PlanNode project = p.project(Assignments.of(b, a.toSymbolReference()), group);
        PredicateEnforcement enforcement = new PredicateEnforcement(
                tester().getPlannerContext(), tester().getSession(), new SymbolAllocator(ImmutableList.of(a, b)), Lookup.from(_ -> alternatives.get().stream()));

        assertThat(enforcement.isEnforcedBy(project, greaterThanTen(b))).isTrue();

        // Replacing the group's alternatives must invalidate the earlier proof.
        PlanNode replacement = p.values(ImmutableList.of(a), ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 6L))));
        alternatives.set(ImmutableList.of(p.limit(100, replacement), replacement));
        assertThat(enforcement.isEnforcedBy(project, greaterThanTen(b))).isFalse();
    }

    @Test
    public void testUnionRequiresEveryInput()
    {
        PlanBuilder p = planBuilder();
        Symbol a = p.symbol("a");
        Symbol b = p.symbol("b");
        Symbol output = p.symbol("output");
        ImmutableListMultimap<Symbol, Symbol> mapping = ImmutableListMultimap.of(output, a, output, b);
        PlanNode filteredB = p.filter(greaterThanTen(b), p.tableScan(ImmutableList.of(b), false));
        assertThat(enforcement(a, b, output).isEnforcedBy(
                p.union(mapping, ImmutableList.of(p.filter(greaterThanTen(a), p.tableScan(ImmutableList.of(a), false)), filteredB)),
                greaterThanTen(output))).isTrue();
        assertThat(enforcement(a, b, output).isEnforcedBy(
                p.union(mapping, ImmutableList.of(p.tableScan(ImmutableList.of(a), false), filteredB)),
                greaterThanTen(output))).isFalse();
    }

    @Test
    public void testExchange()
    {
        PlanBuilder p = planBuilder();
        Symbol a = p.symbol("a");
        assertThat(enforcement(a).isEnforcedBy(
                p.gatheringExchange(REMOTE, p.filter(greaterThanTen(a), p.tableScan(ImmutableList.of(a), false))),
                greaterThanTen(a))).isTrue();
        assertThat(enforcement(a).isEnforcedBy(p.gatheringExchange(REMOTE, p.tableScan(ImmutableList.of(a), false)), greaterThanTen(a))).isFalse();
    }

    @Test
    public void testGroupIdCommonGroupingColumns()
    {
        PlanBuilder p = planBuilder();
        Symbol a = p.symbol("a");
        Symbol b = p.symbol("b");
        Symbol groupId = p.symbol("group_id");
        PlanNode groupIdNode = p.groupId(
                ImmutableList.of(ImmutableList.of(a), ImmutableList.of(a, b)),
                ImmutableList.of(),
                groupId,
                p.filter(and(greaterThanTen(a), greaterThanTen(b)), p.tableScan(ImmutableList.of(a, b), false)));
        assertThat(enforcement(a, b, groupId).isEnforcedBy(groupIdNode, greaterThanTen(a))).isTrue();
        // b is null in the grouping set without it
        assertThat(enforcement(a, b, groupId).isEnforcedBy(groupIdNode, greaterThanTen(b))).isFalse();
    }

    @Test
    public void testUnnestReplicateSymbols()
    {
        PlanBuilder p = planBuilder();
        Symbol a = p.symbol("a");
        Symbol array = p.symbol("array", new ArrayType(BIGINT));
        Symbol element = p.symbol("element");
        PlanNode source = p.filter(greaterThanTen(a), p.tableScan(ImmutableList.of(a, array), false));
        List<UnnestNode.Mapping> mappings = ImmutableList.of(new UnnestNode.Mapping(array, ImmutableList.of(element)));
        assertThat(enforcement(a, array, element).isEnforcedBy(p.unnest(ImmutableList.of(a), mappings, source), greaterThanTen(a))).isTrue();
        // A right unnest produces rows with a null replicate symbol
        assertThat(enforcement(a, array, element).isEnforcedBy(p.unnest(ImmutableList.of(a), mappings, Optional.empty(), RIGHT, source), greaterThanTen(a))).isFalse();
    }

    @Test
    public void testMarkDistinctAndAggregationKeys()
    {
        PlanBuilder p = planBuilder();
        Symbol a = p.symbol("a");
        Symbol b = p.symbol("b");
        Symbol marker = p.symbol("marker", BOOLEAN);
        PlanNode source = p.filter(and(greaterThanTen(a), greaterThanTen(b)), p.tableScan(ImmutableList.of(a, b), false));
        PlanNode markDistinct = p.markDistinct(marker, ImmutableList.of(a), source);
        assertThat(enforcement(a, b, marker).isEnforcedBy(markDistinct, greaterThanTen(a))).isTrue();
        assertThat(enforcement(a, b, marker).isEnforcedBy(markDistinct, greaterThanTen(b))).isFalse();

        PlanNode aggregation = p.aggregation(builder -> builder.singleGroupingSet(a).source(source));
        assertThat(enforcement(a, b).isEnforcedBy(aggregation, greaterThanTen(a))).isTrue();
        assertThat(enforcement(a, b).isEnforcedBy(p.aggregation(builder -> builder.singleGroupingSet(a).source(p.tableScan(ImmutableList.of(a, b), false))), greaterThanTen(a))).isFalse();
    }

    @Test
    public void testValuesWithinRowCountLimit()
    {
        PlanBuilder p = planBuilder();
        Symbol a = p.symbol("a");
        PlanNode values = p.values(ImmutableList.of(a), ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 11L)), ImmutableList.of(new Constant(BIGINT, 12L))));
        assertThat(enforcement(a).isEnforcedBy(values, greaterThanTen(a))).isTrue();
        Session oneRow = Session.builder(tester().getSession())
                .setSystemProperty(PUSH_FILTER_INTO_VALUES_MAX_ROW_COUNT, "1")
                .build();
        assertThat(enforcement(oneRow, a).isEnforcedBy(values, greaterThanTen(a))).isFalse();
    }

    @Test
    public void testNonDeterministicPredicate()
    {
        PlanBuilder p = planBuilder();
        Symbol a = p.symbol("a", DOUBLE);
        Expression random = comparison(GREATER_THAN, new Call(new TestingFunctionResolution().resolveFunction("random", ImmutableList.of()), ImmutableList.of()), new Constant(DOUBLE, 0.5));
        assertThat(enforcement(a).isEnforcedBy(p.filter(random, p.tableScan(ImmutableList.of(a), false)), random)).isFalse();
    }

    @Test
    public void testScanEnforcedConstraint()
    {
        PlanBuilder p = planBuilder();
        Symbol a = p.symbol("a");
        ColumnHandle column = new TestingColumnHandle("a");
        PlanNode constrained = p.tableScan(
                TEST_TABLE_HANDLE,
                ImmutableList.of(a),
                ImmutableMap.of(a, column),
                TupleDomain.withColumnDomains(ImmutableMap.of(column, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 10L)), false))));
        assertThat(enforcement(a).isEnforcedBy(constrained, greaterThanTen(a))).isTrue();
        assertThat(enforcement(a).isEnforcedBy(p.tableScan(ImmutableList.of(a), false), greaterThanTen(a))).isFalse();
    }

    private PlanBuilder planBuilder()
    {
        return new PlanBuilder(new PlanNodeIdAllocator(), tester().getPlannerContext(), tester().getSession());
    }

    private PredicateEnforcement enforcement(Symbol... symbols)
    {
        return enforcement(tester().getSession(), symbols);
    }

    private PredicateEnforcement enforcement(Session session, Symbol... symbols)
    {
        return new PredicateEnforcement(tester().getPlannerContext(), session, new SymbolAllocator(ImmutableList.copyOf(symbols)), Lookup.noLookup());
    }

    private static Expression greaterThanTen(Symbol symbol)
    {
        return comparison(GREATER_THAN, symbol.toSymbolReference(), new Constant(BIGINT, 10L));
    }
}
