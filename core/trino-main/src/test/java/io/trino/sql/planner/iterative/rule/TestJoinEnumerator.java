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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cost.CachingCostProvider;
import io.trino.cost.CachingStatsProvider;
import io.trino.cost.CachingTableStatsProvider;
import io.trino.cost.CostComparator;
import io.trino.cost.CostProvider;
import io.trino.cost.PlanCostEstimate;
import io.trino.cost.RuntimeInfoProvider;
import io.trino.cost.StatsProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.planner.EqualityInference;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult;
import io.trino.sql.planner.iterative.rule.ReorderJoins.JoinEnumerator;
import io.trino.sql.planner.iterative.rule.ReorderJoins.MultiJoinNode;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.sql.planner.iterative.Lookup.noLookup;
import static io.trino.sql.planner.iterative.rule.ReorderJoins.JoinEnumerator.buildJoinGraph;
import static io.trino.sql.planner.iterative.rule.ReorderJoins.JoinEnumerator.generatePartitions;
import static io.trino.sql.planner.iterative.rule.ReorderJoins.JoinEnumerator.isConnected;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJoinEnumerator
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction NEGATION_BIGINT = FUNCTIONS.resolveOperator(OperatorType.NEGATION, ImmutableList.of(BIGINT));

    private PlanTester planTester;

    @BeforeAll
    public void setUp()
    {
        planTester = PlanTester.create(testSessionBuilder().build());
    }

    @AfterAll
    public void tearDown()
    {
        closeAllRuntimeException(planTester);
        planTester = null;
    }

    @Test
    public void testGeneratePartitionsOfCliqueEnumeratesEverySubset()
    {
        // every subset containing the lowest node, except the full set
        assertThat(generatePartitions(0b1111, clique(4)))
                .containsExactly(0b0001, 0b0011, 0b0101, 0b0111, 0b1001, 0b1011, 0b1101);

        assertThat(generatePartitions(0b111, clique(3)))
                .containsExactly(0b001, 0b011, 0b101);
    }

    @Test
    public void testGeneratePartitionsSkipsDisconnectedHalves()
    {
        // 0 - 1 - 2 - 3: both halves are connected only when the chain is split in two
        long[] chain = graph(4, 0, 1, 1, 2, 2, 3);
        assertThat(generatePartitions(0b1111, chain))
                .containsExactly(0b0001, 0b0011, 0b0111);

        // 0 is the center, 1, 2 and 3 are the leaves: the half without the center must be a single leaf
        long[] star = graph(4, 0, 1, 0, 2, 0, 3);
        assertThat(generatePartitions(0b1111, star))
                .containsExactly(0b0111, 0b1011, 0b1101);

        // 0 - 1 and 2 - 3, with no edge in between
        long[] disconnected = graph(4, 0, 1, 2, 3);
        assertThat(generatePartitions(0b1111, disconnected)).isEmpty();
        assertThat(generatePartitions(0b0011, disconnected)).containsExactly(0b0001);
    }

    @Test
    public void testGeneratePartitionsOfSubsetIgnoresNodesOutsideIt()
    {
        long[] chain = graph(4, 0, 1, 1, 2, 2, 3);
        assertThat(generatePartitions(0b1110, chain)).containsExactly(0b0010, 0b0110);
        // 0 and 3 are not adjacent, so they cannot be joined
        assertThat(generatePartitions(0b1001, chain)).isEmpty();
    }

    @Test
    public void testBuildJoinGraphConnectsSourcesSharingAnEqualityClass()
    {
        PlanBuilder p = planBuilder();
        Symbol a = p.symbol("A", BIGINT);
        Symbol b = p.symbol("B", BIGINT);
        Symbol c = p.symbol("C", BIGINT);
        List<PlanNode> sources = ImmutableList.of(p.values(a), p.values(b), p.values(c));

        // a = b and b = c form a single equality class, so every pair of sources is connected
        long[] transitive = buildJoinGraph(sources, new EqualityInference(
                planTester.getPlannerContext(),
                comparison(EQUAL, a.toSymbolReference(), b.toSymbolReference()),
                comparison(EQUAL, b.toSymbolReference(), c.toSymbolReference())));
        assertThat(transitive).containsExactly(0b110, 0b101, 0b011);

        // separate classes: a = b connects the first two sources, and nothing reaches c
        long[] separate = buildJoinGraph(sources, new EqualityInference(
                planTester.getPlannerContext(),
                comparison(EQUAL, a.toSymbolReference(), b.toSymbolReference())));
        assertThat(separate).containsExactly(0b010, 0b001, 0b000);
    }

    @Test
    public void testBuildJoinGraphSeesSymbolsInsideDerivedExpressions()
    {
        PlanBuilder p = planBuilder();
        Symbol a = p.symbol("A", BIGINT);
        Symbol b = p.symbol("B", BIGINT);
        Symbol c = p.symbol("C", BIGINT);
        List<PlanNode> sources = ImmutableList.of(p.values(a), p.values(b), p.values(c));

        // a = b together with -a = c lets the inference derive -b = c, so a join order that
        // starts from the sources of b and c must stay reachable: all three must be connected
        long[] graph = buildJoinGraph(sources, new EqualityInference(
                planTester.getPlannerContext(),
                comparison(EQUAL, a.toSymbolReference(), b.toSymbolReference()),
                comparison(EQUAL, new Call(NEGATION_BIGINT, ImmutableList.of(a.toSymbolReference())), c.toSymbolReference())));
        assertThat(isConnected(0b111, graph)).isTrue();
    }

    @Test
    public void testIsConnected()
    {
        long[] chain = graph(4, 0, 1, 1, 2, 2, 3);
        assertThat(isConnected(0b0010, chain)).isTrue();
        assertThat(isConnected(0b0110, chain)).isTrue();
        assertThat(isConnected(0b1011, chain)).isFalse();
        assertThat(isConnected(0b1111, chain)).isTrue();
    }

    @Test
    public void testDoesNotCreateJoinWhenPartitionedOnCrossJoin()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder p = new PlanBuilder(idAllocator, planTester.getPlannerContext(), planTester.getDefaultSession());
        Symbol a1 = p.symbol("A1");
        Symbol b1 = p.symbol("B1");
        MultiJoinNode multiJoinNode = new MultiJoinNode(
                new LinkedHashSet<>(ImmutableList.of(p.values(a1), p.values(b1))),
                TRUE,
                ImmutableList.of(a1, b1),
                false);
        JoinEnumerator joinEnumerator = new JoinEnumerator(
                new CostComparator(1, 1, 1),
                multiJoinNode.getFilter(),
                multiJoinNode.getSources(),
                createContext(),
                planTester.getPlannerContext());
        JoinEnumerationResult actual = joinEnumerator.createJoinAccordingToPartitioning(0b11, ImmutableSet.copyOf(multiJoinNode.getOutputSymbols()), 0b01);
        assertThat(actual.getPlanNode()).isEmpty();
        assertThat(actual.getCost()).isEqualTo(PlanCostEstimate.infinite());
    }

    private PlanBuilder planBuilder()
    {
        return new PlanBuilder(new PlanNodeIdAllocator(), planTester.getPlannerContext(), planTester.getDefaultSession());
    }

    private Rule.Context createContext()
    {
        PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
        SymbolAllocator symbolAllocator = emptySymbolAllocator();
        CachingStatsProvider statsProvider = new CachingStatsProvider(
                planTester.getStatsCalculator(),
                Optional.empty(),
                noLookup(),
                planTester.getDefaultSession(),
                new CachingTableStatsProvider(planTester.getPlannerContext().getMetadata(), planTester.getDefaultSession(), () -> false),
                RuntimeInfoProvider.noImplementation());
        CachingCostProvider costProvider = new CachingCostProvider(
                planTester.getCostCalculator(),
                statsProvider,
                Optional.empty(),
                planTester.getDefaultSession());

        return new Rule.Context()
        {
            @Override
            public Lookup getLookup()
            {
                return noLookup();
            }

            @Override
            public PlanNodeIdAllocator getIdAllocator()
            {
                return planNodeIdAllocator;
            }

            @Override
            public SymbolAllocator getSymbolAllocator()
            {
                return symbolAllocator;
            }

            @Override
            public Session getSession()
            {
                return planTester.getDefaultSession();
            }

            @Override
            public StatsProvider getStatsProvider()
            {
                return statsProvider;
            }

            @Override
            public CostProvider getCostProvider()
            {
                return costProvider;
            }

            @Override
            public void checkTimeoutNotExhausted() {}

            @Override
            public WarningCollector getWarningCollector()
            {
                return WarningCollector.NOOP;
            }
        };
    }

    private static long[] clique(int nodes)
    {
        long[] neighbors = new long[nodes];
        for (int node = 0; node < nodes; node++) {
            neighbors[node] = ((1L << nodes) - 1) & ~(1L << node);
        }
        return neighbors;
    }

    /**
     * Builds the adjacency masks of an undirected graph from pairs of connected nodes.
     */
    private static long[] graph(int nodes, int... edges)
    {
        checkArgument(edges.length % 2 == 0, "edges must be given in pairs");
        long[] neighbors = new long[nodes];
        for (int i = 0; i < edges.length; i += 2) {
            neighbors[edges[i]] |= 1L << edges[i + 1];
            neighbors[edges[i + 1]] |= 1L << edges[i];
        }
        return neighbors;
    }
}
