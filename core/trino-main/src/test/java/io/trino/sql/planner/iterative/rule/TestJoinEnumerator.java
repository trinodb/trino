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
import io.trino.cost.StatsProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult;
import io.trino.sql.planner.iterative.rule.ReorderJoins.JoinEnumerator;
import io.trino.sql.planner.iterative.rule.ReorderJoins.MultiJoinNode;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.testing.LocalQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.LinkedHashSet;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.sql.planner.iterative.Lookup.noLookup;
import static io.trino.sql.planner.iterative.rule.ReorderJoins.JoinEnumerator.generatePartitions;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@TestInstance(PER_CLASS)
public class TestJoinEnumerator
{
    private LocalQueryRunner queryRunner;

    @BeforeAll
    public void setUp()
    {
        queryRunner = LocalQueryRunner.create(testSessionBuilder().build());
    }

    @AfterAll
    public void tearDown()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    @Test
    public void testGeneratePartitions()
    {
        assertEquals(generatePartitions(4),
                ImmutableSet.of(
                        ImmutableSet.of(0),
                        ImmutableSet.of(0, 1),
                        ImmutableSet.of(0, 2),
                        ImmutableSet.of(0, 3),
                        ImmutableSet.of(0, 1, 2),
                        ImmutableSet.of(0, 1, 3),
                        ImmutableSet.of(0, 2, 3)));

        assertEquals(generatePartitions(3),
                ImmutableSet.of(
                        ImmutableSet.of(0),
                        ImmutableSet.of(0, 1),
                        ImmutableSet.of(0, 2)));
    }

    @Test
    public void testDoesNotCreateJoinWhenPartitionedOnCrossJoin()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder p = new PlanBuilder(idAllocator, queryRunner.getMetadata(), queryRunner.getDefaultSession());
        Symbol a1 = p.symbol("A1");
        Symbol b1 = p.symbol("B1");
        MultiJoinNode multiJoinNode = new MultiJoinNode(
                new LinkedHashSet<>(ImmutableList.of(p.values(a1), p.values(b1))),
                TRUE_LITERAL,
                ImmutableList.of(a1, b1),
                false);
        JoinEnumerator joinEnumerator = new JoinEnumerator(
                queryRunner.getMetadata(),
                new CostComparator(1, 1, 1),
                multiJoinNode.getFilter(),
                createContext());
        JoinEnumerationResult actual = joinEnumerator.createJoinAccordingToPartitioning(multiJoinNode.getSources(), multiJoinNode.getOutputSymbols(), ImmutableSet.of(0));
        assertFalse(actual.getPlanNode().isPresent());
        assertEquals(actual.getCost(), PlanCostEstimate.infinite());
    }

    private Rule.Context createContext()
    {
        PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        CachingStatsProvider statsProvider = new CachingStatsProvider(
                queryRunner.getStatsCalculator(),
                Optional.empty(),
                noLookup(),
                queryRunner.getDefaultSession(),
                symbolAllocator.getTypes(),
                new CachingTableStatsProvider(queryRunner.getMetadata(), queryRunner.getDefaultSession()));
        CachingCostProvider costProvider = new CachingCostProvider(
                queryRunner.getCostCalculator(),
                statsProvider,
                Optional.empty(),
                queryRunner.getDefaultSession(),
                symbolAllocator.getTypes());

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
                return queryRunner.getDefaultSession();
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
}
