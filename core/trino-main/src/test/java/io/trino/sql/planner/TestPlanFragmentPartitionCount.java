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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.graph.Traverser;
import io.trino.Session;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.warnings.WarningCollector;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.security.AllowAllAccessControl;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Function;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestPlanFragmentPartitionCount
{
    private PlanFragmenter planFragmenter;
    private Session session;
    private PlanTester planTester;

    @BeforeAll
    public void setUp()
    {
        session = testSessionBuilder().setCatalog(TEST_CATALOG_NAME).build();
        planTester = PlanTester.create(session);
        planTester.createCatalog(TEST_CATALOG_NAME, new TpchConnectorFactory(), ImmutableMap.of());

        planFragmenter = new PlanFragmenter(
                planTester.getPlannerContext().getMetadata(),
                planTester.getPlannerContext().getFunctionManager(),
                planTester.getTransactionManager(),
                planTester.getCatalogManager(),
                planTester.getPlannerContext().getLanguageFunctionManager(),
                new QueryManagerConfig());
    }

    @AfterAll
    public void tearDown()
    {
        planFragmenter = null;
        session = null;
        planTester.close();
        planTester = null;
    }

    @Test
    public void testPartitionCountInPlanFragment()
    {
        PlanBuilder p = new PlanBuilder(new PlanNodeIdAllocator(), planTester.getPlannerContext(), session);
        Symbol a = p.symbol("a", VARCHAR);
        Symbol b = p.symbol("b", VARCHAR);
        Symbol c = p.symbol("c", VARCHAR);
        Symbol d = p.symbol("d", VARCHAR);
        Symbol f = p.symbol("f", VARCHAR);
        Symbol g = p.symbol("g", VARCHAR);
        Symbol h = p.symbol("h", VARCHAR);
        Symbol i = p.symbol("i", VARCHAR);

        OutputNode output = p.output(o -> o
                .source(
                        p.exchange(e -> e
                                .type(REPARTITION)
                                .addSource(
                                        p.exchange(exc -> exc
                                                .type(REPARTITION)
                                                .addSource(
                                                        p.join(
                                                                INNER,
                                                                p.exchange(ex -> ex
                                                                        .type(REPARTITION)
                                                                        .addSource(p.values(a, b))
                                                                        .addInputsSet(a, b)
                                                                        .fixedHashDistributionPartitioningScheme(ImmutableList.of(a, b), ImmutableList.of(b), 5)),
                                                                p.exchange(ex -> ex
                                                                        .type(REPARTITION)
                                                                        .addSource(p.values(c, d))
                                                                        .addInputsSet(c, d)
                                                                        .fixedHashDistributionPartitioningScheme(ImmutableList.of(c, d), ImmutableList.of(d), 5)),
                                                                new JoinNode.EquiJoinClause(b, d)))
                                                .addInputsSet(a, b, c, d)
                                                .fixedArbitraryDistributionPartitioningScheme(ImmutableList.of(a, b, c, d), 2)))
                                .addSource(p.values(f, g, h, i))
                                .addInputsSet(a, b, c, d)
                                .addInputsSet(f, g, h, i)
                                .fixedHashDistributionPartitioningScheme(
                                        ImmutableList.of(a, b, c, d),
                                        ImmutableList.of(b),
                                        3))));

        Plan plan = new Plan(output, StatsAndCosts.empty());
        SubPlan rootSubPlan = fragment(plan);
        ImmutableMap.Builder<PlanFragmentId, OptionalInt> actualPartitionCount = ImmutableMap.builder();
        Traverser.forTree(SubPlan::getChildren).depthFirstPreOrder(rootSubPlan).forEach(subPlan ->
                actualPartitionCount.put(subPlan.getFragment().getId(), subPlan.getFragment().getPartitionCount()));

        Map<PlanFragmentId, OptionalInt> expectedPartitionCount = ImmutableMap.of(
                // for output fragment
                new PlanFragmentId("0"), OptionalInt.of(3),
                // for union exchange fragment
                new PlanFragmentId("1"), OptionalInt.of(2),
                // for join fragment
                new PlanFragmentId("2"), OptionalInt.of(5),
                // for all other fragments partitionCount should be empty
                new PlanFragmentId("3"), OptionalInt.empty(),
                new PlanFragmentId("4"), OptionalInt.empty(),
                new PlanFragmentId("5"), OptionalInt.empty());

        assertThat(actualPartitionCount.buildOrThrow()).isEqualTo(expectedPartitionCount);
    }

    private SubPlan fragment(Plan plan)
    {
        planTester.getPlannerContext().getLanguageFunctionManager().registerQuery(session);
        return inTransaction(session -> planFragmenter.createSubPlans(session, plan, false, WarningCollector.NOOP));
    }

    private <T> T inTransaction(Function<Session, T> transactionSessionConsumer)
    {
        return transaction(planTester.getTransactionManager(), planTester.getPlannerContext().getMetadata(), new AllowAllAccessControl())
                .singleStatement()
                .execute(session, session -> {
                    // metadata.getCatalogHandle() registers the catalog for the transaction
                    session.getCatalog().ifPresent(catalog -> planTester.getPlannerContext().getMetadata().getCatalogHandle(session, catalog));
                    return transactionSessionConsumer.apply(session);
                });
    }
}
