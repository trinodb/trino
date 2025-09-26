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
package io.trino.sql.planner.sanity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.connector.CatalogHandle;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.groupingSets;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.type.UnknownType.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestValidateAggregationsWithDefaultValues
        extends BasePlanTest
{
    private PlannerContext plannerContext;
    private PlanBuilder builder;
    private Symbol symbol;
    private TableScanNode tableScanNode;

    @BeforeAll
    public void setup()
    {
        plannerContext = getPlanTester().getPlannerContext();
        builder = new PlanBuilder(new PlanNodeIdAllocator(), plannerContext, TEST_SESSION);
        CatalogHandle catalogHandle = getCurrentCatalogHandle();
        TableHandle nationTableHandle = new TableHandle(
                catalogHandle,
                new TpchTableHandle("sf1", "nation", 1.0),
                TestingTransactionHandle.create());
        TpchColumnHandle nationkeyColumnHandle = new TpchColumnHandle("nationkey", BIGINT);
        symbol = new Symbol(UNKNOWN, "nationkey");
        tableScanNode = builder.tableScan(nationTableHandle, ImmutableList.of(symbol), ImmutableMap.of(symbol, nationkeyColumnHandle));
    }

    @Test
    public void testGloballyDistributedFinalAggregationInTheSameStageAsPartialAggregation()
    {
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .groupingSets(groupingSets(ImmutableList.of(symbol), 2, ImmutableSet.of(0)))
                        .source(builder.aggregation(ap -> ap
                                .step(PARTIAL)
                                .groupingSets(groupingSets(ImmutableList.of(symbol), 2, ImmutableSet.of(0)))
                                .source(tableScanNode))));
        assertThatThrownBy(() -> validatePlan(root, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Final aggregation with default value not separated from partial aggregation by remote hash exchange");
    }

    @Test
    public void testSingleNodeFinalAggregationInTheSameStageAsPartialAggregation()
    {
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .groupingSets(groupingSets(ImmutableList.of(symbol), 2, ImmutableSet.of(0)))
                        .source(builder.aggregation(ap -> ap
                                .step(PARTIAL)
                                .groupingSets(groupingSets(ImmutableList.of(symbol), 2, ImmutableSet.of(0)))
                                .source(tableScanNode))));
        assertThatThrownBy(() -> validatePlan(root, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Final aggregation with default value not separated from partial aggregation by local hash exchange");
    }

    @Test
    public void testSingleThreadFinalAggregationInTheSameStageAsPartialAggregation()
    {
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .groupingSets(groupingSets(ImmutableList.of(symbol), 2, ImmutableSet.of(0)))
                        .source(builder.aggregation(ap -> ap
                                .step(PARTIAL)
                                .groupingSets(groupingSets(ImmutableList.of(symbol), 2, ImmutableSet.of(0)))
                                .source(builder.values()))));
        validatePlan(root, true);
    }

    @Test
    public void testGloballyDistributedFinalAggregationSeparatedFromPartialAggregationByRemoteHashExchange()
    {
        Symbol symbol = new Symbol(UNKNOWN, "symbol");
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .groupingSets(groupingSets(ImmutableList.of(symbol), 2, ImmutableSet.of(0)))
                        .source(builder.exchange(e -> e
                                .type(REPARTITION)
                                .scope(REMOTE)
                                .fixedHashDistributionPartitioningScheme(ImmutableList.of(symbol), ImmutableList.of(symbol))
                                .addInputsSet(symbol)
                                .addSource(builder.aggregation(ap -> ap
                                        .step(PARTIAL)
                                        .groupingSets(groupingSets(ImmutableList.of(symbol), 2, ImmutableSet.of(0)))
                                        .source(tableScanNode))))));
        validatePlan(root, false);
    }

    @Test
    public void testSingleNodeFinalAggregationSeparatedFromPartialAggregationByLocalHashExchange()
    {
        Symbol symbol = new Symbol(UNKNOWN, "symbol");
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .groupingSets(groupingSets(ImmutableList.of(symbol), 2, ImmutableSet.of(0)))
                        .source(builder.exchange(e -> e
                                .type(REPARTITION)
                                .scope(LOCAL)
                                .fixedHashDistributionPartitioningScheme(ImmutableList.of(symbol), ImmutableList.of(symbol))
                                .addInputsSet(symbol)
                                .addSource(builder.aggregation(ap -> ap
                                        .step(PARTIAL)
                                        .groupingSets(groupingSets(ImmutableList.of(symbol), 2, ImmutableSet.of(0)))
                                        .source(tableScanNode))))));
        validatePlan(root, true);
    }

    @Test
    public void testWithPartialAggregationBelowJoin()
    {
        Symbol symbol = new Symbol(UNKNOWN, "symbol");
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .groupingSets(groupingSets(ImmutableList.of(symbol), 2, ImmutableSet.of(0)))
                        .source(builder.join(
                                INNER,
                                builder.exchange(e -> e
                                        .type(REPARTITION)
                                        .scope(LOCAL)
                                        .fixedHashDistributionPartitioningScheme(ImmutableList.of(symbol), ImmutableList.of(symbol))
                                        .addInputsSet(symbol)
                                        .addSource(builder.aggregation(ap -> ap
                                                .step(PARTIAL)
                                                .groupingSets(groupingSets(ImmutableList.of(symbol), 2, ImmutableSet.of(0)))
                                                .source(tableScanNode)))),
                                builder.values())));
        validatePlan(root, true);
    }

    @Test
    public void testWithPartialAggregationBelowJoinWithoutSeparatingExchange()
    {
        Symbol symbol = new Symbol(UNKNOWN, "symbol");
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .groupingSets(groupingSets(ImmutableList.of(symbol), 2, ImmutableSet.of(0)))
                        .source(builder.join(
                                INNER,
                                builder.aggregation(ap -> ap
                                        .step(PARTIAL)
                                        .groupingSets(groupingSets(ImmutableList.of(symbol), 2, ImmutableSet.of(0)))
                                        .source(tableScanNode)),
                                builder.values())));
        assertThatThrownBy(() -> validatePlan(root, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Final aggregation with default value not separated from partial aggregation by local hash exchange");
    }

    private void validatePlan(PlanNode root, boolean forceSingleNode)
    {
        getPlanTester().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> plannerContext.getMetadata().getCatalogHandle(session, catalog));
            new ValidateAggregationsWithDefaultValues(forceSingleNode).validate(
                    root,
                    session,
                    plannerContext,
                    WarningCollector.NOOP);
            return null;
        });
    }
}
