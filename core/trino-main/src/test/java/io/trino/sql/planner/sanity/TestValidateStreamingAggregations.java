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
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.plugin.tpch.TpchTransactionHandle;
import io.trino.spi.connector.CatalogHandle;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.PlanNode;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.function.Function;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestValidateStreamingAggregations
        extends BasePlanTest
{
    private PlannerContext plannerContext;
    private TypeAnalyzer typeAnalyzer;
    private PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private TableHandle nationTableHandle;

    @BeforeClass
    public void setup()
    {
        plannerContext = getQueryRunner().getPlannerContext();
        typeAnalyzer = createTestingTypeAnalyzer(plannerContext);

        CatalogHandle catalogHandle = getCurrentCatalogHandle();
        nationTableHandle = new TableHandle(
                catalogHandle,
                new TpchTableHandle("sf1", "nation", 1.0),
                TpchTransactionHandle.INSTANCE);
    }

    @Test
    public void testValidateSuccessful()
    {
        validatePlan(
                p -> p.aggregation(
                        a -> a.step(SINGLE)
                                .singleGroupingSet(p.symbol("nationkey"))
                                .source(
                                        p.tableScan(
                                                nationTableHandle,
                                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                                ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT))))));

        validatePlan(
                p -> p.aggregation(
                        a -> a.step(SINGLE)
                                .singleGroupingSet(p.symbol("unique"), p.symbol("nationkey"))
                                .preGroupedSymbols(p.symbol("unique"), p.symbol("nationkey"))
                                .source(
                                        p.assignUniqueId(p.symbol("unique"),
                                                p.tableScan(
                                                        nationTableHandle,
                                                        ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                                        ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)))))));
    }

    @Test
    public void testValidateFailed()
    {
        assertThatThrownBy(() -> validatePlan(
                p -> p.aggregation(
                        a -> a.step(SINGLE)
                                .singleGroupingSet(p.symbol("nationkey"))
                                .preGroupedSymbols(p.symbol("nationkey"))
                                .source(
                                        p.tableScan(
                                                nationTableHandle,
                                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                                ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)))))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Streaming aggregation with input not grouped on the grouping keys");
    }

    private void validatePlan(Function<PlanBuilder, PlanNode> planProvider)
    {
        getQueryRunner().inTransaction(session -> {
            PlanBuilder builder = new PlanBuilder(idAllocator, plannerContext.getMetadata(), session);
            PlanNode planNode = planProvider.apply(builder);
            TypeProvider types = builder.getTypes();

            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> plannerContext.getMetadata().getCatalogHandle(session, catalog));
            new ValidateStreamingAggregations().validate(planNode, session, plannerContext, typeAnalyzer, types, WarningCollector.NOOP);
            return null;
        });
    }
}
