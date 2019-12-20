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
package io.prestosql.sql.planner.sanity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.connector.CatalogName;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.plugin.tpch.TpchColumnHandle;
import io.prestosql.plugin.tpch.TpchTableHandle;
import io.prestosql.plugin.tpch.TpchTransactionHandle;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.PlanNode;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Function;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.SINGLE;

public class TestValidateStreamingAggregations
        extends BasePlanTest
{
    private Metadata metadata;
    private TypeAnalyzer typeAnalyzer;
    private PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private TableHandle nationTableHandle;

    @BeforeClass
    public void setup()
    {
        metadata = getQueryRunner().getMetadata();
        typeAnalyzer = new TypeAnalyzer(getQueryRunner().getSqlParser(), metadata);

        CatalogName catalogName = getCurrentConnectorId();
        nationTableHandle = new TableHandle(
                catalogName,
                new TpchTableHandle("nation", 1.0),
                TpchTransactionHandle.INSTANCE,
                Optional.empty());
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

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Streaming aggregation with input not grouped on the grouping keys")
    public void testValidateFailed()
    {
        validatePlan(
                p -> p.aggregation(
                        a -> a.step(SINGLE)
                                .singleGroupingSet(p.symbol("nationkey"))
                                .preGroupedSymbols(p.symbol("nationkey"))
                                .source(
                                        p.tableScan(
                                                nationTableHandle,
                                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                                ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT))))));
    }

    private void validatePlan(Function<PlanBuilder, PlanNode> planProvider)
    {
        PlanBuilder builder = new PlanBuilder(idAllocator, metadata);
        PlanNode planNode = planProvider.apply(builder);
        TypeProvider types = builder.getTypes();

        getQueryRunner().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            new ValidateStreamingAggregations().validate(planNode, session, metadata, typeAnalyzer, types, WarningCollector.NOOP);
            return null;
        });
    }
}
