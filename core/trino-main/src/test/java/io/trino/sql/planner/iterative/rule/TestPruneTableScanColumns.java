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
import com.google.common.collect.ImmutableMap;
import io.trino.connector.CatalogName;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.plugin.tpch.TpchTransactionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCALE_FACTOR;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictConstrainedTableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestPruneTableScanColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllOutputsReferenced()
    {
        tester().assertThat(new PruneTableScanColumns(tester().getMetadata()))
                .on(p -> {
                    Symbol orderdate = p.symbol("orderdate", DATE);
                    Symbol totalprice = p.symbol("totalprice", DOUBLE);
                    return p.project(
                            Assignments.of(p.symbol("x"), totalprice.toSymbolReference()),
                            p.tableScan(
                                    new TableHandle(
                                            new CatalogName("local"),
                                            new TpchTableHandle("orders", TINY_SCALE_FACTOR),
                                            TpchTransactionHandle.INSTANCE,
                                            Optional.empty()),
                                    ImmutableList.of(orderdate, totalprice),
                                    ImmutableMap.of(
                                            orderdate, new TpchColumnHandle(orderdate.getName(), DATE),
                                            totalprice, new TpchColumnHandle(totalprice.getName(), DOUBLE))));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("x_", PlanMatchPattern.expression("totalprice_")),
                                strictTableScan("orders", ImmutableMap.of("totalprice_", "totalprice"))));
    }

    @Test
    public void testPruneEnforcedConstraint()
    {
        tester().assertThat(new PruneTableScanColumns(tester().getMetadata()))
                .on(p -> {
                    Symbol orderdate = p.symbol("orderdate", DATE);
                    Symbol totalprice = p.symbol("totalprice", DOUBLE);
                    TpchColumnHandle orderdateHandle = new TpchColumnHandle(orderdate.getName(), DATE);
                    TpchColumnHandle totalpriceHandle = new TpchColumnHandle(totalprice.getName(), DOUBLE);
                    return p.project(
                            Assignments.of(p.symbol("x"), totalprice.toSymbolReference()),
                            p.tableScan(
                                    new TableHandle(
                                            new CatalogName("local"),
                                            new TpchTableHandle("orders", TINY_SCALE_FACTOR),
                                            TpchTransactionHandle.INSTANCE,
                                            Optional.empty()),
                                    List.of(orderdate, totalprice),
                                    Map.of(
                                            orderdate, orderdateHandle,
                                            totalprice, totalpriceHandle),
                                    TupleDomain.withColumnDomains(Map.of(
                                            orderdateHandle, Domain.notNull(DATE),
                                            totalpriceHandle, Domain.notNull(DOUBLE)))));
                })
                .matches(
                        strictProject(
                                Map.of("X", PlanMatchPattern.expression("TOTALPRICE")),
                                strictConstrainedTableScan(
                                        "orders",
                                        Map.of("TOTALPRICE", "totalprice"),
                                        // No orderdate constraint
                                        Map.of("totalprice", Domain.notNull(DOUBLE)))));
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneTableScanColumns(tester().getMetadata()))
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("y"), expression("x")),
                                p.tableScan(
                                        ImmutableList.of(p.symbol("x")),
                                        ImmutableMap.of(p.symbol("x"), new TestingColumnHandle("x")))))
                .doesNotFire();
    }
}
