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

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.assertions.PlanMatchPattern.constrainedIndexSource;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;

public class TestPruneIndexSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllOutputsReferenced()
    {
        tester().assertThat(new PruneIndexSourceColumns())
                .on(p -> buildProjectedIndexSource(p, symbol -> symbol.getName().equals("orderkey")))
                .matches(
                        strictProject(
                                ImmutableMap.of("x", expression("orderkey")),
                                constrainedIndexSource(
                                        "orders",
                                        ImmutableMap.of("orderkey", "orderkey"))));
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneIndexSourceColumns())
                .on(p -> buildProjectedIndexSource(p, Predicates.alwaysTrue()))
                .doesNotFire();
    }

    private PlanNode buildProjectedIndexSource(PlanBuilder p, Predicate<Symbol> projectionFilter)
    {
        Symbol orderkey = p.symbol("orderkey", INTEGER);
        Symbol custkey = p.symbol("custkey", INTEGER);
        Symbol totalprice = p.symbol("totalprice", DOUBLE);
        ColumnHandle orderkeyHandle = new TpchColumnHandle(orderkey.getName(), INTEGER);
        ColumnHandle custkeyHandle = new TpchColumnHandle(custkey.getName(), INTEGER);
        ColumnHandle totalpriceHandle = new TpchColumnHandle(totalprice.getName(), DOUBLE);

        return p.project(
                Assignments.identity(
                        ImmutableList.of(orderkey, custkey, totalprice).stream()
                                .filter(projectionFilter)
                                .collect(toImmutableList())),
                p.indexSource(
                        tester().getCurrentCatalogTableHandle(TINY_SCHEMA_NAME, "orders"),
                        ImmutableSet.of(orderkey, custkey),
                        ImmutableList.of(orderkey, custkey, totalprice),
                        ImmutableMap.of(
                                orderkey, orderkeyHandle,
                                custkey, custkeyHandle,
                                totalprice, totalpriceHandle)));
    }
}
