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
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictConstrainedTableScan;
import static java.util.Collections.emptyMap;

class TestRemoveRedundantIdentityProjectionsInOrder
        extends BaseRuleTest
{
    @Test
    public void testNonIdentity()
    {
        Type orderStatusType = createVarcharType(1);
        tester().assertThat(new RemoveRedundantIdentityProjectionsInOrder())
                .on(p -> {
                    Symbol orderstatus = p.symbol("orderstatus", orderStatusType);
                    return p.project(
                            Assignments.of(p.symbol("x", orderStatusType), orderstatus.toSymbolReference()),
                            p.tableScan(
                                    tester().getCurrentCatalogTableHandle(TINY_SCHEMA_NAME, "orders"),
                                    ImmutableList.of(orderstatus),
                                    ImmutableMap.of(orderstatus, new TpchColumnHandle(orderstatus.getName(), orderStatusType))));
                })
                .doesNotFire();
    }

    @Test
    public void testOutputSymbolsNotInTheSameOrder()
    {
        Type orderStatusType = createVarcharType(1);
        tester().assertThat(new RemoveRedundantIdentityProjectionsInOrder())
                .on(p -> {
                    Symbol orderstatus = p.symbol("orderstatus", orderStatusType);
                    Symbol totalprice = p.symbol("totalprice", DOUBLE);
                    return p.project(
                            Assignments.identity(orderstatus, totalprice),
                            p.tableScan(
                                    tester().getCurrentCatalogTableHandle(TINY_SCHEMA_NAME, "orders"),
                                    ImmutableList.of(totalprice, orderstatus),
                                    ImmutableMap.of(
                                            totalprice, new TpchColumnHandle(totalprice.getName(), DOUBLE),
                                            orderstatus, new TpchColumnHandle(orderstatus.getName(), orderStatusType))));
                })
                .doesNotFire();
    }

    @Test
    public void testOutputSymbolsInTheSameOrder()
    {
        Type orderStatusType = createVarcharType(1);
        tester().assertThat(new RemoveRedundantIdentityProjectionsInOrder())
                .on(p -> {
                    Symbol orderstatus = p.symbol("orderstatus", orderStatusType);
                    Symbol totalprice = p.symbol("totalprice", DOUBLE);
                    return p.project(
                            Assignments.identity(orderstatus, totalprice),
                            p.tableScan(
                                    tester().getCurrentCatalogTableHandle(TINY_SCHEMA_NAME, "orders"),
                                    ImmutableList.of(orderstatus, totalprice),
                                    ImmutableMap.of(
                                            orderstatus, new TpchColumnHandle(orderstatus.getName(), orderStatusType),
                                            totalprice, new TpchColumnHandle(totalprice.getName(), DOUBLE))));
                })
                .matches(
                        strictConstrainedTableScan(
                                "orders",
                                Map.of("orderstatus", "orderstatus", "totalprice", "totalprice"),
                                emptyMap()));
    }
}
