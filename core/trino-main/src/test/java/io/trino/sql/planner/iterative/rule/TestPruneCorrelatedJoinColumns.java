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
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.planner.assertions.PlanMatchPattern.correlatedJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;

public class TestPruneCorrelatedJoinColumns
        extends BaseRuleTest
{
    @Test
    public void testRemoveUnusedCorrelatedJoinNode()
    {
        // retain input of INNER join
        tester().assertThat(new PruneCorrelatedJoinColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol correlationSymbol = p.symbol("correlation_symbol");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.identity(a),
                            p.correlatedJoin(
                                    ImmutableList.of(correlationSymbol),
                                    p.values(a, correlationSymbol),
                                    p.values(1, b)));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", PlanMatchPattern.expression(new Reference(BIGINT, "a"))),
                                values("a", "correlationSymbol")));

        // retain input of LEFT join
        tester().assertThat(new PruneCorrelatedJoinColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol correlationSymbol = p.symbol("correlation_symbol");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.identity(a),
                            p.correlatedJoin(
                                    ImmutableList.of(correlationSymbol),
                                    p.values(a, correlationSymbol),
                                    LEFT,
                                    new Comparison(GREATER_THAN, b.toSymbolReference(), correlationSymbol.toSymbolReference()),
                                    p.values(1, b)));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", PlanMatchPattern.expression(new Reference(BIGINT, "a"))),
                                values("a", "correlationSymbol")));

        // retain subquery of INNER join
        tester().assertThat(new PruneCorrelatedJoinColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.identity(b),
                            p.correlatedJoin(
                                    ImmutableList.of(),
                                    p.values(1, a),
                                    p.values(b)));
                })
                .matches(
                        project(
                                ImmutableMap.of("b", expression(new Reference(BIGINT, "b"))),
                                values("b")));

        // retain subquery of RIGHT join
        tester().assertThat(new PruneCorrelatedJoinColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.identity(b),
                            p.correlatedJoin(
                                    ImmutableList.of(),
                                    p.values(1, a),
                                    RIGHT,
                                    new Comparison(GREATER_THAN, b.toSymbolReference(), a.toSymbolReference()),
                                    p.values(b)));
                })
                .matches(
                        project(
                                ImmutableMap.of("b", expression(new Reference(BIGINT, "b"))),
                                values("b")));
    }

    @Test
    public void testPruneUnreferencedSubquerySymbol()
    {
        tester().assertThat(new PruneCorrelatedJoinColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol correlationSymbol = p.symbol("correlation_symbol");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    return p.project(
                            Assignments.identity(a),
                            p.correlatedJoin(
                                    ImmutableList.of(correlationSymbol),
                                    p.values(a, correlationSymbol),
                                    LEFT,
                                    new Comparison(GREATER_THAN, b.toSymbolReference(), a.toSymbolReference()),
                                    p.filter(
                                            new Comparison(GREATER_THAN, b.toSymbolReference(), correlationSymbol.toSymbolReference()),
                                            p.values(5, b, c))));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", PlanMatchPattern.expression(new Reference(BIGINT, "a"))),
                                correlatedJoin(
                                        ImmutableList.of("correlation_symbol"),
                                        values("a", "correlation_symbol"),
                                        project(
                                                ImmutableMap.of("b", expression(new Reference(BIGINT, "b"))),
                                                node(
                                                        FilterNode.class,
                                                        values("b", "c"))))));
    }

    @Test
    public void testPruneUnreferencedInputSymbol()
    {
        tester().assertThat(new PruneCorrelatedJoinColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol correlationSymbol = p.symbol("correlation_symbol");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.identity(b),
                            p.correlatedJoin(
                                    ImmutableList.of(correlationSymbol),
                                    p.values(a, correlationSymbol),
                                    LEFT,
                                    new Comparison(GREATER_THAN, b.toSymbolReference(), correlationSymbol.toSymbolReference()),
                                    p.filter(
                                            new Comparison(GREATER_THAN_OR_EQUAL, b.toSymbolReference(), correlationSymbol.toSymbolReference()),
                                            p.values(b))));
                })
                .matches(
                        project(
                                ImmutableMap.of("b", expression(new Reference(BIGINT, "b"))),
                                correlatedJoin(
                                        ImmutableList.of("correlation_symbol"),
                                        project(
                                                ImmutableMap.of("correlation_symbol", PlanMatchPattern.expression(new Reference(BIGINT, "correlation_symbol"))),
                                                values("a", "correlation_symbol")),
                                        node(
                                                FilterNode.class,
                                                values("b")))));
    }

    @Test
    public void testDoNotPruneUnreferencedCorrelationSymbol()
    {
        tester().assertThat(new PruneCorrelatedJoinColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol correlationSymbol = p.symbol("correlation_symbol");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.identity(a, b),
                            p.correlatedJoin(
                                    ImmutableList.of(correlationSymbol),
                                    p.values(a, correlationSymbol),
                                    LEFT,
                                    TRUE,
                                    p.values(b)));
                })
                .doesNotFire();
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneCorrelatedJoinColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol correlationSymbol = p.symbol("correlation_symbol");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.identity(a, b),
                            p.correlatedJoin(
                                    ImmutableList.of(correlationSymbol),
                                    p.values(a, correlationSymbol),
                                    LEFT,
                                    TRUE,
                                    p.filter(
                                            new Comparison(GREATER_THAN_OR_EQUAL, b.toSymbolReference(), correlationSymbol.toSymbolReference()),
                                            p.values(b))));
                })
                .doesNotFire();
    }
}
