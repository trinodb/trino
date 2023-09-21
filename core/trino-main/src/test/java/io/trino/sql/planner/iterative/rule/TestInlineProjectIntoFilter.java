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

import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestInlineProjectIntoFilter
        extends BaseRuleTest

{
    @Test
    public void testInlineProjection()
    {
        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        PlanBuilder.expression("a"),
                        p.project(
                                Assignments.of(p.symbol("a"), PlanBuilder.expression("b > 0")),
                                p.values(p.symbol("b")))))
                .matches(
                        project(
                                ImmutableMap.of("a", expression("true")),
                                filter(
                                        "b > 0",
                                        project(
                                                ImmutableMap.of("b", expression("b")),
                                                values("b")))));

        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    return p.filter(
                            PlanBuilder.expression("a AND b > c"),
                            p.project(
                                    Assignments.builder()
                                            .put(a, PlanBuilder.expression("d IS NULL"))
                                            .put(b, PlanBuilder.expression("b"))
                                            .put(c, PlanBuilder.expression("c"))
                                            .build(),
                                    p.values(b, c, d)));
                })
                .matches(
                        project(
                                ImmutableMap.of("b", expression("b"), "c", expression("c"), "a", expression("true")),
                                filter(
                                        "d IS NULL AND b > c",
                                        project(
                                                ImmutableMap.of("d", expression("d"), "b", expression("b"), "c", expression("c")),
                                                values("b", "c", "d")))));
    }

    @Test
    public void testNoSimpleConjuncts()
    {
        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        PlanBuilder.expression("a OR false"),
                        p.project(
                                Assignments.of(p.symbol("a"), PlanBuilder.expression("b > 0")),
                                p.values(p.symbol("b")))))
                .doesNotFire();
    }

    @Test
    public void testMultipleReferencesToConjunct()
    {
        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        PlanBuilder.expression("a AND a"),
                        p.project(
                                Assignments.of(p.symbol("a"), PlanBuilder.expression("b > 0")),
                                p.values(p.symbol("b")))))
                .doesNotFire();

        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        PlanBuilder.expression("a AND (a OR false)"),
                        p.project(
                                Assignments.of(p.symbol("a"), PlanBuilder.expression("b > 0")),
                                p.values(p.symbol("b")))))
                .doesNotFire();
    }

    @Test
    public void testInlineMultiple()
    {
        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        PlanBuilder.expression("a AND b"),
                        p.project(
                                Assignments.of(p.symbol("a"), PlanBuilder.expression("c > 0"), p.symbol("b"), PlanBuilder.expression("c > 5")),
                                p.values(p.symbol("c")))))
                .matches(
                        project(
                                filter(
                                        "c > 0 AND c > 5",
                                        project(
                                                ImmutableMap.of("c", expression("c")),
                                                values("c")))));
    }

    @Test
    public void testInlinePartially()
    {
        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        PlanBuilder.expression("a AND a AND b"),
                        p.project(
                                Assignments.of(p.symbol("a"), PlanBuilder.expression("c > 0"), p.symbol("b"), PlanBuilder.expression("c > 5")),
                                p.values(p.symbol("c")))))
                .matches(
                        project(
                                ImmutableMap.of("a", expression("a"), "b", expression("true")),
                                filter(
                                        "a AND c > 5", // combineConjuncts() removed duplicate conjunct `a`. The predicate is now eligible for further inlining.
                                        project(
                                                ImmutableMap.of("a", expression("c > 0"), "c", expression("c")),
                                                values("c")))));
    }

    @Test
    public void testTrivialProjection()
    {
        // identity projection
        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        PlanBuilder.expression("a"),
                        p.project(
                                Assignments.of(p.symbol("a"), PlanBuilder.expression("a")),
                                p.values(p.symbol("a")))))
                .doesNotFire();

        // renaming projection
        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        PlanBuilder.expression("a"),
                        p.project(
                                Assignments.of(p.symbol("a"), PlanBuilder.expression("b")),
                                p.values(p.symbol("b")))))
                .doesNotFire();
    }

    @Test
    public void testCorrelationSymbol()
    {
        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        PlanBuilder.expression("corr"),
                        p.project(
                                Assignments.of(p.symbol("a"), PlanBuilder.expression("b > 0")),
                                p.values(p.symbol("b")))))
                .doesNotFire();
    }
}
