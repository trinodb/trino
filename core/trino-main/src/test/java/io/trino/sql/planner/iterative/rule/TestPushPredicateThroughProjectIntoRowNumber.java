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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.RowNumberSymbolMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushPredicateThroughProjectIntoRowNumber
        extends BaseRuleTest
{
    @Test
    public void testRowNumberSymbolPruned()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            PlanBuilder.expression("a = 1"),
                            p.project(
                                    Assignments.identity(a),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.empty(),
                                            rowNumber,
                                            p.values(a))));
                })
                .doesNotFire();
    }

    @Test
    public void testNoUpperBoundForRowNumberSymbol()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            PlanBuilder.expression("a = BIGINT '1'"),
                            p.project(
                                    Assignments.identity(a, rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.empty(),
                                            rowNumber,
                                            p.values(a))));
                })
                .doesNotFire();
    }

    @Test
    public void testNonPositiveUpperBoundForRowNumberSymbol()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            PlanBuilder.expression("a = BIGINT '1' AND row_number < BIGINT '-10'"),
                            p.project(
                                    Assignments.identity(a, rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.empty(),
                                            rowNumber,
                                            p.values(a))));
                })
                .matches(values("a", "row_number"));
    }

    @Test
    public void testPredicateNotSatisfied()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            PlanBuilder.expression("row_number > BIGINT '2' AND row_number < BIGINT '5'"),
                            p.project(
                                    Assignments.identity(rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.empty(),
                                            rowNumber,
                                            p.values(a))));
                })
                .matches(filter(
                        "row_number > BIGINT '2' AND row_number < BIGINT '5'",
                        project(
                                ImmutableMap.of("row_number", expression("row_number")),
                                rowNumber(
                                        pattern -> pattern
                                                .maxRowCountPerPartition(Optional.of(4)),
                                        values(ImmutableList.of("a")))
                                        .withAlias("row_number", new RowNumberSymbolMatcher()))));
    }

    @Test
    public void testPredicateNotSatisfiedAndMaxRowCountNotUpdated()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            PlanBuilder.expression("row_number > BIGINT '2' AND row_number < BIGINT '5'"),
                            p.project(
                                    Assignments.identity(rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.of(3),
                                            rowNumber,
                                            p.values(a))));
                })
                .doesNotFire();
    }

    @Test
    public void testPredicateSatisfied()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            PlanBuilder.expression("row_number < BIGINT '5'"),
                            p.project(
                                    Assignments.identity(rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.of(3),
                                            rowNumber,
                                            p.values(a))));
                })
                .matches(project(
                        ImmutableMap.of("row_number", expression("row_number")),
                        rowNumber(
                                pattern -> pattern
                                        .maxRowCountPerPartition(Optional.of(3)),
                                values(ImmutableList.of("a")))
                                .withAlias("row_number", new RowNumberSymbolMatcher())));

        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            PlanBuilder.expression("row_number < BIGINT '3'"),
                            p.project(
                                    Assignments.identity(rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.of(5),
                                            rowNumber,
                                            p.values(a))));
                })
                .matches(project(
                        ImmutableMap.of("row_number", expression("row_number")),
                        rowNumber(
                                pattern -> pattern
                                        .maxRowCountPerPartition(Optional.of(2)),
                                values(ImmutableList.of("a")))
                                .withAlias("row_number", new RowNumberSymbolMatcher())));
    }

    @Test
    public void testPredicatePartiallySatisfied()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            PlanBuilder.expression("row_number < BIGINT '5' AND a > BIGINT '0'"),
                            p.project(
                                    Assignments.identity(rowNumber, a),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.of(3),
                                            rowNumber,
                                            p.values(a))));
                })
                .matches(filter(
                        "a > BIGINT '0'",
                        project(
                                ImmutableMap.of("row_number", expression("row_number"), "a", expression("a")),
                                rowNumber(
                                        pattern -> pattern
                                                .maxRowCountPerPartition(Optional.of(3)),
                                        values(ImmutableList.of("a")))
                                        .withAlias("row_number", new RowNumberSymbolMatcher()))));

        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            PlanBuilder.expression("row_number < BIGINT '5' AND row_number % 2 = BIGINT '0'"),
                            p.project(
                                    Assignments.identity(rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.of(3),
                                            rowNumber,
                                            p.values(a))));
                })
                .matches(filter(
                        "row_number % 2 = BIGINT '0'",
                        project(
                                ImmutableMap.of("row_number", expression("row_number")),
                                rowNumber(
                                        pattern -> pattern
                                                .maxRowCountPerPartition(Optional.of(3)),
                                        values(ImmutableList.of("a")))
                                        .withAlias("row_number", new RowNumberSymbolMatcher()))));
    }
}
