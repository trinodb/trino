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
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;

public class TestReplaceJoinOverConstantWithProject
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireOnJoinWithEmptySource()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(1, p.symbol("a")),
                                p.values(0, p.symbol("b"))))
                .doesNotFire();

        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(0, p.symbol("a")),
                                p.values(1, p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnJoinWithCondition()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(1, p.symbol("a")),
                                p.values(5, p.symbol("b")),
                                new EquiJoinClause(p.symbol("a"), p.symbol("b"))))
                .doesNotFire();

        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(1, p.symbol("a")),
                                p.values(5, p.symbol("b")),
                                expression("a > b")))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnValuesWithMultipleRows()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(5, p.symbol("a")),
                                p.values(5, p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnValuesWithNoOutputs()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(1),
                                p.values(5, p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnValuesWithNonRowExpression()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                INNER,
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a")), ImmutableList.of(expression("CAST(ROW('true') AS ROW(b boolean))"))),
                                p.values(5, p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnOuterJoinWhenSourcePossiblyEmpty()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(1, p.symbol("a")),
                                p.filter(
                                        expression("b > 5"),
                                        p.values(10, p.symbol("b")))))
                .doesNotFire();

        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.filter(
                                        expression("a > 5"),
                                        p.values(10, p.symbol("a"))),
                                p.values(1, p.symbol("b"))))
                .doesNotFire();

        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(1, p.symbol("a")),
                                p.filter(
                                        expression("b > 5"),
                                        p.values(10, p.symbol("b")))))
                .doesNotFire();

        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                FULL,
                                p.filter(
                                        expression("a > 5"),
                                        p.values(10, p.symbol("a"))),
                                p.values(1, p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testReplaceInnerJoinWithProject()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                INNER,
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(expression("ROW(1, 'x')"))),
                                p.values(5, p.symbol("c"))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("1"),
                                        "b", PlanMatchPattern.expression("'x'"),
                                        "c", PlanMatchPattern.expression("c")),
                                values("c")));

        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(5, p.symbol("c")),
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(expression("ROW(1, 'x')")))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("1"),
                                        "b", PlanMatchPattern.expression("'x'"),
                                        "c", PlanMatchPattern.expression("c")),
                                values("c")));
    }

    @Test
    public void testReplaceLeftJoinWithProject()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                LEFT,
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(expression("ROW(1, 'x')"))),
                                p.values(5, p.symbol("c"))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("1"),
                                        "b", PlanMatchPattern.expression("'x'"),
                                        "c", PlanMatchPattern.expression("c")),
                                values("c")));

        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(5, p.symbol("c")),
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(expression("ROW(1, 'x')")))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("1"),
                                        "b", PlanMatchPattern.expression("'x'"),
                                        "c", PlanMatchPattern.expression("c")),
                                values("c")));
    }

    @Test
    public void testReplaceRightJoinWithProject()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(expression("ROW(1, 'x')"))),
                                p.values(5, p.symbol("c"))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("1"),
                                        "b", PlanMatchPattern.expression("'x'"),
                                        "c", PlanMatchPattern.expression("c")),
                                values("c")));

        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(5, p.symbol("c")),
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(expression("ROW(1, 'x')")))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("1"),
                                        "b", PlanMatchPattern.expression("'x'"),
                                        "c", PlanMatchPattern.expression("c")),
                                values("c")));
    }

    @Test
    public void testReplaceFullJoinWithProject()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                FULL,
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(expression("ROW(1, 'x')"))),
                                p.values(5, p.symbol("c"))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("1"),
                                        "b", PlanMatchPattern.expression("'x'"),
                                        "c", PlanMatchPattern.expression("c")),
                                values("c")));

        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(5, p.symbol("c")),
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(expression("ROW(1, 'x')")))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("1"),
                                        "b", PlanMatchPattern.expression("'x'"),
                                        "c", PlanMatchPattern.expression("c")),
                                values("c")));
    }

    @Test
    public void testRemoveOutputDuplicates()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                INNER,
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(expression("ROW(1, 'x')"))),
                                p.values(5, p.symbol("c")),
                                ImmutableList.of(),
                                ImmutableList.of(p.symbol("a"), p.symbol("b"), p.symbol("a"), p.symbol("b")),
                                ImmutableList.of(p.symbol("c"), p.symbol("c")),
                                Optional.empty()))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("1"),
                                        "b", PlanMatchPattern.expression("'x'"),
                                        "c", PlanMatchPattern.expression("c")),
                                values("c")));
    }

    @Test
    public void testNonDeterministicValues()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject())
                .on(p ->
                        p.join(
                                INNER,
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a")), ImmutableList.of(expression("ROW(rand())"))),
                                p.values(5, p.symbol("b"))))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("rand()"),
                                        "b", PlanMatchPattern.expression("b")),
                                values("b")));
    }
}
