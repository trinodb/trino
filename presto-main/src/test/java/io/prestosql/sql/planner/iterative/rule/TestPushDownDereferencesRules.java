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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.sql.planner.assertions.ExpressionMatcher;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.PushDownDereferences.ExtractFromFilter;
import io.prestosql.sql.planner.iterative.rule.PushDownDereferences.ExtractFromJoin;
import io.prestosql.sql.planner.iterative.rule.PushDownDereferences.PushDownDereferenceThrough;
import io.prestosql.sql.planner.iterative.rule.PushDownDereferences.PushDownDereferenceThroughJoin;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.LimitNode;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.filter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.limit;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.unnest;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;

public class TestPushDownDereferencesRules
        extends BaseRuleTest
{
    private static final RowType MSG_TYPE = RowType.from(ImmutableList.of(new RowType.Field(Optional.of("x"), VARCHAR), new RowType.Field(Optional.of("y"), VARCHAR)));

    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new ExtractFromFilter(tester().getTypeAnalyzer()))
                .on(p ->
                        p.filter(expression("x > BIGINT '5'"),
                                p.values(p.symbol("x"))))
                .doesNotFire();

        RowType nestedMsgType = RowType.from(ImmutableList.of(new RowType.Field(Optional.of("nested"), MSG_TYPE)));
        tester().assertThat(new ExtractFromFilter(tester().getTypeAnalyzer()))
                .on(p ->
                        p.filter(expression("msg.nested.x != 'foo' and CAST(msg.nested as JSON) is not null"),
                                p.values(p.symbol("msg", nestedMsgType))))
                .doesNotFire();
    }

    @Test
    public void testExtractFromFilter()
    {
        tester().assertThat(new ExtractFromFilter(tester().getTypeAnalyzer()))
                .on(p ->
                        p.filter(expression("msg.x <> 'foo'"),
                                p.values(p.symbol("msg", MSG_TYPE))))
                .matches(
                        project(ImmutableMap.of("msg", PlanMatchPattern.expression("msg")),
                                filter("msg_x <> 'foo'",
                                        project(ImmutableMap.of("msg_x", PlanMatchPattern.expression("msg.x")),
                                                values("msg")))));
    }

    @Test
    public void testExtractFromJoin()
    {
        tester().assertThat(new ExtractFromJoin(tester().getTypeAnalyzer()))
                .on(p ->
                        p.join(INNER,
                                p.values(p.symbol("msg1", MSG_TYPE)),
                                p.values(p.symbol("msg2", MSG_TYPE)),
                                p.expression("msg1.x + msg2.y > BIGINT '10'")))
                .matches(
                        join(INNER, ImmutableList.of(), Optional.of("msg1_x + msg2_y > BIGINT '10'"),
                                project(
                                        ImmutableMap.of("msg1_x", PlanMatchPattern.expression("msg1.x")),
                                        values("msg1")),
                                project(
                                        ImmutableMap.of("msg2_y", PlanMatchPattern.expression("msg2.y")),
                                        values("msg2"))));
    }

    @Test
    public void testPushDownDereferenceThrough()
    {
        tester().assertThat(new PushDownDereferenceThrough<>(LimitNode.class, tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("msg_x"), expression("msg.x"))
                                        .put(p.symbol("msg_y"), expression("msg.y"))
                                        .put(p.symbol("z"), expression("z"))
                                        .build(),
                                p.limit(10,
                                        p.values(p.symbol("msg", MSG_TYPE), p.symbol("z")))))
                .matches(
                        project(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("msg_x", PlanMatchPattern.expression("x"))
                                        .put("msg_y", PlanMatchPattern.expression("y"))
                                        .put("z", PlanMatchPattern.expression("z"))
                                        .build(),
                                limit(10,
                                        project(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("x", PlanMatchPattern.expression("msg.x"))
                                                        .put("y", PlanMatchPattern.expression("msg.y"))
                                                        .put("z", PlanMatchPattern.expression("z"))
                                                        .build(),
                                                values("msg", "z")))));
    }

    @Test
    public void testPushdownDereferenceThroughProject()
    {
        tester().assertThat(new PushDownDereferences.PushDownDereferenceThroughProject(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("x"), expression("msg.x")),
                                p.project(
                                        Assignments.of(p.symbol("y"), expression("y")),
                                        p.values(p.symbol("msg", MSG_TYPE), p.symbol("y")))))
                .matches(
                        project(
                                ImmutableMap.of("x", PlanMatchPattern.expression("msg_x")),
                                project(
                                        ImmutableMap.<String, ExpressionMatcher>builder()
                                                .put("msg_x", PlanMatchPattern.expression("msg.x"))
                                                .put("y", PlanMatchPattern.expression("y"))
                                                .build(),
                                        values("msg", "y"))));
    }

    @Test
    public void testPushDownDereferenceThroughJoin()
    {
        tester().assertThat(new PushDownDereferenceThroughJoin(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("left_x"), expression("msg1.x"))
                                        .put(p.symbol("right_y"), expression("msg2.y"))
                                        .put(p.symbol("z"), expression("z"))
                                        .build(),
                                p.join(INNER,
                                        p.values(p.symbol("msg1", MSG_TYPE)),
                                        p.values(p.symbol("msg2", MSG_TYPE), p.symbol("z")))))
                .matches(
                        project(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("left_x", PlanMatchPattern.expression("x"))
                                        .put("right_y", PlanMatchPattern.expression("y"))
                                        .put("z", PlanMatchPattern.expression("z"))
                                        .build(),
                                join(INNER, ImmutableList.of(),
                                        project(
                                                ImmutableMap.of("x", PlanMatchPattern.expression("msg1.x")),
                                                values("msg1")),
                                        project(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("y", PlanMatchPattern.expression("msg2.y"))
                                                        .put("z", PlanMatchPattern.expression("z"))
                                                        .build(),
                                                values("msg2", "z")))));
    }

    @Test
    public void testPushdownDereferecesThroughSemiJoin()
    {
        tester().assertThat(new PushDownDereferences.PushDownDereferenceThroughSemiJoin(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("left_x"), expression("msg1.x"))
                                        .put(p.symbol("right_y"), expression("msg2.y"))
                                        .build(),
                                p.semiJoin(p.symbol("left"),
                                        p.symbol("right"),
                                        p.symbol("match"),
                                        Optional.empty(),
                                        Optional.empty(),
                                        p.values(p.symbol("msg1", MSG_TYPE), p.symbol("left")),
                                        p.values(p.symbol("msg2", MSG_TYPE), p.symbol("right")))))
                .matches(
                        project(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("left_x", PlanMatchPattern.expression("msg1_x"))
                                        .put("right_y", PlanMatchPattern.expression("msg2_y"))
                                        .build(),
                                semiJoin("left",
                                        "right",
                                        "match",
                                        project(
                                                ImmutableMap.of("msg1_x", PlanMatchPattern.expression("msg1.x")),
                                                values("msg1", "left")),
                                        project(
                                                ImmutableMap.of("msg2_y", PlanMatchPattern.expression("msg2.y")),
                                                values("msg2", "right")))));
    }

    @Test
    public void testPushdownDereferencesThroughUnnest()
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        tester().assertThat(new PushDownDereferences.PushDownDereferenceThroughUnnest(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("x"), expression("msg.x")),
                                p.unnest(ImmutableList.of(p.symbol("msg", MSG_TYPE)),
                                        ImmutableMap.of(p.symbol("field"), ImmutableList.of(p.symbol("arr", arrayType))),
                                        Optional.empty(),
                                        INNER,
                                        Optional.empty(),
                                        p.values(p.symbol("msg", MSG_TYPE), p.symbol("arr", arrayType)))))
                .matches(
                        project(
                                ImmutableMap.of("x", PlanMatchPattern.expression("msg_x")),
                                unnest(
                                        project(
                                                ImmutableMap.of("msg_x", PlanMatchPattern.expression("msg.x")),
                                                values("msg", "arr")))));
    }
}
