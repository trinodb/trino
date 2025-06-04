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
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.correlatedJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestTransformExistsApplyToCorrelatedJoin
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new TransformExistsApplyToCorrelatedJoin(tester().getPlannerContext()))
                .on(p -> p.values(p.symbol("a")))
                .doesNotFire();
        tester().assertThat(new TransformExistsApplyToCorrelatedJoin(tester().getPlannerContext()))
                .on(p ->
                        p.correlatedJoin(
                                ImmutableList.of(p.symbol("a")),
                                p.values(p.symbol("a")),
                                p.values(p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void testRewrite()
    {
        tester().assertThat(new TransformExistsApplyToCorrelatedJoin(tester().getPlannerContext()))
                .on(p ->
                        p.apply(
                                ImmutableMap.of(p.symbol("b", BOOLEAN), new ApplyNode.Exists()),
                                ImmutableList.of(),
                                p.values(),
                                p.values()))
                .matches(correlatedJoin(
                        ImmutableList.of(),
                        values(),
                        project(
                                ImmutableMap.of("b", PlanMatchPattern.expression(new Coalesce(new Reference(BOOLEAN, "aggrbool"), FALSE))),
                                aggregation(ImmutableMap.of("aggrbool", aggregationFunction("bool_or", ImmutableList.of("subquery"))),
                                        project(ImmutableMap.of("subquery", PlanMatchPattern.expression(TRUE)), values())))));
    }

    @Test
    public void testRewritesToLimit()
    {
        tester().assertThat(new TransformExistsApplyToCorrelatedJoin(tester().getPlannerContext()))
                .on(p ->
                        p.apply(
                                ImmutableMap.of(p.symbol("b", BOOLEAN), new ApplyNode.Exists()),
                                ImmutableList.of(p.symbol("corr")),
                                p.values(p.symbol("corr")),
                                p.project(Assignments.of(),
                                        p.filter(
                                                new Comparison(EQUAL, new Reference(BIGINT, "corr"), new Reference(BIGINT, "column")),
                                                p.values(p.symbol("column"))))))
                .matches(
                        project(ImmutableMap.of("b", PlanMatchPattern.expression(new Coalesce(new Reference(BOOLEAN, "subquery"), FALSE))),
                                correlatedJoin(
                                        ImmutableList.of("corr"),
                                        values("corr"),
                                        project(
                                                ImmutableMap.of("subquery", PlanMatchPattern.expression(TRUE)),
                                                limit(1,
                                                        project(ImmutableMap.of(),
                                                                node(FilterNode.class,
                                                                        values("column"))))))));
    }
}
