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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;

public class TestDecorrelateViaPlainJoinForms
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", fromTypes());

    private static Expression nonDeterministic(String correlation)
    {
        // random() < corr : correlated (references the outer symbol) and non-deterministic.
        return comparison(LESS_THAN, new Call(RANDOM, ImmutableList.of()), new Reference(DOUBLE, correlation));
    }

    @Test
    public void tryPlainJoinLimitDeclinesNonDeterministicCondition()
    {
        // A non-deterministic correlated predicate over a LATERAL LIMIT must be evaluated per base
        // row, before the bound; the plain-join LIMIT form would apply it in the join ON above a
        // global limit — a different sampling. So the form declines and the shape falls to the next
        // rule (the generic pushdown). Delete the isDeterministic gate in
        // PlainJoinForms.tryPlainJoinLimit and the form fires, failing this test.
        tester().assertThat(new DecorrelateViaPlainJoinForms(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr", DOUBLE)),
                        p.values(p.symbol("corr", DOUBLE)),
                        INNER,
                        TRUE,
                        p.limit(1, p.filter(nonDeterministic("corr"), p.values(p.symbol("a", DOUBLE))))))
                .doesNotFire();
    }

    @Test
    public void tryPlainJoinLimitFiresOnDeterministicCondition()
    {
        // Control for the decline test: the identical shape with a deterministic predicate is
        // handled by the plain-join LIMIT form, confirming the decline above is due to the
        // determinism gate rather than the shape not reaching the form.
        tester().assertThat(new DecorrelateViaPlainJoinForms(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr", DOUBLE)),
                        p.values(p.symbol("corr", DOUBLE)),
                        INNER,
                        TRUE,
                        p.limit(1, p.filter(
                                comparison(LESS_THAN, new Constant(DOUBLE, 1.0), new Reference(DOUBLE, "corr")),
                                p.values(p.symbol("a", DOUBLE))))))
                .matches(join(INNER, builder -> builder
                        .filter(comparison(LESS_THAN, new Constant(DOUBLE, 1.0), new Reference(DOUBLE, "corr")))
                        .left(values("corr"))
                        .right(anyTree(values("a")))));
    }

    @Test
    public void tryGroupedAggregationLeftJoinDeclinesNonDeterministicCondition()
    {
        // A non-deterministic correlated predicate over a grouped aggregation must be evaluated per
        // input row, not once per group; the clone-free grouped LEFT-join form would lift it into
        // the ON (evaluated per group). So the form declines and the shape falls to the magic-set.
        // Delete the isDeterministic gate in PlainJoinForms.tryGroupedAggregationLeftJoin and the
        // form fires, failing this test.
        tester().assertThat(new DecorrelateViaPlainJoinForms(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr", DOUBLE)),
                        p.values(p.symbol("corr", DOUBLE)),
                        LEFT,
                        TRUE,
                        p.aggregation(ab -> ab
                                .singleGroupingSet(p.symbol("x", DOUBLE))
                                .addAggregation(p.symbol("count"), PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of())
                                .source(p.filter(nonDeterministic("corr"), p.values(p.symbol("x", DOUBLE)))))))
                .doesNotFire();
    }

    @Test
    public void tryGroupedAggregationLeftJoinFiresOnDeterministicCondition()
    {
        // Control for the decline test: the identical shape with a deterministic predicate over the
        // grouping key is handled by the clone-free grouped LEFT-join form, confirming the decline
        // above is due to the determinism gate rather than the shape not reaching the form.
        tester().assertThat(new DecorrelateViaPlainJoinForms(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr", DOUBLE)),
                        p.values(p.symbol("corr", DOUBLE)),
                        LEFT,
                        TRUE,
                        p.aggregation(ab -> ab
                                .singleGroupingSet(p.symbol("x", DOUBLE))
                                .addAggregation(p.symbol("count"), PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of())
                                .source(p.filter(
                                        comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Reference(DOUBLE, "corr")),
                                        p.values(p.symbol("x", DOUBLE)))))))
                .matches(join(LEFT, builder -> builder
                        .filter(comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Reference(DOUBLE, "corr")))
                        .left(values("corr"))
                        .right(anyTree(values("x")))));
    }
}
