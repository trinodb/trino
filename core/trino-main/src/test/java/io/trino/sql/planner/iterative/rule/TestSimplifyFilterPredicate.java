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
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;

public class TestSimplifyFilterPredicate
        extends BaseRuleTest
{
    @Test
    public void testSimplifyIfExpression()
    {
        // true result iff the condition is true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("IF(a, true, false)"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "a",
                                values("a")));

        // true result iff the condition is true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("IF(a, true)"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "a",
                                values("a")));

        // true result iff the condition is null or false
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("IF(a, false, true)"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "a IS NULL OR NOT a",
                                values("a")));

        // true result iff the condition is null or false
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("IF(a, null, true)"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "a IS NULL OR NOT a",
                                values("a")));

        // always true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("IF(a, true, true)"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "true",
                                values("a")));

        // always false
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("IF(a, false, false)"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "false",
                                values("a")));

        // both results equal
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("IF(a, b > 0, b > 0)"),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                "b > 0",
                                values("a", "b")));

        // both results are equal non-deterministic expressions
        FunctionCall randomFunction = new FunctionCall(
                tester().getMetadata().resolveFunction(tester().getSession(), QualifiedName.of("random"), ImmutableList.of()).toQualifiedName(),
                ImmutableList.of());
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new IfExpression(
                                expression("a"),
                                new ComparisonExpression(EQUAL, randomFunction, new LongLiteral("0")),
                                new ComparisonExpression(EQUAL, randomFunction, new LongLiteral("0"))),
                        p.values(p.symbol("a"))))
                .doesNotFire();

        // always null (including the default) -> simplified to FALSE
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("IF(a, null)"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "false",
                                values("a")));

        // condition is true -> first branch
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("IF(true, a, NOT a)"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "a",
                                values("a")));

        // condition is true -> second branch
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("IF(false, a, NOT a)"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "NOT a",
                                values("a")));

        // condition is true, no second branch -> the result is null, simplified to FALSE
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("IF(false, a)"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "false",
                                values("a")));

        // not known result (`b`) - cannot optimize
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("IF(a, true, b)"),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testSimplifyNullIfExpression()
    {
        // NULLIF(x, y) returns true if and only if: x != y AND x = true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("NULLIF(a, b)"),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                "a AND (b IS NULL OR NOT b)",
                                values("a", "b")));
    }

    @Test
    public void testSimplifySearchedCaseExpression()
    {
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE " +
                                "          WHEN a < 0 THEN true " +
                                "          WHEN a = 0 THEN false " +
                                "          WHEN a > 0 THEN true " +
                                "          ELSE false " +
                                "       END"),
                        p.values(p.symbol("a"))))
                .doesNotFire();

        // all results true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE " +
                                "           WHEN a < 0 THEN true " +
                                "           WHEN a = 0 THEN true " +
                                "           WHEN a > 0 THEN true " +
                                "           ELSE true " +
                                "        END"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "true",
                                values("a")));

        // all results not true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE " +
                                "           WHEN a < 0 THEN false " +
                                "           WHEN a = 0 THEN null " +
                                "           WHEN a > 0 THEN false " +
                                "           ELSE false " +
                                "        END"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "false",
                                values("a")));

        // all results not true (including default null result)
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE " +
                                "           WHEN a < 0 THEN false " +
                                "           WHEN a = 0 THEN null " +
                                "           WHEN a > 0 THEN false " +
                                "        END"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "false",
                                values("a")));

        // one result true, and remaining results not true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE " +
                                "           WHEN a < 0 THEN false " +
                                "           WHEN a = 0 THEN null " +
                                "           WHEN a > 0 THEN true " +
                                "           ELSE false " +
                                "        END"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "((a < 0) IS NULL OR NOT (a < 0)) AND ((a = 0) IS NULL OR NOT (a = 0)) AND (a > 0)",
                                values("a")));

        // first result true, and remaining results not true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE " +
                                "           WHEN a < 0 THEN true " +
                                "           WHEN a = 0 THEN null " +
                                "           WHEN a > 0 THEN false " +
                                "           ELSE false " +
                                "        END"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "a < 0",
                                values("a")));

        // all results not true, and default true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE " +
                                "           WHEN a < 0 THEN false " +
                                "           WHEN a = 0 THEN null " +
                                "           WHEN a > 0 THEN false " +
                                "           ELSE true " +
                                "        END"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "((a < 0) IS NULL OR NOT (a < 0)) AND ((a = 0) IS NULL OR NOT (a = 0)) AND ((a > 0) IS NULL OR NOT (a > 0))",
                                values("a")));

        // all conditions not true - return the default
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE " +
                                "           WHEN false THEN a " +
                                "           WHEN false THEN a " +
                                "           WHEN null THEN a " +
                                "           ELSE b " +
                                "        END"),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                "b",
                                values("a", "b")));

        // all conditions not true, no default specified - return false
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE " +
                                "           WHEN false THEN a " +
                                "           WHEN false THEN NOT a " +
                                "           WHEN null THEN a " +
                                "        END"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "false",
                                values("a")));

        // not true conditions preceding true condition - return the result associated with the true condition
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE " +
                                "           WHEN false THEN a " +
                                "           WHEN null THEN NOT a " +
                                "           WHEN true THEN b " +
                                "        END"),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                "b",
                                values("a", "b")));

        // remove not true condition and move the result associated with the first true condition to default
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE " +
                                "           WHEN false THEN a " +
                                "           WHEN b THEN NOT a " +
                                "           WHEN true THEN b " +
                                "        END"),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                "CASE WHEN b THEN NOT a ELSE b END",
                                values("a", "b")));

        // move the result associated with the first true condition to default
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE " +
                                "           WHEN b < 0 THEN a " +
                                "           WHEN b > 0 THEN NOT a " +
                                "           WHEN true THEN b " +
                                "           WHEN true THEN NOT b " +
                                "        END"),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                "CASE " +
                                        "           WHEN b < 0 THEN a " +
                                        "           WHEN b > 0 THEN NOT a " +
                                        "           ELSE b " +
                                        "        END",
                                values("a", "b")));

        // cannot remove any clause
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE " +
                                "           WHEN b < 0 THEN a " +
                                "           WHEN b > 0 THEN NOT a " +
                                "           ELSE b " +
                                "        END"),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testSimplifySimpleCaseExpression()
    {
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE a" +
                                "           WHEN b THEN true " +
                                "           WHEN b + 1 THEN false " +
                                "           ELSE true " +
                                "        END"),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .doesNotFire();

        // comparison with null returns null - no WHEN branch matches, return default value
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE null" +
                                "           WHEN null THEN true " +
                                "           WHEN a THEN false " +
                                "           ELSE b " +
                                "        END"),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                "b",
                                values("a", "b")));

        // comparison with null returns null - no WHEN branch matches, the result is default null, simplified to FALSE
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE null" +
                                "           WHEN null THEN true " +
                                "           WHEN a THEN false " +
                                "        END"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "false",
                                values("a")));

        // all results true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE a" +
                                "           WHEN b + 1 THEN true " +
                                "           WHEN b + 2 THEN true " +
                                "           ELSE true " +
                                "        END"),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                "true",
                                values("a", "b")));

        // all results not true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE a" +
                                "           WHEN b + 1 THEN false " +
                                "           WHEN b + 2 THEN null " +
                                "           ELSE false " +
                                "        END"),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                "false",
                                values("a", "b")));

        // all results not true (including default null result)
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("CASE a" +
                                "           WHEN b + 1 THEN false " +
                                "           WHEN b + 2 THEN null " +
                                "        END"),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                "false",
                                values("a", "b")));
    }

    @Test
    public void testCastNull()
    {
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        expression("IF(a, CAST(CAST(CAST(null AS boolean) AS bigint) AS boolean), false)"),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                "false",
                                values("a")));
    }
}
