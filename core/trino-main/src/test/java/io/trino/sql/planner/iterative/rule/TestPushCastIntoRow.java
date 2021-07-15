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

import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestPushCastIntoRow
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        // anonymous row type
        test("CAST(ROW(1) AS row(bigint))", "ROW(CAST(1 AS bigint))");
        test("CAST(ROW(1, 'a') AS row(bigint, varchar))", "ROW(CAST(1 AS bigint), CAST('a' AS varchar))");
        test("CAST(CAST(ROW(1, 'a') AS row(smallint, varchar)) AS ROW(bigint, varchar))", "ROW(CAST(CAST(1 AS smallint) AS bigint), CAST(CAST('a' AS varchar) AS varchar))");

        // named fields in top-level cast preserved
        test("CAST(CAST(ROW(1, 'a') AS row(smallint, varchar)) AS ROW(x bigint, varchar))", "CAST(ROW(CAST(1 AS smallint), CAST('a' AS varchar)) AS ROW(x bigint, varchar))");
        test("CAST(CAST(ROW(1, 'a') AS row(a smallint, b varchar)) AS ROW(x bigint, varchar))", "CAST(ROW(CAST(1 AS smallint), CAST('a' AS varchar)) AS ROW(x bigint, varchar))");

        // expression nested in another unrelated expression
        test("CAST(ROW(1) AS row(bigint))[1]", "ROW(CAST(1 AS bigint))[1]");

        // don't insert CAST(x AS unknown)
        test("CAST(ROW(NULL) AS row(unknown))", "ROW(NULL)");
    }

    private void test(String original, String unwrapped)
    {
        tester().assertThat(new PushCastIntoRow().projectExpressionRewrite())
                .on(p -> p.project(
                        Assignments.builder()
                                .put(p.symbol("output"), expression(original))
                                .build(),
                        p.values()))
                .matches(
                        project(Map.of("output", PlanMatchPattern.expression(unwrapped)),
                                values()));
    }
}
