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
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestUnwrapRowSubscript
        extends BaseRuleTest
{
    @Test
    public void testSimpleSubscript()
    {
        test("ROW(1)[1]", "1");
        test("ROW(1, 2)[1]", "1");
        test("ROW(ROW(1, 2), 3)[1][2]", "2");
    }

    @Test
    public void testWithCast()
    {
        test("CAST(ROW(1, 2) AS row(a bigint, b bigint))[1]", "CAST(1 AS bigint)");
        test("CAST(ROW(1, 2) AS row(bigint, bigint))[1]", "CAST(1 AS bigint)");

        test(
                "CAST(CAST(ROW(ROW(1, 2), 3) AS row(row(smallint, smallint), bigint))[1] AS ROW(x bigint, y bigint))[2]",
                "CAST(CAST(2 AS smallint) AS bigint)");
    }

    @Test
    public void testWithTryCast()
    {
        test("TRY_CAST(ROW(1, 2) AS row(a bigint, b bigint))[1]", "TRY_CAST(1 AS bigint)");
        test("TRY_CAST(ROW(1, 2) AS row(bigint, bigint))[1]", "TRY_CAST(1 AS bigint)");

        test(
                "TRY_CAST(TRY_CAST(ROW(ROW(1, 2), 3) AS row(row(smallint, smallint), bigint))[1] AS ROW(x bigint, y bigint))[2]",
                "TRY_CAST(TRY_CAST(2 AS smallint) AS bigint)");
    }

    private void test(@Language("SQL") String original, @Language("SQL") String unwrapped)
    {
        tester().assertThat(new UnwrapRowSubscript().projectExpressionRewrite())
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
