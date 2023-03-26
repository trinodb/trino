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

import io.trino.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static java.lang.String.format;

public class TestRemoveFalseFiltersAfterDomainTranslator
        extends BasePlanTest
{
    @Test
    public void testPredicateIsFalse()
    {
        testRewrite("FALSE");
        testRewrite("NOT (TRUE)");
    }

    @Test
    public void testCastNullToBoolean()
    {
        testRewriteWithAndWithoutNegation("CAST(null AS boolean)");
    }

    @Test
    public void testLogicalExpression()
    {
        testRewrite("TRUE AND CAST(null AS boolean)");
        testRewrite("col = BIGINT '44' AND CAST(null AS boolean)");
        testRewrite("col = BIGINT '44' AND FALSE");
    }

    @Test
    public void testComparison()
    {
        testRewriteWithAndWithoutNegation("col > CAST(null AS BIGINT)");
        testRewriteWithAndWithoutNegation("col >= CAST(null AS BIGINT)");
        testRewriteWithAndWithoutNegation("col < CAST(null AS BIGINT)");
        testRewriteWithAndWithoutNegation("col <= CAST(null AS BIGINT)");
        testRewriteWithAndWithoutNegation("col = CAST(null AS BIGINT)");
        testRewriteWithAndWithoutNegation("col != CAST(null AS BIGINT)");
    }

    @Test
    public void testIn()
    {
        testRewriteWithAndWithoutNegation("col IN (CAST(null AS BIGINT))");
    }

    @Test
    public void testBetween()
    {
        testRewrite("col BETWEEN (CAST(null AS BIGINT)) AND BIGINT '44'");
        testRewrite("col BETWEEN BIGINT '44' AND (CAST(null AS BIGINT))");
    }

    private void testRewriteWithAndWithoutNegation(String inputPredicate)
    {
        testRewrite(inputPredicate);
        testRewrite(format("NOT (%s)", inputPredicate));
    }

    private void testRewrite(String inputPredicate)
    {
        assertPlan(String.format("SELECT * FROM (VALUES BIGINT '1') t(col) WHERE %s", inputPredicate),
                output(
                        values("a")));
    }
}
