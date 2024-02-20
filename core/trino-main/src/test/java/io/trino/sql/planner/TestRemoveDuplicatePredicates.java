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
package io.trino.sql.planner;

import io.trino.sql.planner.assertions.BasePlanTest;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestRemoveDuplicatePredicates
        extends BasePlanTest
{
    @Test
    public void testAnd()
    {
        assertPlan(
                "SELECT * FROM (VALUES 1) t(a) WHERE a = 1 AND 1 = a AND a = 1",
                anyTree(
                        filter("A = 1",
                                values("A"))));
    }

    @Test
    public void testOr()
    {
        assertPlan(
                "SELECT * FROM (VALUES 1) t(a) WHERE a = 1 OR 1 = a OR a = 1",
                anyTree(
                        filter("A = 1",
                                values("A"))));
    }
}
