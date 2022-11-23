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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.testing.LocalQueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.sql.planner.LogicalPlanner.Stage.CREATED;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.union;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestRecursiveCte
        extends BasePlanTest
{
    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setSystemProperty("max_recursion_depth", "1");

        return LocalQueryRunner.create(sessionBuilder.build());
    }

    @Test
    public void testRecursiveQuery()
    {
        @Language("SQL") String sql = "WITH RECURSIVE t(n) AS (" +
                "                SELECT 1" +
                "                UNION ALL" +
                "                SELECT n + 2 FROM t WHERE n < 6" +
                "                )" +
                "                SELECT * from t";

        PlanMatchPattern pattern =
                anyTree(
                        union(
                                // base term
                                project(project(project(
                                        ImmutableMap.of("expr", expression("1")),
                                        values()))),
                                // first recursion step
                                project(project(project(
                                        ImmutableMap.of("expr_0", expression("expr + 2")),
                                        filter(
                                                "expr < 6",
                                                project(project(project(
                                                        ImmutableMap.of("expr", expression("1")),
                                                        values()))))))),
                                // "post-recursion" step with convergence assertion
                                filter(
                                        "IF((count >= BIGINT '0'), " +
                                                format("CAST(fail(INTEGER '%d', VARCHAR 'Recursion depth limit exceeded (1). Use ''max_recursion_depth'' session property to modify the limit.') AS boolean), ",
                                                        NOT_SUPPORTED.toErrorCode().getCode()) +
                                                "true)",
                                        window(windowBuilder -> windowBuilder
                                                        .addFunction(
                                                                "count",
                                                                functionCall("count", ImmutableList.of())),
                                                project(project(project(
                                                        ImmutableMap.of("expr_1", expression("expr + 2")),
                                                        filter(
                                                                "expr < 6",
                                                                project(
                                                                        ImmutableMap.of("expr", expression("expr_0")),
                                                                        project(project(project(
                                                                                ImmutableMap.of("expr_0", expression("expr + 2")),
                                                                                filter(
                                                                                        "expr < 6",
                                                                                        project(project(project(
                                                                                                ImmutableMap.of("expr", expression("1")),
                                                                                                values()))))))))))))))));

        assertPlan(sql, CREATED, pattern);
    }
}
