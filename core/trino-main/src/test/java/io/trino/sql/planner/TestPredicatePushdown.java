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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;

public class TestPredicatePushdown
        extends AbstractPredicatePushdownTest
{
    public TestPredicatePushdown()
    {
        super(true);
    }

    @Test
    @Override
    public void testCoercions()
    {
        // Ensure constant equality predicate is pushed to the other side of the join
        // when type coercions are involved

        // values have the same type (varchar(4)) in both tables
        assertPlan(
                "WITH " +
                        "    t(k, v) AS (SELECT nationkey, CAST(name AS varchar(4)) FROM nation)," +
                        "    u(k, v) AS (SELECT nationkey, CAST(name AS varchar(4)) FROM nation) " +
                        "SELECT 1 " +
                        "FROM t JOIN u ON t.k = u.k AND t.v = u.v " +
                        "WHERE t.v = 'x'",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("t_k", "u_k")
                                .dynamicFilter("t_k", "u_k")
                                .left(
                                        project(
                                                filter(
                                                        "CAST('x' AS varchar(4)) = CAST(t_v AS varchar(4))",
                                                        tableScan("nation", ImmutableMap.of("t_k", "nationkey", "t_v", "name")))))
                                .right(
                                        anyTree(
                                                project(
                                                        filter(
                                                                "CAST('x' AS varchar(4)) = CAST(u_v AS varchar(4))",
                                                                tableScan("nation", ImmutableMap.of("u_k", "nationkey", "u_v", "name")))))))));

        // values have different types (varchar(4) vs varchar(5)) in each table
        assertPlan(
                "WITH " +
                        "    t(k, v) AS (SELECT nationkey, CAST(name AS varchar(4)) FROM nation)," +
                        "    u(k, v) AS (SELECT nationkey, CAST(name AS varchar(5)) FROM nation) " +
                        "SELECT 1 " +
                        "FROM t JOIN u ON t.k = u.k AND t.v = u.v " +
                        "WHERE t.v = 'x'",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("t_k", "u_k")
                                .dynamicFilter("t_k", "u_k")
                                .left(
                                        project(
                                                filter(
                                                        "CAST('x' AS varchar(4)) = CAST(t_v AS varchar(4))",
                                                        tableScan("nation", ImmutableMap.of("t_k", "nationkey", "t_v", "name")))))
                                .right(
                                        anyTree(
                                                project(
                                                        filter(
                                                                "CAST('x' AS varchar(5)) = CAST(u_v AS varchar(5))",
                                                                tableScan("nation", ImmutableMap.of("u_k", "nationkey", "u_v", "name")))))))));
    }
}
