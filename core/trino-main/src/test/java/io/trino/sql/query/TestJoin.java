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
package io.trino.sql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.plan.JoinNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestJoin
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testCrossJoinEliminationWithOuterJoin()
    {
        assertThat(assertions.query(
                "WITH " +
                        "  a AS (SELECT id FROM (VALUES (1)) AS t(id))," +
                        "  b AS (SELECT id FROM (VALUES (1)) AS t(id))," +
                        "  c AS (SELECT id FROM (VALUES ('1')) AS t(id))," +
                        "  d as (SELECT id FROM (VALUES (1)) AS t(id))" +
                        "SELECT a.id " +
                        "FROM a " +
                        "LEFT JOIN b ON a.id = b.id " +
                        "JOIN c ON a.id = CAST(c.id AS bigint) " +
                        "JOIN d ON d.id = a.id"))
                .matches("VALUES 1");
    }

    @Test
    public void testJoinOnNan()
    {
        assertThat(assertions.query(
                "WITH t(x) AS (VALUES if(rand() > 0, nan())) " + // TODO: remove if(rand() > 0, ...) once https://github.com/trinodb/trino/issues/4119 is fixed
                        "SELECT * FROM t t1 JOIN t t2 ON NOT t1.x < t2.x"))
                .matches("VALUES (nan(), nan())");
    }

    @Test
    public void testInPredicateInJoinCriteria()
    {
        // IN with subquery containing column references
        assertThat(assertions.query("" +
                "WITH " +
                "    t(x, y) AS (VALUES (1, 10), (2, 20)), " +
                "    u(x) AS (VALUES 1, 2), " +
                "    w(z) AS (VALUES 10, 20) " +
                "SELECT *\n" +
                "FROM t LEFT JOIN u ON t.x = u.x AND t.y IN (SELECT z FROM w)"))
                .matches("VALUES (2, 20, 2), (1, 10, 1)");

        assertThat(assertions.query("SELECT * FROM (VALUES 1, 2, NULL) t(x) JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES 1)"))
                .matches("VALUES (1, 1), (1, 3), (1, NULL)");

        assertThat(assertions.query("SELECT * FROM (VALUES 1, 2, NULL) t(x) LEFT JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES 1)"))
                .matches("VALUES (1, 1), (1, 3), (1, NULL), (2, NULL), (NULL, NULL)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) RIGHT JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES 1)"))
                .matches("VALUES (1, 1), (1, 3), (1, NULL)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) FULL JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES 1)"))
                .matches("VALUES (1, 1), (1, 3), (1, NULL), (2, NULL), (NULL, NULL)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES 1)"))
                .matches("VALUES (1, 1), (2, 1), (NULL, 1)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) LEFT JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES 1)"))
                .matches("VALUES (1, 1), (2, 1), (NULL, 1)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) RIGHT JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES 1)"))
                .matches("VALUES (1, 1), (2, 1), (NULL, 1), (NULL, 3), (NULL, NULL)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) FULL JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES 1)"))
                .matches("VALUES (1, 1), (2, 1), (NULL, 1), (NULL, 3), (NULL, NULL)");

        // correlated subquery in inner join clause
        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (SELECT v.x FROM (VALUES 1, 2) v(x) WHERE u.x = v.x)"))
                .matches("VALUES (1,1)");

        assertThat(assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (SELECT v.x FROM (VALUES 1, 2) v(x) WHERE t.x = v.x)"))
                .matches("VALUES (1,1)");

        // correlation in join clause not allowed for outer join
        assertThatThrownBy(() -> assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) FULL JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES t.x)"))
                .hasMessage("line 1:93: Reference to column 't.x' from outer scope not allowed in this context");

        assertThatThrownBy(() -> assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) FULL JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES u.x)"))
                .hasMessage("line 1:93: Reference to column 'u.x' from outer scope not allowed in this context");

        assertThatThrownBy(() -> assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) FULL JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES t.x)"))
                .hasMessage("line 1:93: Reference to column 't.x' from outer scope not allowed in this context");

        assertThatThrownBy(() -> assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) FULL JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES u.x)"))
                .hasMessage("line 1:93: Reference to column 'u.x' from outer scope not allowed in this context");

        assertThatThrownBy(() -> assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) LEFT JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES t.x)"))
                .hasMessage("line 1:93: Reference to column 't.x' from outer scope not allowed in this context");

        assertThatThrownBy(() -> assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) LEFT JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES u.x)"))
                .hasMessage("line 1:93: Reference to column 'u.x' from outer scope not allowed in this context");

        assertThatThrownBy(() -> assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) LEFT JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES t.x)"))
                .hasMessage("line 1:93: Reference to column 't.x' from outer scope not allowed in this context");

        assertThatThrownBy(() -> assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) LEFT JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES u.x)"))
                .hasMessage("line 1:93: Reference to column 'u.x' from outer scope not allowed in this context");

        assertThatThrownBy(() -> assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) RIGHT JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES t.x)"))
                .hasMessage("line 1:94: Reference to column 't.x' from outer scope not allowed in this context");

        assertThatThrownBy(() -> assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) RIGHT JOIN (VALUES 1, 3, NULL) u(x) ON u.x IN (VALUES u.x)"))
                .hasMessage("line 1:94: Reference to column 'u.x' from outer scope not allowed in this context");

        assertThatThrownBy(() -> assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) RIGHT JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES t.x)"))
                .hasMessage("line 1:94: Reference to column 't.x' from outer scope not allowed in this context");

        assertThatThrownBy(() -> assertions.query(
                "SELECT * FROM (VALUES 1, 2, NULL) t(x) RIGHT JOIN (VALUES 1, 3, NULL) u(x) ON t.x IN (VALUES u.x)"))
                .hasMessage("line 1:94: Reference to column 'u.x' from outer scope not allowed in this context");
    }

    @Test
    public void testQuantifiedComparisonInJoinCriteria()
    {
        assertThat(assertions.query("SELECT * FROM (VALUES 1, 2, NULL) t(x) RIGHT JOIN (VALUES 1, 3, NULL) u(x) ON u.x > ALL (VALUES 1)"))
                .matches("VALUES (1, 3), (2, 3), (NULL, 3), (NULL, 1), (NULL, NULL)");

        assertThat(assertions.query("SELECT * FROM (VALUES 1, 2, NULL) t(x) JOIN (VALUES 1, 3, NULL) u(x) ON t.x + u.x > ALL (VALUES 2)"))
                .matches("VALUES (1, 3), (2, 1), (2, 3)");

        // TODO: this should fail during analysis, but currently fails during planning
        //   StatementAnalyzer.visitJoin needs to be updated to check whether the join criteria is an InPredicate or QualifiedComparison
        //   with mixed references to both sides of the join. For that, the Expression needs to be analyzed against a hybrid scope made of both branches
        //   of the join, instead of using the output scope of the Join node. This, in turn requires adding support for multiple scopes in ExpressionAnalyzer
        assertThatThrownBy(() -> assertions.query("SELECT * FROM (VALUES 1, 2, NULL) t(x) RIGHT JOIN (VALUES 1, 3, NULL) u(x) ON t.x + u.x > ALL (VALUES 1)"));
    }

    @Test
    public void testOutputDuplicatesInsensitiveJoin()
    {
        assertions.assertQueryAndPlan(
                "SELECT t.x, count(*) FROM (VALUES 1, 2) t(x) JOIN (VALUES 2, 2) u(x) ON t.x = u.x GROUP BY t.x",
                "VALUES (2, BIGINT '2')",
                anyTree(
                        aggregation(
                                ImmutableMap.of("COUNT", functionCall("count", ImmutableList.of())),
                                anyTree(
                                        join(INNER, ImmutableList.of(),
                                                anyTree(
                                                        values("y")),
                                                values())
                                                .with(JoinNode.class, not(JoinNode::isMaySkipOutputDuplicates))))));

        assertions.assertQueryAndPlan(
                "SELECT t.x FROM (VALUES 1, 2) t(x) JOIN (VALUES 2, 2) u(x) ON t.x = u.x GROUP BY t.x",
                "VALUES 2",
                anyTree(
                        aggregation(
                                ImmutableMap.of(),
                                FINAL,
                                anyTree(
                                        join(INNER, ImmutableList.of(),
                                                anyTree(
                                                        values("y")),
                                                values())
                                                .with(JoinNode.class, JoinNode::isMaySkipOutputDuplicates)))));
    }
}
