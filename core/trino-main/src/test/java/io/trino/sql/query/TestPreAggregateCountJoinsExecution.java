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

import io.trino.Session;
import io.trino.operator.aggregation.CountJoinAggregation;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.PlanNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.stream.IntStream;

import static io.trino.SystemSessionProperties.PRE_AGGREGATE_COUNT_JOINS;
import static io.trino.testing.assertions.Assert.assertEqualsIgnoreOrder;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestPreAggregateCountJoinsExecution
{
    private final QueryAssertions assertions = new QueryAssertions();
    private final Session enabled = assertions.sessionBuilder().setSystemProperty(PRE_AGGREGATE_COUNT_JOINS, "true").build();

    @AfterAll
    public void close()
    {
        assertions.close();
    }

    @Test
    public void testDuplicatesCompositeKeysAndNulls()
    {
        String rows = IntStream.range(0, 240)
                .mapToObj(i -> "(%s, %s, %s)".formatted(i % 7 == 0 ? "NULL" : i % 4, i % 11 == 0 ? "NULL" : (i / 4) % 2, i % 5 == 0 ? "NULL" : i % 3))
                .collect(joining(","));
        String from = " FROM (VALUES " + rows + ") l(k, k2, a) JOIN (VALUES " + rows + ") r(k, k2, b) ON l.k = r.k AND l.k2 = r.k2";
        assertEquivalent("SELECT l.a, r.b, count(*)" + from + " GROUP BY l.a, r.b", true);
        assertEquivalent("SELECT count(*)" + from, true);
        assertEquivalent("SELECT l.k, count(*)" + from + " GROUP BY l.k", true);
        assertEquivalent("SELECT count(l.a)" + from, false);
        assertEquivalent("SELECT count(DISTINCT l.a)" + from, false);
        assertEquivalent("SELECT count(*) FILTER (WHERE l.a = 1)" + from, false);
        assertEquivalent("SELECT count(*)" + from + " AND l.a < r.b", false);
        assertEquivalent("SELECT l.a, r.b, count(*)" + from + " GROUP BY GROUPING SETS ((l.a), (r.b), ())", false);
        assertEquivalent("SELECT count(*)" + from.replace("JOIN", "LEFT JOIN"), false);
    }

    @Test
    public void testEmptyAndNonMatchingInputs()
    {
        for (String left : new String[] {"(SELECT 1 k WHERE false)", "(VALUES NULL)", "(VALUES 1, 1)"}) {
            String from = " FROM " + left + " l(k) JOIN (VALUES 2, 2) r(k) ON l.k = r.k";
            assertThat(assertions.query(enabled, "SELECT count(*)" + from)).matches("VALUES BIGINT '0'");
            assertThat(assertions.query(enabled, "SELECT l.k, count(*)" + from + " GROUP BY l.k")).returnsEmptyResult();
        }
        assertThat(assertions.query("SELECT \"$internal$count_join\"(x, x) FROM (SELECT BIGINT '1' x WHERE false)"))
                .matches("VALUES BIGINT '0'");
    }

    @Test
    public void testCompressedJoinWithNoMatches()
    {
        String left = IntStream.range(0, 100).mapToObj(i -> "(%s, %s)".formatted(i % 2, i % 2)).collect(joining(","));
        String right = IntStream.range(0, 100).mapToObj(i -> "(%s, %s)".formatted(i % 2, 1 - i % 2)).collect(joining(","));
        String from = " FROM (VALUES " + left + ") l(k,j) JOIN (VALUES " + right + ") r(k,j) ON l.k=r.k AND l.j=r.j";
        assertEquivalent("SELECT count(*)" + from, true);
        assertEquivalent("SELECT l.k, count(*)" + from + " GROUP BY l.k", true);
    }

    @Test
    public void testCountOverflowSemantics()
    {
        assertThat(assertions.query("SELECT \"$internal$count_join\"(x, y) FROM (VALUES (BIGINT '9223372036854775807', BIGINT '2'), (BIGINT '1', BIGINT '3')) t(x,y)"))
                .matches("VALUES BIGINT '1'");
        assertThat(assertions.query("SELECT \"$internal$count_join\"(x, y) FROM (VALUES (BIGINT '-9223372036854775808', BIGINT '-1')) t(x,y)"))
                .matches("VALUES BIGINT '-9223372036854775808'");
    }

    @Test
    public void testLowDuplication()
    {
        String rows = IntStream.range(0, 100).mapToObj(Integer::toString).collect(joining(","));
        assertEquivalent("SELECT count(*) FROM (VALUES " + rows + ") l(k) JOIN (VALUES " + rows + ") r(k) ON l.k=r.k", false);
    }

    private void assertEquivalent(String sql, boolean rewritten)
    {
        var baseline = assertions.getQueryRunner().executeWithPlan(assertions.getDefaultSession(), sql);
        var optimized = assertions.getQueryRunner().executeWithPlan(enabled, sql);
        assertThat(optimized.result().getTypes()).isEqualTo(baseline.result().getTypes());
        assertEqualsIgnoreOrder(optimized.result().getMaterializedRows(), baseline.result().getMaterializedRows());
        assertThat(hasCountJoin(baseline.queryPlan().orElseThrow().getRoot())).isFalse();
        assertThat(hasCountJoin(optimized.queryPlan().orElseThrow().getRoot())).as(sql).isEqualTo(rewritten);
    }

    static boolean hasCountJoin(PlanNode node)
    {
        if (node instanceof AggregationNode aggregation && aggregation.getAggregations().values().stream()
                .anyMatch(value -> value.getResolvedFunction().signature().getName().functionName().equals(CountJoinAggregation.NAME))) {
            return true;
        }
        return node.getSources().stream().anyMatch(TestPreAggregateCountJoinsExecution::hasCountJoin);
    }
}
