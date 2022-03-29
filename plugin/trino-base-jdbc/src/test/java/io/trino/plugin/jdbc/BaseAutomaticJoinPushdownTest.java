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
package io.trino.plugin.jdbc;

import com.google.common.base.Strings;
import io.trino.Session;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.Test;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseAutomaticJoinPushdownTest
        extends AbstractTestQueryFramework
{
    @Test
    public void testJoinPushdownWithEmptyStatsInitially()
    {
        Session session = joinPushdownAutomatic(getSession());

        try (TestTable left = joinTestTable("left", 2_000, 500);
                TestTable right = joinTestTable("right", 1_000, 1_000)) {
            // pushdown should not happen without stats even if allowed join_to_tables ration is extremely high

            // no stats on left and right
            assertThat(query(maxJoinToTablesRatio(session, 50.0), format("SELECT * FROM %s l JOIN %s r ON l.key = r.key", left.getName(), right.getName()))).isNotFullyPushedDown(joinOverTableScans());

            // stats only for left
            gatherStats(left.getName());
            assertThat(query(maxJoinToTablesRatio(session, 50.0), format("SELECT * FROM %s l JOIN %s r ON l.key = r.key", left.getName(), right.getName()))).isNotFullyPushedDown(joinOverTableScans());

            // both tables with stats
            gatherStats(right.getName());
            assertThat(query(maxJoinToTablesRatio(session, 50.0), format("SELECT * FROM %s l JOIN %s r ON l.key = r.key", left.getName(), right.getName()))).isFullyPushedDown();
        }
    }

    @Test
    public void testCrossJoinNoPushdown()
    {
        Session session = joinPushdownAutomatic(getSession());

        try (TestTable left = joinTestTable("left", 1_000, 1);
                TestTable right = joinTestTable("right", 100, 1)) {
            gatherStats(left.getName());
            gatherStats(right.getName());

            // single NDV in each table logically results in a cross join; should not be pushed down even at high allowed join_to_tables ratio
            assertThat(query(maxJoinToTablesRatio(session, 5.0), format("SELECT * FROM %s l JOIN %s r ON l.key = r.key", left.getName(), right.getName()))).isNotFullyPushedDown(joinOverTableScans());
        }
    }

    @Test
    public void testJoinPushdownAutomatic()
    {
        Session session = joinPushdownAutomatic(getSession());

        try (TestTable left = joinTestTable("left", 6_000, 750);
                TestTable right = joinTestTable("right", 1_000, 1_000)) {
            gatherStats(left.getName());
            gatherStats(right.getName());

            String simpleJoinQuery = "SELECT * FROM %s l JOIN %s r ON l.key = r.key";
            // estimated left table size is ~444_000 bytes
            // estimated right table size is ~74_000 bytes
            // estimated join size is ~834_000

            // with default configuration such join should not be pushed down;
            // allowed join_to_tables ratio is 1.25 hence join size need to be less than (444_000 + 74_000) * 1.25 == 647_500
            assertThat(query(session, format(simpleJoinQuery, left.getName(), right.getName())))
                    .isNotFullyPushedDown(joinOverTableScans());

            // relax allowed ratio to 2.0; base line is 834_000 / (444_000 + 74_000) == 1.61 but we add some margin to cover possible mistakes in NDV calculations.
            assertThat(query(maxJoinToTablesRatio(session, 2.0), format(simpleJoinQuery, left.getName(), right.getName())))
                    .isFullyPushedDown();

            // keep ratio on level which allows pushdown but allow only very small tables in join pushdown
            Session onlySmallTablesAllowed = Session.builder(maxJoinToTablesRatio(session, 2.0))
                    .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_automatic_max_table_size", "1kB")
                    .build();
            assertThat(query(onlySmallTablesAllowed, format(simpleJoinQuery, left.getName(), right.getName()))).isNotFullyPushedDown(joinOverTableScans());

            // a query which constraints size of join output; only join key is in output
            String smallJoinOutputQuery = format("SELECT l.key FROM %s l JOIN %s r ON l.key = r.key", left.getName(), right.getName());
            // estimated left table size is ~54_000
            // estimated right table size is ~9_000
            // estimated join size is ~54_000 (same as left table)

            // allowed join_to_tables ratio is 1.25 and join size is 54_000 which is less than (54_000 + 9_000) * 1.25 == 78_750 and is pushed down
            assertThat(query(session, smallJoinOutputQuery)).isFullyPushedDown();

            // if we move threshold lower it will not be pushed down any more
            assertThat(query(maxJoinToTablesRatio(session, 1.0), format(simpleJoinQuery, left.getName(), right.getName())))
                    .isNotFullyPushedDown(joinOverTableScans());
        }
    }

    /**
     * Automatic join pushdown requires stats for join sources, so this will work only if aggregation pushdown
     * does not prevent stats from being present.
     */
    @Test
    public void testAutomaticJoinPushdownOverAggregationPushdown()
    {
        Session session = joinPushdownAutomatic(getSession());

        try (TestTable left = joinTestTable("left", 1_000, 100);
                TestTable right = joinTestTable("right", 100, 50)) {
            gatherStats(left.getName());
            gatherStats(right.getName());

            assertThat(query(session, format("" +
                            "SELECT * " +
                            "FROM %s l " +
                            "JOIN (SELECT DISTINCT key FROM %s) r ON l.key = r.key",
                    left.getName(),
                    right.getName())))
                    .isFullyPushedDown();
        }
    }

    /**
     * Automatic join pushdown requires stats for join sources, so this will work only if first join pushdown
     * does not prevent stats from being present for the second join pushdown to take place.
     */
    @Test
    public void testAutomaticJoinPushdownTwice()
    {
        Session session = joinPushdownAutomatic(getSession());

        try (TestTable first = joinTestTable("first", 1_000, 1_000);
                TestTable second = joinTestTable("second", 1_000, 1_000);
                TestTable third = joinTestTable("third", 1_000, 1_000)) {
            gatherStats(first.getName());
            gatherStats(second.getName());
            gatherStats(third.getName());

            assertThat(query(session, format("" +
                            "SELECT * " +
                            "FROM %s first, %s second, %s third " +
                            "WHERE first.key = second.key AND second.key = third.key " +
                            "AND third.intpadding = 42", // one table is highly filtered for the join pushdown to always make sense
                    first.getName(),
                    second.getName(),
                    third.getName())))
                    .isFullyPushedDown();
        }
    }

    protected static PlanMatchPattern joinOverTableScans()
    {
        return node(JoinNode.class,
                anyTree(node(TableScanNode.class)),
                anyTree(node(TableScanNode.class)));
    }

    private TestTable joinTestTable(String name, long rowsCount, int keyDistinctValues)
    {
        String sourceTable = "tpch.tiny.orders";
        checkArgument(rowsCount < ((long) computeScalar("SELECT count(*) FROM " + sourceTable)), "rowsCount too high: %s", rowsCount);
        String padding = Strings.repeat("x", 50);
        return new TestTable(
                tableCreator(),
                name,
                format("(key, padding, intpadding) AS SELECT mod(orderkey, %s), '%s', orderkey FROM %s ORDER BY orderkey LIMIT %s", keyDistinctValues, padding, sourceTable, rowsCount));
    }

    protected SqlExecutor tableCreator()
    {
        return getQueryRunner()::execute;
    }

    protected abstract void gatherStats(String tableName);

    protected Session joinPushdownAutomatic(Session session)
    {
        return Session.builder(joinPushdownEnabled(session))
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "AUTOMATIC")
                .build();
    }

    protected Session joinPushdownEnabled(Session session)
    {
        // If join pushdown gets enabled by default, tests should use default session
        verify(!new JdbcMetadataConfig().isJoinPushdownEnabled());
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_enabled", "true")
                .build();
    }

    private Session maxJoinToTablesRatio(Session session, double ratio)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_automatic_max_join_to_tables_ratio", String.valueOf(ratio))
                .build();
    }
}
