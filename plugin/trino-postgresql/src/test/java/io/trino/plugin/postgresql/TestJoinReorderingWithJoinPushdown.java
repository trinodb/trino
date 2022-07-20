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
package io.trino.plugin.postgresql;

import io.trino.Session;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.jdbc.JdbcJoinPushdownSessionProperties.JOIN_PUSHDOWN_STRATEGY;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.JOIN_PUSHDOWN_ENABLED;
import static io.trino.plugin.postgresql.PostgreSqlQueryRunner.createPostgreSqlQueryRunner;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJoinReorderingWithJoinPushdown
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingPostgreSqlServer postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());
        DistributedQueryRunner distributedQueryRunner = createPostgreSqlQueryRunner(
                postgreSqlServer,
                Map.of(),
                Map.of(),
                List.of(CUSTOMER, NATION));

        postgreSqlServer.execute("ANALYZE " + CUSTOMER.getTableName());
        postgreSqlServer.execute("ANALYZE " + NATION.getTableName());

        return distributedQueryRunner;
    }

    @Test
    public void testJoinReordering()
    {
        // disable join pushdown in default session
        // also disable dynamic filtering to simplify plans for ease of matching
        Session session = Session.builder(getSession())
                .setSystemProperty("enable_dynamic_filtering", "false")
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), JOIN_PUSHDOWN_ENABLED, "false")
                .build();
        Session joinPushdownEnabled = Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), JOIN_PUSHDOWN_ENABLED, "true")
                // force a join pushdown if possible otherwise join may not be pushed down depending on statistics
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), JOIN_PUSHDOWN_STRATEGY, "EAGER")
                .build();

        String sql = "SELECT c.name, o.clerk, n.name " +
                "FROM customer c " +
                "INNER JOIN nation n ON c.nationkey = n.nationkey " +
                "INNER JOIN tpch.tiny.orders o ON c.custkey = o.custkey";

        PlanMatchPattern joinWithoutFilter =
                anyTree(
                        join(INNER, List.of(equiJoinClause("o_custkey", "c_custkey")), Optional.empty(), Optional.of(PARTITIONED),
                                anyTree(
                                        tableScan("orders", Map.of("o_custkey", "custkey"))),
                                anyTree(
                                        join(INNER, List.of(equiJoinClause("c_nationkey", "n_nationkey")), Optional.empty(), Optional.of(PARTITIONED),
                                                anyTree(
                                                        tableScan("customer", Map.of("c_custkey", "custkey", "c_nationkey", "nationkey"))),
                                                anyTree(
                                                        tableScan("nation", Map.of("n_nationkey", "nationkey")))))));

        PlanMatchPattern joinWithoutFilterPushedDown =
                anyTree(
                        node(JoinNode.class,
                                anyTree(
                                        tableScan("orders")),
                                anyTree(
                                        tableScan("_generated_query"))));

        // no reordering needed and pushdown is possible
        assertThat(query(session, sql)).isNotFullyPushedDown(joinWithoutFilter);
        assertThat(query(joinPushdownEnabled, sql)).isNotFullyPushedDown(joinWithoutFilterPushedDown);
        assertThat(query(session, sql + " WHERE o.orderkey >= 0")).isNotFullyPushedDown(joinWithoutFilter);
        assertThat(query(joinPushdownEnabled, sql + " WHERE o.orderkey >= 0")).isNotFullyPushedDown(joinWithoutFilterPushedDown);

        PlanMatchPattern joinWithSelectiveFilterReordered =
                anyTree(
                        join(INNER, List.of(equiJoinClause("n_nationkey", "c_nationkey")), Optional.empty(), Optional.of(PARTITIONED),
                                anyTree(
                                        tableScan("nation", Map.of("n_nationkey", "nationkey"))),
                                anyTree(
                                        join(INNER, List.of(equiJoinClause("c_custkey", "o_custkey")), Optional.empty(), Optional.of(PARTITIONED),
                                                anyTree(
                                                        tableScan("customer", Map.of("c_custkey", "custkey", "c_nationkey", "nationkey"))),
                                                anyTree(
                                                        tableScan("orders", Map.of("o_custkey", "custkey")))))));

        // join with a highly selective filter on orders causes reordering (and prevents pushdown)
        assertThat(query(session, sql + " WHERE o.orderkey = 1")).isNotFullyPushedDown(joinWithSelectiveFilterReordered);
        assertThat(query(joinPushdownEnabled, sql + " WHERE o.orderkey = 1")).isNotFullyPushedDown(joinWithSelectiveFilterReordered);

        PlanMatchPattern joinWithFilterReordered =
                anyTree(
                        join(INNER, List.of(equiJoinClause("c_nationkey", "n_nationkey")), Optional.empty(), Optional.of(PARTITIONED),
                                anyTree(
                                        join(INNER, List.of(equiJoinClause("c_custkey", "o_custkey")), Optional.empty(), Optional.of(PARTITIONED),
                                                anyTree(
                                                        tableScan("customer", Map.of("c_custkey", "custkey", "c_nationkey", "nationkey"))),
                                                anyTree(
                                                        tableScan("orders", Map.of("o_custkey", "custkey"))))),
                                anyTree(
                                        tableScan("nation", Map.of("n_nationkey", "nationkey")))));

        // join with a filter on orders causes reordering (and prevents pushdown)
        assertThat(query(session, sql + " WHERE o.orderkey < 500")).isNotFullyPushedDown(joinWithFilterReordered);
        assertThat(query(joinPushdownEnabled, sql + " WHERE o.orderkey < 500")).isNotFullyPushedDown(joinWithFilterReordered);
    }
}
