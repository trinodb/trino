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
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.sql.ir.Let;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIssue30608
{
    @Test
    public void test()
    {
        Session session = testSessionBuilder()
                .setCatalog("mock")
                .setSchema("default")
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "split_to_subqueries")
                .build();

        try (PlanTester planTester = PlanTester.create(session)) {
            // The rule only fires for connectors that allow splitting the read into multiple subqueries
            planTester.createCatalog(
                    "mock",
                    MockConnectorFactory.builder()
                            .withAllowSplittingReadIntoMultipleSubQueries(true)
                            .withGetTableHandle((_, schemaTableName) -> new MockConnectorTableHandle(schemaTableName))
                            .withGetColumns(_ -> ImmutableList.of(
                                    new ColumnMetadata("orderkey", BIGINT),
                                    new ColumnMetadata("partkey", BIGINT),
                                    new ColumnMetadata("comment", VARCHAR)))
                            .build(),
                    ImmutableMap.of());

            planTester.inTransaction(transactionSession -> {
                Plan plan = planTester.createPlan(
                        transactionSession,
                        "SELECT count(DISTINCT orderkey), count(DISTINCT partkey) FROM test_table WHERE substring(comment, 1, 3) BETWEEN 'a' AND 'b'");

                // The rule fired: each distinct aggregation reads its own copy of the source subtree,
                // and each copy carries its own Let with a consistently renamed binder
                assertThat(searchFrom(plan.getRoot()).whereIsInstanceOfAny(JoinNode.class).count()).isEqualTo(1);
                assertThat(searchFrom(plan.getRoot()).whereIsInstanceOfAny(TableScanNode.class).count()).isEqualTo(2);

                List<PlanNode> filters = searchFrom(plan.getRoot()).whereIsInstanceOfAny(FilterNode.class).findAll();
                assertThat(filters).hasSize(2);
                assertThat(filters).allSatisfy(filter -> assertThat(((FilterNode) filter).getPredicate()).isInstanceOf(Let.class));
                return null;
            });
        }
    }
}
