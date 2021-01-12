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

package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.SortingProperty;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestStreamingAggregations
        extends BasePlanTest
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final String TEST_SCHEMA = "test_schema";

    private static final SchemaTableName tableA = new SchemaTableName(TEST_SCHEMA, "table_a");
    private static final String columnNameA = "col_a";
    private static final ColumnHandle columnHandleA = new MockConnectorColumnHandle(columnNameA, VARCHAR);
    private static final String columnNameB = "col_b";
    private static final ColumnHandle columnHandleB = new MockConnectorColumnHandle(columnNameB, VARCHAR);

    private static final SchemaTableName tableB = new SchemaTableName(TEST_SCHEMA, "table_b");

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        Session session = testSessionBuilder()
                .setCatalog(MOCK_CATALOG)
                .setSchema(TEST_SCHEMA)
                .build();
        LocalQueryRunner queryRunner = LocalQueryRunner.builder(session).build();
        MockConnectorFactory mockFactory = MockConnectorFactory.builder()
                .withGetTableProperties((connectorSession, handle) -> {
                    MockConnectorTableHandle tableHandle = (MockConnectorTableHandle) handle;
                    if (tableHandle.getTableName().equals(tableA)) {
                        return new ConnectorTableProperties(
                                TupleDomain.all(),
                                Optional.empty(),
                                Optional.of(ImmutableSet.of(columnHandleA)),
                                Optional.empty(),
                                ImmutableList.of(
                                        new SortingProperty<>(columnHandleA, SortOrder.ASC_NULLS_FIRST)));
                    }
                    else if (tableHandle.getTableName().equals(tableB)) {
                        return new ConnectorTableProperties(
                                TupleDomain.all(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(
                                        new SortingProperty<>(columnHandleB, SortOrder.ASC_NULLS_FIRST)));
                    }
                    throw new IllegalArgumentException();
                })
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(tableA) || schemaTableName.equals(tableB)) {
                        return ImmutableList.of(
                                new ColumnMetadata(columnNameA, VARCHAR),
                                new ColumnMetadata(columnNameB, VARCHAR));
                    }
                    throw new IllegalArgumentException();
                })
                .build();
        queryRunner.createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());
        return queryRunner;
    }

    @Test
    public void testStreamingAggregationWithBucketedSortedTable()
    {
        // table_a is sorted and stream-partitioned by col_a
        assertDistributedPlan(
                "SELECT col_a, count(*) FROM table_a group by 1",
                anyTree(
                        aggregation(
                                singleGroupingSet("t_col_a"),
                                ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                                ImmutableList.of("t_col_a"), // streaming
                                Optional.empty(),
                                SINGLE,
                                tableScan("table_a", ImmutableMap.of("t_col_a", "col_a")))));

        // table_b is sorted by col_b
        // Streaming partial aggregation is currently not performed
        assertDistributedPlan(
                "SELECT col_b, count(*) FROM table_b group by 1",
                anyTree(
                        aggregation(
                                ImmutableMap.of("final_count", functionCall("count", ImmutableList.of("partial_count"))),
                                FINAL,
                                anyTree(
                                        exchange(REMOTE, REPARTITION,
                                                aggregation(
                                                        ImmutableMap.of("partial_count", functionCall("count", ImmutableList.of())),
                                                        PARTIAL,
                                                        anyTree(tableScan("table_b", ImmutableMap.of("t_col_b", "col_b")))))))));
    }
}
