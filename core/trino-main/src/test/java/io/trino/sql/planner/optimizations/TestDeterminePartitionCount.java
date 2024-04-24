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
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.PlanTester;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.MAX_HASH_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.MIN_HASH_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.MIN_INPUT_ROWS_PER_TASK;
import static io.trino.SystemSessionProperties.MIN_INPUT_SIZE_PER_TASK;
import static io.trino.SystemSessionProperties.RETRY_POLICY;
import static io.trino.spi.statistics.TableStatistics.empty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestDeterminePartitionCount
        extends BasePlanTest
{
    @Override
    protected PlanTester createPlanTester()
    {
        String catalogName = "mock";
        MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                .withGetTableHandle((session, tableName) -> {
                    if (tableName.getTableName().equals("table_with_stats_a")
                            || tableName.getTableName().equals("table_with_stats_b")
                            || tableName.getTableName().equals("small_table_with_stats")
                            || tableName.getTableName().equals("table_without_stats_a")
                            || tableName.getTableName().equals("table_without_stats_b")) {
                        return new MockConnectorTableHandle(tableName);
                    }
                    return null;
                })
                .withGetColumns(schemaTableName -> ImmutableList.of(
                        new ColumnMetadata("column_a", VARCHAR),
                        new ColumnMetadata("column_b", VARCHAR)))
                .withGetTableStatistics(tableName -> {
                    if (tableName.getTableName().equals("table_with_stats_a")
                            || tableName.getTableName().equals("table_with_stats_b")) {
                        return new TableStatistics(
                                Estimate.of(200),
                                ImmutableMap.of(
                                        new MockConnectorColumnHandle("column_a", VARCHAR),
                                        new ColumnStatistics(Estimate.of(0), Estimate.of(10000), Estimate.of(DataSize.of(100, MEGABYTE).toBytes()), Optional.empty()),
                                        new MockConnectorColumnHandle("column_b", VARCHAR),
                                        new ColumnStatistics(Estimate.of(0), Estimate.of(10000), Estimate.of(DataSize.of(100, MEGABYTE).toBytes()), Optional.empty())));
                    }
                    if (tableName.getTableName().equals("small_table_with_stats")) {
                        return new TableStatistics(
                                Estimate.of(20),
                                ImmutableMap.of(
                                        new MockConnectorColumnHandle("column_a", VARCHAR),
                                        new ColumnStatistics(Estimate.of(0), Estimate.of(20), Estimate.of(DataSize.of(1, KILOBYTE).toBytes()), Optional.empty()),
                                        new MockConnectorColumnHandle("column_b", VARCHAR),
                                        new ColumnStatistics(Estimate.of(0), Estimate.of(20), Estimate.of(DataSize.of(1, KILOBYTE).toBytes()), Optional.empty())));
                    }
                    return empty();
                })
                .withName(catalogName)
                .build();

        Session session = testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema("default")
                .build();
        PlanTester planTester = PlanTester.create(session, 100);
        planTester.createCatalog(
                catalogName,
                connectorFactory,
                ImmutableMap.of());
        return planTester;
    }

    @Test
    public void testSimpleSelect()
    {
        @Language("SQL") String query = "SELECT * FROM table_with_stats_a";

        // DeterminePartitionCount optimizer rule should not fire since no remote exchanges are present
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(MAX_HASH_PARTITION_COUNT, "100")
                        .setSystemProperty(MIN_HASH_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .build(),
                output(
                        node(TableScanNode.class)));
    }

    @Test
    public void testSimpleFilter()
    {
        @Language("SQL") String query = "SELECT column_a FROM table_with_stats_a WHERE column_b IS NULL";

        // DeterminePartitionCount optimizer rule should not fire since no remote exchanges are present
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(MAX_HASH_PARTITION_COUNT, "100")
                        .setSystemProperty(MIN_HASH_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .build(),
                output(
                        project(
                                filter(
                                        new IsNull(new Reference(BIGINT, "column_b")),
                                        tableScan("table_with_stats_a", ImmutableMap.of("column_a", "column_a", "column_b", "column_b"))))));
    }

    @Test
    public void testSimpleCount()
    {
        @Language("SQL") String query = "SELECT count(*) FROM table_with_stats_a";

        // DeterminePartitionCount optimizer rule should not fire since no remote repartition exchanges are present
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(MAX_HASH_PARTITION_COUNT, "100")
                        .setSystemProperty(MIN_HASH_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .build(),
                output(
                        node(AggregationNode.class,
                                exchange(LOCAL,
                                        exchange(REMOTE, GATHER, Optional.empty(),
                                                node(AggregationNode.class,
                                                        node(TableScanNode.class)))))));
    }

    @Test
    public void testPlanWhenTableStatisticsArePresent()
    {
        @Language("SQL") String query = """
                SELECT count(column_a) FROM table_with_stats_a group by column_b
                """;

        // DeterminePartitionCount optimizer rule should fire and set the partitionCount to 10 for remote exchanges
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(MAX_HASH_PARTITION_COUNT, "21")
                        .setSystemProperty(MIN_HASH_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .build(),
                output(
                        project(
                                node(AggregationNode.class,
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPARTITION, Optional.of(10),
                                                        node(AggregationNode.class,
                                                                node(TableScanNode.class))))))));
    }

    @Test
    public void testDoesNotSetPartitionCountWhenNodeCountIsSmall()
    {
        @Language("SQL") String query = """
                SELECT count(column_a) FROM table_with_stats_a group by column_b
                """;

        // DeterminePartitionCount shouldn't put partition count when 2 * "determined partition count"
        // is greater or equal to number of workers.
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(MAX_HASH_PARTITION_COUNT, "20")
                        .setSystemProperty(MIN_HASH_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .build(),
                output(
                        project(
                                node(AggregationNode.class,
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPARTITION, Optional.empty(),
                                                        node(AggregationNode.class,
                                                                node(TableScanNode.class))))))));
    }

    @Test
    public void testPlanWhenTableStatisticsAreAbsent()
    {
        @Language("SQL") String query = """
                SELECT * FROM table_without_stats_a as a JOIN table_without_stats_b as b ON a.column_a = b.column_a
                """;

        // DeterminePartitionCount optimizer rule should not fire and partitionCount will remain empty for remote exchanges
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(MAX_HASH_PARTITION_COUNT, "10")
                        .setSystemProperty(MIN_HASH_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .build(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("column_a", "column_a_0")
                                .right(exchange(LOCAL,
                                        exchange(REMOTE, Optional.empty(),
                                                tableScan("table_without_stats_b", ImmutableMap.of("column_a_0", "column_a", "column_b_1", "column_b")))))
                                .left(exchange(REMOTE, Optional.empty(),
                                        node(FilterNode.class,
                                                tableScan("table_without_stats_a", ImmutableMap.of("column_a", "column_a", "column_b", "column_b"))))))));
    }

    @Test
    public void testPlanWhenCrossJoinIsPresent()
    {
        @Language("SQL") String query = """
                SELECT * FROM table_with_stats_a CROSS JOIN table_with_stats_b
                """;

        // DeterminePartitionCount optimizer rule should not fire and partitionCount will remain empty for remote exchanges
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(MAX_HASH_PARTITION_COUNT, "10")
                        .setSystemProperty(MIN_HASH_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .build(),
                output(
                        join(INNER, builder -> builder
                                .right(exchange(LOCAL,
                                        exchange(REMOTE, Optional.empty(),
                                                tableScan("table_with_stats_b", ImmutableMap.of("column_a_0", "column_a", "column_b_1", "column_b")))))
                                .left(tableScan("table_with_stats_a", ImmutableMap.of("column_a", "column_a", "column_b", "column_b"))))));
    }

    @Test
    public void testPlanWhenCrossJoinIsScalar()
    {
        @Language("SQL") String query = """
                SELECT * FROM table_with_stats_a CROSS JOIN (select max(column_a) from table_with_stats_b) t(a)
                """;

        // DeterminePartitionCount optimizer rule should not fire since no remote repartitioning exchanges are present
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(MAX_HASH_PARTITION_COUNT, "20")
                        .setSystemProperty(MIN_HASH_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .build(),
                output(
                        join(INNER, builder -> builder
                                .right(
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPLICATE, Optional.empty(),
                                                        node(AggregationNode.class,
                                                                exchange(LOCAL,
                                                                        exchange(REMOTE, GATHER, Optional.empty(),
                                                                                node(AggregationNode.class,
                                                                                        node(TableScanNode.class))))))))
                                .left(node(TableScanNode.class)))));
    }

    @Test
    public void testPlanWhenJoinNodeStatsAreAbsent()
    {
        @Language("SQL") String query = """
                SELECT * FROM table_with_stats_a as a JOIN table_with_stats_b as b ON a.column_b = b.column_b
                """;

        // DeterminePartitionCount optimizer rule should not fire and partitionCount will remain empty for remote exchanges
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(MAX_HASH_PARTITION_COUNT, "10")
                        .setSystemProperty(MIN_HASH_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .build(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("column_b", "column_b_1")
                                .right(exchange(LOCAL,
                                        exchange(REMOTE, Optional.empty(),
                                                tableScan("table_with_stats_b", ImmutableMap.of("column_a_0", "column_a", "column_b_1", "column_b")))))
                                .left(exchange(REMOTE, Optional.empty(),
                                        node(FilterNode.class,
                                                tableScan("table_with_stats_a", ImmutableMap.of("column_a", "column_a", "column_b", "column_b"))))))));
    }

    @Test
    public void testPlanWhenJoinNodeOutputIsBiggerThanRowsScanned()
    {
        @Language("SQL") String query = """
                SELECT a.column_a FROM table_with_stats_a as a JOIN table_with_stats_b as b ON a.column_a = b.column_a
                """;

        // DeterminePartitionCount optimizer rule should fire and set the partitionCount to 10 for remote exchanges
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(MAX_HASH_PARTITION_COUNT, "50")
                        .setSystemProperty(MIN_HASH_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .build(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("column_a", "column_a_0")
                                .right(exchange(LOCAL,
                                        // partition count should be more than 5 because of the presence of expanding join operation
                                        exchange(REMOTE, Optional.of(10),
                                                tableScan("table_with_stats_b", ImmutableMap.of("column_a_0", "column_a")))))
                                .left(exchange(REMOTE, Optional.of(10),
                                        node(FilterNode.class,
                                                tableScan("table_with_stats_a", ImmutableMap.of("column_a", "column_a"))))))));
    }

    @Test
    public void testEstimatedPartitionCountShouldNotBeGreaterThanMaxLimit()
    {
        @Language("SQL") String query = """
                SELECT * FROM table_with_stats_a as a JOIN table_with_stats_b as b ON a.column_a = b.column_a
                """;

        // DeterminePartitionCount optimizer rule should not fire and partitionCount will remain empty for remote exchanges
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(MAX_HASH_PARTITION_COUNT, "5")
                        .setSystemProperty(MIN_HASH_PARTITION_COUNT, "2")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .build(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("column_a", "column_a_0")
                                .right(exchange(LOCAL,
                                        exchange(REMOTE, Optional.empty(),
                                                tableScan("table_with_stats_b", ImmutableMap.of("column_a_0", "column_a", "column_b_1", "column_b")))))
                                .left(exchange(REMOTE, Optional.empty(),
                                        node(FilterNode.class,
                                                tableScan("table_with_stats_a", ImmutableMap.of("column_a", "column_a", "column_b", "column_b"))))))));
    }

    @Test
    public void testEstimatedPartitionCountShouldNotBeLessThanMinLimit()
    {
        @Language("SQL") String query = """
                SELECT a.column_a FROM table_with_stats_a as a JOIN table_with_stats_b as b ON a.column_a = b.column_a
                """;

        // DeterminePartitionCount optimizer rule estimate the partition count to 10 but because min limit is 15, it will set it to 15
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(MAX_HASH_PARTITION_COUNT, "40")
                        .setSystemProperty(MIN_HASH_PARTITION_COUNT, "15")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .build(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("column_a", "column_a_0")
                                .right(exchange(LOCAL,
                                        exchange(REMOTE, Optional.of(15),
                                                tableScan("table_with_stats_b", ImmutableMap.of("column_a_0", "column_a")))))
                                .left(exchange(REMOTE, Optional.of(15),
                                        node(FilterNode.class,
                                                tableScan("table_with_stats_a", ImmutableMap.of("column_a", "column_a"))))))));
    }

    @Test
    public void testPlanWhenUnionNodeOutputIsBiggerThanJoinOutput()
    {
        @Language("SQL") String query = """
                SELECT a.column_b
                FROM table_with_stats_a as a
                JOIN table_with_stats_b as b
                ON a.column_a = b.column_a
                UNION ALL
                SELECT column_b
                FROM table_with_stats_b
                """;

        // DeterminePartitionCount optimizer rule should fire and set the partitionCount to 20 for remote exchanges
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(MAX_HASH_PARTITION_COUNT, "50")
                        .setSystemProperty(MIN_HASH_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .build(),
                output(
                        exchange(REMOTE, GATHER,
                                join(INNER, builder -> builder
                                        .equiCriteria("column_a", "column_a_1")
                                        .right(exchange(LOCAL,
                                                // partition count should be 15 with just join node but since we also have union, it should be 20
                                                exchange(REMOTE, REPARTITION, Optional.of(20),
                                                        tableScan("table_with_stats_b", ImmutableMap.of("column_a_1", "column_a")))))
                                        // partition count should be 15 with just join node but since we also have union, it should be 20
                                        .left(exchange(REMOTE, REPARTITION, Optional.of(20),
                                                node(FilterNode.class,
                                                        tableScan("table_with_stats_a", ImmutableMap.of("column_a", "column_a", "column_b_0", "column_b")))))),
                                tableScan("table_with_stats_b", ImmutableMap.of("column_b_4", "column_b")))));
    }

    @Test
    public void testPlanWhenEstimatedPartitionCountBasedOnRowsIsMoreThanOutputSize()
    {
        @Language("SQL") String query = """
                SELECT count(column_a) FROM table_with_stats_a group by column_b
                """;

        // DeterminePartitionCount optimizer rule should fire and set the partitionCount to 10 for remote exchanges
        // based on rows count
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(MAX_HASH_PARTITION_COUNT, "100")
                        .setSystemProperty(MIN_HASH_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "20")
                        .build(),
                output(
                        project(
                                node(AggregationNode.class,
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPARTITION, Optional.of(10),
                                                        node(AggregationNode.class,
                                                                node(TableScanNode.class))))))));
    }

    @Test
    public void testFireWithTaskRetries()
    {
        @Language("SQL") String query = """
                SELECT count(column_a) FROM table_with_stats_a group by column_b
                """;
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(RETRY_POLICY, "task")
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT, "21")
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .build(),
                output(
                        project(
                                node(AggregationNode.class,
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION, Optional.of(10),
                                                        node(AggregationNode.class,
                                                                node(TableScanNode.class))))))));
    }

    @Test
    public void testFireWithTaskRetriesMinEnforced()
    {
        @Language("SQL") String query = """
                SELECT count(column_a) FROM table_with_stats_a group by column_b
                """;
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(RETRY_POLICY, "task")
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT, "21")
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT, "11")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .build(),
                output(
                        project(
                                node(AggregationNode.class,
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION, Optional.of(11),
                                                        node(AggregationNode.class,
                                                                node(TableScanNode.class))))))));
    }

    @Test
    public void testFireWithTaskRetriesMaxEnforced()
    {
        @Language("SQL") String query = """
                SELECT count(column_a) FROM table_with_stats_a group by column_b
                """;
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(RETRY_POLICY, "task")
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT, "8")
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .build(),
                output(
                        project(
                                node(AggregationNode.class,
                                        exchange(LOCAL,
                                                exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION, Optional.empty(),
                                                        node(AggregationNode.class,
                                                                node(TableScanNode.class))))))));
    }

    @Test
    public void testFireWithTaskRetriesJoinHashDistribution()
    {
        @Language("SQL") String query = """
                SELECT a.column_a FROM table_with_stats_a as a JOIN table_with_stats_b as b ON a.column_a = b.column_a
                """;

        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(RETRY_POLICY, "task")
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT, "50")
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .build(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("column_a", "column_a_0")
                                .left(exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION, Optional.of(10),
                                        node(FilterNode.class,
                                                tableScan("table_with_stats_a", ImmutableMap.of("column_a", "column_a")))))
                                .right(exchange(LOCAL,
                                        exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION, Optional.of(10),
                                                node(DynamicFilterSourceNode.class,
                                                        tableScan("table_with_stats_b", ImmutableMap.of("column_a_0", "column_a")))))))));
    }

    @Test
    public void testFireWithTaskRetriesJoinBroadcastJoin()
    {
        @Language("SQL") String query = """
                SELECT a.column_a FROM table_with_stats_a as a JOIN table_with_stats_b as b ON a.column_a = b.column_a
                """;

        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(RETRY_POLICY, "task")
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT, "50")
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "broadcast")
                        .build(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("column_a", "column_a_0")
                                        .left(node(FilterNode.class,
                                                tableScan("table_with_stats_a", ImmutableMap.of("column_a", "column_a"))))
                                        .right(exchange(LOCAL,
                                                exchange(REMOTE, REPLICATE, FIXED_BROADCAST_DISTRIBUTION, Optional.empty(),
                                                        node(DynamicFilterSourceNode.class,
                                                                tableScan("table_with_stats_b", ImmutableMap.of("column_a_0", "column_a")))))))));
    }

    @Test
    public void testFireWithTaskRetriesTwoPartitionedJoins()
    {
        @Language("SQL") String query = """
                SELECT a.column_a FROM
                   (table_with_stats_a as a JOIN table_with_stats_b as b ON a.column_a = b.column_a)
                   JOIN
                   small_table_with_stats s ON a.column_b = s.column_b
                """;

        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(RETRY_POLICY, "task")
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT, "50")
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned") // enforce partitioned joins
                        .setSystemProperty(JOIN_REORDERING_STRATEGY, "none") // keep syntactic order
                        .build(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("column_b", "column_b_3")
                                .left(exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION, Optional.of(15),
                                                        join(INNER, builder2 -> builder2
                                                                .equiCriteria("column_a", "column_a_0")
                                                                .left(exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION, Optional.of(15),
                                                                        node(FilterNode.class,
                                                                                tableScan("table_with_stats_a", ImmutableMap.of("column_a", "column_a", "column_b", "column_b")))))
                                                                .right(exchange(LOCAL,
                                                                        exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION, Optional.of(15),
                                                                                node(DynamicFilterSourceNode.class,
                                                                                        tableScan("table_with_stats_b", ImmutableMap.of("column_a_0", "column_a")))))))))
                                .right(exchange(LOCAL,
                                        exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION, Optional.of(15),
                                                node(DynamicFilterSourceNode.class,
                                                        tableScan("small_table_with_stats", ImmutableMap.of("column_b_3", "column_b")))))))));
    }

    @Test
    public void testFireWithTaskRetriesTwoBroadcastJoins()
    {
        @Language("SQL") String query = """
                SELECT a.column_a FROM
                   (table_with_stats_a as a JOIN table_with_stats_b as b ON a.column_a = b.column_a)
                   JOIN
                   small_table_with_stats s ON a.column_b = s.column_b
                """;

        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT, "50")
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "broadcast") // enforce broadcast joins
                        .setSystemProperty(JOIN_REORDERING_STRATEGY, "none") // keep syntactic order
                        .build(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("column_b", "column_b_3")
                                .left(join(INNER, builder2 -> builder2
                                                .equiCriteria("column_a", "column_a_0")
                                        .left(node(FilterNode.class,
                                                tableScan("table_with_stats_a", ImmutableMap.of("column_a", "column_a", "column_b", "column_b"))))
                                        .right(exchange(LOCAL,
                                                exchange(REMOTE, REPLICATE, FIXED_BROADCAST_DISTRIBUTION, Optional.empty(),
                                                        tableScan("table_with_stats_b", ImmutableMap.of("column_a_0", "column_a")))))))
                                .right(exchange(LOCAL,
                                        exchange(REMOTE, REPLICATE, FIXED_BROADCAST_DISTRIBUTION, Optional.empty(),
                                                tableScan("small_table_with_stats", ImmutableMap.of("column_b_3", "column_b"))))))));
    }

    @Test
    public void testFireWithTaskRetriesBroadcastJoinOverPartitionedJoin()
    {
        @Language("SQL") String query = """
                SELECT a.column_a FROM
                   (table_with_stats_a as a JOIN table_with_stats_b as b ON a.column_a = b.column_a)
                   JOIN
                   small_table_with_stats s ON a.column_b = s.column_b
                """;

        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(RETRY_POLICY, "task")
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT, "50")
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .setSystemProperty(JOIN_REORDERING_STRATEGY, "none") // keep syntactic order
                        .build(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("column_b", "column_b_3")
                                .left(join(INNER, builder2 -> builder2
                                        .equiCriteria("column_a", "column_a_0")
                                        .left(exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION, Optional.of(15),
                                                node(FilterNode.class,
                                                        tableScan("table_with_stats_a", ImmutableMap.of("column_a", "column_a", "column_b", "column_b")))))
                                        .right(exchange(LOCAL,
                                                exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION, Optional.of(15),
                                                        node(DynamicFilterSourceNode.class,
                                                                tableScan("table_with_stats_b", ImmutableMap.of("column_a_0", "column_a"))))))))
                                .right(exchange(LOCAL,
                                        exchange(REMOTE, REPLICATE, FIXED_BROADCAST_DISTRIBUTION, Optional.empty(),
                                                node(DynamicFilterSourceNode.class,
                                                        tableScan("small_table_with_stats", ImmutableMap.of("column_b_3", "column_b")))))))));
    }

    @Test
    public void testFireWithTaskRetriesArbitraryExchangeOverHashDistributedStage()
    {
        @Language("SQL") String query = """
                SELECT j.column_a FROM
                   ((SELECT a.column_a, a.column_b FROM table_with_stats_a as a JOIN table_with_stats_b as b ON a.column_a = b.column_a) UNION ALL SELECT * FROM small_table_with_stats s) j
                   JOIN
                   small_table_with_stats s2 ON j.column_b = s2.column_b
                """;

        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(RETRY_POLICY, "task")
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT, "50")
                        .setSystemProperty(FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT, "4")
                        .setSystemProperty(MIN_INPUT_SIZE_PER_TASK, "20MB")
                        .setSystemProperty(MIN_INPUT_ROWS_PER_TASK, "400")
                        .setSystemProperty(JOIN_REORDERING_STRATEGY, "none") // keep syntactic order
                        .build(),
                output(
                        join(INNER, builder -> builder
                                .ignoreEquiCriteria() // criteria uses new symbols output by exchange left exchange which cannot be expressed in plan matcher
                                .left(exchange(REMOTE, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION, Optional.empty(),
                                        join(INNER, builder2 -> builder2
                                                .equiCriteria("column_a_0", "column_a_2")
                                                .left(exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION, Optional.of(15),
                                                        node(FilterNode.class,
                                                                tableScan("table_with_stats_a", ImmutableMap.of("column_a_0", "column_a", "column_b_1", "column_b")))))
                                                .right(exchange(LOCAL,
                                                        exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION, Optional.of(15),
                                                                node(DynamicFilterSourceNode.class,
                                                                        tableScan("table_with_stats_b", ImmutableMap.of("column_a_2", "column_a"))))))),
                                        node(FilterNode.class,
                                                tableScan("small_table_with_stats", ImmutableMap.of("column_a_4", "column_a", "column_b_5", "column_b")))))

                                .right(exchange(LOCAL,
                                        exchange(REMOTE, REPLICATE, FIXED_BROADCAST_DISTRIBUTION, Optional.empty(),
                                                node(DynamicFilterSourceNode.class,
                                                        tableScan("small_table_with_stats", ImmutableMap.of("column_b_7", "column_b")))))))));
    }
}
