/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.operator.OperatorStats;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.spi.QueryId;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.testing.QueryRunner;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.stream.IntStream;

import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerSessionProperties.PARALLEL_CONNECTIONS_COUNT;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestSqlServerParallelQueries
        extends AbstractTestQueryFramework
{
    private static final String COLUMN = "col";
    private TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.sqlServer = closeAfterClass(new TestingSqlServer());
        return StarburstSqlServerQueryRunner.builder(sqlServer)
                .withEnterpriseFeatures()
                .build();
    }

    @Test(dataProvider = "partitionRangesDataProvider")
    public void testReadingPartitionedTable(int range)
    {
        String tableName = "partitioned" + randomNameSuffix();
        createPartitionedTable(tableName, 4, range);
        fillTableWithData(tableName, 3);

        verifyTableSplitCount(tableName, Optional.empty(), 1);
        verifyTableSplitCount(tableName, Optional.of(4), 4);

        dropTable(tableName);
    }

    @Test
    public void testReadingPartitionedEmptyTable()
    {
        String tableName = "partitioned" + randomNameSuffix();
        createPartitionedTable(tableName, 4, 10);

        verifyTableSplitCount(tableName, Optional.empty(), 1);
        verifyTableSplitCount(tableName, Optional.of(4), 4);

        dropTable(tableName);
    }

    @Test(dataProvider = "partitionRangesDataProvider")
    public void testReadingPartitionedTableWithMaxSplits(int range)
    {
        String tableName = "partitioned" + randomNameSuffix();
        createPartitionedTable(tableName, 4, range);
        fillTableWithData(tableName, 3);

        verifyTableSplitCount(tableName, Optional.empty(), 1);
        verifyTableSplitCount(tableName, Optional.of(2), 2);
        verifyTableSplitCount(tableName, Optional.of(3), 2);

        dropTable(tableName);
    }

    @Test(dataProvider = "partitionRangesDataProvider")
    public void testReadingTableWithMaxSplitsPerScan(int range)
    {
        String tableName = "partitioned_big" + randomNameSuffix();
        createPartitionedTable(tableName, 100, range);
        fillTableWithData(tableName, 1000);

        verifyTableSplitCount(tableName, Optional.empty(), 1);
        verifyTableSplitCount(tableName, Optional.of(1), 1);
        verifyTableSplitCount(tableName, Optional.of(7), 7);
        verifyTableSplitCount(tableName, Optional.of(99), 50);
        verifyTableSplitCount(tableName, Optional.of(100), 100);
        verifyTableSplitCount(tableName, Optional.of(101), 100);
        verifyTableSplitCount(tableName, Optional.of(102), 100);
        verifyTableSplitCount(tableName, Optional.of(103), 100);
        verifyTableSplitCount(tableName, Optional.of(150), 100);

        dropTable(tableName);
    }

    @Test
    public void testReadingTableWithMaxSplitsPerScanHugePartition()
    {
        String tableName = "partitioned_big" + randomNameSuffix();
        createPartitionedTable(tableName, 1000, 2);
        fillTableWithData(tableName, 1000);

        verifyTableSplitCount(tableName, Optional.empty(), 1);
        verifyTableSplitCount(tableName, Optional.of(500), 500);
        verifyTableSplitCount(tableName, Optional.of(600), 500);
        verifyTableSplitCount(tableName, Optional.of(1000), 1000);

        dropTable(tableName);
    }

    @Test
    public void testReadingNonPartitionedTable()
    {
        String tableName = "non_partitioned" + randomNameSuffix();
        createNonPartitionedTable(tableName);
        fillTableWithData(tableName, 5);

        verifyTableSplitCount(tableName, Optional.empty(), 1);
        verifyTableSplitCount(tableName, Optional.empty(), 1);

        dropTable(tableName);
    }

    private void fillTableWithData(String tableName, int numberOfRows)
    {
        if (numberOfRows < 1) {
            return;
        }
        String values = IntStream.range(0, numberOfRows)
                .boxed()
                .map(value -> format("(%s)", value))
                .collect(joining(","));
        String query = format("INSERT INTO %s (%s) VALUES %s", tableName, COLUMN, values);

        this.sqlServer.execute(query);
    }

    private void dropTable(String tableName)
    {
        sqlServer.execute(format("DROP TABLE %s", tableName));
    }

    private void verifyTableSplitCount(String tableName, Optional<Integer> connectionsCount, int expectedSplits)
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        Session.SessionBuilder sessionBuilder = Session.builder(getSession());

        connectionsCount.ifPresent(value -> sessionBuilder.setCatalogSessionProperty("sqlserver", PARALLEL_CONNECTIONS_COUNT, value.toString()));

        Session session = sessionBuilder.build();

        MaterializedResultWithQueryId result = queryRunner.executeWithQueryId(session, format("SELECT * FROM %s", tableName));
        QueryId queryId = result.getQueryId();
        QueryInfo fullQueryInfo = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId);

        // Verify if TableScanOperator has expectedSplitCount split count (totalDrivers)
        OperatorStats scanOperatorStats = findScanOperatorStats(fullQueryInfo);

        assertThat(scanOperatorStats.getTotalDrivers()).isEqualTo(expectedSplits);
    }

    private static OperatorStats findScanOperatorStats(QueryInfo info)
    {
        return info.getQueryStats().getOperatorSummaries().stream()
                .filter(operatorStats -> operatorStats.getOperatorType().equalsIgnoreCase("TableScanOperator"))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Could not find TableScanOperator statistics"));
    }

    private void createPartitionedTable(String tableName, int partitionCount, int range)
    {
        String partitionRanges = IntStream.range(1, partitionCount)
                .map(operand -> operand * range)
                .boxed()
                .map(String::valueOf)
                .collect(joining(", "));
        sqlServer.execute(format("CREATE PARTITION FUNCTION %sPartitionFunction (int) AS RANGE RIGHT FOR VALUES (%s)", tableName, partitionRanges));
        sqlServer.execute(format("CREATE PARTITION SCHEME %sPartitionScheme AS PARTITION %sPartitionFunction ALL TO ('PRIMARY')", tableName, tableName));
        sqlServer.execute(format("CREATE TABLE %s (%s int) ON %sPartitionScheme (%s)", tableName, COLUMN, tableName, COLUMN));
    }

    private void createNonPartitionedTable(String tableName)
    {
        sqlServer.execute(format("CREATE TABLE %s(%s int)", tableName, COLUMN));
    }

    @DataProvider
    public static Object[][] partitionRangesDataProvider()
    {
        return new Object[][] {
                {1},
                {2},
                {10},
                {100},
                {1000},
        };
    }
}
