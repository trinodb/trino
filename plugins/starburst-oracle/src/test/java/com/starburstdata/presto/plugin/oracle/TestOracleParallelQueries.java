/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.operator.OperatorStats;
import io.trino.spi.QueryId;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import org.testng.annotations.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.presto.plugin.oracle.OracleParallelismType.NO_PARALLELISM;
import static com.starburstdata.presto.plugin.oracle.OracleParallelismType.PARTITIONS;
import static com.starburstdata.presto.plugin.oracle.StarburstOracleSessionProperties.MAX_SPLITS_PER_SCAN;
import static com.starburstdata.presto.plugin.oracle.StarburstOracleSessionProperties.PARALLELISM_TYPE;
import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.executeInOracle;
import static java.lang.String.format;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestOracleParallelQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(TestingStarburstOracleServer.connectionProperties())
                        .put("oracle.number.default-scale", "3")
                        .buildOrThrow())
                .withUnlockEnterpriseFeatures(true)
                .build();
    }

    @Test
    public void testReadingPartitionedTable()
    {
        String tableName = randomTableName("partitioned");
        createPartitionedTable(tableName, "a NUMBER, b NUMBER, c NUMBER", "a", 4);
        insertIntoTable(tableName,
                "a, b, c",
                ImmutableList.of(
                        1, 2, 3,
                        4, 5, 6,
                        7, 6, 8));

        verifyTableSplitCount(tableName, "b", "GPhxvpcxVrk=", NO_PARALLELISM, Optional.empty(), 1);
        verifyTableSplitCount(tableName, "c", "/GYVBejTO3U=", PARTITIONS, Optional.empty(), 4);

        dropTable(tableName);
    }

    @Test
    public void testReadingPartitionedTableWithMaxSplits()
    {
        String tableName = randomTableName("partitioned");
        createPartitionedTable(tableName, "a NUMBER, b NUMBER, c NUMBER", "a", 4);
        insertIntoTable(tableName,
                "a, b, c",
                ImmutableList.of(
                        1, 2, 3,
                        4, 5, 6,
                        7, 6, 8));

        verifyTableSplitCount(tableName, "b", "GPhxvpcxVrk=", NO_PARALLELISM, Optional.empty(), 1);
        verifyTableSplitCount(tableName, "c", "/GYVBejTO3U=", PARTITIONS, Optional.of(2), 2);
        verifyTableSplitCount(tableName, "c", "/GYVBejTO3U=", PARTITIONS, Optional.of(3), 2);

        dropTable(tableName);
    }

    @Test
    public void testReadingTableWithMaxSplitsPerScan()
    {
        String tableName = randomTableName("partitioned_big");
        createPartitionedTable(tableName, "a NUMBER, b NUMBER, c NUMBER", "a", 100);

        insertIntoTable(tableName,
                "a, b, c",
                IntStream.range(0, 99999).boxed().collect(toImmutableList()));

        verifyTableSplitCount(tableName, "a", "82outvYm50s=", NO_PARALLELISM, Optional.empty(), 1);

        verifyTableSplitCount(tableName, "a", "82outvYm50s=", PARTITIONS, Optional.of(1), 1);
        verifyTableSplitCount(tableName, "b", "BaJpI0zFEZ0=", PARTITIONS, Optional.of(7), 7);
        verifyTableSplitCount(tableName, "c", "KgAXfidTzXE=", PARTITIONS, Optional.of(100), 100);
        verifyTableSplitCount(tableName, "c", "KgAXfidTzXE=", PARTITIONS, Optional.of(101), 100);

        dropTable(tableName);
    }

    @Test
    public void testReadingNonPartitionedTable()
    {
        String tableName = randomTableName("non_partitioned");
        createNonPartitionedTable(tableName, "a NUMBER, b NUMBER");
        insertIntoTable(tableName, "a, b", ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 6, 8, 11));

        verifyTableSplitCount(tableName, "a", "Gv+Z64FGbJw=", NO_PARALLELISM, Optional.empty(), 1);
        verifyTableSplitCount(tableName, "b", "meGP+zKVR8U=", PARTITIONS, Optional.empty(), 1);

        dropTable(tableName);
    }

    private static void insertIntoTable(String tableName, String columns, List<Object> values)
    {
        int columnsCount = columns.split(",").length;

        List<List<Object>> rows = Lists.partition(values, columnsCount);

        String arguments = String.join(", ", Collections.nCopies(columnsCount, "?"));
        String query = format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, arguments);

        executeInOracle(connection -> {
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                for (List<Object> row : rows) {
                    verify(row.size() == columnsCount, "values[i].size differs from columns count");

                    for (int j = 0; j < columnsCount; j++) {
                        preparedStatement.setObject(j + 1, row.get(j));
                    }

                    preparedStatement.execute();
                }
            }
            catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static String randomTableName(String prefix)
    {
        return format("%s_%d", prefix, Math.abs(ThreadLocalRandom.current().nextLong()));
    }

    private static void dropTable(String tableName)
    {
        executeInOracle(format("DROP TABLE %s", tableName));
    }

    private void verifyTableSplitCount(String tableName, String column, String expectedChecksum, OracleParallelismType parallelismType, Optional<Integer> maxSplits, int expectedSplits)
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        Session.SessionBuilder sessionBuilder = Session.builder(getSession())
                .setCatalogSessionProperty("oracle", PARALLELISM_TYPE, parallelismType.name());

        maxSplits.ifPresent(value -> sessionBuilder.setCatalogSessionProperty("oracle", MAX_SPLITS_PER_SCAN, value.toString()));

        Session session = sessionBuilder.build();

        ResultWithQueryId<MaterializedResult> result = queryRunner.executeWithQueryId(session, format("SELECT TO_BASE64(CHECKSUM(%s)) FROM %s", column, tableName));
        QueryId queryId = result.getQueryId();
        QueryInfo fullQueryInfo = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId);

        // Verify if TableScanOperator has expectedSplitCount split count (totalDrivers)
        OperatorStats scanOperatorStats = findScanOperatorStats(fullQueryInfo);

        assertThat(scanOperatorStats.getTotalDrivers()).isEqualTo(expectedSplits);
        assertThat(result.getResult().getOnlyValue()).isEqualTo(expectedChecksum);
    }

    private static OperatorStats findScanOperatorStats(QueryInfo info)
    {
        return info.getQueryStats().getOperatorSummaries().stream()
                .filter(operatorStats -> operatorStats.getOperatorType().equalsIgnoreCase("TableScanOperator"))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Could not find TableScanOperator statistics"));
    }

    private static void createPartitionedTable(String tableName, String columns, String partitionColumn, int partitionCount)
    {
        executeInOracle(format("CREATE TABLE %s(%s) PARTITION BY HASH (%s) PARTITIONS %d", tableName, columns, partitionColumn, partitionCount));
    }

    private static void createNonPartitionedTable(String tableName, String columns)
    {
        executeInOracle(format("CREATE TABLE %s(%s)", tableName, columns));
    }
}
