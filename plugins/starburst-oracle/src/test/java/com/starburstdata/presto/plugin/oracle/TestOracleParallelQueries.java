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
import io.prestosql.Session;
import io.prestosql.execution.QueryInfo;
import io.prestosql.operator.OperatorStats;
import io.prestosql.spi.QueryId;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.ResultWithQueryId;
import io.prestosql.tpch.TpchTable;
import org.testng.annotations.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static com.starburstdata.presto.plugin.oracle.OracleSessionProperties.CONCURRENCY_TYPE;
import static com.starburstdata.presto.plugin.oracle.OracleSessionProperties.MAX_SPLITS_PER_SCAN;
import static com.starburstdata.presto.plugin.oracle.TestingOracleServer.executeInOracle;
import static java.lang.String.format;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestOracleParallelQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createOracleQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("connection-url", TestingOracleServer.getJdbcUrl())
                        .put("connection-user", TestingOracleServer.USER)
                        .put("connection-password", TestingOracleServer.PASSWORD)
                        .put("allow-drop-table", "true")
                        .put("oracle.number.default-scale", "3")
                        .build(),
                Function.identity(),
                ImmutableList.of(TpchTable.ORDERS, TpchTable.NATION));
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

        verifyTableSplitCount(tableName, "b", "GPhxvpcxVrk=", OracleConcurrencyType.NO_CONCURRENCY, Optional.empty(), 1);
        verifyTableSplitCount(tableName, "c", "/GYVBejTO3U=", OracleConcurrencyType.PARTITIONS, Optional.empty(), 4);

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

        verifyTableSplitCount(tableName, "b", "GPhxvpcxVrk=", OracleConcurrencyType.NO_CONCURRENCY, Optional.empty(), 1);
        verifyTableSplitCount(tableName, "c", "/GYVBejTO3U=", OracleConcurrencyType.PARTITIONS, Optional.of(2), 2);
        verifyTableSplitCount(tableName, "c", "/GYVBejTO3U=", OracleConcurrencyType.PARTITIONS, Optional.of(3), 2);

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

        verifyTableSplitCount(tableName, "a", "82outvYm50s=", OracleConcurrencyType.NO_CONCURRENCY, Optional.empty(), 1);

        verifyTableSplitCount(tableName, "a", "82outvYm50s=", OracleConcurrencyType.PARTITIONS, Optional.of(1), 1);
        verifyTableSplitCount(tableName, "b", "BaJpI0zFEZ0=", OracleConcurrencyType.PARTITIONS, Optional.of(7), 7);
        verifyTableSplitCount(tableName, "c", "KgAXfidTzXE=", OracleConcurrencyType.PARTITIONS, Optional.of(100), 100);
        verifyTableSplitCount(tableName, "c", "KgAXfidTzXE=", OracleConcurrencyType.PARTITIONS, Optional.of(101), 100);

        dropTable(tableName);
    }

    @Test
    public void testReadingNonPartitionedTable()
    {
        String tableName = randomTableName("non_partitioned");
        createNonPartitionedTable(tableName, "a NUMBER, b NUMBER");
        insertIntoTable(tableName, "a, b", ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 6, 8, 11));

        verifyTableSplitCount(tableName, "a", "Gv+Z64FGbJw=", OracleConcurrencyType.NO_CONCURRENCY, Optional.empty(), 1);
        verifyTableSplitCount(tableName, "b", "meGP+zKVR8U=", OracleConcurrencyType.PARTITIONS, Optional.empty(), 1);

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

    private void verifyTableSplitCount(String tableName, String column, String expectedChecksum, OracleConcurrencyType concurrencyType, Optional<Integer> maxSplits, int expectedSplits)
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        Session.SessionBuilder sessionBuilder = Session.builder(getSession())
                .setCatalogSessionProperty("oracle", CONCURRENCY_TYPE, concurrencyType.name());

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
