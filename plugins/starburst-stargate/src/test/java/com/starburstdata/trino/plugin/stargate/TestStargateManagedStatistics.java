/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.starburstdata.managed.statistics.BaseManagedStatisticsTest;
import com.starburstdata.managed.statistics.ManagedStatisticsDataTypeTest;
import com.starburstdata.managed.statistics.ManagedStatisticsDataTypeTest.ColumnStatistics;
import com.starburstdata.presto.testing.testcontainers.TestingEventLoggerPostgreSqlServer;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAndTrinoInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createRemoteStarburstQueryRunnerWithMemory;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStargateManagedStatistics
        extends BaseManagedStatisticsTest
{
    private static final ColumnStatistics NULL_STATISTICS = new ColumnStatistics("0.0", "0.0", "1.0", "null");

    private TrinoSqlExecutor remoteExecutor;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner starburstEnterprise = closeAfterClass(createRemoteStarburstQueryRunnerWithMemory(List.of(LINE_ITEM), Optional.empty()));
        remoteExecutor = new TrinoSqlExecutor(
                starburstEnterprise,
                Session.builder(starburstEnterprise.getDefaultSession())
                        .setCatalog("memory")
                        .setSchema("tiny")
                        .build());

        TestingEventLoggerPostgreSqlServer starburstStorage = closeAfterClass(new TestingEventLoggerPostgreSqlServer());
        return StargateQueryRunner.builder(starburstEnterprise, "memory")
                .enableWrites()
                .withStarburstStorage(starburstStorage)
                .withManagedStatistics()
                .build();
    }

    @Override
    protected SqlExecutor onRemote()
    {
        return remoteExecutor;
    }

    @Override // because onRemote() throws `This connector does not support modifying table rows`, and it's not possible to check with hasBehavior(SUPPORTS_DELETE)
    public void testRowCount()
    {
        String rowCountStats = """
                VALUES
                ('quantity', null, DOUBLE '50.0', DOUBLE '0.0', null, '1.0', '50.0'),
                ('extendedprice', null, DOUBLE '35921.0', DOUBLE '0.0', null, '904.0', '94949.5'),
                (null, null, null, null, DOUBLE '%s', null, null)""";
        String rowCountX2 = "120350.0";
        String rowCountX3 = "180525.0";

        try (TestTable testTable = new TestTable(onRemote(), "lineitem", "AS SELECT * FROM tpch.tiny.lineitem")) {
            String tableName = testTable.getName();
            assertStats(tableName, expectedEmptyStats());

            assertUpdate("ALTER TABLE " + tableName + " EXECUTE collect_statistics");
            assertStats(tableName, expectedTwoColumnsStats());

            // change row count *2
            onRemote().execute("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.lineitem");

            // row count is estimated on the stats for the most recent column, even if column was not requested in query
            assertUpdate("ALTER TABLE " + tableName + " EXECUTE collect_statistics(columns => ARRAY['linenumber'])");
            assertThat(query("SHOW STATS FOR (SELECT extendedprice, quantity FROM " + tableName + ")"))
                    .skippingTypesCheck()
                    .matches(rowCountStats.formatted(rowCountX2));

            // change row count *3
            onRemote().execute("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.lineitem");

            // row count is estimated on the stats for the most recent column
            assertUpdate("ALTER TABLE " + tableName + " EXECUTE collect_statistics(columns => ARRAY['quantity'])");
            assertStats(tableName, rowCountStats.formatted(rowCountX3));

            // do not test onRemote().execute("DELETE FROM " + tableName) because onRemote() throws `This connector does not support modifying table rows`

            // dropping statistics for table drops statistics for all columns, therefore row count could not be estimated
            assertUpdate("ALTER TABLE " + tableName + " EXECUTE drop_statistics");
            assertStats(tableName, expectedEmptyStats());
        }
    }

    @Test
    @Override
    public void testTrinoTypes()
    {
        ManagedStatisticsDataTypeTest.create()
                .addRoundTrip("boolean", "true", new ColumnStatistics("null", "1.0", "0.0", "null"))
                .addRoundTrip("boolean", "null", NULL_STATISTICS)
                .addRoundTrip("tinyint", "-42", new ColumnStatistics("null", "1.0", "0.0", "-42"))
                .addRoundTrip("tinyint", "null", NULL_STATISTICS)
                .addRoundTrip("smallint", "32456", new ColumnStatistics("null", "1.0", "0.0", "32456"))
                .addRoundTrip("smallint", "null", NULL_STATISTICS)
                .addRoundTrip("integer", "1234567890", new ColumnStatistics("null", "1.0", "0.0", "1234567890"))
                .addRoundTrip("integer", "null", NULL_STATISTICS)
                .addRoundTrip("bigint", "123456789012", new ColumnStatistics("null", "1.0", "0.0", "123456789012"))
                .addRoundTrip("bigint", "null", NULL_STATISTICS)

                .addRoundTrip("real", "123.45", new ColumnStatistics("null", "1.0", "0.0", "123.45"))
                .addRoundTrip("real", "null", NULL_STATISTICS)
                .addRoundTrip("real", "nan()", new ColumnStatistics("null", "1.0", "0.0", "null"))
                .addRoundTrip("real", "-infinity()", new ColumnStatistics("null", "1.0", "0.0", "null"))
                .addRoundTrip("real", "+infinity()", new ColumnStatistics("null", "1.0", "0.0", "null"))

                .addRoundTrip("double", "123.45", new ColumnStatistics("null", "1.0", "0.0", "123.45"))
                .addRoundTrip("double", "null", NULL_STATISTICS)

                .addRoundTrip("decimal", "1.11", new ColumnStatistics("null", "1.0", "0.0", "1.0"))
                .addRoundTrip("decimal", "null", NULL_STATISTICS)
                .addRoundTrip("decimal(38,1)", "1.11", new ColumnStatistics("null", "1.0", "0.0", "1.1"))
                .addRoundTrip("decimal(38,1)", "null", NULL_STATISTICS)
                .addRoundTrip("decimal(38,2)", "1.11", new ColumnStatistics("null", "1.0", "0.0", "1.11"))
                .addRoundTrip("decimal(38,2)", "null", NULL_STATISTICS)

                .addRoundTrip("char", "''", new ColumnStatistics("1.0", "1.0", "0.0", "null"))
                .addRoundTrip("char", "'a'", new ColumnStatistics("1.0", "1.0", "0.0", "null"))
                .addRoundTrip("char(255)", "'%s'".formatted("a".repeat(255)), new ColumnStatistics("255.0", "1.0", "0.0", "null"))

                .addRoundTrip("varchar", "'text_a'", new ColumnStatistics("6.0", "1.0", "0.0", "null"))
                .addRoundTrip("varchar", "null", NULL_STATISTICS)
                .addRoundTrip("varchar(32)", "'e'", new ColumnStatistics("1.0", "1.0", "0.0", "null"))
                .addRoundTrip("varchar(32)", "NULL", NULL_STATISTICS)
                .addRoundTrip("varchar(150)", "'fgh'", new ColumnStatistics("3.0", "1.0", "0.0", "null"))
                .addRoundTrip("varchar(150)", "NULL", NULL_STATISTICS)

                .addRoundTrip("varbinary", "NULL", NULL_STATISTICS)
                .addRoundTrip("varbinary", "X''", new ColumnStatistics("0.0", "1.0", "0.0", "null"))
                .addRoundTrip("varbinary", "X'68656C6C6F'", new ColumnStatistics("5.0", "1.0", "0.0", "null"))

                .addRoundTrip("date", "DATE '1952-04-03'", new ColumnStatistics("null", "1.0", "0.0", "1952-04-03"))
                .addRoundTrip("date", "null", NULL_STATISTICS)

                .addRoundTrip("time", "TIME '19:01:17.123'", new ColumnStatistics("null", "1.0", "0.0", "null"))
                .addRoundTrip("time", "null", NULL_STATISTICS)
                .addRoundTrip("time(6)", "TIME '03:17:17.123456'", new ColumnStatistics("null", "1.0", "0.0", "null"))
                .addRoundTrip("time(6)", "null", NULL_STATISTICS)
                .addRoundTrip("time(12)", "TIME '23:59:59.123456789012'", new ColumnStatistics("null", "1.0", "0.0", "null"))
                .addRoundTrip("time(12)", "null", NULL_STATISTICS)

                .addRoundTrip("time with time zone", "TIME '19:01:17.123 +05:45'", new ColumnStatistics("null", "1.0", "0.0", "null"))
                .addRoundTrip("time with time zone", "null", NULL_STATISTICS)
                .addRoundTrip("time(6) with time zone", "TIME '03:17:17.123456 +05:45'", new ColumnStatistics("null", "1.0", "0.0", "null"))
                .addRoundTrip("time(6) with time zone", "null", NULL_STATISTICS)
                .addRoundTrip("time(12) with time zone", "TIME '23:59:59.123456789012 +05:45'", new ColumnStatistics("null", "1.0", "0.0", "null"))
                .addRoundTrip("time(12) with time zone", "null", NULL_STATISTICS)

                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 01:01:01.123456'", new ColumnStatistics("null", "1.0", "0.0", "1970-01-01 01:01:01.123456"))
                .addRoundTrip("timestamp(6)", "null", NULL_STATISTICS)
                .addRoundTrip("timestamp(12)", "TIMESTAMP '1970-01-01 01:01:01.123456789012'", new ColumnStatistics("null", "1.0", "0.0", "1970-01-01 01:01:01.123456"))
                .addRoundTrip("timestamp(12)", "null", NULL_STATISTICS)

                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '12345-01-01 01:23:45.123456 Asia/Kathmandu'", new ColumnStatistics("null", "1.0", "0.0", "+12344-12-31 19:38:45.123 UTC"))
                .addRoundTrip("timestamp(6) with time zone", "null", NULL_STATISTICS)
                .addRoundTrip("timestamp(12) with time zone", "TIMESTAMP '12345-01-01 01:23:45.123456789012 Asia/Kathmandu'", new ColumnStatistics("null", "1.0", "0.0", "+12344-12-31 19:38:45.123 UTC"))
                .addRoundTrip("timestamp(12) with time zone", "null", NULL_STATISTICS)

                .addRoundTrip("json", "JSON '{\"a\":1,\"b\":2}'", new ColumnStatistics("null", "1.0", "0.0", "null"))
                .addRoundTrip("json", "CAST(NULL AS JSON)", NULL_STATISTICS)

                .execute(getQueryRunner(), remoteConnectorCreated("test_types"))
                .execute(getQueryRunner(), remoteTrinoCreatedRemoteConnectorInserted("test_types"))
                .execute(getQueryRunner(), remoteConnectorCreateAsSelect("test_types"))
                .execute(getQueryRunner(), remoteConnectorCreateAndInsert("test_types"));
    }

    private DataSetup remoteConnectorCreated(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(remoteExecutor, tableNamePrefix);
    }

    private DataSetup remoteTrinoCreatedRemoteConnectorInserted(String tableNamePrefix)
    {
        return remoteTrinoCreatedRemoteConnectorInserted(getSession(), tableNamePrefix);
    }

    private DataSetup remoteTrinoCreatedRemoteConnectorInserted(Session session, String tableNamePrefix)
    {
        return new CreateAndTrinoInsertDataSetup(remoteExecutor, new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup remoteConnectorCreateAsSelect(String tableNamePrefix)
    {
        return remoteConnectorCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup remoteConnectorCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup remoteConnectorCreateAndInsert(String tableNamePrefix)
    {
        return remoteConnectorCreateAndInsert(getSession(), tableNamePrefix);
    }

    private DataSetup remoteConnectorCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }
}
