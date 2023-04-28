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

import com.google.common.collect.ImmutableList;
import com.starburstdata.managed.statistics.BaseManagedStatisticsTest;
import com.starburstdata.managed.statistics.ManagedStatisticsDataTypeTest;
import com.starburstdata.managed.statistics.ManagedStatisticsDataTypeTest.ColumnStatistics;
import com.starburstdata.presto.testing.testcontainers.TestingEventLoggerPostgreSqlServer;
import io.trino.Session;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testng.annotations.Test;

import static io.trino.tpch.TpchTable.LINE_ITEM;

public class TestSqlServerManagedStatistics
        extends BaseManagedStatisticsTest
{
    private static final ColumnStatistics NULL_STATISTICS = new ColumnStatistics("0.0", "0.0", "1.0", "null");
    private static final ColumnStatistics UNSUPPORTED_STATISTICS = new ColumnStatistics("null", "null", "null", "null");

    private TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        JdbcDatabaseContainer<?> starburstPersistence = closeAfterClass(new TestingEventLoggerPostgreSqlServer());
        this.sqlServer = closeAfterClass(new TestingSqlServer());
        return StarburstSqlServerQueryRunner.builder(sqlServer)
                .withEnterpriseFeatures()
                .withStarburstStorage(starburstPersistence)
                .withManagedStatistics()
                .withTables(ImmutableList.of(LINE_ITEM))
                .build();
    }

    @Test
    @Override
    public void testTrinoTypes()
    {
        ManagedStatisticsDataTypeTest.create()
                .addRoundTrip("bit", "1", new ColumnStatistics("null", "1.0", "0.0", "null"))
                .addRoundTrip("bit", "null", NULL_STATISTICS)

                .addRoundTrip("float", "1234567890123456789.0123456789", new ColumnStatistics("null", "1.0", "0.0", "1.23456789012345677E18"))
                .addRoundTrip("float", "null", NULL_STATISTICS)
                .addRoundTrip("float(24)", "1234567890123456789.0123456789", new ColumnStatistics("null", "1.0", "0.0", "1.23456794E18"))
                .addRoundTrip("float(24)", "null", NULL_STATISTICS)
                .addRoundTrip("float(53)", "1234567890123456789.0123456789", new ColumnStatistics("null", "1.0", "0.0", "1.23456789012345677E18"))
                .addRoundTrip("float(53)", "null", NULL_STATISTICS)

                .addRoundTrip("double precision", "123.456E10", new ColumnStatistics("null", "1.0", "0.0", "1.23456E12"))
                .addRoundTrip("double precision", "null", NULL_STATISTICS)

                .addRoundTrip("char(4001)", "'text_c'", new ColumnStatistics("6.0", "1.0", "0.0", "null"))

                .addRoundTrip("nchar", "null", NULL_STATISTICS)
                .addRoundTrip("nchar", "''", new ColumnStatistics("0.0", "1.0", "0.0", "null"))
                .addRoundTrip("nchar", "'a'", new ColumnStatistics("1.0", "1.0", "0.0", "null"))
                .addRoundTrip("nchar(255)", "'text_a'", new ColumnStatistics("6.0", "1.0", "0.0", "null"))

                .addRoundTrip("varchar(4001)", "'text_c'", new ColumnStatistics("6.0", "1.0", "0.0", "null"))

                .addRoundTrip("nvarchar(5)", "N'攻殻機動隊'", new ColumnStatistics("5.0", "1.0", "0.0", "null"))
                .addRoundTrip("nvarchar(5)", "NULL", NULL_STATISTICS)
                .addRoundTrip("nvarchar(150)", "N'攻殻機動隊'", new ColumnStatistics("5.0", "1.0", "0.0", "null"))
                .addRoundTrip("nvarchar(150)", "NULL", NULL_STATISTICS)

                .addRoundTrip("text", "N'攻殻機動隊'", UNSUPPORTED_STATISTICS)
                .addRoundTrip("text", "NULL", UNSUPPORTED_STATISTICS)
                .addRoundTrip("ntext", "N'攻殻機動隊'", UNSUPPORTED_STATISTICS)
                .addRoundTrip("ntext", "NULL", UNSUPPORTED_STATISTICS)

                .addRoundTrip("varbinary(10)", "NULL", NULL_STATISTICS)
                .addRoundTrip("varbinary(20)", "0x", new ColumnStatistics("0.0", "1.0", "0.0", "null"))
                .addRoundTrip("varbinary(30)", "0x68656C6C6F", new ColumnStatistics("5.0", "1.0", "0.0", "null"))

                .addRoundTrip("date", "'1952-04-03'", new ColumnStatistics("null", "1.0", "0.0", "1952-04-03"))
                .addRoundTrip("time", "'19:01:17.123'", new ColumnStatistics("null", "1.0", "0.0", "null"))
                .addRoundTrip("time(7)", "'03:17:17.1234567'", new ColumnStatistics("null", "1.0", "0.0", "null"))

                .addRoundTrip("DATETIME2(7)", "'2020-09-27 12:34:56.1234567'", new ColumnStatistics("null", "1.0", "0.0", "2020-09-27 12:34:56.123456"))
                .addRoundTrip("DATETIME2(7)", "null", NULL_STATISTICS)

                .addRoundTrip("DATETIMEOFFSET", "'2020-09-27 00:00:00.1234567+07:00'", new ColumnStatistics("null", "1.0", "0.0", "2020-09-26 17:00:00.123 UTC"))
                .addRoundTrip("DATETIMEOFFSET", "null", NULL_STATISTICS)
                .addRoundTrip("DATETIMEOFFSET(7)", "'2020-09-27 00:00:00.1234567+07:00'", new ColumnStatistics("null", "1.0", "0.0", "2020-09-26 17:00:00.123 UTC"))
                .addRoundTrip("DATETIMEOFFSET(7)", "null", NULL_STATISTICS)

                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_types"));

        ManagedStatisticsDataTypeTest.create()
                .addRoundTrip("boolean", "true", new ColumnStatistics("null", "1.0", "0.0", "null"))
                .addRoundTrip("boolean", "null", NULL_STATISTICS)

                .addRoundTrip("double", "123.456E10", new ColumnStatistics("null", "1.0", "0.0", "1.23456E12"))
                .addRoundTrip("double", "null", NULL_STATISTICS)

                .addRoundTrip("char(4001)", "'text_c'", new ColumnStatistics("6.0", "1.0", "0.0", "null"))

                .addRoundTrip("varchar", "'text_a'", new ColumnStatistics("6.0", "1.0", "0.0", "null"))
                .addRoundTrip("varchar", "null", NULL_STATISTICS)

                .addRoundTrip("varbinary", "NULL", NULL_STATISTICS)
                .addRoundTrip("varbinary", "X''", new ColumnStatistics("0.0", "1.0", "0.0", "null"))
                .addRoundTrip("varbinary", "X'68656C6C6F'", new ColumnStatistics("5.0", "1.0", "0.0", "null"))

                .addRoundTrip("date", "DATE '1952-04-03'", new ColumnStatistics("null", "1.0", "0.0", "1952-04-03"))
                .addRoundTrip("time", "TIME '19:01:17.123'", new ColumnStatistics("null", "1.0", "0.0", "null"))
                .addRoundTrip("time(7)", "TIME '03:17:17.1234567'", new ColumnStatistics("null", "1.0", "0.0", "null"))

                .addRoundTrip("timestamp", "TIMESTAMP '1970-01-01 01:01:01.1234567'", new ColumnStatistics("null", "1.0", "0.0", "1970-01-01 01:01:01.123"))
                .addRoundTrip("timestamp", "null", NULL_STATISTICS)
                .addRoundTrip("timestamp(7)", "TIMESTAMP '1970-01-01 01:01:01.1234567'", new ManagedStatisticsDataTypeTest.ColumnStatistics("null", "1.0", "0.0", "1970-01-01 01:01:01.123456"))
                .addRoundTrip("timestamp(7)", "null", NULL_STATISTICS)
                .execute(getQueryRunner(), trinoCreateAndInsert("test_types"));

        ManagedStatisticsDataTypeTest.create()
                .addRoundTrip("tinyint", "42", new ColumnStatistics("null", "1.0", "0.0", "42"))
                .addRoundTrip("tinyint", "null", NULL_STATISTICS)
                .addRoundTrip("smallint", "32456", new ColumnStatistics("null", "1.0", "0.0", "32456"))
                .addRoundTrip("smallint", "null", NULL_STATISTICS)
                .addRoundTrip("integer", "1234567890", new ColumnStatistics("null", "1.0", "0.0", "1234567890"))
                .addRoundTrip("integer", "null", NULL_STATISTICS)
                .addRoundTrip("bigint", "123456789012", new ColumnStatistics("null", "1.0", "0.0", "123456789012"))
                .addRoundTrip("bigint", "null", NULL_STATISTICS)

                .addRoundTrip("real", "123.45", new ColumnStatistics("null", "1.0", "0.0", "123.45"))
                .addRoundTrip("real", "null", NULL_STATISTICS)

                .addRoundTrip("decimal", "1.11", new ColumnStatistics("null", "1.0", "0.0", "1.0"))
                .addRoundTrip("decimal", "null", NULL_STATISTICS)
                .addRoundTrip("decimal(38,1)", "1.11", new ColumnStatistics("null", "1.0", "0.0", "1.1"))
                .addRoundTrip("decimal(38,1)", "null", NULL_STATISTICS)
                .addRoundTrip("decimal(38,2)", "1.11", new ColumnStatistics("null", "1.0", "0.0", "1.11"))
                .addRoundTrip("decimal(38,2)", "null", NULL_STATISTICS)

                .addRoundTrip("char", "''", new ColumnStatistics("0.0", "1.0", "0.0", "null"))
                .addRoundTrip("char", "'a'", new ColumnStatistics("1.0", "1.0", "0.0", "null"))
                .addRoundTrip("char(255)", "'text_a'", new ColumnStatistics("6.0", "1.0", "0.0", "null"))

                .addRoundTrip("varchar(32)", "'e'", new ColumnStatistics("1.0", "1.0", "0.0", "null"))
                .addRoundTrip("varchar(32)", "NULL", NULL_STATISTICS)
                .addRoundTrip("varchar(150)", "'fgh'", new ColumnStatistics("3.0", "1.0", "0.0", "null"))
                .addRoundTrip("varchar(150)", "NULL", NULL_STATISTICS)

                .addRoundTrip("date", "null", NULL_STATISTICS)
                .addRoundTrip("time", "null", NULL_STATISTICS)
                .addRoundTrip("time(7)", "null", NULL_STATISTICS)

                .execute(getQueryRunner(), sqlServerCreateAndInsert("test_types"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_types"));
    }

    private DataSetup trinoCreateAndInsert(String tableNamePrefix)
    {
        return trinoCreateAndInsert(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup sqlServerCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(onRemoteDatabase(), tableNamePrefix);
    }

    private SqlExecutor onRemoteDatabase()
    {
        return sqlServer::execute;
    }
}
