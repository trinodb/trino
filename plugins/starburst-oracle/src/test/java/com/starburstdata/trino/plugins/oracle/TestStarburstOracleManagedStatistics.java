/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starburstdata.managed.statistics.BaseManagedStatisticsTest;
import com.starburstdata.presto.testing.testcontainers.TestingOracleServer;
import io.trino.Session;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testng.annotations.Test;

import static com.starburstdata.trino.plugins.oracle.TestingStarburstOracleServer.getJdbcUrl;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.oracle.OracleSessionProperties.NUMBER_ROUNDING_MODE;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static java.math.RoundingMode.UNNECESSARY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStarburstOracleManagedStatistics
        extends BaseManagedStatisticsTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        JdbcDatabaseContainer<?> starburstStorage = closeAfterClass(new TestingOracleServer());

        return OracleQueryRunner.builder()
                .withUnlockEnterpriseFeatures(true)
                .withNodesCount(3)
                .withStarburstStorage(starburstStorage)
                .withManagedStatistics()
                .withConnectorProperties(ImmutableMap.of(
                        "connection-url", getJdbcUrl(),
                        "connection-user", OracleTestUsers.USER,
                        "connection-password", OracleTestUsers.PASSWORD))
                .withTables(ImmutableList.of(LINE_ITEM))
                .build();
    }

    @Test
    @Override
    public void testTrinoTypes()
    {
        try (TestTable testTable = new TestTable(getQueryRunner()::execute, "trino_types", """
                (
                bigint_column BIGINT,
                integer_column INTEGER,
                smallint_column SMALLINT,
                tinyint_column TINYINT,
                date_column DATE,
                timestamp_column TIMESTAMP(3),
                timestamptz_column TIMESTAMP(3) WITH TIME ZONE,
                real_column REAL,
                double_column DOUBLE,
                varchar_column VARCHAR(20),
                clob_column VARCHAR,
                char_column CHAR(10),
                decimal_column DECIMAL,
                decimal_1_scale_column DECIMAL(38,1),
                decimal_2_scale_column DECIMAL(38,2),
                varbinary_column VARBINARY)
                """,
                ImmutableList.of(
                        "11111, 5555, 111, 11, DATE '0001-01-01', TIMESTAMP '1970-01-01 01:01:01.123456', TIMESTAMP '1970-01-01 01:01:01.123456 UTC', 1.141592, 121.456E10, '123456', '123456', '0123456', 1.11, 1.11, 1.11, X'1234567890'",
                        "22222, null, 222, 22, DATE '1582-10-04', TIMESTAMP '9999-12-31 23:59:59.999000', TIMESTAMP '9999-12-31 23:59:59.987654 UTC', 2.141592, 122.456E10, 'text_b', 'text_b', '0text_b', 2.22, 2.22, 2.22, X'68656C6C6F'",
                        "33333, 1001, 333, 33, DATE '1582-10-05', TIMESTAMP '0001-01-01 00:00:00.000000', null                                      , 3.141592, 123.456E10,  null   ,  null   ,  null    , 3.33, 3.33, 3.33, X'68656C6C6F'",
                        "44444, null, 444, 44, DATE '1582-10-14', null                                  , null                                      , 4.141592, 124.456E10, 'text_c', 'text_c', '0text_c', 4.44, 4.44, 4.44, X'68656C6C6F'",
                        "55555, 1001, 555, 55, DATE '1952-04-03', TIMESTAMP '1970-01-01 01:01:01.523456', TIMESTAMP '1970-01-01 01:01:01.523456 UTC', 5.141592, 125.456E10, 'text_c', 'text_c', '0text_c', 5.55, 5.55, 5.55, X'68656C6C6F'"))) {
            String tableName = testTable.getName();

            String emptyStats = """
                    VALUES
                    ('bigint_column', null, null, null, null, null, null),
                    ('integer_column', null, null, null, null, null, null),
                    ('smallint_column', null, null, null, null, null, null),
                    ('tinyint_column', null, null, null, null, null, null),
                    ('date_column', null, null, null, null, null, null),
                    ('timestamp_column', null, null, null, null, null, null),
                    ('timestamptz_column', null, null, null, null, null, null),
                    ('real_column', null, null, null, null, null, null),
                    ('double_column', null, null, null, null, null, null),
                    ('varchar_column', null, null, null, null, null, null),
                    ('clob_column', null, null, null, null, null, null),
                    ('char_column', null, null, null, null, null, null),
                    ('decimal_column', null, null, null, null, null, null),
                    ('decimal_1_scale_column', null, null, null, null, null, null),
                    ('decimal_2_scale_column', null, null, null, null, null, null),
                    ('varbinary_column', null, null, null, null, null, null),
                    (null, null, null, null, null, null, null)
                    """;
            assertThat(query("SHOW STATS FOR (SELECT * FROM " + tableName + ")"))
                    .skippingTypesCheck()
                    .matches(emptyStats);

            assertUpdate("ALTER TABLE " + tableName + " EXECUTE collect_statistics");

            assertThat(query("SHOW STATS FOR (SELECT * FROM " + tableName + ")"))
                    .skippingTypesCheck()
                    .matches("""
                            VALUES
                            ('bigint_column',          null,          DOUBLE '5.0', DOUBLE '0.0', null,         '11111.0',                    '55555.0'),
                            ('integer_column',         null,          DOUBLE '2.0', DOUBLE '0.4', null,         '1001.0',                     '5555.0'),
                            ('smallint_column',        null,          DOUBLE '5.0', DOUBLE '0.0', null,         '111.0',                      '555.0'),
                            ('tinyint_column',         null,          DOUBLE '5.0', DOUBLE '0.0', null,         '11.0',                       '55.0'),
                            ('date_column',            null,          DOUBLE '4.0', DOUBLE '0.0', null,         '0001-01-01 00:00:00',        '1952-04-03 00:00:00'),
                            ('timestamp_column',       null,          DOUBLE '4.0', DOUBLE '0.2', null,         '0001-01-01 00:00:00.000',    '9999-12-31 23:59:59.999'),
                            ('timestamptz_column',     null,          DOUBLE '3.0', DOUBLE '0.4', null,         '1970-01-01 01:01:01.123 UTC','9999-12-31 23:59:59.988 UTC'),
                            ('real_column',            null,          DOUBLE '5.0', DOUBLE '0.0', null,         '1.141592',                   '5.141592'),
                            ('double_column',          null,          DOUBLE '5.0', DOUBLE '0.0', null,         '1.21456E12',                 '1.25456E12'),
                            ('varchar_column',         DOUBLE '24.0', DOUBLE '3.0', DOUBLE '0.2', null,         null,                          null),
                            ('clob_column',            DOUBLE '24.0', null,         DOUBLE '0.2', null,         null,                          null),
                            ('char_column',            DOUBLE '40.0', DOUBLE '3.0', DOUBLE '0.2', null,         null,                          null),
                            ('decimal_column',         null,          DOUBLE '5.0', DOUBLE '0.0', null,         '1.0',                         '6.0'),
                            ('decimal_1_scale_column', null,          DOUBLE '5.0', DOUBLE '0.0', null,         '1.1',                         '5.6'),
                            ('decimal_2_scale_column', null,          DOUBLE '5.0', DOUBLE '0.0', null,         '1.11',                        '5.55'),
                            ('varbinary_column',       DOUBLE '25.0', null,         DOUBLE '0.0', null,         null,                          null),
                            (null,                     null,          null,         null,         DOUBLE '5.0', null,                          null)
                            """);

            // cleanup
            assertUpdate("ALTER TABLE " + tableName + " EXECUTE drop_statistics");
        }
    }

    @Test
    public void testNativeTypes()
    {
        try (TestTable testTable = new TestTable(TestingStarburstOracleServer::executeInOracle, "native_types", """
                (
                raw_column raw(2000),
                nclob_column nclob,
                number_column number,
                nvarchar2_column nvarchar2(13),
                varchar2_column varchar2(10 char),
                binary_float_column binary_float,
                binary_double_column binary_double,
                float_column float)
                """,
                ImmutableList.of(
                        "hextoraw('68656C6C6F'), 'nclob', 1, 'a', 'a', 1.0, 1.0, 1.0",
                        "hextoraw('68656C6C6F'), 'nclob', 1, 'a', 'a', 1.0, 1.0, 1.0",
                        "hextoraw('68656C6C6F'), 'nclob', 1, 'a', 'a', 1.0, 1.0, 1.0",
                        "hextoraw('68656C6C6F'), 'nclob', 1, 'a', 'a', 1.0, 1.0, 1.0",
                        "hextoraw('68656C6C6F'), 'nclob', 1, 'a', 'a', 1.0, 1.0, 1.0"))) {
            String tableName = testTable.getName();

            String emptyStats = """
                    VALUES
                    ('raw_column', null, null, null, null, null, null),
                    ('nclob_column', null, null, null, null, null, null),
                    ('number_column', null, null, null, null, null, null),
                    ('nvarchar2_column', null, null, null, null, null, null),
                    ('varchar2_column', null, null, null, null, null, null),
                    ('binary_float_column', null, null, null, null, null, null),
                    ('binary_double_column', null, null, null, null, null, null),
                    ('float_column', null, null, null, null, null, null),
                    (null, null, null, null, null, null, null)
                    """;
            Session session = Session.builder(getSession())
                    .setCatalogSessionProperty("oracle", UNSUPPORTED_TYPE_HANDLING, CONVERT_TO_VARCHAR.name())
                    .setCatalogSessionProperty("oracle", NUMBER_ROUNDING_MODE, UNNECESSARY.name())
                    .build();

            assertThat(query(session, "SHOW STATS FOR (SELECT * FROM " + tableName + ")"))
                    .skippingTypesCheck()
                    .matches(emptyStats);

            assertUpdate(session, "ALTER TABLE " + tableName + " EXECUTE collect_statistics");

            assertThat(query(session, "SHOW STATS FOR (SELECT * FROM " + tableName + ")"))
                    .skippingTypesCheck()
                    .matches("""
                            VALUES
                            ('raw_column',           DOUBLE '50.0', DOUBLE '1.0', DOUBLE '0.0', null,         null,  null),
                            ('nclob_column',         DOUBLE '25.0', null,         DOUBLE '0.0', null,         null,  null),
                            ('number_column',        DOUBLE '5.0',  DOUBLE '1.0', DOUBLE '0.0', null,         null,  null),
                            ('nvarchar2_column',     DOUBLE '5.0',  DOUBLE '1.0', DOUBLE '0.0', null,         null,  null),
                            ('varchar2_column',      DOUBLE '5.0',  DOUBLE '1.0', DOUBLE '0.0', null,         null,  null),
                            ('binary_double_column', null,          DOUBLE '1.0', DOUBLE '0.0', null,         '1.0', '1.0'),
                            ('binary_float_column',  null,          DOUBLE '1.0', DOUBLE '0.0', null,         '1.0', '1.0'),
                            ('float_column',         null,          DOUBLE '1.0', DOUBLE '0.0', null,         '1.0', '1.0'),
                            (null,                   null,          null,         null,         DOUBLE '5.0', null,  null)
                            """);

            // cleanup
            assertUpdate("ALTER TABLE " + tableName + " EXECUTE drop_statistics");
        }
    }
}
