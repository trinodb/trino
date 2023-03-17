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
import com.starburstdata.presto.testing.testcontainers.TestingEventLoggerPostgreSqlServer;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testng.annotations.Test;

import static io.trino.tpch.TpchTable.LINE_ITEM;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlServerManagedStatistics
        extends BaseManagedStatisticsTest
{
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
        try (TestTable testTable = new TestTable(getQueryRunner()::execute, "trino_types", """
                (
                bigint_column BIGINT,
                integer_column INTEGER,
                smallint_column SMALLINT,
                tinyint_column TINYINT,
                boolean_column BOOLEAN,
                date_column DATE,
                time_column TIME,
                timestamp_column TIMESTAMP(6),
                decimal_column DECIMAL,
                decimal_1_scale_column DECIMAL(38,1),
                decimal_2_scale_column DECIMAL(38,2),
                real_column REAL,
                double_column DOUBLE,
                varchar_column VARCHAR,
                varbinary_column VARBINARY)
                """,
                ImmutableList.of(
                        "11111, 5555, 111, 11, true , DATE '0001-01-01', TIME '00:00:00.000000', TIMESTAMP '1970-01-01 01:01:01.123456', 1.11, 1.11, 1.11, 1.141592, 121.456E10, '123456', X'1234567890'",
                        "22222, null, 222, 22, null , DATE '1582-10-04', TIME '00:13:42.000000', TIMESTAMP '9999-12-31 23:59:59.999000', 2.22, 2.22, 2.22, 2.141592, 122.456E10, 'text_b', X'68656C6C6F'",
                        "33333, 1001, 333, 33, null , DATE '1582-10-05', TIME '03:17:17.000000', TIMESTAMP '0001-01-01 00:00:00.000000', 3.33, 3.33, 3.33, 3.141592, 123.456E10,  null   , X'68656C6C6F'",
                        "44444, null, 444, 44, null , DATE '1582-10-14', null,                   null                                  , 4.44, 4.44, 4.44, 4.141592, 124.456E10, 'text_c', X'68656C6C6F'",
                        "55555, 1001, 555, 55, false, DATE '1952-04-03', TIME '19:01:17.000000', TIMESTAMP '1970-01-01 01:01:01.523456', 5.55, 5.55, 5.55, 5.141592, 125.456E10, 'text_c', X'68656C6C6F'"))) {
            String tableName = testTable.getName();

            String emptyStats = """
                    VALUES
                    ('bigint_column', null, null, null, null, null, null),
                    ('integer_column', null, null, null, null, null, null),
                    ('smallint_column', null, null, null, null, null, null),
                    ('tinyint_column', null, null, null, null, null, null),
                    ('boolean_column', null, null, null, null, null, null),
                    ('date_column', null, null, null, null, null, null),
                    ('time_column', null, null, null, null, null, null),
                    ('timestamp_column', null, null, null, null, null, null),
                    ('decimal_column', null, null, null, null, null, null),
                    ('decimal_1_scale_column', null, null, null, null, null, null),
                    ('decimal_2_scale_column', null, null, null, null, null, null),
                    ('real_column', null, null, null, null, null, null),
                    ('double_column', null, null, null, null, null, null),
                    ('varchar_column', null, null, null, null, null, null),
                    ('varbinary_column', null, null, null, null, null, null),
                    (null, null, null, null, null, null, null)
                    """;
            assertThat(query("SHOW STATS FOR (SELECT * FROM " + tableName + ")"))
                    .skippingTypesCheck()
                    .matches(emptyStats);

            assertUpdate("ALTER TABLE " + tableName + " EXECUTE collect_statistics");

            assertThat(query("SHOW STATS FOR (SELECT * FROM " + tableName + ")"))
                    .skippingTypesCheck()
                    .matches("VALUES"
                            + "('bigint_column',          null,          DOUBLE '5.0', DOUBLE '0.0', null,         '11111',                       '55555'),"
                            + "('integer_column',         null,          DOUBLE '2.0', DOUBLE '0.4', null,         '1001',                        '5555'),"
                            + "('smallint_column',        null,          DOUBLE '5.0', DOUBLE '0.0', null,         '111',                         '555'),"
                            + "('tinyint_column',         null,          DOUBLE '5.0', DOUBLE '0.0', null,         '11',                          '55'),"
                            + "('boolean_column',         null,          DOUBLE '2.0', DOUBLE '0.6', null,         null,                          null),"
                            + "('date_column',            null,          DOUBLE '5.0', DOUBLE '0.0', null,         '0001-01-01',                  '1952-04-03'),"
                            + "('time_column',            null,          DOUBLE '4.0', DOUBLE '0.2', null,         null,                          null),"
                            + "('timestamp_column',       null,          DOUBLE '4.0', DOUBLE '0.2', null,         '0001-01-01 00:00:00.000000',  '9999-12-31 23:59:59.999008')," // not exact match of the MAX value - note 8 at the end
                            + "('decimal_column',         null,          DOUBLE '5.0', DOUBLE '0.0', null,         '1.0',                         '6.0'),"
                            + "('decimal_1_scale_column', null,          DOUBLE '5.0', DOUBLE '0.0', null,         '1.1',                         '5.6'),"
                            + "('decimal_2_scale_column', null,          DOUBLE '5.0', DOUBLE '0.0', null,         '1.11',                        '5.55'),"
                            + "('real_column',            null,          DOUBLE '5.0', DOUBLE '0.0', null,         '1.141592',                    '5.141592'),"
                            + "('double_column',          null,          DOUBLE '5.0', DOUBLE '0.0', null,         '1.21456E12',                  '1.25456E12'),"
                            + "('varchar_column',         DOUBLE '24.0', DOUBLE '3.0', DOUBLE '0.2', null,         null,                          null),"
                            + "('varbinary_column',       DOUBLE '25.0', DOUBLE '2.0', DOUBLE '0.0', null,         null,                          null),"
                            + "(null,                     null,          null,         null,         DOUBLE '5.0', null,                          null)");

            // cleanup
            assertUpdate("ALTER TABLE " + tableName + " EXECUTE drop_statistics");
        }
    }

    @Test
    public void testNativeTypes()
    {
        try (TestTable testTable = new TestTable(sqlServer::execute, "native_types", """
                (
                datetime_column DATETIME2(6),
                datetimeoffset_column DATETIMEOFFSET,
                text_column text,
                ntext_column ntext,
                double_precision_column double precision,
                bit_column bit,
                float_column float)
                """,
                ImmutableList.of(
                        "'1970-01-01 00:00:00.123000', '1970-01-01 00:00:00', 'text1', 'text1', 1.0E100, 1, 1E100",
                        "'1970-01-01 00:00:00.123000', '1970-01-01 00:00:00', 'text2', 'text2', 2,       1, 1.0",
                        "'1970-01-01 00:00:00.123000', '1970-01-01 00:00:00', null,     null,   null,    1, 1.0",
                        "'1970-01-01 00:00:00.123000', '1970-01-01 00:00:00', 'text3', 'text3', 1,       1, 1.0",
                        "'1970-01-01 00:00:00.123000', '1970-01-01 00:00:00', 'text4', 'text4', 1E10,    0, 1E10"))) {
            String tableName = testTable.getName();

            String emptyStats = """
                    VALUES
                    ('datetime_column', null, null, null, null, null, null),
                    ('datetimeoffset_column', null, null, null, null, null, null),
                    ('text_column', null, null, null, null, null, null),
                    ('ntext_column', null, null, null, null, null, null),
                    ('double_precision_column', null, null, null, null, null, null),
                    ('bit_column', null, null, null, null, null, null),
                    ('float_column', null, null, null, null, null, null),
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
                            ('datetime_column',         null, DOUBLE '1.0', DOUBLE '0.0', null,         '1970-01-01 00:00:00.123000',  '1970-01-01 00:00:00.123000'),
                            ('datetimeoffset_column',   null, null,         null,         null,         null,                          null),
                            ('text_column',             null, null,         null,         null,         null,                          null),
                            ('ntext_column',            null, null,         null,         null,         null,                          null),
                            ('double_precision_column', null, DOUBLE '4.0', DOUBLE '0.2', null,         '1.0',                         '1.0E100'),
                            ('bit_column',              null, DOUBLE '2.0', DOUBLE '0.0', null,         null,                          null),
                            ('float_column',            null, DOUBLE '3.0', DOUBLE '0.0', null,         '1.0',                         '1.0E100'),
                            (null,                      null, null,         null,         DOUBLE '5.0', null,                          null)
                            """);

            // cleanup
            assertUpdate("ALTER TABLE " + tableName + " EXECUTE drop_statistics");
        }
    }
}
