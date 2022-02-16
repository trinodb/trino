/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.jdbcBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.TEST_DATABASE;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.TEST_WAREHOUSE;
import static java.lang.String.format;

// TODO: Remove singleThreaded=true once https://github.com/trinodb/trino/issues/11067 gets fixed (https://starburstdata.atlassian.net/browse/SEP-8578)
@Test(singleThreaded = true)
public class TestJdbcSnowflakeWarehouseSwitching
        extends AbstractTestQueryFramework
{
    protected static final String COMPUTE_WAREHOUSE = "COMPUTE_WH";
    protected static final String INVALID_WAREHOUSE = "NOT_EXISTING_WH";

    protected final SnowflakeServer server = new SnowflakeServer();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return jdbcBuilder()
                .withServer(server)
                .withConnectorProperties(impersonationDisabled())
                .withConnectionPooling()
                .build();
    }

    @BeforeClass
    public void initialize()
            throws SQLException
    {
        server.executeOnDatabase(TEST_DATABASE, format("CREATE VIEW IF NOT EXISTS %s.current_warehouse (warehouse) AS SELECT current_warehouse();", TEST_SCHEMA));
    }

    @Test
    public void testDefaultWarehouse()
    {
        assertQuery("SELECT * FROM current_warehouse", format("VALUES ('%s')", TEST_WAREHOUSE));
        assertQuery("SELECT regionkey FROM nation WHERE name = 'ALGERIA'", "VALUES (0)");
    }

    @Test
    public void testSwitchToExistingWarehouse()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("snowflake", "warehouse", COMPUTE_WAREHOUSE)
                .build();

        assertQuery(session, "SELECT * FROM current_warehouse", format("VALUES ('%s')", COMPUTE_WAREHOUSE));
        assertQuery(session, "SELECT regionkey FROM nation WHERE name = 'ALGERIA'", "VALUES (0)");
    }

    @Test
    public void testSwitchNotExistingWarehouse()
    {
        Session invalidWarehouseSession = Session.builder(getSession())
                .setCatalogSessionProperty("snowflake", "warehouse", INVALID_WAREHOUSE)
                .build();

        assertQuery(invalidWarehouseSession, "SELECT * FROM current_warehouse", "VALUES (NULL)");
        assertQueryFails(invalidWarehouseSession, "SELECT regionkey FROM nation WHERE name = 'ALGERIA'", "Could not query Snowflake due to invalid warehouse configuration. " +
                "Fix configuration or select an active Snowflake warehouse with 'warehouse' catalog session property.");

        // Disable statistics precalculation so that code will throw while fetching rows in JdbcRecordCursor
        Session sessionWithoutStatisticsPrecalculation = Session.builder(invalidWarehouseSession)
                .setSystemProperty("statistics_precalculation_for_pushdown_enabled", "false")
                .build();

        // TODO: https://starburstdata.atlassian.net/browse/SEP-6500
        assertQueryFails(sessionWithoutStatisticsPrecalculation, "SELECT COUNT(1) FROM nation WHERE name = 'ALGERIA'",
                "No active warehouse selected in the current session.  Select an active warehouse with the 'use warehouse' command.\n");
    }
}
