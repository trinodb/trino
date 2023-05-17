/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testng.services.ManageTestResources;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.jdbcBuilder;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeServer.TEST_WAREHOUSE;
import static io.trino.tpch.TpchTable.NATION;
import static java.lang.String.format;

public class TestJdbcSnowflakeWarehouseSwitching
        extends AbstractTestQueryFramework
{
    protected static final String COMPUTE_WAREHOUSE = "COMPUTE_WH";
    protected static final String INVALID_WAREHOUSE = "NOT_EXISTING_WH";

    @ManageTestResources.Suppress(because = "Mock to remote server")
    protected final SnowflakeServer server = new SnowflakeServer();
    @ManageTestResources.Suppress(because = "Used by mocks")
    protected final Closer closer = Closer.create();
    @ManageTestResources.Suppress(because = "Mock to remote database")
    protected final TestDatabase testDB = closer.register(server.createTestDatabase());

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createBuilder()
                .withServer(server)
                .withDatabase(Optional.of(testDB.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(impersonationDisabled())
                .withTpchTables(ImmutableList.of(NATION))
                .build();
    }

    protected SnowflakeQueryRunner.Builder createBuilder()
    {
        return jdbcBuilder();
    }

    @BeforeClass
    public void initialize()
            throws SQLException
    {
        server.executeOnDatabase(testDB.getName(), format("CREATE VIEW IF NOT EXISTS %s.current_warehouse (warehouse) AS SELECT current_warehouse();", TEST_SCHEMA));
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        closer.close();
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
