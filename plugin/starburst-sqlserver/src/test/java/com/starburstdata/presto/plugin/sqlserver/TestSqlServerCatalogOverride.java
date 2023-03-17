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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.ALICE_USER;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.CATALOG;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerSessionProperties.OVERRIDE_CATALOG;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestSqlServerCatalogOverride
        extends AbstractTestQueryFramework
{
    private static final Session STANDARD_SESSION = testSessionBuilder()
            .setCatalog(CATALOG)
            .setSchema(TEST_SCHEMA)
            .setIdentity(Identity.ofUser(ALICE_USER))
            .build();
    private static final Session OVERRIDDEN_SESSION = testSessionBuilder()
            .setCatalog(CATALOG)
            .setSchema(TEST_SCHEMA)
            .setIdentity(Identity.ofUser(ALICE_USER))
            .setCatalogSessionProperty(CATALOG, OVERRIDE_CATALOG, "master")
            .build();

    private TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = closeAfterClass(new TestingSqlServer());
        return StarburstSqlServerQueryRunner.builder(sqlServer)
                .withEnterpriseFeatures()
                .withConnectorProperties(ImmutableMap.of("sqlserver.override-catalog.enabled", "true"))
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        sqlServer.close();
    }

    @Test
    public void testSelectFromTable()
    {
        // Table 'spt_monitor' exists only in master db
        assertQueryFails(STANDARD_SESSION, "SELECT COUNT(*) FROM spt_monitor", ".*Table 'sqlserver.dbo.spt_monitor' does not exist");
        assertQuerySucceeds(OVERRIDDEN_SESSION, "SELECT COUNT(*) FROM spt_monitor");
    }

    @Test
    public void testShowSchemas()
    {
        sqlServer.execute("CREATE SCHEMA test_catalog_override");

        assertQuery(STANDARD_SESSION, "SHOW SCHEMAS LIKE 'test_catalog_override'",
                "VALUES (CAST ('test_catalog_override' AS varchar))");
        assertQueryReturnsEmptyResult(OVERRIDDEN_SESSION, "SHOW SCHEMAS LIKE 'test_catalog_override'");
    }
}
