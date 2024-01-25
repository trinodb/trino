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

import com.starburstdata.presto.redirection.AbstractTableScanRedirectionTest;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.QueryRunner;

import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestStarburstSqlServerTableScanRedirection
        extends AbstractTableScanRedirectionTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String catalogName = "sqlserver_" + randomNameSuffix();
        TestingSqlServer sqlServer = closeAfterClass(new TestingSqlServer());
        return StarburstSqlServerQueryRunner.builder(sqlServer)
                .withEnterpriseFeatures()
                .withCatalog(catalogName)
                .withConnectorProperties(getRedirectionProperties(catalogName, "dbo"))
                .withTables(REQUIRED_TPCH_TABLES)
                .build();
    }
}
