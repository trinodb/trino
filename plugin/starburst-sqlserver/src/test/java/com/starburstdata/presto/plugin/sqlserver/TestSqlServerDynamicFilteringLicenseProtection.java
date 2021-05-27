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

import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.AbstractDynamicFilteringLicenseProtectionTest;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.QueryRunner;

import java.util.List;
import java.util.Map;

import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.createStarburstSqlServerQueryRunner;

public class TestSqlServerDynamicFilteringLicenseProtection
        extends AbstractDynamicFilteringLicenseProtectionTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingSqlServer server = closeAfterClass(new TestingSqlServer());
        return createStarburstSqlServerQueryRunner(server, false, Map.of(), List.of());
    }
}
