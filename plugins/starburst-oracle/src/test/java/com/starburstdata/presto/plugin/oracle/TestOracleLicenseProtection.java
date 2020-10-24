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
import io.prestosql.Session;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.connectionProperties;
import static io.prestosql.tpch.TpchTable.NATION;

public class TestOracleLicenseProtection
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withConnectorProperties(connectionProperties())
                .withTables(ImmutableList.of(NATION))
                .withUnlockEnterpriseFeatures(false)
                .build();
    }

    @Test
    public void testLicenseProtectionOfParallelismViaSessionProperty()
    {
        Session noParallelismSession = Session.builder(getSession())
                .setCatalogSessionProperty("oracle", "parallelism_type", "no_parallelism")
                .build();

        Session partitionsParallelismSession = Session.builder(getSession())
                .setCatalogSessionProperty("oracle", "parallelism_type", "partitions")
                .build();

        assertQuery(noParallelismSession, "SELECT * FROM nation");
        assertQueryFails(partitionsParallelismSession, "SELECT * FROM nation", "Valid license required to use the feature: oracle-extensions");
    }
}
