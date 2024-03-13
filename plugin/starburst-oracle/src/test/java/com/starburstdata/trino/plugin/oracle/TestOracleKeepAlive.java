/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.SharedResource.Lease;
import org.junit.jupiter.api.Test;

public class TestOracleKeepAlive
        extends AbstractTestQueryFramework
{
    private Lease<TestingStarburstOracleServer> oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = closeAfterClass(TestingStarburstOracleServer.getInstance());
        QueryRunner queryRunner = OracleQueryRunner.builder(oracleServer)
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("oracle.keep-alive.enabled", "true")
                        .put("oracle.keep-alive.interval", "30s")
                        .buildOrThrow())
                .build();
        queryRunner.installPlugin(new BlackHolePlugin());
        queryRunner.createCatalog("blackhole", "blackhole");
        return queryRunner;
    }

    @Test
    public void testKeepAlive()
    {
        oracleServer.get().executeInOracle("ALTER SYSTEM SET RESOURCE_LIMIT = TRUE");
        oracleServer.get().executeInOracle("ALTER PROFILE DEFAULT LIMIT IDLE_TIME 1"); // 1 minute
        getQueryRunner().execute("CREATE TABLE blackhole.default.a(a bigint) " +
                "WITH (split_count = 2, pages_per_split = 1, rows_per_page = 100, page_processing_delay = '90s')"); // give Oracle some leeway

        assertQuerySucceeds("CREATE TABLE testKeepAliveEnabled AS (SELECT * FROM blackhole.default.a)");
    }
}
