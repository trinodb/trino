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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.oracle.BaseOracleConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.SharedResource;
import io.trino.testing.TestingConnectorBehavior;
import org.testng.annotations.AfterClass;

import static com.starburstdata.trino.plugins.oracle.OracleQueryRunner.createSession;
import static com.starburstdata.trino.plugins.oracle.OracleTestUsers.createStandardUsers;
import static com.starburstdata.trino.plugins.oracle.OracleTestUsers.createUser;
import static java.lang.String.format;

public class TestStarburstOracleParallelConnectorSmokeTest
        extends BaseOracleConnectorSmokeTest
{
    public static final String PARTITIONED_USER = "partitioned_user";

    private SharedResource.Lease<TestingStarburstOracleServer> oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = closeAfterClass(TestingStarburstOracleServer.getInstance());
        return OracleQueryRunner.builder(oracleServer)
                .withUnlockEnterpriseFeatures(true) // parallelism is license protected in SEP
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("oracle.parallelism-type", "PARTITIONS")
                        .put("oracle.parallel.max-splits-per-scan", "17")
                        .buildOrThrow())
                .withTables(REQUIRED_TPCH_TABLES)
                .withSessionModifier(session -> createSession(PARTITIONED_USER, PARTITIONED_USER))
                .withCreateUsers(this::createUsers)
                .withProvisionTables(this::partitionTables)
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        oracleServer = null;
    }

    protected void createUsers()
    {
        createStandardUsers(oracleServer.get());
        createUser(oracleServer.get(), PARTITIONED_USER);
        oracleServer.get().executeInOracle(format("GRANT SELECT ON user_context to %s", PARTITIONED_USER));
    }

    private void partitionTables()
    {
        oracleServer.get().executeInOracle(format("ALTER TABLE %s.nation MODIFY PARTITION BY RANGE (regionkey) INTERVAL (2) (PARTITION before_1 VALUES LESS THAN (1))", PARTITIONED_USER));
        oracleServer.get().executeInOracle(format("ALTER TABLE %s.region MODIFY PARTITION BY HASH(name) PARTITIONS 3", PARTITIONED_USER));
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_LIMIT_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN:
                // Full pushdown is disabled for parallel connector for correctness - see StarburstOracleClient#is(TopN)LimitGuaranteed
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }
}
