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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.oracle.BaseOracleConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;

import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createSession;
import static com.starburstdata.presto.plugin.oracle.OracleTestUsers.createStandardUsers;
import static com.starburstdata.presto.plugin.oracle.OracleTestUsers.createUser;
import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.executeInOracle;
import static java.lang.String.format;

public class TestStarburstOracleParallelConnectorSmokeTest
        extends BaseOracleConnectorSmokeTest
{
    public static final String PARTITIONED_USER = "partitioned_user";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withUnlockEnterpriseFeatures(true) // parallelism is license protected
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(TestingStarburstOracleServer.connectionProperties())
                        .put("oracle.parallelism-type", "PARTITIONS")
                        .put("oracle.parallel.max-splits-per-scan", "17")
                        .buildOrThrow())
                .withTables(REQUIRED_TPCH_TABLES)
                .withSessionModifier(session -> createSession(PARTITIONED_USER, PARTITIONED_USER))
                .withCreateUsers(TestStarburstOracleParallelConnectorSmokeTest::createUsers)
                .withProvisionTables(TestStarburstOracleParallelConnectorSmokeTest::partitionTables)
                .build();
    }

    private static void createUsers()
    {
        createStandardUsers();
        createUser(PARTITIONED_USER, OracleTestUsers.KERBERIZED_USER);
        executeInOracle(format("GRANT SELECT ON user_context to %s", PARTITIONED_USER));
    }

    private static void partitionTables()
    {
        executeInOracle(format("ALTER TABLE %s.nation MODIFY PARTITION BY RANGE (regionkey) INTERVAL (2) (PARTITION before_1 VALUES LESS THAN (1))", PARTITIONED_USER));
        executeInOracle(format("ALTER TABLE %s.region MODIFY PARTITION BY HASH(name) PARTITIONS 3", PARTITIONED_USER));
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
