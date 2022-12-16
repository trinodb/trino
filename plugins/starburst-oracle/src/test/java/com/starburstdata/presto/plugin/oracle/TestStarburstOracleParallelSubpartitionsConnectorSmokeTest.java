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
import static com.starburstdata.presto.plugin.oracle.OracleTestUsers.KERBERIZED_USER;
import static com.starburstdata.presto.plugin.oracle.OracleTestUsers.createStandardUsers;
import static com.starburstdata.presto.plugin.oracle.OracleTestUsers.createUser;
import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.executeInOracle;
import static java.lang.String.format;

public class TestStarburstOracleParallelSubpartitionsConnectorSmokeTest
        extends BaseOracleConnectorSmokeTest
{
    private static final String SUBPARTITIONED_USER = "subpartitioned";

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
                .withSessionModifier(session -> createSession(SUBPARTITIONED_USER, SUBPARTITIONED_USER))
                .withCreateUsers(TestStarburstOracleParallelSubpartitionsConnectorSmokeTest::createUsers)
                .withProvisionTables(TestStarburstOracleParallelSubpartitionsConnectorSmokeTest::partitionTables)
                .build();
    }

    private static void createUsers()
    {
        createStandardUsers();
        createUser(SUBPARTITIONED_USER, KERBERIZED_USER);
        executeInOracle(format("GRANT SELECT ON user_context to %s", SUBPARTITIONED_USER));
    }

    private static void partitionTables()
    {
        executeInOracle(format("""
                ALTER TABLE %s.nation MODIFY PARTITION BY RANGE (nationkey) INTERVAL (5)
                SUBPARTITION BY LIST (regionkey)
                SUBPARTITION TEMPLATE (
                    SUBPARTITION africa VALUES (0),
                    SUBPARTITION america VALUES (1),
                    SUBPARTITION asia VALUES (2),
                    SUBPARTITION europe VALUES (3),
                    SUBPARTITION middle_east VALUES (4)
                ) (PARTITION before_4 VALUES LESS THAN (4))""", SUBPARTITIONED_USER));

        executeInOracle(format("ALTER TABLE %s.region MODIFY PARTITION BY HASH(name) PARTITIONS 3", SUBPARTITIONED_USER));
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_LIMIT_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN:
                // Full pushdown is disabled for parallel connector for correctness
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }
}
