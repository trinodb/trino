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
import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.QueryRunner;

import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createSession;
import static com.starburstdata.presto.plugin.oracle.OracleTestUsers.createStandardUsers;
import static com.starburstdata.presto.plugin.oracle.OracleTestUsers.createUser;
import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.executeInOracle;
import static io.prestosql.tpch.TpchTable.CUSTOMER;
import static io.prestosql.tpch.TpchTable.NATION;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static io.prestosql.tpch.TpchTable.REGION;
import static java.lang.String.format;

public class TestOracleParallelAggregationPushdown
        extends BaseStarburstOracleAggregationPushdownTest
{
    public static final String PARTITIONED_USER = "partitioned_agg_user";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withUnlockEnterpriseFeatures(true)
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(TestingStarburstOracleServer.connectionProperties())
                        .put("oracle.parallelism-type", "PARTITIONS")
                        .put("oracle.concurrent.max-splits-per-scan", "17")
                        .build())
                .withSessionModifier(session -> createSession(PARTITIONED_USER, PARTITIONED_USER))
                .withTables(ImmutableList.of(CUSTOMER, NATION, ORDERS, REGION))
                .withCreateUsers(TestOracleParallelAggregationPushdown::createUsers)
                .withProvisionTables(TestOracleParallelAggregationPushdown::partitionTables)
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
        executeInOracle(format("ALTER TABLE %s.orders MODIFY PARTITION BY RANGE (orderdate) INTERVAL (NUMTODSINTERVAL(1,'DAY')) (PARTITION before_1900 VALUES LESS THAN (TO_DATE('01-JAN-1900','dd-MON-yyyy')))", PARTITIONED_USER));
        executeInOracle(format("ALTER TABLE %s.nation MODIFY PARTITION BY HASH(name) PARTITIONS 3", PARTITIONED_USER));
    }
}
