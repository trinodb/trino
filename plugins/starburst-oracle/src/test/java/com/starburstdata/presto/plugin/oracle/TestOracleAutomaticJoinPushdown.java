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

import com.starburstdata.presto.plugin.jdbc.joinpushdown.BaseAutomaticJoinPushdownTest;
import io.trino.testing.QueryRunner;

import java.sql.CallableStatement;
import java.sql.SQLException;

import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.executeInOracle;

public class TestOracleAutomaticJoinPushdown
        extends BaseAutomaticJoinPushdownTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withUnlockEnterpriseFeatures(true)
                .withConnectorProperties(TestingStarburstOracleServer.connectionProperties())
                .build();
    }

    @Override
    protected void gatherStats(String tableName)
    {
        executeInOracle(connection -> {
            try (CallableStatement statement = connection.prepareCall("{CALL DBMS_STATS.GATHER_TABLE_STATS(?, ?)}")) {
                statement.setString(1, OracleTestUsers.USER);
                statement.setString(2, tableName);
                statement.execute();
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
