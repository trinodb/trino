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
import io.prestosql.plugin.oracle.TestOracleDistributedQueries;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.JdbcSqlExecutor;
import io.prestosql.testing.sql.SqlExecutor;
import org.testng.SkipException;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStarburstOracleDistributedQueries
        extends TestOracleDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(TestingStarburstOracleServer.connectionProperties())
                        .put("oracle.connection-pool.enabled", "true")
                        .put("oracle.connection-pool.max-size", "10")
                        .put("allow-drop-table", "true")
                        .build())
                .build();
    }

    @Override
    public void testColumnName(String columnName)
    {
        if (columnName.equals("a\"quote")) {
            // Quote is not supported within column name
            assertThatThrownBy(() -> super.testColumnName(columnName))
                    .hasMessageMatching("Oracle does not support escaping '\"' in identifiers");
            throw new SkipException("works incorrectly, column name is trimmed");
        }

        super.testColumnName(columnName);
    }

    @Override
    protected SqlExecutor createJdbcSqlExecutor()
    {
        Properties properties = new Properties();
        properties.setProperty("user", OracleTestUsers.USER);
        properties.setProperty("password", OracleTestUsers.PASSWORD);
        return new JdbcSqlExecutor(TestingStarburstOracleServer.getJdbcUrl(), properties);
    }
}
