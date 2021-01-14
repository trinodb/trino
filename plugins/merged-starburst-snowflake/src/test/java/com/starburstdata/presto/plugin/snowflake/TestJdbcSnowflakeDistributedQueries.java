/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import io.trino.testing.QueryRunner;

import java.util.Optional;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationEnabled;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.jdbcBuilder;

public class TestJdbcSnowflakeDistributedQueries
        extends BaseSnowflakeDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return jdbcBuilder()
                .withAdditionalProperties(impersonationEnabled())
                .build();
    }

    @Override
    protected boolean supportsCommentOnTable()
    {
        return false;
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        // TODO https://starburstdata.atlassian.net/browse/PRESTO-3389
        // Snowflake's JDBC client has a bug which truncates large double value, adjust the test to use small enough values
        if (dataMappingTestSetup.getTrinoTypeName().equals("double")) {
            return Optional.of(new DataMappingTestSetup("double", "DOUBLE '123456789012.123'", "DOUBLE '999999999999.999'"));
        }
        return super.filterDataMappingSmokeTestData(dataMappingTestSetup);
    }
}
