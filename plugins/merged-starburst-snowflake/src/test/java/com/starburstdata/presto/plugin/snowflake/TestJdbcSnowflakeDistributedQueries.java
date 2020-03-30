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

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationEnabled;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.jdbcBuilder;

public class TestJdbcSnowflakeDistributedQueries
        extends BaseSnowflakeDistributedQueries
{
    TestJdbcSnowflakeDistributedQueries()
    {
        super(() -> jdbcBuilder()
                .withAdditionalProperties(impersonationEnabled())
                .build());
    }
}
