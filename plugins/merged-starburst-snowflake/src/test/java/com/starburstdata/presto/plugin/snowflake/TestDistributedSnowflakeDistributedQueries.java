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

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationEnabled;

public class TestDistributedSnowflakeDistributedQueries
        extends BaseSnowflakeDistributedQueries
{
    TestDistributedSnowflakeDistributedQueries()
    {
        super(() -> distributedBuilder()
                .withAdditionalProperties(impersonationEnabled())
                .build());
    }
}
