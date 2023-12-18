/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.parallelBuilder;

public class TestParallelSnowflakeWithFixedRole
        extends TestJdbcSnowflakeWithFixedRole
{
    @Override
    protected SnowflakeQueryRunner.Builder createBuilder()
    {
        return parallelBuilder();
    }
}
