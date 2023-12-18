/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.faulttolerant;

import com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner;
import io.trino.operator.RetryPolicy;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.parallelBuilder;

public class TestParallelSnowflakeTaskFailureRecoveryTest
        extends BaseSnowflakeFailureRecoveryTest
{
    public TestParallelSnowflakeTaskFailureRecoveryTest()
    {
        super(RetryPolicy.TASK);
    }

    @Override
    protected SnowflakeQueryRunner.Builder getBuilder()
    {
        return parallelBuilder();
    }
}
