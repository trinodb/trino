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

import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.AbstractDynamicFilteringTest;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;

public class TestSnowflakeDynamicFiltering
        extends AbstractDynamicFilteringTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return distributedBuilder()
                .withAdditionalProperties(impersonationDisabled())
                .build();
    }

    @Override
    // TODO: investigate to fix TNT-18
    @Test(enabled = false)
    public void testDynamicFilteringWithAggregationAggregateColumn()
    {
        super.testDynamicFilteringWithAggregationAggregateColumn();
    }

    @Override
    // TODO: investigate to fix TNT-18
    @Test(enabled = false)
    public void testDynamicFiltering()
    {
        super.testDynamicFiltering();
    }

    @Override
    protected String getDynamicFilteringJmxTableName()
    {
        // SF connector uses ConnectorObjectNameGeneratorModule which overrides default name generation
        return "jmx.current.\"com.starburstdata.presto.plugin.jdbc.dynamicfiltering:catalog=snowflake,name=snowflake,type=dynamicfilteringstats\"";
    }

    @Override
    protected boolean supportsSplitDynamicFiltering()
    {
        return true;
    }

    @Override
    protected boolean isJoinPushdownEnabledByDefault()
    {
        return false;
    }
}
