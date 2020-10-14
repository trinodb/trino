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
import io.prestosql.testing.QueryRunner;
import io.prestosql.testng.services.Flaky;

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
    @Flaky(issue = "https://github.com/prestosql/presto/issues/5172", match = "java.lang.AssertionError: .*expected:<2> to be greater than <2>")
    public void testDynamicFiltering()
    {
        super.testDynamicFiltering();
    }

    @Override
    @Flaky(issue = "https://github.com/prestosql/presto/issues/5172", match = "java.lang.AssertionError: expected \\[3] but found \\[\\d]")
    public void testDynamicFilteringWithLimit()
    {
        super.testDynamicFilteringWithLimit();
    }

    @Override
    protected String getDynamicFilteringJmxTableName()
    {
        // SF connector uses ConnectorObjectNameGeneratorModule which overrides default name generation
        return "jmx.current.\"com.starburstdata.presto.plugin.jdbc.dynamicfiltering:catalog=snowflake,name=snowflake,type=dynamicfilteringstats\"";
    }

    @Override
    protected boolean isAssertNumberOfSplits()
    {
        return true;
    }
}
