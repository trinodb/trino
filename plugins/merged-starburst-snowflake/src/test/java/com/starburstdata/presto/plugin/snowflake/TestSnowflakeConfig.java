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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeImpersonationType.NONE;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeImpersonationType.ROLE;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestSnowflakeConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SnowflakeConfig.class)
                .setImpersonationType(NONE)
                .setWarehouse(null)
                .setDatabase(null)
                .setRole(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("snowflake.impersonation-type", "ROLE")
                .put("snowflake.warehouse", "warehouse")
                .put("snowflake.database", "database")
                .put("snowflake.role", "role")
                .build();

        SnowflakeConfig expected = new SnowflakeConfig()
                .setImpersonationType(ROLE)
                .setWarehouse("warehouse")
                .setDatabase("database")
                .setRole("role");

        assertFullMapping(properties, expected);
    }
}
