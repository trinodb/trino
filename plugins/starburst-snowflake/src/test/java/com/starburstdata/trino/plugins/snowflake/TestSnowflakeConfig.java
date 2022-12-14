/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestSnowflakeConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SnowflakeConfig.class)
                .setWarehouse(null)
                .setDatabase(null)
                .setRole(null)
                .setDatabasePrefixForSchemaEnabled(false)
                .setExperimentalPushdownEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("snowflake.warehouse", "warehouse")
                .put("snowflake.database", "database")
                .put("snowflake.role", "role")
                .put("snowflake.database-prefix-for-schema.enabled", "true")
                .put("snowflake.experimental-pushdown.enabled", "true")
                .buildOrThrow();

        SnowflakeConfig expected = new SnowflakeConfig()
                .setWarehouse("warehouse")
                .setDatabase("database")
                .setRole("role")
                .setDatabasePrefixForSchemaEnabled(true)
                .setExperimentalPushdownEnabled(true);

        assertFullMapping(properties, expected);
    }
}
