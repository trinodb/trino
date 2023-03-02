/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamicfiltering;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestDynamicRowFilteringConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DynamicRowFilteringConfig.class)
                .setDynamicRowFilteringEnabled(true)
                .setDynamicRowFilterSelectivityThreshold(0.7)
                .setDynamicRowFilteringWaitTimeout(new Duration(0, SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("dynamic-row-filtering.enabled", "false")
                .put("dynamic-row-filtering.selectivity-threshold", "0.9")
                .put("dynamic-row-filtering.wait-timeout", "10s")
                .buildOrThrow();

        DynamicRowFilteringConfig expected = new DynamicRowFilteringConfig()
                .setDynamicRowFilteringEnabled(false)
                .setDynamicRowFilterSelectivityThreshold(0.9)
                .setDynamicRowFilteringWaitTimeout(new Duration(10, SECONDS));

        assertFullMapping(properties, expected);
    }
}
