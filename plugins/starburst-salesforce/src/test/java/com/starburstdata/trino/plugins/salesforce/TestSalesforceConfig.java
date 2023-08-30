/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.salesforce;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestSalesforceConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SalesforceConfig.class)
                .setSandboxEnabled(false)
                .setDriverLoggingEnabled(false)
                .setDriverLoggingLocation(System.getProperty("java.io.tmpdir") + "/salesforce.log")
                .setDriverLoggingVerbosity(3)
                .setExtraJdbcProperties(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("salesforce.enable-sandbox", "true")
                .put("salesforce.driver-logging.enabled", "true")
                .put("salesforce.driver-logging.location", "/tmp/foo")
                .put("salesforce.driver-logging.verbosity", "5")
                .put("salesforce.extra-jdbc-properties", "foo=bar;")
                .buildOrThrow();

        SalesforceConfig expected = new SalesforceConfig()
                .setSandboxEnabled(true)
                .setDriverLoggingEnabled(true)
                .setDriverLoggingLocation("/tmp/foo")
                .setDriverLoggingVerbosity(5)
                .setExtraJdbcProperties("foo=bar;");

        assertFullMapping(properties, expected);
    }
}
