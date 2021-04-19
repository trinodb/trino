/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.salesforce;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.starburstdata.presto.plugin.salesforce.SalesforceConfig.SALESFORCE_CONNECTOR_KEY_VALUE;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSalesforceConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SalesforceConfig.class)
                .setKey(null)
                .setUser(null)
                .setPassword(null)
                .setSecurityToken(null)
                .setSandboxEnabled(false)
                .setDriverLoggingEnabled(false)
                .setDriverLoggingLocation(System.getProperty("java.io.tmpdir") + "/salesforce.log")
                .setDriverLoggingVerbosity(3)
                .setExtraJdbcProperties(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("salesforce.key", SALESFORCE_CONNECTOR_KEY_VALUE)
                .put("salesforce.user", "user")
                .put("salesforce.password", "password")
                .put("salesforce.security-token", "foobar")
                .put("salesforce.enable-sandbox", "true")
                .put("salesforce.driver-logging.enabled", "true")
                .put("salesforce.driver-logging.location", "/tmp/foo")
                .put("salesforce.driver-logging.verbosity", "5")
                .put("salesforce.extra-jdbc-properties", "foo=bar;")
                .build();

        SalesforceConfig expected = new SalesforceConfig()
                .setKey(SALESFORCE_CONNECTOR_KEY_VALUE)
                .setUser("user")
                .setPassword("password")
                .setSecurityToken("foobar")
                .setSandboxEnabled(true)
                .setDriverLoggingEnabled(true)
                .setDriverLoggingLocation("/tmp/foo")
                .setDriverLoggingVerbosity(5)
                .setExtraJdbcProperties("foo=bar;");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testNullKey()
    {
        assertThatThrownBy(() -> new SalesforceConfig().validate())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Users must specify a correct value for 'salesforce.key' in the connector config to use the Salesforce connector. Contact Starburst Support for details.");
    }

    @Test
    public void testIncorrectKey()
    {
        assertThatThrownBy(() -> new SalesforceConfig().setKey("foobar").validate())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Users must specify a correct value for 'salesforce.key' in the connector config to use the Salesforce connector. Contact Starburst Support for details.");
    }
}
