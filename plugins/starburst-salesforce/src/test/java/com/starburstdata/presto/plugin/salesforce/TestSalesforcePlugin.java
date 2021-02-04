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
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSalesforcePlugin
{
    @Test
    public void testLicenseRequired()
    {
        Plugin plugin = new SalesforcePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThatThrownBy(() -> factory.create("test", ImmutableMap.of(), new TestingConnectorContext()))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("PrestoLicenseException: Valid license required to use the feature: salesforce");
    }

    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new TestingSalesforcePlugin(false);
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("salesforce.user", requireNonNull(System.getProperty("salesforce.test.user1.username"), "salesforce.test.user1.username is not set"))
                        .put("salesforce.password", requireNonNull(System.getProperty("salesforce.test.user1.password"), "salesforce.test.user1.password is not set"))
                        .put("salesforce.security-token", requireNonNull(System.getProperty("salesforce.test.user1.security-token"), "salesforce.test.user1.security-token is not set"))
                        .put("salesforce.enable-sandbox", "true")
                        .build(),
                new TestingConnectorContext());
    }
}
