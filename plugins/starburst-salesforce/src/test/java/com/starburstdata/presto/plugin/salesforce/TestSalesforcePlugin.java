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
                .hasStackTraceContaining("StarburstLicenseException: Valid license required to use the feature: salesforce");
    }

    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new TestingSalesforcePlugin(false);
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("salesforce.user", "user")
                        .put("salesforce.password", "password")
                        .put("salesforce.security-token", "token")
                        .put("salesforce.enable-sandbox", "true")
                        .build(),
                new TestingConnectorContext());
    }
}
