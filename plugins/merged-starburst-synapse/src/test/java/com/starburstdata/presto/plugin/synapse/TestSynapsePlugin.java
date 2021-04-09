/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSynapsePlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new TestingSynapsePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", ImmutableMap.of("connection-url", "jdbc:sqlserver:test"), new TestingConnectorContext());
    }

    @Test
    public void testLicenseRequired()
    {
        Plugin plugin = new StarburstSynapsePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThatThrownBy(() -> factory.create("test", ImmutableMap.of("connection-url", "jdbc:sqlserver:test"), new TestingConnectorContext()))
                .isInstanceOf(RuntimeException.class)
                .hasToString("com.starburstdata.presto.license.PrestoLicenseException: Valid license required to use the feature: synapse");
    }

    @Test
    public void testLicenseRequiredForImpersonation()
    {
        Plugin plugin = new StarburstSynapsePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "connection-url", "jdbc:sqlserver:test",
                        "synapse.impersonation.enabled", "true"),
                new TestingConnectorContext()))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("PrestoLicenseException: Valid license required to use the feature: synapse");
    }
}
