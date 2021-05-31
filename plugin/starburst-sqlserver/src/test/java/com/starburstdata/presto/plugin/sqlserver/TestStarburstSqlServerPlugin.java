/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerConfig.SQLSERVER_OVERRIDE_CATALOG_ENABLED;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerConfig.SQLSERVER_OVERRIDE_CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStarburstSqlServerPlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new StarburstSqlServerPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", ImmutableMap.of("connection-url", "jdbc:sqlserver:test"), new TestingConnectorContext());
    }

    @Test
    public void testLicenseRequiredForImpersonation()
    {
        Plugin plugin = new StarburstSqlServerPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "connection-url", "jdbc:sqlserver:test",
                        "sqlserver.impersonation.enabled", "true"),
                new TestingConnectorContext()))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("StarburstLicenseException: Valid license required to use the feature: jdbc-impersonation");
    }

    @Test
    public void testConfigValidation()
    {
        Plugin plugin = new StarburstSqlServerPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "connection-url", "jdbc:sqlserver:test",
                        SQLSERVER_OVERRIDE_CATALOG_NAME, "irrelevant"),
                new TestingConnectorContext()))
                .hasMessageContaining(SQLSERVER_OVERRIDE_CATALOG_ENABLED + " needs to be set in order to use " + SQLSERVER_OVERRIDE_CATALOG_NAME + " parameter");
    }
}
