/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.sqlserver;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.starburstdata.trino.plugin.sqlserver.StarburstSqlServerConfig.SQLSERVER_OVERRIDE_CATALOG_ENABLED;
import static com.starburstdata.trino.plugin.sqlserver.StarburstSqlServerConfig.SQLSERVER_OVERRIDE_CATALOG_NAME;
import static com.starburstdata.trino.plugin.sqlserver.StarburstSqlServerQueryRunner.NOOP_LICENSE_MANAGER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStarburstSqlServerPlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new StarburstSqlServerPlugin(NOOP_LICENSE_MANAGER);
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", ImmutableMap.of("connection-url", "jdbc:sqlserver:test"), new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testConfigValidation()
    {
        Plugin plugin = new StarburstSqlServerPlugin(NOOP_LICENSE_MANAGER);
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
