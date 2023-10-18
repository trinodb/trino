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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestSnowflakePlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new TestingSnowflakePlugin();
        List<ConnectorFactory> connectorFactories = ImmutableList.copyOf(plugin.getConnectorFactories());
        assertEquals(connectorFactories.size(), 3);

        connectorFactories.get(0).create("test",
                ImmutableMap.of(
                        "connection-url", "jdbc:snowflake:test",
                        "snowflake.role", "test",
                        "snowflake.database", "test",
                        "snowflake.warehouse", "test"),
                new TestingConnectorContext())
                .shutdown();

        connectorFactories.get(1).create(
                "test",
                ImmutableMap.of(
                        "connection-url", "jdbc:snowflake:test",
                        "snowflake.database", "test",
                        "snowflake.stage-schema", "test",
                        "snowflake.warehouse", "test"),
                new TestingConnectorContext())
                .shutdown();

        connectorFactories.get(2).create(
                "test",
                ImmutableMap.of(
                        "connection-url", "jdbc:snowflake:test",
                        "snowflake.role", "test",
                        "snowflake.database", "test",
                        "snowflake.warehouse", "test"),
                new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testCreateConnectorWithProxySettings()
    {
        Plugin plugin = new TestingSnowflakePlugin();
        List<ConnectorFactory> connectorFactories = ImmutableList.copyOf(plugin.getConnectorFactories());
        assertEquals(connectorFactories.size(), 3);

        int created = 0;
        for (ConnectorFactory factory : connectorFactories) {
            try {
                factory.create(
                                "test",
                                ImmutableMap.of(
                                        "connection-url", "jdbc:snowflake:test",
                                        "snowflake.database", "test",
                                        "snowflake.warehouse", "test",
                                        "snowflake.proxy.enabled", "true",
                                        "snowflake.proxy.host", "localhost",
                                        "snowflake.proxy.port", "9000"),
                                new TestingConnectorContext())
                        .shutdown();
                created++;
            }
            catch (Exception e) {
                assertThat(e).hasMessageContaining("Distributed connector does not support proxy settings");
            }
        }
        assertThat(created).isEqualTo(2); // parallel and jdbc should succeed while distributed will fail
    }
}
