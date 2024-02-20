/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSnowflakePlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new TestingSnowflakePlugin();
        List<ConnectorFactory> connectorFactories = ImmutableList.copyOf(plugin.getConnectorFactories());
        assertThat(connectorFactories.size()).isEqualTo(2);

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

        for (ConnectorFactory factory : connectorFactories) {
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
        }
    }
}
