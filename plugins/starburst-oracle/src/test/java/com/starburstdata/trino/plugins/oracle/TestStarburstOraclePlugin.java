/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.oracle;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.starburstdata.trino.plugins.oracle.OracleQueryRunner.NOOP_LICENSE_MANAGER;

public class TestStarburstOraclePlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new StarburstOraclePlugin(NOOP_LICENSE_MANAGER);
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", ImmutableMap.of(
                "connection-url", "jdbc:oracle:thin:@test",
                "connection-user", "test",
                "connection-password", "password"
        ), new TestingConnectorContext());
    }

    @Test
    public void testLicenseProtectionOfAggregationPushdown()
    {
        Plugin plugin = new StarburstOraclePlugin(NOOP_LICENSE_MANAGER);
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        // aggregation pushdown works without license
        factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("connection-url", "jdbc:oracle:thin:@test")
                                .put("connection-user", "test")
                                .put("connection-password", "password")
                                .put("aggregation-pushdown.enabled", "true")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testLicenseProtectionOfParallelism()
    {
        Plugin plugin = new StarburstOraclePlugin(NOOP_LICENSE_MANAGER);
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        // default configuration (no paralellism) works without license
        factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("connection-url", "jdbc:oracle:thin:@test")
                        .put("connection-user", "test")
                        .put("connection-password", "password")
                        .buildOrThrow(),
                new TestingConnectorContext())
                .shutdown();

        // explicit no paralellism works without license
        factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("connection-url", "jdbc:oracle:thin:@test")
                        .put("connection-user", "test")
                        .put("connection-password", "password")
                        .put("oracle.parallelism-type", "no_parallelism")
                        .buildOrThrow(),
                new TestingConnectorContext())
                .shutdown();

        // partitions parallelism works without license
        factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("connection-url", "jdbc:oracle:thin:@test")
                                .put("connection-user", "test")
                                .put("connection-password", "password")
                                .put("oracle.parallelism-type", "partitions")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }
}
