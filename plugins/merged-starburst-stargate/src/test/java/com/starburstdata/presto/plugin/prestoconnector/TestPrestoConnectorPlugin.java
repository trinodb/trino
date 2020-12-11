/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import com.google.common.collect.ImmutableMap;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPrestoConnectorPlugin
{
    @Test
    public void testLicenseRequired()
    {
        Plugin plugin = new PrestoConnectorPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThatThrownBy(() -> factory.create("test", ImmutableMap.of("connection-url", "jdbc:presto://localhost:8080/test", "connection-user", "presto"), new TestingConnectorContext()))

                .isInstanceOf(RuntimeException.class)
                .hasToString("com.starburstdata.presto.license.PrestoLicenseException: Valid license required to use the feature: presto-connector");
    }

    @Test
    public void testCreateConnector()
    {
        createTestingPlugin(ImmutableMap.of("connection-url", "jdbc:presto://localhost:8080/test", "connection-user", "presto"));

        assertThatThrownBy(() -> createTestingPlugin(ImmutableMap.of("connection-url", "jdbc:presto://localhost:8080/", "connection-user", "presto")))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Invalid Presto JDBC URL: catalog and/or schema is not provided");

        assertThatThrownBy(() -> createTestingPlugin(ImmutableMap.of("connection-url", "test", "connection-user", "presto")))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Invalid Presto JDBC URL");

        assertThatThrownBy(() -> createTestingPlugin(ImmutableMap.of("connection-url", "jdbc:presto://localhost:8080/test", "presto.authentication.type", "PASSWORD")))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Connection user is not configured");
    }

    public static void createTestingPlugin(Map<String, String> properties)
    {
        Plugin plugin = new TestingPrestoConnectorPlugin(false);
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", properties, new TestingConnectorContext());
    }
}
