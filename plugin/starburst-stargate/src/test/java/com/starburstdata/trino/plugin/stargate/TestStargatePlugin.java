/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.google.common.collect.ImmutableMap;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.collect.Streams.stream;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStargatePlugin
{
    @Test
    public void testCreateConnector()
    {
        createTestingPlugin(ImmutableMap.of("connection-url", "jdbc:trino://localhost:8080/test", "connection-user", "presto"));
    }

    @Test
    public void testValidateConnectionUrl()
    {
        // connection-url not set
        assertThatThrownBy(() -> createTestingPlugin(Map.of("connection-user", "presto")))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Invalid configuration property connection-url: must not be null");

        // connection-url is empty
        assertThatThrownBy(() -> createTestingPlugin(Map.of("connection-url", "", "connection-user", "presto")))
                .isInstanceOf(ApplicationConfigurationException.class)
                // We need only one validation error, but we currently have more than one.
                .hasMessageContaining("Invalid configuration property connection-url: must match")
                .hasMessageContaining("Invalid configuration property with prefix '': Invalid Starburst JDBC URL, sample format: jdbc:trino://localhost:8080/catalog_name");

        // connection-url is bogus
        assertThatThrownBy(() -> createTestingPlugin(Map.of("connection-url", "test", "connection-user", "presto")))
                .isInstanceOf(ApplicationConfigurationException.class)
                // We need only one validation error, but we currently have more than one.
                .hasMessageContaining("Invalid configuration property connection-url: must match")
                .hasMessageContaining("Invalid configuration property with prefix '': Invalid Starburst JDBC URL, sample format: jdbc:trino://localhost:8080/catalog_name");

        // catalog not set
        assertThatThrownBy(() -> createTestingPlugin(Map.of("connection-url", "jdbc:trino://localhost:8080/", "connection-user", "presto")))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Invalid Starburst JDBC URL: catalog is not provided");

        // schema is set
        assertThatThrownBy(() -> createTestingPlugin(Map.of("connection-url", "jdbc:trino://localhost:8080/test/some_schema", "connection-user", "presto")))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Invalid Starburst JDBC URL: schema must not be provided");
    }

    @Test
    public void testValidateConnectionUser()
    {
        // PASSWORD used per default
        assertThatThrownBy(() -> createTestingPlugin(Map.of("connection-url", "jdbc:trino://localhost:8080/test")))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Connection user is not configured");
    }

    @Test
    public void testSslPropertiesRequireSslEnabled()
    {
        createTestingPlugin(ImmutableMap.of(
                "connection-url", "jdbc:trino://localhost:8080/test",
                "connection-user", "presto",
                "ssl.enabled", "true",
                "ssl.truststore.password", "password"));

        assertThatThrownBy(() ->
                createTestingPlugin(ImmutableMap.of(
                        "connection-url", "jdbc:trino://localhost:8080/test",
                        "connection-user", "presto",
                        "ssl.truststore.password", "password")))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Configuration property 'ssl.truststore.password' was not used");
    }

    public static void createTestingPlugin(Map<String, String> properties)
    {
        createTestingPlugin("stargate", properties);
    }

    public static void createTestingPlugin(String connectorName, Map<String, String> properties)
    {
        Plugin plugin = new TestingStargatePlugin(false);

        ConnectorFactory factory = stream(plugin.getConnectorFactories().iterator())
                .filter(connector -> connector.getName().equals(connectorName))
                .collect(toOptional())
                .orElseThrow();

        factory.create("test", properties, new TestingConnectorContext())
                .shutdown();
    }
}
