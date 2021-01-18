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
import com.google.common.io.Resources;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
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
                .hasToString("com.starburstdata.presto.license.PrestoLicenseException: Valid license required to use the feature: starburst-connector");
    }

    @Test
    public void testCreateConnector()
    {
        createTestingPlugin(ImmutableMap.of("connection-url", "jdbc:presto://localhost:8080/test", "connection-user", "presto"));
    }

    @Test
    public void testValidateConnectionUrl()
    {
        // connection-url not set
        assertThatThrownBy(() -> createTestingPlugin(Map.of("connection-user", "presto")))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Invalid configuration property connection-url: may not be null");

        // connection-url is empty
        assertThatThrownBy(() -> createTestingPlugin(Map.of("connection-url", "", "connection-user", "presto")))
                .isInstanceOf(ApplicationConfigurationException.class)
                // We need only one validation error, but we currently have more than one.
                .hasMessageContaining("Invalid configuration property connection-url: must match the following regular expression:")
                .hasMessageContaining("Invalid configuration property with prefix '': Invalid Starburst JDBC URL");

        // connection-url is bogus
        assertThatThrownBy(() -> createTestingPlugin(Map.of("connection-url", "test", "connection-user", "presto")))
                .isInstanceOf(ApplicationConfigurationException.class)
                // We need only one validation error, but we currently have more than one.
                .hasMessageContaining("Invalid configuration property connection-url: must match the following regular expression:")
                .hasMessageContaining("Invalid configuration property with prefix '': Invalid Starburst JDBC URL");

        // catalog not set
        assertThatThrownBy(() -> createTestingPlugin(Map.of("connection-url", "jdbc:presto://localhost:8080/", "connection-user", "presto")))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Invalid Starburst JDBC URL: catalog is not provided");

        // schema is set
        assertThatThrownBy(() -> createTestingPlugin(Map.of("connection-url", "jdbc:presto://localhost:8080/test/some_schema", "connection-user", "presto")))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Invalid Starburst JDBC URL: schema must not be provided");
    }

    @Test
    public void testValidateConnectionUser()
    {
        // PASSWORD used per default
        assertThatThrownBy(() -> createTestingPlugin(Map.of("connection-url", "jdbc:presto://localhost:8080/test")))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Connection user is not configured");

        // PASSWORD authentication explicitly enabled
        assertThatThrownBy(() -> createTestingPlugin(Map.of("connection-url", "jdbc:presto://localhost:8080/test", "starburst.authentication.type", "PASSWORD")))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Connection user is not configured");
    }

    @Test
    public void testAuthToLocalVerification()
    {
        String authToLocalFilePath = Resources.getResource("test-user-impersonation.auth-to-local.json").getPath();

        assertThatThrownBy(() ->
                createTestingPlugin(ImmutableMap.of(
                        "connection-url", "jdbc:presto://localhost:8080/test",
                        "connection-user", "presto",
                        "auth-to-local.config-file", authToLocalFilePath)))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("property 'auth-to-local.config-file' was not used");

        createTestingPlugin(ImmutableMap.of(
                "connection-url", "jdbc:presto://localhost:8080/test",
                "connection-user", "presto",
                "auth-to-local.config-file", authToLocalFilePath,
                "starburst.impersonation.enabled", "true"));
    }

    @Test
    public void testPasswordPathThroughWithUserImpersonation()
    {
        String authToLocalFilePath = Resources.getResource("test-user-impersonation.auth-to-local.json").getPath();
        assertThatThrownBy(() ->
                createTestingPlugin(ImmutableMap.of(
                        "connection-url", "jdbc:presto://localhost:8080/test",
                        "starburst.impersonation.enabled", "true",
                        "auth-to-local.config-file", authToLocalFilePath,
                        "starburst.authentication.type", "PASSWORD_PASS_THROUGH")))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("property 'auth-to-local.config-file' was not used");
    }

    public static void createTestingPlugin(Map<String, String> properties)
    {
        Plugin plugin = new TestingPrestoConnectorPlugin(false);
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", properties, new TestingConnectorContext());
    }
}
