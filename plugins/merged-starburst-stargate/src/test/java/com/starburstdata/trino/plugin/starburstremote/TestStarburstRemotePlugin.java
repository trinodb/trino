/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.starburstremote;

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

public class TestStarburstRemotePlugin
{
    private static final String AUTH_TO_LOCAL_FILE = Resources.getResource("test-user-impersonation.auth-to-local.json").getPath();

    @Test
    public void testLicenseRequired()
    {
        Plugin plugin = new StarburstRemotePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThatThrownBy(() -> factory.create("test", ImmutableMap.of("connection-url", "jdbc:trino://localhost:8080/test", "connection-user", "presto"), new TestingConnectorContext()))
                .isInstanceOf(RuntimeException.class)
                .hasToString("com.starburstdata.presto.license.PrestoLicenseException: Valid license required to use the feature: starburst-connector");
    }

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
                .hasMessageContaining("Invalid configuration property connection-url: may not be null");

        // connection-url is empty
        assertThatThrownBy(() -> createTestingPlugin(Map.of("connection-url", "", "connection-user", "presto")))
                .isInstanceOf(ApplicationConfigurationException.class)
                // We need only one validation error, but we currently have more than one.
                .hasMessageContaining("Invalid configuration property connection-url: must match the following regular expression:")
                .hasMessageContaining("Invalid configuration property with prefix '': Invalid Starburst JDBC URL, sample format: jdbc:trino://localhost:8080/catalog_name");

        // connection-url is bogus
        assertThatThrownBy(() -> createTestingPlugin(Map.of("connection-url", "test", "connection-user", "presto")))
                .isInstanceOf(ApplicationConfigurationException.class)
                // We need only one validation error, but we currently have more than one.
                .hasMessageContaining("Invalid configuration property connection-url: must match the following regular expression:")
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

        // PASSWORD authentication explicitly enabled
        assertThatThrownBy(() -> createTestingPlugin(Map.of("connection-url", "jdbc:trino://localhost:8080/test", "starburst.authentication.type", "PASSWORD")))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Connection user is not configured");
    }

    @Test
    public void testAuthToLocalVerification()
    {
        assertThatThrownBy(() ->
                createTestingPlugin(ImmutableMap.of(
                        "connection-url", "jdbc:trino://localhost:8080/test",
                        "connection-user", "presto",
                        "auth-to-local.config-file", AUTH_TO_LOCAL_FILE)))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("property 'auth-to-local.config-file' was not used");

        createTestingPlugin(ImmutableMap.of(
                "connection-url", "jdbc:trino://localhost:8080/test",
                "connection-user", "presto",
                "auth-to-local.config-file", AUTH_TO_LOCAL_FILE,
                "starburst.impersonation.enabled", "true"));
    }

    @Test
    public void testPasswordPathThroughWithUserImpersonation()
    {
        String authToLocalFilePath = Resources.getResource("test-user-impersonation.auth-to-local.json").getPath();
        assertThatThrownBy(() ->
                createTestingPlugin(ImmutableMap.of(
                        "connection-url", "jdbc:trino://localhost:8080/test",
                        "starburst.impersonation.enabled", "true",
                        "auth-to-local.config-file", authToLocalFilePath,
                        "starburst.authentication.type", "PASSWORD_PASS_THROUGH")))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("property 'auth-to-local.config-file' was not used");
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

    @Test
    public void testPasswordAuthMayUseSsl()
    {
        createTestingPlugin(ImmutableMap.of(
                "connection-url", "jdbc:trino://localhost:8080/test",
                "connection-user", "presto",
                "starburst.authentication.type", "PASSWORD",
                "ssl.enabled", "true",
                "ssl.truststore.path", "/dev/null"));

        createTestingPlugin(ImmutableMap.of(
                "connection-url", "jdbc:trino://localhost:8080/test",
                "connection-user", "presto",
                "starburst.authentication.type", "PASSWORD",
                "ssl.enabled", "false"));
    }

    @Test
    public void testKerberosValidations()
    {
        Map<String, String> kerberosProperties = new ImmutableMap.Builder<String, String>()
                .put("connection-url", "jdbc:trino://localhost:8080/hive")
                .put("connection-user", "user")
                .put("starburst.authentication.type", "KERBEROS")
                .put("kerberos.config", "/dev/null")
                .put("kerberos.client.keytab", "/dev/null")
                .put("kerberos.client.principal", "client@kerberos.com")
                .put("kerberos.remote.service-name", "remote-service")
                .build();

        assertThatThrownBy(() -> createTestingPlugin(kerberosProperties))
                .hasMessageContaining("SSL must be enabled when using Kerberos authentication");

        Map<String, String> withSsl = new ImmutableMap.Builder<String, String>()
                .putAll(kerberosProperties)
                .put("ssl.enabled", "true")
                .build();

        createTestingPlugin(withSsl);

        testPropertyNotUsed(withSsl, "connection-password", "super-secret", "connection-password should not be set when using Kerberos authentication");
    }

    @Test
    public void testKerberosUserImpersonationValidations()
    {
        Map<String, String> kerberosProperties = new ImmutableMap.Builder<String, String>()
                .put("connection-url", "jdbc:trino://localhost:8080/hive")
                .put("starburst.authentication.type", "KERBEROS")
                .put("kerberos.config", "/dev/null")
                .put("kerberos.client.keytab", "/dev/null")
                .put("kerberos.client.principal", "client@kerberos.com")
                .put("kerberos.remote.service-name", "remote-service")
                .put("ssl.enabled", "true")
                .put("starburst.impersonation.enabled", "true")
                .put("auth-to-local.config-file", AUTH_TO_LOCAL_FILE)
                .build();

        createTestingPlugin(kerberosProperties);

        testPropertyNotUsed(kerberosProperties, "connection-user", "a-user", "Configuration property 'connection-user' was not used");
        testPropertyNotUsed(kerberosProperties, "connection-password", "super-secret", "Configuration property 'connection-password' was not used");
    }

    private static void testPropertyNotUsed(Map<String, String> baseProperties, String key, String value, String errorMessage)
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .putAll(baseProperties)
                .put(key, value)
                .build();

        assertThatThrownBy(() -> createTestingPlugin(properties))
                .hasMessageContaining(errorMessage);
    }

    public static void createTestingPlugin(Map<String, String> properties)
    {
        Plugin plugin = new TestingStarburstRemotePlugin(false);
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", properties, new TestingConnectorContext());
    }
}
