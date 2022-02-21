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
import com.google.common.io.Resources;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import java.util.Map;

import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.collect.Streams.stream;
import static com.starburstdata.trino.plugin.stargate.StargateAuthenticationType.PASSWORD_PASS_THROUGH;
import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStargatePlugin
{
    private static final String AUTH_TO_LOCAL_FILE = Resources.getResource("test-user-impersonation.auth-to-local.json").getPath();

    @Test
    public void testLicenseRequired()
    {
        Plugin plugin = new StargatePlugin();

        for (ConnectorFactory factory : plugin.getConnectorFactories()) {
            assertThatThrownBy(() -> factory.create("test", ImmutableMap.of("connection-url", "jdbc:trino://localhost:8080/test", "connection-user", "presto"), new TestingConnectorContext()))
                    .isInstanceOf(RuntimeException.class)
                    .hasToString("com.starburstdata.presto.license.StarburstLicenseException: Valid license required to use the feature: stargate");
        }
    }

    @Test
    public void testCreateConnector()
    {
        createTestingPlugin(ImmutableMap.of("connection-url", "jdbc:trino://localhost:8080/test", "connection-user", "presto"));
    }

    @Test
    public void testCreateLegacyConnector()
    {
        createTestingPlugin("starburst-remote", ImmutableMap.of("connection-url", "jdbc:trino://localhost:8080/test", "connection-user", "presto"));
    }

    @Test
    public void testLegacyConnectorAllMethodsOverridden()
    {
        assertAllMethodsOverridden(ConnectorFactory.class, StargatePlugin.LegacyConnectorFactory.class);
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
        assertThatThrownBy(() -> createTestingPlugin(Map.of("connection-url", "jdbc:trino://localhost:8080/test", "stargate.authentication.type", "PASSWORD")))
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
                "stargate.impersonation.enabled", "true"));
    }

    @Test
    public void testPasswordPathThroughWithUserImpersonation()
    {
        String authToLocalFilePath = Resources.getResource("test-user-impersonation.auth-to-local.json").getPath();
        assertThatThrownBy(() ->
                createTestingPlugin(ImmutableMap.of(
                        "connection-url", "jdbc:trino://localhost:8080/test",
                        "stargate.impersonation.enabled", "true",
                        "auth-to-local.config-file", authToLocalFilePath,
                        "stargate.authentication.type", "PASSWORD_PASS_THROUGH")))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("property 'auth-to-local.config-file' was not used");
    }

    @Test
    public void testImpersonationNotAllowedWithPasswordPassThrough()
    {
        assertThatThrownBy(() -> createTestingPlugin(
                ImmutableMap.<String, String>builder()
                        .put("connection-url", "jdbc:trino://localhost:8080/test")
                        .put("stargate.impersonation.enabled", "true")
                        .put("stargate.authentication.type", PASSWORD_PASS_THROUGH.name())
                        .buildOrThrow()))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("Impersonation is not allowed when using credentials pass-through");
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
                "stargate.authentication.type", "PASSWORD",
                "ssl.enabled", "true",
                "ssl.truststore.path", "/dev/null"));

        createTestingPlugin(ImmutableMap.of(
                "connection-url", "jdbc:trino://localhost:8080/test",
                "connection-user", "presto",
                "stargate.authentication.type", "PASSWORD",
                "ssl.enabled", "false"));
    }

    @Test
    public void testKerberosValidations()
    {
        Map<String, String> kerberosProperties = ImmutableMap.<String, String>builder()
                .put("connection-url", "jdbc:trino://localhost:8080/hive")
                .put("connection-user", "user")
                .put("stargate.authentication.type", "KERBEROS")
                .put("kerberos.config", "/dev/null")
                .put("kerberos.client.keytab", "/dev/null")
                .put("kerberos.client.principal", "client@kerberos.com")
                .put("kerberos.remote.service-name", "remote-service")
                .buildOrThrow();

        assertThatThrownBy(() -> createTestingPlugin(kerberosProperties))
                .hasMessageContaining("SSL must be enabled when using Kerberos authentication");

        Map<String, String> withSsl = ImmutableMap.<String, String>builder()
                .putAll(kerberosProperties)
                .put("ssl.enabled", "true")
                .buildOrThrow();

        createTestingPlugin(withSsl);

        testPropertyNotUsed(withSsl, "connection-password", "super-secret", "connection-password should not be set when using Kerberos authentication");
    }

    @Test
    public void testKerberosUserImpersonationValidations()
    {
        Map<String, String> kerberosProperties = ImmutableMap.<String, String>builder()
                .put("connection-url", "jdbc:trino://localhost:8080/hive")
                .put("stargate.authentication.type", "KERBEROS")
                .put("kerberos.config", "/dev/null")
                .put("kerberos.client.keytab", "/dev/null")
                .put("kerberos.client.principal", "client@kerberos.com")
                .put("kerberos.remote.service-name", "remote-service")
                .put("ssl.enabled", "true")
                .put("stargate.impersonation.enabled", "true")
                .put("auth-to-local.config-file", AUTH_TO_LOCAL_FILE)
                .buildOrThrow();

        createTestingPlugin(kerberosProperties);

        testPropertyNotUsed(kerberosProperties, "connection-user", "a-user", "Configuration property 'connection-user' was not used");
        testPropertyNotUsed(kerberosProperties, "connection-password", "super-secret", "Configuration property 'connection-password' was not used");
    }

    private static void testPropertyNotUsed(Map<String, String> baseProperties, String key, String value, String errorMessage)
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .putAll(baseProperties)
                .put(key, value)
                .buildOrThrow();

        assertThatThrownBy(() -> createTestingPlugin(properties))
                .hasMessageContaining(errorMessage);
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

        factory.create("test", properties, new TestingConnectorContext());
    }
}
