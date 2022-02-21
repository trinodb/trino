/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.Resources.getResource;
import static com.starburstdata.presto.plugin.oracle.OracleAuthenticationType.KERBEROS_PASS_THROUGH;
import static com.starburstdata.presto.plugin.oracle.OracleAuthenticationType.PASSWORD_PASS_THROUGH;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStarburstOraclePlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new StarburstOraclePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", ImmutableMap.of(
                "connection-url", "jdbc:oracle:thin:@test",
                "connection-user", "test",
                "connection-password", "password"
        ), new TestingConnectorContext());
    }

    @Test
    public void testUserNotUsedWithKerberos()
    {
        Plugin plugin = new StarburstOraclePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("connection-url", "jdbc:oracle:thin:@test")
                        .put("oracle.authentication.type", "KERBEROS")
                        .put("kerberos.client.principal", "test@TESTING-KRB.STARBURSTDATA.COM")
                        .put("kerberos.client.keytab", getResource("krb/client/test.keytab").getPath())
                        .put("kerberos.config", getResource("krb/krb5.conf").getPath())
                        .put("connection-user", "WHAT?!")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Configuration property 'connection-user' was not used");
    }

    @Test
    public void testLicenseRequiredForImpersonation()
    {
        Plugin plugin = new StarburstOraclePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "connection-url", "jdbc:oracle:thin:@test",
                        "connection-user", "test",
                        "connection-password", "password",
                        "oracle.impersonation.enabled", "true"),
                new TestingConnectorContext()))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("StarburstLicenseException: Valid license required to use the feature: jdbc-impersonation");
    }

    @Test
    public void testLicenseProtectionOfKerberos()
    {
        Plugin plugin = new StarburstOraclePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("connection-url", "jdbc:oracle:thin:@test")
                        .put("oracle.authentication.type", "KERBEROS")
                        .put("kerberos.client.principal", "test@TESTING-KRB.STARBURSTDATA.COM")
                        .put("kerberos.client.keytab", getResource("krb/client/test.keytab").getPath())
                        .put("kerberos.config", getResource("krb/krb5.conf").getPath())
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("StarburstLicenseException: Valid license required to use the feature: jdbc-kerberos");
    }

    @Test
    public void testExplicitAggregationPushdownRequiresLicense()
    {
        Plugin plugin = new StarburstOraclePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("connection-url", "jdbc:oracle:thin:@test")
                        .put("connection-user", "test")
                        .put("connection-password", "password")
                        .put("aggregation-pushdown.enabled", "true")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("StarburstLicenseException: Valid license required to use the feature: oracle-extensions");
    }

    @Test
    public void testParallelismRequiresLicense()
    {
        Plugin plugin = new StarburstOraclePlugin();
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

        // partitions based parallelism requires license
        assertThatThrownBy(() ->
                factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("connection-url", "jdbc:oracle:thin:@test")
                                .put("connection-user", "test")
                                .put("connection-password", "password")
                                .put("oracle.parallelism-type", "partitions")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                        .shutdown())
                .hasMessageContaining("Valid license required to use the feature: oracle-extensions");
    }

    @Test
    public void testImpersonationNotAllowedWithPasswordPassThrough()
    {
        Plugin plugin = new TestingStarburstOraclePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("connection-url", "jdbc:oracle:test")
                        .put("connection-user", "test")
                        .put("connection-password", "password")
                        .put("oracle.impersonation.enabled", "true")
                        .put("oracle.authentication.type", PASSWORD_PASS_THROUGH.name())
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("Impersonation is not allowed when using credentials pass-through");
    }

    @Test
    public void testImpersonationNotAllowedWithPasswordPassThroughAndConnectionPooling()
    {
        Plugin plugin = new TestingStarburstOraclePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("connection-url", "jdbc:greenplum:test")
                        .put("oracle.impersonation.enabled", "true")
                        .put("oracle.authentication.type", PASSWORD_PASS_THROUGH.name())
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("Impersonation is not allowed when using credentials pass-through");
    }

    @Test
    public void testImpersonationNotAllowedWithKerberosPassThrough()
    {
        Plugin plugin = new TestingStarburstOraclePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("connection-url", "jdbc:greenplum:test")
                        .put("oracle.impersonation.enabled", "true")
                        .put("oracle.authentication.type", KERBEROS_PASS_THROUGH.name())
                        .put("http.authentication.krb5.config", ".")
                        .put("http-server.authentication.krb5.service-name", "starburst")
                        .put("internal-communication.shared-secret", "I can't tell, it is a secret")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("Impersonation is not allowed when using credentials pass-through");
    }
}
