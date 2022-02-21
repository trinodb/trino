/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.kerberos.TestingKdcServer;
import com.starburstdata.presto.testing.SystemPropertiesLock.PropertiesLock;
import com.starburstdata.presto.testing.TestingResourceLock.ResourceLock;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Path;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.starburstdata.presto.license.TestingLicenseManager.NOOP_LICENSE_MANAGER;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerConfig.SQLSERVER_OVERRIDE_CATALOG_ENABLED;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerConfig.SQLSERVER_OVERRIDE_CATALOG_NAME;
import static com.starburstdata.presto.testing.SystemPropertiesLock.lockSystemProperties;
import static io.airlift.testing.Closeables.closeAll;
import static java.nio.file.Files.createTempFile;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStarburstSqlServerPlugin
{
    private ResourceLock<PropertiesLock> propertiesLock;
    private TestingKdcServer kdcServer;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        propertiesLock = lockSystemProperties();
        kdcServer = new TestingKdcServer(propertiesLock);
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        closeAll(kdcServer, propertiesLock);
    }

    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new StarburstSqlServerPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", ImmutableMap.of("connection-url", "jdbc:sqlserver:test"), new TestingConnectorContext());
    }

    @Test
    public void testLicenseRequiredForImpersonation()
    {
        Plugin plugin = new StarburstSqlServerPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "connection-url", "jdbc:sqlserver:test",
                        "sqlserver.impersonation.enabled", "true"),
                new TestingConnectorContext()))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("StarburstLicenseException: Valid license required to use the feature: jdbc-impersonation");
    }

    @Test
    public void testConfigValidation()
    {
        Plugin plugin = new StarburstSqlServerPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "connection-url", "jdbc:sqlserver:test",
                        SQLSERVER_OVERRIDE_CATALOG_NAME, "irrelevant"),
                new TestingConnectorContext()))
                .hasMessageContaining(SQLSERVER_OVERRIDE_CATALOG_ENABLED + " needs to be set in order to use " + SQLSERVER_OVERRIDE_CATALOG_NAME + " parameter");
    }

    @Test
    public void testKerberosWithUserName()
            throws Exception
    {
        Plugin plugin = new StarburstSqlServerPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        Path keytab = createTempFile(null, null);
        Path config = createTempFile(null, null);

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("connection-url", "jdbc:sqlserver:test?user=admin")
                        .put("sqlserver.authentication.type", "KERBEROS")
                        .put("kerberos.client.principal", "principal")
                        .put("kerberos.client.keytab", keytab.toString())
                        .put("kerberos.config", config.toString())
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageMatching("(?s)Unable to create injector, see the following errors:.*" +
                        "Cannot specify 'user' parameter in JDBC URL when using Kerberos authentication.*");
    }

    @Test
    public void testKerberosPassThroughWithUserName()
    {
        Plugin plugin = new StarburstSqlServerPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("connection-url", "jdbc:sqlserver:test?user=admin")
                        .put("sqlserver.authentication.type", "KERBEROS_PASS_THROUGH")
                        .put("http.authentication.krb5.config", kdcServer.getKrb5ConfFile().getPath())
                        .put("internal-communication.shared-secret", "secret")
                        .put("http-server.authentication.krb5.service-name", "client")
                        .put("http-server.authentication.krb5.keytab", kdcServer.getServerKeytab().getPath())
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageMatching("(?s)Unable to create injector, see the following errors:.*" +
                        "Cannot specify 'user' parameter in JDBC URL when using Kerberos authentication.*");
    }

    @Test
    public void testKerberosPassThroughWithImpersonation()
    {
        StarburstSqlServerPlugin plugin = new StarburstSqlServerPlugin();
        ConnectorFactory factory = plugin.getConnectorFactory(NOOP_LICENSE_MANAGER);

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("connection-url", "jdbc:sqlserver:test")
                        .put("sqlserver.authentication.type", "KERBEROS_PASS_THROUGH")
                        .put("http.authentication.krb5.config", kdcServer.getKrb5ConfFile().getPath())
                        .put("internal-communication.shared-secret", "secret")
                        .put("http-server.authentication.krb5.service-name", "client")
                        .put("http-server.authentication.krb5.keytab", kdcServer.getServerKeytab().getPath())
                        .put("sqlserver.impersonation.enabled", "true")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageMatching("(?s).*Impersonation is not allowed when using credentials pass-through.*");
    }
}
