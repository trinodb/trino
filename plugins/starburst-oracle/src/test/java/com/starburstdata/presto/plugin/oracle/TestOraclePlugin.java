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
import com.google.inject.CreationException;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.Resources.getResource;
import static org.testng.Assert.expectThrows;

public class TestOraclePlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new TestingOraclePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", ImmutableMap.of("connection-url", "test"), new TestingConnectorContext());
    }

    @Test
    public void testUserNotUsedWithKerberos()
    {
        Plugin plugin = new TestingOraclePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        expectThrows(CreationException.class, () -> factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("connection-url", "test")
                        .put("oracle.authentication.type", "KERBEROS")
                        .put("kerberos.client.principal", "test@TESTING-KRB.STARBURSTDATA.COM")
                        .put("kerberos.client.keytab", getResource("krb/client/test.keytab").getPath())
                        .put("kerberos.config", getResource("krb/krb5.conf").getPath())
                        .put("connection-user", "WHAT?!")
                        .build(),
                new TestingConnectorContext())
        ).getMessage().contains("Configuration property 'connection-user' was not used");
    }
}
