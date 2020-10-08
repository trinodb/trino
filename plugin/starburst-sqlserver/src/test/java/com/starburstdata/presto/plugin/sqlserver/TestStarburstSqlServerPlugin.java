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
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStarburstSqlServerPlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new StarburstSqlServerPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", ImmutableMap.of("connection-url", "test"), new TestingConnectorContext());
    }

    @Test
    public void testLicenseRequiredForImpersonation()
    {
        Plugin plugin = new StarburstSqlServerPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "connection-url", "test",
                        "sqlserver.impersonation.enabled", "true"),
                new TestingConnectorContext()))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("com.starburstdata.presto.license.PrestoLicenseException: Valid license required to use the feature: jdbc-impersonation");
    }
}
