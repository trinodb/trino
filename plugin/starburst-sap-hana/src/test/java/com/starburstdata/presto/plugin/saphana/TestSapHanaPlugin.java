/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.testing.TestingConnectorContext;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestSapHanaPlugin
{
    @Test
    public void testLicenseRequired()
    {
        Plugin plugin = new SapHanaPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        Assertions.assertThatThrownBy(() -> factory.create("test", ImmutableMap.of("connection-url", "test"), new TestingConnectorContext()))
                .isInstanceOf(RuntimeException.class)
                .hasToString("com.starburstdata.presto.license.PrestoLicenseException: Valid license required to use the feature: sap-hana");
    }

    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new TestingSapHanaPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", ImmutableMap.of("connection-url", "test"), new TestingConnectorContext());
    }
}
