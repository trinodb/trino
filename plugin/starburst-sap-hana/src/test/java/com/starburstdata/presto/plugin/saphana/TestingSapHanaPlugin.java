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

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.jdbc.JdbcConnectorFactory;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

public class TestingSapHanaPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new JdbcConnectorFactory(SapHanaPlugin.CONNECTOR_NAME, SapHanaClientModule::new));
    }
}
