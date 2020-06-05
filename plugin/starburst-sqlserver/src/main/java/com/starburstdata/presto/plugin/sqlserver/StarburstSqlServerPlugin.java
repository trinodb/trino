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

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.jdbc.JdbcConnectorFactory;
import io.prestosql.plugin.jdbc.JdbcConnectorFactory.JdbcModuleProvider;
import io.prestosql.plugin.jdbc.credential.CredentialProviderModule;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

import static io.airlift.configuration.ConfigurationAwareModules.combine;

public class StarburstSqlServerPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new JdbcConnectorFactory(
                "sqlserver",
                (JdbcModuleProvider) catalogName -> combine(new CredentialProviderModule(), new StarburstSqlServerClientModule(catalogName))));
    }
}
