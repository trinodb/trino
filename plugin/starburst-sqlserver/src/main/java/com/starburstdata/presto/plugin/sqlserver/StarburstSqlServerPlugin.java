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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import com.starburstdata.presto.license.LicenseModule;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.jdbc.DynamicFilteringJdbcConnectorFactory;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

import static io.airlift.configuration.ConfigurationAwareModule.combine;

public class StarburstSqlServerPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(getConnectoryFactory(new LicenseModule()));
    }

    @VisibleForTesting
    DynamicFilteringJdbcConnectorFactory getConnectoryFactory(Module licenseModule)
    {
        return new DynamicFilteringJdbcConnectorFactory(
                "sqlserver",
                (String catalog) -> combine(licenseModule, new StarburstSqlServerClientModule(catalog)));
    }
}
