/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.sqlserver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.starburstdata.trino.plugin.license.LicenseManager;
import io.trino.plugin.jdbc.JdbcConnectorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static java.util.Objects.requireNonNull;

public class StarburstSqlServerPlugin
        implements Plugin
{
    private final LicenseManager licenseManager;

    public StarburstSqlServerPlugin(LicenseManager licenseManager)
    {
        this.licenseManager = licenseManager;
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(getConnectorFactory(licenseManager));
    }

    @VisibleForTesting
    ConnectorFactory getConnectorFactory(LicenseManager licenseManager)
    {
        requireNonNull(licenseManager, "licenseManager is null");
        return new JdbcConnectorFactory(
                "sqlserver",
                combine(
                        binder -> binder.bind(LicenseManager.class).toInstance(licenseManager),
                        new StarburstSqlServerClientModule()));
    }
}
