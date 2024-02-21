/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import com.google.common.collect.ImmutableList;
import com.starburstdata.trino.plugin.license.LicenseManager;
import io.trino.plugin.jdbc.JdbcConnectorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static java.util.Objects.requireNonNull;

public class SapHanaPlugin
        implements Plugin
{
    public static final String CONNECTOR_NAME = "sap_hana";

    private final LicenseManager licenseManager;

    public SapHanaPlugin()
    {
        this(() -> true);
    }

    public SapHanaPlugin(LicenseManager licenseManager)
    {
        this.licenseManager = requireNonNull(licenseManager, "licenseManager is null");
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new JdbcConnectorFactory(
                CONNECTOR_NAME,
                combine(
                        binder -> binder.bind(LicenseManager.class).toInstance(licenseManager),
                        new SapHanaClientModule())));
    }
}
