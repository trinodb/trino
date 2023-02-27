/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.starburstdata.presto.license.LicenceCheckingConnectorFactory;
import com.starburstdata.presto.license.LicenseManager;
import com.starburstdata.presto.license.LicenseManagerProvider;
import io.trino.plugin.jdbc.JdbcConnectorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import static com.starburstdata.presto.license.StarburstFeature.STARGATE;
import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static java.util.Objects.requireNonNull;

public class StargatePlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return getConnectorFactories(new LicenseManagerProvider().get(), false);
    }

    @VisibleForTesting
    Iterable<ConnectorFactory> getConnectorFactories(LicenseManager licenseManager, boolean enableWrites)
    {
        return ImmutableList.of(new LicenceCheckingConnectorFactory(STARGATE, getConnectorFactory(licenseManager, enableWrites), licenseManager));
    }

    private ConnectorFactory getConnectorFactory(LicenseManager licenseManager, boolean enableWrites)
    {
        requireNonNull(licenseManager, "licenseManager is null");
        return new JdbcConnectorFactory(
                // "stargate" will be used also for the parallel variant, with implementation chosen by a configuration property
                "stargate",
                combine(
                        binder -> binder.bind(LicenseManager.class).toInstance(licenseManager),
                        binder -> binder.bind(Boolean.class).annotatedWith(EnableWrites.class).toInstance(enableWrites),
                        new StargateModule()));
    }
}
