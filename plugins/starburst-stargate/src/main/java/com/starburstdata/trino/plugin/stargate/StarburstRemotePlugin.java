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
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.jdbc.DynamicFilteringJdbcConnectorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import static com.starburstdata.presto.license.StarburstFeature.STARBURST_CONNECTOR;
import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static java.util.Objects.requireNonNull;

public class StarburstRemotePlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new LicenceCheckingConnectorFactory(STARBURST_CONNECTOR, getConnectorFactory(new LicenseManagerProvider().get(), false)));
    }

    @VisibleForTesting
    ConnectorFactory getConnectorFactory(LicenseManager licenseManager, boolean enableWrites)
    {
        requireNonNull(licenseManager, "licenseManager is null");
        return new DynamicFilteringJdbcConnectorFactory(
                // "starburst-remote" will be used also for the parallel variant, with implementation chosen by a configuration property
                "starburst-remote",
                combine(
                        binder -> binder.bind(LicenseManager.class).toInstance(licenseManager),
                        binder -> binder.bind(Boolean.class).annotatedWith(EnableWrites.class).toInstance(enableWrites),
                        new StarburstRemoteModule()),
                licenseManager);
    }
}
