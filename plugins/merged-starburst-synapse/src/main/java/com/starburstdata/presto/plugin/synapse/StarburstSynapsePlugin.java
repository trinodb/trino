/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import com.starburstdata.presto.license.LicenceCheckingConnectorFactory;
import com.starburstdata.presto.license.LicenseManager;
import com.starburstdata.presto.license.LicenseManagerProvider;
import com.starburstdata.presto.license.StarburstFeature;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.jdbc.DynamicFilteringJdbcConnectorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static java.util.Objects.requireNonNull;

public class StarburstSynapsePlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new LicenceCheckingConnectorFactory(
                StarburstFeature.SYNAPSE,
                getConnectorFactory(new LicenseManagerProvider().get(), binder -> {})));
    }

    @VisibleForTesting
    ConnectorFactory getConnectorFactory(LicenseManager licenseManager, Module extensions)
    {
        requireNonNull(licenseManager, "licenseManager is null");
        return DynamicFilteringJdbcConnectorFactory.create(
                "synapse",
                combine(
                        binder -> binder.bind(LicenseManager.class).toInstance(licenseManager),
                        new StarburstSynapseClientModule(),
                        extensions),
                licenseManager);
    }
}
