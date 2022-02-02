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
import io.airlift.log.Logger;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static com.starburstdata.presto.license.StarburstFeature.STARGATE;
import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static java.util.Objects.requireNonNull;

public class StargatePlugin
        implements Plugin
{
    private static final Logger log = Logger.get(StargatePlugin.class);

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return getConnectorFactories(new LicenseManagerProvider().get(), false);
    }

    @VisibleForTesting
    Iterable<ConnectorFactory> getConnectorFactories(LicenseManager licenseManager, boolean enableWrites)
    {
        ConnectorFactory connectorFactory = new LicenceCheckingConnectorFactory(STARGATE, getConnectorFactory(licenseManager, enableWrites), licenseManager);
        return ImmutableList.of(connectorFactory, new LegacyConnectorFactory(connectorFactory));
    }

    private ConnectorFactory getConnectorFactory(LicenseManager licenseManager, boolean enableWrites)
    {
        requireNonNull(licenseManager, "licenseManager is null");
        return new DynamicFilteringJdbcConnectorFactory(
                // "stargate" will be used also for the parallel variant, with implementation chosen by a configuration property
                "stargate",
                combine(
                        binder -> binder.bind(LicenseManager.class).toInstance(licenseManager),
                        binder -> binder.bind(Boolean.class).annotatedWith(EnableWrites.class).toInstance(enableWrites),
                        new StargateModule()),
                licenseManager);
    }

    @VisibleForTesting
    static class LegacyConnectorFactory
            implements ConnectorFactory
    {
        private final ConnectorFactory delegate;

        public LegacyConnectorFactory(ConnectorFactory delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public String getName()
        {
            return "starburst-remote";
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            log.warn("Connector name 'starburst-remote' is deprecated. Use '%s' instead.", delegate.getName());
            return delegate.create(catalogName, config, context);
        }
    }
}
