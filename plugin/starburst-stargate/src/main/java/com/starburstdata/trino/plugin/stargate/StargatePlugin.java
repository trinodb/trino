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
import com.google.inject.Scopes;
import com.starburstdata.trino.plugin.license.LicenseManager;
import io.trino.plugin.jdbc.ExtraCredentialsBasedIdentityCacheMappingModule;
import io.trino.plugin.jdbc.JdbcConnectorFactory;
import io.trino.plugin.jdbc.JdbcMetadataFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static java.util.Objects.requireNonNull;

public class StargatePlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return getConnectorFactories(() -> true, false);
    }

    @VisibleForTesting
    Iterable<ConnectorFactory> getConnectorFactories(LicenseManager licenseManager, boolean enableWrites)
    {
        return ImmutableList.of(getConnectorFactory(licenseManager, enableWrites));
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
                        binder -> binder.install(new ExtraCredentialsBasedIdentityCacheMappingModule()),
                        binder -> newOptionalBinder(binder, JdbcMetadataFactory.class).setBinding().to(StargateMetadataFactory.class).in(Scopes.SINGLETON),
                        new StargateAuthenticationModule(),
                        new StargateModule()));
    }
}
