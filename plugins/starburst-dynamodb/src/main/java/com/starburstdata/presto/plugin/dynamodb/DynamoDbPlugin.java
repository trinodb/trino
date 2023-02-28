/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.dynamodb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.starburstdata.presto.license.LicenceCheckingConnectorFactory;
import com.starburstdata.presto.license.LicenseManager;
import com.starburstdata.presto.license.LicenseManagerProvider;
import io.trino.plugin.jdbc.JdbcConnectorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static java.util.Objects.requireNonNull;

public class DynamoDbPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new LicenceCheckingConnectorFactory(getConnectorFactory(new LicenseManagerProvider().get(), false)));
    }

    @VisibleForTesting
    ConnectorFactory getConnectorFactory(LicenseManager licenseManager, boolean enableWrites)
    {
        requireNonNull(licenseManager, "licenseManager is null");
        return new JdbcConnectorFactory(
                "dynamodb",
                combine(
                        binder -> binder.bind(LicenseManager.class).toInstance(licenseManager),
                        binder -> binder.bind(Boolean.class).annotatedWith(EnableWrites.class).toInstance(enableWrites),
                        new DynamoDbModule()));
    }
}
