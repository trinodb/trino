/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.starburstdata.presto.license.LicenceCheckingConnectorFactory;
import com.starburstdata.presto.license.LicenseManager;
import com.starburstdata.presto.license.LicenseManagerProvider;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.jdbc.DynamicFilteringJdbcConnectorFactory;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

import static com.starburstdata.presto.license.StarburstPrestoFeature.SAP_HANA;
import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static java.util.Objects.requireNonNull;

public class SapHanaPlugin
        implements Plugin
{
    public static final String CONNECTOR_NAME = "sap-hana";

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new LicenceCheckingConnectorFactory(SAP_HANA, getConnectorFactory(new LicenseManagerProvider().get())));
    }

    @VisibleForTesting
    ConnectorFactory getConnectorFactory(LicenseManager licenseManager)
    {
        requireNonNull(licenseManager, "licenseManager is null");
        return new DynamicFilteringJdbcConnectorFactory(
                CONNECTOR_NAME,
                combine(
                        binder -> binder.bind(LicenseManager.class).toInstance(licenseManager),
                        new SapHanaClientModule()),
                licenseManager);
    }
}
