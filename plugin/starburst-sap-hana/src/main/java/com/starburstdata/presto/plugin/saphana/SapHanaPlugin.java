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
import com.google.inject.Module;
import com.starburstdata.presto.license.LicenceCheckingConnectorFactory;
import com.starburstdata.presto.license.LicenseModule;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.jdbc.DynamicFilteringJdbcConnectorFactory;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

import static com.starburstdata.presto.license.StarburstPrestoFeature.SAP_HANA;
import static io.airlift.configuration.ConfigurationAwareModule.combine;

public class SapHanaPlugin
        implements Plugin
{
    public static final String CONNECTOR_NAME = "sap-hana";

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new LicenceCheckingConnectorFactory(SAP_HANA, getConnectorFactory(new LicenseModule())));
    }

    @VisibleForTesting
    ConnectorFactory getConnectorFactory(Module module)
    {
        return new DynamicFilteringJdbcConnectorFactory(CONNECTOR_NAME, (String catalogName) -> combine(module, new SapHanaClientModule(catalogName)));
    }
}
