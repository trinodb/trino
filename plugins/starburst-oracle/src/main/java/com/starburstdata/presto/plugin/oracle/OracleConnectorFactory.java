/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.starburstdata.presto.license.LicenseManager;
import io.trino.plugin.jdbc.JdbcConnectorFactory;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static java.util.Objects.requireNonNull;

public class OracleConnectorFactory
        implements ConnectorFactory
{
    private final ConnectorFactory delegate;

    public OracleConnectorFactory(LicenseManager licenseManager)
    {
        this.delegate = new JdbcConnectorFactory("oracle", catalogName -> {
            requireNonNull(licenseManager, "licenseManager is null");
            return combine(
                    binder -> binder.bind(LicenseManager.class).toInstance(licenseManager),
                    new OracleClientModule(catalogName, licenseManager));
        });
    }

    @Override
    public String getName()
    {
        return this.delegate.getName();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return this.delegate.create(catalogName, config, context);
    }
}
