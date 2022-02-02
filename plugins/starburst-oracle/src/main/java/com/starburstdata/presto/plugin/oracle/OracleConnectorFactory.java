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

import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static java.util.Objects.requireNonNull;

public class OracleConnectorFactory
        extends JdbcConnectorFactory
{
    public OracleConnectorFactory(LicenseManager licenseManager)
    {
        super("oracle", catalogName -> {
            requireNonNull(licenseManager, "licenseManager is null");
            return combine(
                    binder -> binder.bind(LicenseManager.class).toInstance(licenseManager),
                    new OracleClientModule(catalogName, licenseManager));
        });
    }
}
