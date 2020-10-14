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
import io.prestosql.plugin.jdbc.JdbcConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;

import static com.starburstdata.presto.license.TestingLicenseManager.NOOP_LICENSE_MANAGER;
import static io.airlift.configuration.ConfigurationAwareModule.combine;

public class TestingOracleConnectorFactory
        extends JdbcConnectorFactory
{
    public TestingOracleConnectorFactory()
    {
        super("oracle", catalogName -> {
            LicenseManager licenseManager = NOOP_LICENSE_MANAGER;
            return combine(
                    binder -> binder.bind(LicenseManager.class).toInstance(licenseManager),
                    new OracleClientModule(catalogName, licenseManager));
        });
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new OracleHandleResolver();
    }
}
