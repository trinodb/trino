/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.oracle;

import com.starburstdata.presto.license.LicenseManager;
import io.trino.plugin.jdbc.JdbcConnectorFactory;

import static com.starburstdata.presto.license.TestingLicenseManager.NOOP_LICENSE_MANAGER;
import static io.airlift.configuration.ConfigurationAwareModule.combine;

public class TestingOracleConnectorFactory
        extends JdbcConnectorFactory
{
    public TestingOracleConnectorFactory()
    {
        super("oracle", combine(
                    binder -> binder.bind(LicenseManager.class).toInstance(NOOP_LICENSE_MANAGER),
                    new OracleClientModule(NOOP_LICENSE_MANAGER)));
    }
}
