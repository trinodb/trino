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

import com.starburstdata.presto.license.TestingLicenseModule;
import io.prestosql.plugin.jdbc.JdbcConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;

import static io.airlift.configuration.ConfigurationAwareModule.combine;

public class TestingOracleConnectorFactory
        extends JdbcConnectorFactory
{
    public TestingOracleConnectorFactory()
    {
        super("oracle", catalogName -> {
            return combine(new TestingLicenseModule(), new OracleClientModule(catalogName));
        });
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new OracleHandleResolver();
    }
}
