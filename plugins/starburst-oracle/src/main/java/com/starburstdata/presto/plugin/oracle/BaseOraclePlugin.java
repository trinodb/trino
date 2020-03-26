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

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.base.LicenceCheckingConnectorFactory;
import io.prestosql.plugin.jdbc.JdbcConnectorFactory;
import io.prestosql.plugin.jdbc.JdbcDiagnosticModule;
import io.prestosql.plugin.jdbc.JdbcModule;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

public class BaseOraclePlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        JdbcConnectorFactory connectorFactory = new JdbcConnectorFactory(
                "oracle",
                catalogName -> {
                    return ImmutableList.of(
                            new JdbcModule(),
                            new JdbcDiagnosticModule(catalogName),
                            new OracleAuthenticationModule(catalogName),
                            new OracleClientModule());
                },
                BaseOraclePlugin.class.getClassLoader());
        return ImmutableList.of(new LicenceCheckingConnectorFactory(connectorFactory, "oracle"));
    }
}
