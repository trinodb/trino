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

import io.prestosql.plugin.jdbc.JdbcConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;

public class OracleConnectorFactory
        extends JdbcConnectorFactory
{
    public OracleConnectorFactory()
    {
        super("oracle", OracleClientModule::new);
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new OracleHandleResolver();
    }
}
