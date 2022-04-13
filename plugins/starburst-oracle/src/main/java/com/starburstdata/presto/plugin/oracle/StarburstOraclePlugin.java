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
import com.starburstdata.presto.license.LicenseManagerProvider;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

public class StarburstOraclePlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(OracleConnectorFactory.create(new LicenseManagerProvider().get()));
    }
}
