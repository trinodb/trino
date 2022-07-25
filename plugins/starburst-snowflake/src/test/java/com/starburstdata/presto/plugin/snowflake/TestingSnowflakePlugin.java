/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import io.trino.spi.connector.ConnectorFactory;

import static com.starburstdata.presto.license.TestingLicenseManager.NOOP_LICENSE_MANAGER;

public class TestingSnowflakePlugin
        extends SnowflakePlugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return getConnectorFactoriesWithLicensing(NOOP_LICENSE_MANAGER);
    }
}
