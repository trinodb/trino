/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import io.trino.spi.connector.ConnectorFactory;

import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.NOOP_LICENSE_MANAGER;

public class TestingStargatePlugin
        extends StargatePlugin
{
    private final boolean enableWrites;

    public TestingStargatePlugin(boolean enableWrites)
    {
        this.enableWrites = enableWrites;
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return getConnectorFactories(NOOP_LICENSE_MANAGER, enableWrites);
    }
}
