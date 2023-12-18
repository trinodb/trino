/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.salesforce;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorFactory;

import static com.starburstdata.trino.plugin.salesforce.SalesforceQueryRunner.NOOP_LICENSE_MANAGER;

public class TestingSalesforcePlugin
        extends SalesforcePlugin
{
    private final boolean enableWrites;

    public TestingSalesforcePlugin(boolean enableWrites)
    {
        super(NOOP_LICENSE_MANAGER);
        this.enableWrites = enableWrites;
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(getConnectorFactory(enableWrites));
    }
}
