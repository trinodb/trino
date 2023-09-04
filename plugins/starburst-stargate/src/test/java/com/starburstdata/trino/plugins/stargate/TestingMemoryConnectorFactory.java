/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.stargate;

import com.starburstdata.trino.plugins.toolkit.ForwardingConnector;
import com.starburstdata.trino.plugins.toolkit.ForwardingConnectorFactory;
import io.trino.plugin.memory.MemoryConnectorFactory;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static com.google.common.base.Verify.verify;

class TestingMemoryConnectorFactory
        extends ForwardingConnectorFactory
{
    private final ConnectorFactory delegate = new MemoryConnectorFactory();

    @Override
    protected ConnectorFactory delegate()
    {
        return delegate;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        Connector delegate = delegate().create(catalogName, config, context);
        return new ForwardingConnector()
        {
            @Override
            protected Connector delegate()
            {
                return delegate;
            }

            @Override
            public boolean isSingleStatementWritesOnly()
            {
                // This fakes support for transactions. This is required to make enableWrites Stargate's test mode work.
                verify(super.isSingleStatementWritesOnly(), "super.isSingleStatementWritesOnly no longer returns true");
                return false;
            }
        };
    }
}
