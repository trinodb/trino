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

import com.starburstdata.presto.plugin.toolkit.ForwardingConnector;
import com.starburstdata.presto.plugin.toolkit.ForwardingConnectorFactory;
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
                // TODO: note that the check is currently reversed, and will change when we upgrade to https://github.com/trinodb/trino/commit/905f0334c4f1e8d70d95b1fa4eb572e80d0d8197
                //  verify(super.isSingleStatementWritesOnly(), "super.isSingleStatementWritesOnly no longer returns true");
                verify(!super.isSingleStatementWritesOnly(), "once super.isSingleStatementWritesOnly starts returning true, the code here should be updated");
                return false;
            }
        };
    }
}
