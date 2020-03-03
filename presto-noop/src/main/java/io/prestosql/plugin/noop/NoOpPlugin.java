/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.noop;

import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.transaction.IsolationLevel;

import java.util.Map;

import static java.util.Collections.singletonList;

public final class NoOpPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return singletonList(new NoOpConnectorFactory());
    }

    private static class NoOpConnectorFactory
            implements ConnectorFactory
    {
        @Override
        public String getName()
        {
            return "noop";
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            return new NoOpConnector();
        }

        @Override
        public ConnectorHandleResolver getHandleResolver()
        {
            return new ConnectorHandleResolver()
            {
                @Override
                public Class<? extends ColumnHandle> getColumnHandleClass()
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass()
                {
                    return NoOpConnector.NoOpConnectorTransactionHandle.class;
                }
            };
        }
    }

    private static class NoOpConnector
            implements Connector
    {
        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
        {
            return NoOpConnectorTransactionHandle.TRANSACTION_HANDLE;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
        {
            return new NoOpMetadata();
        }

        private enum NoOpConnectorTransactionHandle
                implements ConnectorTransactionHandle
        {
            TRANSACTION_HANDLE;
        }
    }

    private static class NoOpMetadata
            implements ConnectorMetadata {}
}
