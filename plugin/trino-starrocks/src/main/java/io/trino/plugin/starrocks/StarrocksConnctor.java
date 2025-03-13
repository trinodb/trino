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
package io.trino.plugin.starrocks;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;

import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;

public class StarrocksConnctor
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final StarrocksClient client;
    private final StarrocksMetadata metadata;
    private final StarrocksSplitManager splitManager;
    private final StarrocksPageSourceProvider pageSourceProvider;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final StarrocksTableProperties tableProperties;

    @Inject
    public StarrocksConnctor(
            LifeCycleManager lifeCycleManager,
            StarrocksClient client,
            StarrocksConfig config,
            StarrocksSessionProperties sessionProperties)
    {
        this.lifeCycleManager = lifeCycleManager;
        this.client = client;
        this.metadata = new StarrocksMetadata(client, config);
        this.splitManager = new StarrocksSplitManager(client);
        this.pageSourceProvider = new StarrocksPageSourceProvider(client);
        this.sessionProperties = sessionProperties.getSessionProperties();
        this.tableProperties = new StarrocksTableProperties();
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return StarrocksTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public void shutdown()
    {
        lifeCycleManager.stop();
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties.getTableProperties();
    }
}
