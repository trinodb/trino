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
package io.trino.plugin.thrift;

import com.google.common.collect.ImmutableSet;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorIndexProvider;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ThriftConnector
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final ThriftMetadata metadata;
    private final ThriftSplitManager splitManager;
    private final ThriftPageSourceProvider pageSourceProvider;
    private final ThriftSessionProperties sessionProperties;
    private final ThriftIndexProvider indexProvider;

    @Inject
    public ThriftConnector(
            LifeCycleManager lifeCycleManager,
            ThriftMetadata metadata,
            ThriftSplitManager splitManager,
            ThriftPageSourceProvider pageSourceProvider,
            ThriftSessionProperties sessionProperties,
            ThriftIndexProvider indexProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        this.indexProvider = requireNonNull(indexProvider, "indexProvider is null");
    }

    @Override
    public Set<Class<?>> getHandleClasses()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(ThriftTableHandle.class)
                .add(ThriftColumnHandle.class)
                .add(ThriftConnectorSplit.class)
                .add(ThriftIndexHandle.class)
                .add(ThriftTransactionHandle.class)
                .build();
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return ThriftTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties.getSessionProperties();
    }

    @Override
    public ConnectorIndexProvider getIndexProvider()
    {
        return indexProvider;
    }

    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }
}
