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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.Set;

public class MemoryConnector
        implements Connector
{
    private final MemoryMetadata metadata;
    private final MemorySplitManager splitManager;
    private final MemoryPageSourceProvider pageSourceProvider;
    private final MemoryPageSinkProvider pageSinkProvider;

    @Inject
    public MemoryConnector(
            MemoryMetadata metadata,
            MemorySplitManager splitManager,
            MemoryPageSourceProvider pageSourceProvider,
            MemoryPageSinkProvider pageSinkProvider)
    {
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.pageSourceProvider = pageSourceProvider;
        this.pageSinkProvider = pageSinkProvider;
    }

    @Override
    public Set<Class<?>> getHandleClasses()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(MemoryTableHandle.class)
                .add(MemoryColumnHandle.class)
                .add(MemorySplit.class)
                .add(MemoryOutputTableHandle.class)
                .add(MemoryInsertTableHandle.class)
                .add(MemoryTransactionHandle.class)
                .build();
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return MemoryTransactionHandle.INSTANCE;
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
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }
}
