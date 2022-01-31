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
package io.trino.plugin.atop;

import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AtopConnector
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final AtopMetadata metadata;
    private final AtopSplitManager splitManager;
    private final AtopPageSourceProvider pageSourceProvider;
    private final Optional<ConnectorAccessControl> accessControl;

    @Inject
    public AtopConnector(
            LifeCycleManager lifeCycleManager,
            AtopMetadata metadata,
            AtopSplitManager splitManager,
            AtopPageSourceProvider pageSourceProvider,
            Optional<ConnectorAccessControl> accessControl)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return AtopTransactionHandle.INSTANCE;
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
    public ConnectorAccessControl getAccessControl()
    {
        return accessControl.orElseThrow(UnsupportedOperationException::new);
    }

    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }
}
