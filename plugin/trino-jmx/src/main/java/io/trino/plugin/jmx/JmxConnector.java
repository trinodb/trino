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
package io.trino.plugin.jmx;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class JmxConnector
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final JmxMetadata jmxMetadata;
    private final JmxSplitManager jmxSplitManager;
    private final JmxRecordSetProvider jmxRecordSetProvider;

    @Inject
    public JmxConnector(
            LifeCycleManager lifeCycleManager,
            JmxMetadata jmxMetadata,
            JmxSplitManager jmxSplitManager,
            JmxRecordSetProvider jmxRecordSetProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.jmxMetadata = requireNonNull(jmxMetadata, "jmxMetadata is null");
        this.jmxSplitManager = requireNonNull(jmxSplitManager, "jmxSplitManager is null");
        this.jmxRecordSetProvider = requireNonNull(jmxRecordSetProvider, "jmxRecordSetProvider is null");
    }

    @Override
    public JmxMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return jmxMetadata;
    }

    @Override
    public JmxSplitManager getSplitManager()
    {
        return jmxSplitManager;
    }

    @Override
    public JmxRecordSetProvider getRecordSetProvider()
    {
        return jmxRecordSetProvider;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return JmxTransactionHandle.INSTANCE;
    }

    @Override
    public void shutdown()
    {
        lifeCycleManager.stop();
    }
}
