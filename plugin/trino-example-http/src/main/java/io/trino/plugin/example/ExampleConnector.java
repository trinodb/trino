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
package io.trino.plugin.example;

import com.google.common.collect.ImmutableSet;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.Set;

import static io.trino.plugin.example.ExampleTransactionHandle.INSTANCE;
import static java.util.Objects.requireNonNull;

public class ExampleConnector
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final ExampleMetadata metadata;
    private final ExampleSplitManager splitManager;
    private final ExampleRecordSetProvider recordSetProvider;

    @Inject
    public ExampleConnector(
            LifeCycleManager lifeCycleManager,
            ExampleMetadata metadata,
            ExampleSplitManager splitManager,
            ExampleRecordSetProvider recordSetProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public Set<Class<?>> getHandleClasses()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(ExampleTableHandle.class)
                .add(ExampleColumnHandle.class)
                .add(ExampleSplit.class)
                .add(ExampleTransactionHandle.class)
                .build();
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return INSTANCE;
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
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }
}
