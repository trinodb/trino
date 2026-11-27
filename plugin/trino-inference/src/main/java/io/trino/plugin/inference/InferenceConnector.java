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
package io.trino.plugin.inference;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class InferenceConnector
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final InferenceMetadata metadata;
    private final FunctionProvider functionProvider;
    private final ConnectorSplitManager splitManager;
    private final Set<ConnectorTableFunction> tableFunctions;

    @Inject
    public InferenceConnector(
            LifeCycleManager lifeCycleManager,
            InferenceMetadata metadata,
            FunctionProvider functionProvider,
            ConnectorSplitManager splitManager,
            Set<ConnectorTableFunction> tableFunctions)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionProvider = requireNonNull(functionProvider, "functionProvider is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.tableFunctions = tableFunctions.stream()
                .map(function -> new ClassLoaderSafeConnectorTableFunction(function, getClass().getClassLoader()))
                .collect(toImmutableSet());
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return InferenceTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public Optional<FunctionProvider> getFunctionProvider()
    {
        return Optional.of(functionProvider);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return tableFunctions;
    }

    @Override
    public void shutdown()
    {
        lifeCycleManager.stop();
    }

    public enum InferenceTransactionHandle
            implements ConnectorTransactionHandle
    {
        INSTANCE
    }
}
