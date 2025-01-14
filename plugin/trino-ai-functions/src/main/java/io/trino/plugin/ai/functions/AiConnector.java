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
package io.trino.plugin.ai.functions;

import com.google.inject.Inject;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AiConnector
        implements Connector
{
    private final ConnectorMetadata metadata;
    private final FunctionProvider functionProvider;

    @Inject
    public AiConnector(ConnectorMetadata metadata, FunctionProvider functionProvider)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionProvider = requireNonNull(functionProvider, "functionProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return AiTransactionHandle.INSTANCE;
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
}
