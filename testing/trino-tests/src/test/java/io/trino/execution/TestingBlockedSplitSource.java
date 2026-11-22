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
package io.trino.execution;

import com.google.common.collect.ImmutableSet;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.TestingTableFunctions.EmptySourceFunction;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.testing.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

final class TestingBlockedSplitSource
{
    @Language("SQL")
    static final String BLOCKED_QUERY = "SELECT count(*) FROM TABLE(blocked.system.empty_source())";

    private TestingBlockedSplitSource() {}

    static void installBlockedSplitSource(DistributedQueryRunner runner, Supplier<CompletableFuture<List<ConnectorSplit>>> nextBatch)
    {
        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withTableFunctions(ImmutableSet.of(new EmptySourceFunction()))
                .withFunctionProvider(Optional.of(new FunctionProvider()
                {
                    @Override
                    public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle handle)
                    {
                        return new EmptySourceFunction.EmptySourceFunctionProcessorProvider();
                    }
                }))
                .withTableFunctionSplitSources(_ -> new ConnectorSplitSource()
                {
                    private CompletableFuture<List<ConnectorSplit>> batch;

                    @Override
                    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
                    {
                        batch = nextBatch.get();
                        return batch;
                    }

                    @Override
                    public boolean isFinished()
                    {
                        return batch != null && batch.isDone();
                    }

                    @Override
                    public void close() {}
                })
                .build()));
        runner.createCatalog("blocked", "mock");
    }
}
