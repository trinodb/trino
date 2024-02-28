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
package io.trino.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.node.NodeInfo;
import io.trino.execution.SqlTaskManager;
import io.trino.metadata.CatalogManager;
import io.trino.server.InternalCommunicationConfig;
import io.trino.spi.connector.CatalogHandle;
import io.trino.transaction.TransactionManager;

import java.util.List;
import java.util.Set;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class TestingLocalCatalogPruneTask
        extends CatalogPruneTask
{
    private final SqlTaskManager sqlTaskManagerToPrune;

    public TestingLocalCatalogPruneTask(
            TransactionManager transactionManager,
            CatalogManager catalogManager,
            ConnectorServicesProvider connectorServicesProvider,
            NodeInfo nodeInfo,
            CatalogPruneTaskConfig catalogPruneTaskConfig,
            SqlTaskManager sqlTaskManagerToPrune)
    {
        super(
                transactionManager,
                catalogManager,
                connectorServicesProvider,
                nodeInfo,
                new ServiceSelector() {
                    @Override
                    public String getType()
                    {
                        throw new UnsupportedOperationException("No services to select");
                    }

                    @Override
                    public String getPool()
                    {
                        throw new UnsupportedOperationException("No pool");
                    }

                    @Override
                    public List<ServiceDescriptor> selectAllServices()
                    {
                        return ImmutableList.of();
                    }

                    @Override
                    public ListenableFuture<List<ServiceDescriptor>> refresh()
                    {
                        return immediateFuture(ImmutableList.of());
                    }
                },
                new TestingHttpClient(request -> {
                    throw new UnsupportedOperationException("Testing Locl Catalog Prune Task does not make http calls");
                }),
                catalogPruneTaskConfig,
                new InternalCommunicationConfig());
        this.sqlTaskManagerToPrune = requireNonNull(sqlTaskManagerToPrune, "sqlTaskManagerToPrune is null");
    }

    @Override
    void pruneWorkerCatalogs(Set<ServiceDescriptor> online, List<CatalogHandle> activeCatalogs)
    {
        sqlTaskManagerToPrune.pruneCatalogs(ImmutableSet.copyOf(activeCatalogs));
    }
}
