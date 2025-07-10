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

import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.node.NodeInfo;
import io.trino.client.NodeVersion;
import io.trino.execution.SqlTaskManager;
import io.trino.metadata.CatalogManager;
import io.trino.node.InternalNode;
import io.trino.node.TestingInternalNodeManager;
import io.trino.spi.connector.CatalogHandle;
import io.trino.transaction.TransactionManager;

import java.net.URI;
import java.util.List;
import java.util.Set;

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
                new InternalNode(nodeInfo.getNodeId(), URI.create("https://example.com"), new NodeVersion("test"), false),
                TestingInternalNodeManager.createDefault(),
                new TestingHttpClient(request -> {
                    throw new UnsupportedOperationException("Testing Local Catalog Prune Task does not make http calls");
                }),
                catalogPruneTaskConfig);
        this.sqlTaskManagerToPrune = requireNonNull(sqlTaskManagerToPrune, "sqlTaskManagerToPrune is null");
    }

    @Override
    void pruneWorkerCatalogs(Set<URI> online, List<CatalogHandle> activeCatalogs)
    {
        sqlTaskManagerToPrune.pruneCatalogs(ImmutableSet.copyOf(activeCatalogs));
    }
}
