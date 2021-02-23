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
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class ConnectorAwareNodeManager
        implements NodeManager
{
    private final InternalNodeManager nodeManager;
    private final String environment;
    private final CatalogName catalogName;
    private final boolean schedulerIncludeCoordinator;

    public ConnectorAwareNodeManager(InternalNodeManager nodeManager, String environment, CatalogName catalogName, boolean schedulerIncludeCoordinator)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.schedulerIncludeCoordinator = schedulerIncludeCoordinator;
    }

    @Override
    public Set<Node> getAllNodes()
    {
        return ImmutableSet.copyOf(nodeManager.getActiveConnectorNodes(catalogName));
    }

    @Override
    public Set<Node> getWorkerNodes()
    {
        return nodeManager.getActiveConnectorNodes(catalogName).stream()
                .filter(node -> schedulerIncludeCoordinator || !node.isCoordinator())
                .collect(toImmutableSet());
    }

    @Override
    public Node getCurrentNode()
    {
        return nodeManager.getCurrentNode();
    }

    @Override
    public String getEnvironment()
    {
        return environment;
    }
}
