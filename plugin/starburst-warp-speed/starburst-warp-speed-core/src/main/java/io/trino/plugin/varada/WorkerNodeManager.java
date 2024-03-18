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
package io.trino.plugin.varada;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.varada.util.UriUtils;
import io.trino.plugin.varada.util.VaradaInitializedServiceMarker;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;

import java.net.URI;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@SuppressWarnings("deprecation")
@Singleton
public class WorkerNodeManager
        implements VaradaInitializedServiceMarker
{
    private static final Logger logger = Logger.get(WorkerNodeManager.class);

    private final NodeManager nodeManager;
    private final WorkerCapacityManager workerCapacityManager;

    @Inject
    public WorkerNodeManager(NodeManager nodeManager,
            WorkerCapacityManager workerCapacityManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.workerCapacityManager = requireNonNull(workerCapacityManager, "workerCapacityManager is null");
    }

    @Override
    public void init()
    {
        logger.debug("worker node [%s] is initialising", getCurrentNodeIdentifier());
        startWorkerNode();
    }

    void startWorkerNode()
    {
        workerCapacityManager.initWorker();
    }

    public boolean isWorkerReady()
    {
        boolean isWorkerReady = workerCapacityManager.isWorkerInitialized();
        if (!isWorkerReady) {
            logger.debug("isWorkerReady::%s=[false]", getCurrentNodeIdentifier());
        }
        return isWorkerReady;
    }

    public URI getCurrentNodeHttpUri()
    {
        return UriUtils.getHttpUri(nodeManager.getCurrentNode());
    }

    public URI getCurrentNodeOrigHttpUri()
    {
        return UriUtils.getHttpUri(nodeManager.getCurrentNode());
    }

    public URI getCoordinatorNodeHttpUri()
    {
        Optional<Node> coordinator = nodeManager.getAllNodes()
                .stream()
                .filter(Node::isCoordinator)
                .findFirst();
        return coordinator.map(UriUtils::getHttpUri).orElse(null);
    }

    public String getCurrentNodeIdentifier()
    {
        return nodeManager.getCurrentNode().getNodeIdentifier();
    }
}
