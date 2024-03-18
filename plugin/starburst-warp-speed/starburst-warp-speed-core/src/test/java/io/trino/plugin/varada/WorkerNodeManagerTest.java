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

import io.airlift.http.client.HttpUriBuilder;
import io.trino.plugin.varada.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.varada.util.NodeUtils;
import io.trino.plugin.varada.util.UriUtils;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkerNodeManagerTest
{
    private WorkerNodeManager workerNodeManager;
    private NodeManager nodeManager;
    private int nodeId;

    @BeforeEach
    public void before()
    {
        nodeManager = mock(NodeManager.class);
        Node node = NodeUtils.node(nodeId, true);
        when(nodeManager.getCurrentNode()).thenReturn(node);
        workerNodeManager = new WorkerNodeManager(nodeManager,
                mock(WorkerCapacityManager.class));
        nodeId = 0;
    }

    @Test
    public void testWorkerMultipleWorkers()
    {
        assertWorkerInitialized();
    }

    @SuppressWarnings("deprecation")
    private void assertWorkerInitialized()
    {
        Node coordinator = NodeUtils.node(nodeId, true);
        Node currentNode = NodeUtils.node(nodeId, false);
        Node anotherNode = NodeUtils.node(nodeId, false);
        Set<Node> allNodes = Set.of(coordinator, currentNode, anotherNode);
        when(nodeManager.getAllNodes()).thenReturn(allNodes);

        Set<Node> workerNodes = Set.of(currentNode, anotherNode);
        when(nodeManager.getWorkerNodes()).thenReturn(workerNodes);

        when(nodeManager.getCurrentNode()).thenReturn(currentNode);

        workerNodeManager.startWorkerNode();
        assertThat(workerNodeManager.getCurrentNodeHttpUri()).isEqualTo(HttpUriBuilder.uriBuilderFrom(UriUtils.getHttpUri(currentNode)).build());
        assertThat(workerNodeManager.getCoordinatorNodeHttpUri()).isEqualTo(HttpUriBuilder.uriBuilderFrom(UriUtils.getHttpUri(coordinator)).build());
    }
}
