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
package io.trino.plugin.varada.util;

import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeUtils
{
    private NodeUtils() {}

    public static Node node(int nodeId, boolean isCoordinator)
    {
        return node(String.valueOf(nodeId), isCoordinator);
    }

    @SuppressWarnings({"DeprecatedApi", "deprecation"})
    public static Node node(String nodeId, boolean isCoordinator)
    {
        HostAddress hostAddress = HostAddress.fromString("aaa-" + nodeId + ".com:8088");
        Node node = mock(Node.class);
        when(node.isCoordinator()).thenReturn(isCoordinator);
        when(node.getHost()).thenReturn(hostAddress.getHostText());
        when(node.getHostAndPort()).thenReturn(hostAddress);
        when(node.getNodeIdentifier()).thenReturn(String.valueOf(nodeId));
        return node;
    }

    public static NodeManager mockNodeManager()
    {
        NodeManager nodeManager = mock(NodeManager.class);
        Node currentNode = node(String.valueOf(1), true);
        when(nodeManager.getCurrentNode()).thenReturn(currentNode);
        return nodeManager;
    }
}
