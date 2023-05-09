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
package io.trino.hdfs.rubix;

import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class TrinoClusterManager
        extends ClusterManager
{
    private static volatile Optional<NodeManager> nodeManager = Optional.empty();

    public static void setNodeManager(NodeManager value)
    {
        nodeManager = Optional.of(requireNonNull(value, "value is null"));
    }

    @Override
    public ClusterType getClusterType()
    {
        return ClusterType.PRESTOSQL_CLUSTER_MANAGER;
    }

    @Override
    public List<String> getNodesInternal()
    {
        return nodeManager
                .orElseThrow(() -> new IllegalStateException("NodeManager not set"))
                .getWorkerNodes().stream()
                .filter(node -> !node.isCoordinator())
                .map(Node::getHost)
                .collect(toImmutableList());
    }

    @Override
    protected String getCurrentNodeHostname()
    {
        return nodeManager
                .map(NodeManager::getCurrentNode)
                .map(Node::getHost)
                .orElseGet(super::getCurrentNodeHostname);
    }

    @Override
    protected String getCurrentNodeHostAddress()
    {
        return nodeManager
                .map(NodeManager::getCurrentNode)
                .map(Node::getHostAndPort)
                .flatMap(TrinoClusterManager::toInetAddress)
                .map(InetAddress::getHostAddress)
                .orElseGet(super::getCurrentNodeHostAddress);
    }

    private static Optional<InetAddress> toInetAddress(HostAddress address)
    {
        try {
            return Optional.ofNullable(address.toInetAddress());
        }
        catch (UnknownHostException ignored) {
            return Optional.empty();
        }
    }
}
