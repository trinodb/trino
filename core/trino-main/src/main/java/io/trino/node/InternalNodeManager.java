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
package io.trino.node;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import io.trino.spi.HostAddress;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.ImmutableSetMultimap.toImmutableSetMultimap;
import static java.util.Objects.requireNonNull;

public interface InternalNodeManager
{
    default Set<InternalNode> getNodes(NodeState state)
    {
        return switch (state) {
            case ACTIVE -> getAllNodes().activeNodes();
            case INACTIVE -> getAllNodes().inactiveNodes();
            case DRAINING -> getAllNodes().drainingNodes();
            case DRAINED -> getAllNodes().drainedNodes();
            case SHUTTING_DOWN -> getAllNodes().shuttingDownNodes();
            case INVALID, GONE -> ImmutableSet.of();
        };
    }

    default NodesSnapshot getActiveNodesSnapshot()
    {
        return new NodesSnapshot(getAllNodes().activeNodes());
    }

    default Set<InternalNode> getCoordinators()
    {
        return getAllNodes().activeCoordinators();
    }

    /**
     * Active nodes belonging to the given node group, or all active nodes when the group is empty,
     * meaning execution is not restricted to a group.
     */
    default Set<InternalNode> getActiveNodesInGroup(Optional<String> nodeGroup)
    {
        Set<InternalNode> activeNodes = getAllNodes().activeNodes();
        if (nodeGroup.isEmpty()) {
            return activeNodes;
        }
        return activeNodes.stream()
                .filter(node -> node.getNodeGroups().contains(nodeGroup.get()))
                .collect(toImmutableSet());
    }

    AllNodes getAllNodes();

    boolean isGone(HostAddress hostAddress);

    boolean refreshNodes(boolean forceAndWait);

    void addNodeChangeListener(Consumer<AllNodes> listener);

    void removeNodeChangeListener(Consumer<AllNodes> listener);

    class NodesSnapshot
    {
        private final Set<InternalNode> allNodes;
        private final Supplier<SetMultimap<String, InternalNode>> nodesByGroup;

        public NodesSnapshot(Set<InternalNode> allActiveNodes)
        {
            requireNonNull(allActiveNodes, "allActiveNodes is null");
            this.allNodes = ImmutableSet.copyOf(allActiveNodes);
            this.nodesByGroup = memoize(() -> allNodes.stream()
                    .flatMap(node -> node.getNodeGroups().stream().map(group -> Map.entry(group, node)))
                    .collect(toImmutableSetMultimap(Entry::getKey, Entry::getValue)));
        }

        public Set<InternalNode> getAllNodes()
        {
            return allNodes;
        }

        /**
         * Nodes belonging to the given node group, or all nodes when the group is empty,
         * meaning execution is not restricted to a group.
         */
        public Set<InternalNode> getNodesInGroup(Optional<String> nodeGroup)
        {
            if (nodeGroup.isEmpty()) {
                return allNodes;
            }
            return nodesByGroup.get().get(nodeGroup.get());
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("allNodes", allNodes)
                    .toString();
        }
    }
}
