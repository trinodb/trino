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
package io.trino.metadata;

import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public interface InternalNodeManager
{
    Set<InternalNode> getNodes(NodeState state);

    NodesSnapshot getActiveNodesSnapshot();

    InternalNode getCurrentNode();

    Set<InternalNode> getCoordinators();

    AllNodes getAllNodes();

    void refreshNodes();

    void addNodeChangeListener(Consumer<AllNodes> listener);

    void removeNodeChangeListener(Consumer<AllNodes> listener);

    class NodesSnapshot
    {
        private final Set<InternalNode> allNodes;

        public NodesSnapshot(Set<InternalNode> allActiveNodes)
        {
            requireNonNull(allActiveNodes, "allActiveNodes is null");
            this.allNodes = ImmutableSet.copyOf(allActiveNodes);
        }

        public Set<InternalNode> getAllNodes()
        {
            return allNodes;
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
