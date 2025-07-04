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
package io.trino.plugin.memory;

import io.trino.spi.Node;
import io.trino.spi.NodeManager;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Comparator.comparing;

public final class MemoryNodesUtil
{
    private MemoryNodesUtil() {}

    public static List<Node> getSortedWorkerNodes(NodeManager nodeManager)
    {
        Set<Node> allNodes = nodeManager.getRequiredWorkerNodes();
        checkState(!allNodes.isEmpty(), "No worker nodes available");
        return allNodes.stream()
                .sorted(comparing(node -> node.getHostAndPort().toString()))
                .collect(toImmutableList());
    }
}
