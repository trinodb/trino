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
package io.trino.execution.scheduler;

import io.airlift.units.DataSize;
import io.trino.metadata.InternalNode;

import static java.util.Objects.requireNonNull;

public class NodeInfo
{
    private final InternalNode node;
    private final DataSize maxMemory;

    public static NodeInfo unlimitedMemoryNode(InternalNode node)
    {
        return new NodeInfo(node, DataSize.ofBytes(Long.MAX_VALUE));
    }

    public NodeInfo(InternalNode node, DataSize maxMemory)
    {
        this.node = requireNonNull(node, "node is null");
        this.maxMemory = requireNonNull(maxMemory, "maxMemory is null");
    }

    public InternalNode getNode()
    {
        return node;
    }

    public DataSize getMaxMemory()
    {
        return maxMemory;
    }
}
