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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.trino.metadata.InternalNode;

import java.util.List;
import java.util.Set;

import static io.airlift.discovery.client.ServiceDescriptor.serviceDescriptor;
import static io.airlift.discovery.client.ServiceSelectorConfig.DEFAULT_POOL;

public class TrinoNodeServiceSelector
            implements ServiceSelector
{
    @GuardedBy("this")
    private List<ServiceDescriptor> descriptors = ImmutableList.of();

    public synchronized void announceNodes(Set<InternalNode> activeNodes, Set<InternalNode> inactiveNodes)
    {
        ImmutableList.Builder<ServiceDescriptor> descriptors = ImmutableList.builder();
        for (InternalNode node : Iterables.concat(activeNodes, inactiveNodes)) {
            descriptors.add(serviceDescriptor("trino")
                    .setNodeId(node.getNodeIdentifier())
                    .addProperty("http", node.getInternalUri().toString())
                    .addProperty("node_version", node.getNodeVersion().toString())
                    .addProperty("coordinator", String.valueOf(node.isCoordinator()))
                    .build());
        }

        this.descriptors = descriptors.build();
    }

    @Override
    public String getType()
    {
        return "trino";
    }

    @Override
    public String getPool()
    {
        return DEFAULT_POOL;
    }

    @Override
    public synchronized List<ServiceDescriptor> selectAllServices()
    {
        return descriptors;
    }

    @Override
    public ListenableFuture<List<ServiceDescriptor>> refresh()
    {
        throw new UnsupportedOperationException();
    }
}
