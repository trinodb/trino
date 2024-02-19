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
package io.trino.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import org.ishugaliy.allgood.consistent.hash.ConsistentHash;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ConsistentHashingAddressProvider
{
    private static final Logger log = Logger.get(ConsistentHashingAddressProvider.class);
    private static final long WORKER_NODES_CACHE_TIMEOUT_SECS = 5;

    private final ConsistentHash<TrinoNode> consistentHashRing = HashRing.<TrinoNode>newBuilder()
            .hasher(DefaultHasher.METRO_HASH)
            .build();
    private final NodeManager nodeManager;
    private volatile long lastRefreshTime;

    public ConsistentHashingAddressProvider(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        refreshHashRing();
    }

    public HostAddress getPreferredAddress(String key)
    {
        refreshHashRingIfNeeded();
        return consistentHashRing.locate(key)
                .orElseThrow(() -> new RuntimeException("Unable to locate key: " + key))
                .getHostAndPort();
    }

    public void refreshHashRingIfNeeded()
    {
        if (nanosSince(lastRefreshTime).getValue(SECONDS) > WORKER_NODES_CACHE_TIMEOUT_SECS) {
            /// Double lock checking pattern to reduce lock contention
            synchronized (this) {
                if (nanosSince(lastRefreshTime).getValue(SECONDS) > WORKER_NODES_CACHE_TIMEOUT_SECS) {
                    refreshHashRing();
                }
            }
        }
    }

    @VisibleForTesting
    synchronized void refreshHashRing()
    {
        try {
            Set<TrinoNode> trinoNodes = nodeManager.getWorkerNodes().stream()
                    .map(TrinoNode::of)
                    .collect(toImmutableSet());
            lastRefreshTime = System.nanoTime();
            Set<TrinoNode> hashRingNodes = consistentHashRing.getNodes();
            Set<TrinoNode> removedNodes = Sets.difference(hashRingNodes, trinoNodes);
            Set<TrinoNode> newNodes = Sets.difference(trinoNodes, hashRingNodes);
            if (!newNodes.isEmpty()) {
                consistentHashRing.addAll(newNodes);
            }
            if (!removedNodes.isEmpty()) {
                removedNodes.forEach(consistentHashRing::remove);
            }
        }
        catch (Exception e) {
            log.error(e, "Error refreshing hash ring");
        }
    }

    private record TrinoNode(String nodeIdentifier, HostAddress hostAndPort)
            implements org.ishugaliy.allgood.consistent.hash.node.Node
    {
        public static TrinoNode of(Node node)
        {
            return new TrinoNode(node.getNodeIdentifier(), node.getHostAndPort());
        }

        public HostAddress getHostAndPort()
        {
            return hostAndPort;
        }

        @Override
        public String getKey()
        {
            return nodeIdentifier;
        }
    }
}
