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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.node.InternalNode;
import io.trino.node.InternalNodeManager;
import io.trino.node.NodeState;
import io.trino.spi.HostAddress;
import org.ishugaliy.allgood.consistent.hash.ConsistentHash;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;
import org.ishugaliy.allgood.consistent.hash.node.Node;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Maps a split's cache key to a stable list of worker host addresses using a consistent-hash ring.
 * Worker membership is refreshed lazily on access (bounded by a short time window), so no
 * background executor is needed.
 */
public class ConsistentHashingAddressProvider
{
    private static final Logger log = Logger.get(ConsistentHashingAddressProvider.class);
    private static final long WORKER_NODES_CACHE_TIMEOUT_SECS = 5;

    private final InternalNodeManager nodeManager;
    private final int preferredHostsCount;
    private final Comparator<HostAddress> hostAddressComparator = Comparator.comparing(HostAddress::getHostText).thenComparing(HostAddress::getPort);

    private final ConsistentHash<TrinoNode> consistentHashRing = HashRing.<TrinoNode>newBuilder()
            .hasher(DefaultHasher.METRO_HASH)
            .build();
    private volatile long lastRefreshTime;

    @Inject
    public ConsistentHashingAddressProvider(InternalNodeManager nodeManager, ConsistentHashingAddressProviderConfig config)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.preferredHostsCount = config.getPreferredHostsCount();
        refreshHashRing();
    }

    public List<HostAddress> getHosts(String cacheKey)
    {
        refreshHashRingIfNeeded();
        return consistentHashRing.locate(cacheKey, preferredHostsCount)
                .stream()
                .map(TrinoNode::getHostAndPort)
                .sorted(hostAddressComparator)
                .collect(toImmutableList());
    }

    private void refreshHashRingIfNeeded()
    {
        if (nanosSince(lastRefreshTime).getValue(SECONDS) > WORKER_NODES_CACHE_TIMEOUT_SECS) {
            // Double-checked locking to reduce contention on the hash ring's write path
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
            Set<TrinoNode> trinoNodes = nodeManager.getNodes(NodeState.ACTIVE).stream()
                    .filter(node -> !node.isCoordinator())
                    .map(TrinoNode::of)
                    .collect(toImmutableSet());
            lastRefreshTime = System.nanoTime();
            Set<TrinoNode> hashRingNodes = consistentHashRing.getNodes();
            Set<TrinoNode> removedNodes = Sets.difference(hashRingNodes, trinoNodes);
            Set<TrinoNode> newNodes = Sets.difference(trinoNodes, hashRingNodes);
            // Avoid acquiring a write lock in consistentHashRing if possible
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
            implements Node
    {
        public static TrinoNode of(InternalNode node)
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
