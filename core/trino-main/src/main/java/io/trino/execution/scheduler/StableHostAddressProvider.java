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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.XxHash64;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.units.Duration.nanosSince;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Maps a split's cache key to a stable list of worker host addresses. Keys spread evenly across
 * workers, and a membership change reassigns only the keys placed on the affected workers. Worker
 * membership comes from {@link NodeManager#getWorkerNodes()} and is refreshed lazily on access,
 * bounded by a short time window.
 */
public class StableHostAddressProvider
{
    private static final Logger log = Logger.get(StableHostAddressProvider.class);
    private static final long WORKER_NODES_CACHE_TIMEOUT_SECS = 5;

    private final NodeManager nodeManager;
    private final int preferredHostsCount;

    private volatile Snapshot snapshot = new Snapshot(ImmutableSet.of(), new long[0], new HostAddress[0]);
    private volatile long lastRefreshTime;

    @Inject
    public StableHostAddressProvider(NodeManager nodeManager, StableHostAddressProviderConfig config)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.preferredHostsCount = config.getPreferredHostsCount();
        refreshSnapshot();
    }

    public List<HostAddress> getHosts(String cacheKey)
    {
        refreshSnapshotIfNeeded();
        Snapshot snapshot = this.snapshot;
        long[] nodeHashes = snapshot.nodeHashes();
        int nodeCount = nodeHashes.length;
        if (nodeCount == 0) {
            return ImmutableList.of();
        }
        long keyHash = XxHash64.hash(cacheKey.getBytes(UTF_8));
        int hostsCount = Math.min(preferredHostsCount, nodeCount);
        long[] topWeights = new long[hostsCount];
        HostAddress[] topHosts = new HostAddress[hostsCount];
        int filled = 0;
        // Rendezvous hashing: route the key to its hostsCount highest-weight workers. Bounded
        // insertion keeps the top entries in one allocation-free pass; a sorted/limit stream would
        // sort all nodes and allocate on every split
        for (int i = 0; i < nodeCount; i++) {
            long weight = murmurHash3(nodeHashes[i] ^ keyHash);
            if (filled < hostsCount) {
                insertDescending(topWeights, topHosts, filled, weight, snapshot.hosts()[i]);
                filled++;
            }
            else if (weight > topWeights[hostsCount - 1]) {
                insertDescending(topWeights, topHosts, hostsCount - 1, weight, snapshot.hosts()[i]);
            }
        }
        return ImmutableList.copyOf(topHosts);
    }

    // Places the candidate into the weight-descending top list, shifting lower entries down
    private static void insertDescending(long[] topWeights, HostAddress[] topHosts, int startPosition, long weight, HostAddress host)
    {
        int position = startPosition;
        while (position > 0 && topWeights[position - 1] < weight) {
            topWeights[position] = topWeights[position - 1];
            topHosts[position] = topHosts[position - 1];
            position--;
        }
        topWeights[position] = weight;
        topHosts[position] = host;
    }

    private void refreshSnapshotIfNeeded()
    {
        if (nanosSince(lastRefreshTime).getValue(SECONDS) > WORKER_NODES_CACHE_TIMEOUT_SECS) {
            // Double-checked locking so only one thread rebuilds the snapshot
            synchronized (this) {
                if (nanosSince(lastRefreshTime).getValue(SECONDS) > WORKER_NODES_CACHE_TIMEOUT_SECS) {
                    refreshSnapshot();
                }
            }
        }
    }

    @VisibleForTesting
    synchronized void refreshSnapshot()
    {
        try {
            Set<TrinoNode> trinoNodes = nodeManager.getWorkerNodes().stream()
                    .map(TrinoNode::of)
                    .collect(toImmutableSet());
            lastRefreshTime = System.nanoTime();
            if (snapshot.nodes().equals(trinoNodes)) {
                return;
            }
            snapshot = buildSnapshot(trinoNodes);
        }
        catch (Exception e) {
            log.error(e, "Error refreshing worker node snapshot");
        }
    }

    private static Snapshot buildSnapshot(Set<TrinoNode> trinoNodes)
    {
        long[] nodeHashes = new long[trinoNodes.size()];
        HostAddress[] hosts = new HostAddress[trinoNodes.size()];
        int index = 0;
        for (TrinoNode node : trinoNodes) {
            nodeHashes[index] = node.nodeHash();
            hosts[index] = node.hostAndPort();
            index++;
        }
        return new Snapshot(trinoNodes, nodeHashes, hosts);
    }

    private record Snapshot(Set<TrinoNode> nodes, long[] nodeHashes, HostAddress[] hosts) {}

    private record TrinoNode(String nodeIdentifier, HostAddress hostAndPort, long nodeHash)
    {
        private TrinoNode
        {
            requireNonNull(nodeIdentifier, "nodeIdentifier is null");
            requireNonNull(hostAndPort, "hostAndPort is null");
        }

        public static TrinoNode of(Node node)
        {
            return new TrinoNode(node.getNodeIdentifier(), node.getHostAndPort(), XxHash64.hash(node.getNodeIdentifier().getBytes(UTF_8)));
        }
    }
}
