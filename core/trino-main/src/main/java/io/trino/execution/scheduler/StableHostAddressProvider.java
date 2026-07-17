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
import io.trino.node.InternalNode;
import io.trino.node.InternalNodeManager;
import io.trino.node.NodeState;
import io.trino.spi.HostAddress;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.units.Duration.nanosSince;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Maps a split's cache key to a stable list of worker host addresses using rendezvous (highest
 * random weight) hashing over a precomputed bucket table. Worker membership is refreshed lazily on
 * access, bounded by a short time window, and the table is rebuilt only when membership changes.
 */
public class StableHostAddressProvider
{
    private static final Logger log = Logger.get(StableHostAddressProvider.class);
    private static final long WORKER_NODES_CACHE_TIMEOUT_SECS = 5;
    // Fixed so a key always maps to the same bucket across membership changes; only the bucket's
    // hosts shift on rebuild, keeping reassignment minimal. Assumes worker count stays well below this.
    private static final int BUCKET_COUNT = 2048;

    private final InternalNodeManager nodeManager;
    private final int preferredHostsCount;

    private volatile Snapshot snapshot = new Snapshot(ImmutableSet.of(), ImmutableList.of());
    private volatile long lastRefreshTime;

    @Inject
    public StableHostAddressProvider(InternalNodeManager nodeManager, StableHostAddressProviderConfig config)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.preferredHostsCount = config.getPreferredHostsCount();
        refreshSnapshot();
    }

    public List<HostAddress> getHosts(String cacheKey)
    {
        refreshSnapshotIfNeeded();
        List<List<HostAddress>> bucketToHosts = snapshot.bucketToHosts();
        if (bucketToHosts.isEmpty()) {
            return ImmutableList.of();
        }
        int bucket = Math.floorMod(XxHash64.hash(cacheKey.getBytes(UTF_8)), BUCKET_COUNT);
        return bucketToHosts.get(bucket);
    }

    private void refreshSnapshotIfNeeded()
    {
        if (nanosSince(lastRefreshTime).getValue(SECONDS) > WORKER_NODES_CACHE_TIMEOUT_SECS) {
            // Double-checked locking so only one thread rebuilds the bucket table
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
            Set<TrinoNode> trinoNodes = nodeManager.getNodes(NodeState.ACTIVE).stream()
                    .filter(node -> !node.isCoordinator())
                    .map(TrinoNode::of)
                    .collect(toImmutableSet());
            lastRefreshTime = System.nanoTime();
            Snapshot existing = snapshot;
            if (existing != null && existing.nodes().equals(trinoNodes)) {
                return;
            }
            snapshot = buildSnapshot(trinoNodes);
        }
        catch (Exception e) {
            log.error(e, "Error refreshing worker node bucket table");
        }
    }

    private Snapshot buildSnapshot(Set<TrinoNode> trinoNodes)
    {
        if (trinoNodes.isEmpty()) {
            return new Snapshot(ImmutableSet.of(), ImmutableList.of());
        }
        List<TrinoNode> nodes = ImmutableList.copyOf(trinoNodes);
        int hostsCount = Math.min(preferredHostsCount, nodes.size());
        List<List<HostAddress>> bucketToHosts = IntStream.range(0, BUCKET_COUNT)
                .mapToObj(bucket -> computeHosts(nodes, bucket, hostsCount))
                .collect(toImmutableList());
        return new Snapshot(trinoNodes, bucketToHosts);
    }

    private List<HostAddress> computeHosts(List<TrinoNode> nodes, int bucket, int hostsCount)
    {
        long bucketHash = XxHash64.hash(bucket);
        return nodes.stream()
                .map(node -> new WeightedNode(XxHash64.hash(node.nodeHash(), bucketHash), node.hostAndPort()))
                .sorted(Comparator.comparingLong(WeightedNode::weight).reversed())
                .limit(hostsCount)
                .map(WeightedNode::hostAndPort)
                .collect(toImmutableList());
    }

    private record Snapshot(Set<TrinoNode> nodes, List<List<HostAddress>> bucketToHosts) {}

    private record WeightedNode(long weight, HostAddress hostAndPort) {}

    private record TrinoNode(String nodeIdentifier, HostAddress hostAndPort, long nodeHash)
    {
        private TrinoNode
        {
            requireNonNull(nodeIdentifier, "nodeIdentifier is null");
            requireNonNull(hostAndPort, "hostAndPort is null");
        }

        public static TrinoNode of(InternalNode node)
        {
            return new TrinoNode(node.getNodeIdentifier(), node.getHostAndPort(), XxHash64.hash(node.getNodeIdentifier().getBytes(UTF_8)));
        }
    }
}
