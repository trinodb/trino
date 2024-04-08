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
package io.trino.filesystem.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.ishugaliy.allgood.consistent.hash.ConsistentHash;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class ConsistentHashingHostAddressProvider
        implements CachingHostAddressProvider
{
    private static final Logger log = Logger.get(ConsistentHashingHostAddressProvider.class);

    private final NodeManager nodeManager;
    private final ScheduledExecutorService hashRingUpdater = newSingleThreadScheduledExecutor(daemonThreadsNamed("hash-ring-refresher-%s"));
    private final int replicationFactor;
    private final Comparator<HostAddress> hostAddressComparator = Comparator.comparing(HostAddress::getHostText).thenComparing(HostAddress::getPort);

    private final ConsistentHash<TrinoNode> consistentHashRing = HashRing.<TrinoNode>newBuilder()
            .hasher(DefaultHasher.METRO_HASH)
            .build();

    @Inject
    public ConsistentHashingHostAddressProvider(NodeManager nodeManager, ConsistentHashingHostAddressProviderConfig configuration)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.replicationFactor = configuration.getPreferredHostsCount();
    }

    @Override
    public List<HostAddress> getHosts(String splitPath, List<HostAddress> defaultAddresses)
    {
        return consistentHashRing.locate(splitPath, replicationFactor)
                .stream()
                .map(TrinoNode::getHostAndPort)
                .sorted(hostAddressComparator)
                .collect(toImmutableList());
    }

    @PostConstruct
    public void startRefreshingHashRing()
    {
        hashRingUpdater.scheduleWithFixedDelay(this::refreshHashRing, 5, 5, TimeUnit.SECONDS);
        refreshHashRing();
    }

    @PreDestroy
    public void destroy()
    {
        hashRingUpdater.shutdownNow();
    }

    @VisibleForTesting
    synchronized void refreshHashRing()
    {
        try {
            ImmutableSet<TrinoNode> trinoNodes = nodeManager.getWorkerNodes().stream().map(TrinoNode::of).collect(toImmutableSet());
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
