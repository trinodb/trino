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
package io.trino.sql.planner;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.slice.XxHash64;
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.scheduler.BucketNodeMap;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSelector;
import io.trino.metadata.Split;
import io.trino.node.InternalNode;
import io.trino.operator.RetryPolicy;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.split.EmptySplit;
import io.trino.sql.planner.NodePartitionMap.BucketToPartition;
import io.trino.sql.planner.SystemPartitioningHandle.SystemPartitioning;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionMaxPartitionCount;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.execution.TaskManagerConfig.MAX_WRITER_COUNT;
import static io.trino.operator.exchange.LocalExchange.SCALE_WRITERS_MAX_PARTITIONS_PER_WRITER;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.util.Failures.checkCondition;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class NodePartitioningManager
{
    private final NodeScheduler nodeScheduler;
    private final CatalogServiceProvider<ConnectorNodePartitioningProvider> partitioningProvider;

    @Inject
    public NodePartitioningManager(
            NodeScheduler nodeScheduler,
            CatalogServiceProvider<ConnectorNodePartitioningProvider> partitioningProvider)
    {
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
        this.partitioningProvider = requireNonNull(partitioningProvider, "partitioningProvider is null");
    }

    public NodePartitionMap getNodePartitioningMap(Session session, PartitioningHandle partitioningHandle, int partitionCount)
    {
        return getNodePartitioningMap(session, partitioningHandle, new HashMap<>(), new AtomicReference<>(), partitionCount);
    }

    /**
     * This method is recursive for MergePartitioningHandle. It caches the node mappings
     * to ensure that both the insert and update layouts use the same mapping.
     */
    private NodePartitionMap getNodePartitioningMap(
            Session session,
            PartitioningHandle partitioningHandle,
            Map<Integer, List<InternalNode>> bucketToNodeCache,
            AtomicReference<List<InternalNode>> systemPartitioningCache,
            int partitionCount)
    {
        requireNonNull(session, "session is null");
        requireNonNull(partitioningHandle, "partitioningHandle is null");

        if (partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle) {
            return new NodePartitionMap(systemBucketToNode(session, partitioningHandle, systemPartitioningCache, partitionCount), _ -> {
                throw new UnsupportedOperationException("System distribution does not support source splits " + partitioningHandle);
            });
        }

        if (partitioningHandle.getConnectorHandle() instanceof MergePartitioningHandle mergeHandle) {
            return mergeHandle.getNodePartitioningMap(handle ->
                    getNodePartitioningMap(session, handle, bucketToNodeCache, systemPartitioningCache, partitionCount));
        }

        List<InternalNode> bucketToNode;
        Optional<ConnectorBucketNodeMap> optionalMap = getConnectorBucketNodeMap(session, partitioningHandle);
        if (optionalMap.isEmpty()) {
            bucketToNode = systemBucketToNode(session, FIXED_HASH_DISTRIBUTION, systemPartitioningCache, partitionCount);
        }
        else {
            ConnectorBucketNodeMap connectorBucketNodeMap = optionalMap.get();

            // safety check for crazy partitioning
            checkArgument(connectorBucketNodeMap.getBucketCount() < 1_000_000, "Too many buckets in partitioning: %s", connectorBucketNodeMap.getBucketCount());

            if (connectorBucketNodeMap.hasFixedMapping()) {
                bucketToNode = getFixedMapping(connectorBucketNodeMap);
                verify(bucketToNode.size() == connectorBucketNodeMap.getBucketCount(), "Fixed mapping size does not match bucket count");
            }
            else {
                List<InternalNode> allNodes = getAllNodes(session);
                bucketToNode = bucketToNodeCache.computeIfAbsent(
                        connectorBucketNodeMap.getBucketCount(),
                        bucketCount -> createArbitraryBucketToNode(connectorBucketNodeMap.getCacheKeyHint(), allNodes.subList(0, Math.min(allNodes.size(), partitionCount)), bucketCount));
            }
        }

        int[] bucketToPartition = new int[bucketToNode.size()];
        BiMap<InternalNode, Integer> nodeToPartition = HashBiMap.create();
        int nextPartitionId = 0;
        for (int bucket = 0; bucket < bucketToNode.size(); bucket++) {
            InternalNode node = bucketToNode.get(bucket);
            Integer partitionId = nodeToPartition.get(node);
            if (partitionId == null) {
                partitionId = nextPartitionId;
                nextPartitionId++;
                nodeToPartition.put(node, partitionId);
            }
            bucketToPartition[bucket] = partitionId;
        }

        List<InternalNode> partitionToNode = IntStream.range(0, nodeToPartition.size())
                .mapToObj(partitionId -> nodeToPartition.inverse().get(partitionId))
                .collect(toImmutableList());

        boolean hasFixedMapping = optionalMap.map(ConnectorBucketNodeMap::hasFixedMapping).orElse(false);
        return new NodePartitionMap(
                partitionToNode,
                new BucketToPartition(bucketToPartition, hasFixedMapping),
                getSplitToBucket(session, partitioningHandle, bucketToNode.size()));
    }

    private List<InternalNode> systemBucketToNode(Session session, PartitioningHandle partitioningHandle, AtomicReference<List<InternalNode>> nodesCache, int partitionCount)
    {
        SystemPartitioning partitioning = ((SystemPartitioningHandle) partitioningHandle.getConnectorHandle()).getPartitioning();

        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session);

        List<InternalNode> nodes = switch (partitioning) {
            case COORDINATOR_ONLY -> ImmutableList.of(nodeSelector.selectCurrentNode());
            case SINGLE -> nodeSelector.selectRandomNodes(1);
            case FIXED -> {
                List<InternalNode> value = nodesCache.get();
                if (value == null) {
                    value = nodeSelector.selectRandomNodes(partitionCount);
                    nodesCache.set(value);
                }
                yield value;
            }
            default -> throw new IllegalArgumentException("Unsupported plan distribution " + partitioning);
        };
        checkCondition(!nodes.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
        return nodes;
    }

    public Optional<Integer> getBucketCount(Session session, PartitioningHandle partitioningHandle)
    {
        if (partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle) {
            return Optional.empty();
        }

        ConnectorPartitioningHandle connectorHandle = partitioningHandle.getConnectorHandle();
        if (connectorHandle instanceof MergePartitioningHandle mergeHandle) {
            return mergeHandle.getBucketCount(handle -> getBucketCount(session, handle))
                    .or(() -> Optional.of(getDefaultBucketCount(session)));
        }
        Optional<ConnectorBucketNodeMap> bucketNodeMap = getConnectorBucketNodeMap(session, partitioningHandle);
        return Optional.of(bucketNodeMap.map(ConnectorBucketNodeMap::getBucketCount).orElseGet(() -> getDefaultBucketCount(session)));
    }

    public BucketNodeMap getBucketNodeMap(Session session, PartitioningHandle partitioningHandle, int partitionCount)
    {
        Optional<ConnectorBucketNodeMap> bucketNodeMap = getConnectorBucketNodeMap(session, partitioningHandle);
        int bucketCount = bucketNodeMap.map(ConnectorBucketNodeMap::getBucketCount).orElseGet(() -> getDefaultBucketCount(session));
        ToIntFunction<Split> splitToBucket = getSplitToBucket(session, partitioningHandle, bucketCount);

        if (bucketNodeMap.map(ConnectorBucketNodeMap::hasFixedMapping).orElse(false)) {
            return new BucketNodeMap(splitToBucket, getFixedMapping(bucketNodeMap.get()));
        }

        long seed = bucketNodeMap.map(ConnectorBucketNodeMap::getCacheKeyHint).orElse(ThreadLocalRandom.current().nextLong());
        List<InternalNode> nodes = getAllNodes(session);
        nodes = nodes.subList(0, Math.min(nodes.size(), partitionCount));
        return new BucketNodeMap(splitToBucket, createArbitraryBucketToNode(seed, nodes, bucketCount));
    }

    /**
     * Query plans typically divide data into buckets to help split the work among Trino nodes.  How many buckets?  That is dictated by the connector, via
     * {@link #getConnectorBucketNodeMap}.  But when that returns empty, this method should be used to determine how many buckets to create.
     *
     * @return The default bucket count to use when the connector doesn't provide a number.
     */
    public int getDefaultBucketCount(Session session)
    {
        // The default bucket count is used by both remote and local exchanges to assign buckets to nodes and drivers. The goal is to have enough
        // buckets to evenly distribute them across tasks or drivers. If number of buckets is too low, then some tasks or drivers will be idle.
        // Excessive number of buckets would make bucket lists or arrays too large.

        // For remote exchanges bucket count can be assumed to be equal to node count multiplied by some constant.
        // Multiplying by a constant allows to better distribute skewed buckets which would otherwise be assigned
        // to a single node.
        int remoteBucketCount;
        if (getRetryPolicy(session) != RetryPolicy.TASK) {
            // Pipeline schedulers typically create as many tasks as there are nodes.
            remoteBucketCount = getNodeCount(session) * 3;
        }
        else {
            // The FTE scheduler usually creates as many tasks as there are partitions.
            // TODO: get the actual number of partitions if PartitioningHandle ever offers it or if we get the partitioning scheme here as a parameter.
            remoteBucketCount = getFaultTolerantExecutionMaxPartitionCount(session) * 3;
        }

        // For local exchanges we need to multiply MAX_WRITER_COUNT by SCALE_WRITERS_MAX_PARTITIONS_PER_WRITER to account
        // for local exchanges that are used to distributed data between table writer drivers.
        int localBucketCount = MAX_WRITER_COUNT * SCALE_WRITERS_MAX_PARTITIONS_PER_WRITER;

        return max(remoteBucketCount, localBucketCount);
    }

    public int getNodeCount(Session session)
    {
        return getAllNodes(session).size();
    }

    private List<InternalNode> getAllNodes(Session session)
    {
        return nodeScheduler.createNodeSelector(session).allNodes();
    }

    private static List<InternalNode> getFixedMapping(ConnectorBucketNodeMap connectorBucketNodeMap)
    {
        return connectorBucketNodeMap.getFixedMapping().stream()
                .map(InternalNode.class::cast)
                .collect(toImmutableList());
    }

    public Optional<ConnectorBucketNodeMap> getConnectorBucketNodeMap(Session session, PartitioningHandle partitioningHandle)
    {
        CatalogHandle catalogHandle = requiredCatalogHandle(partitioningHandle);
        ConnectorNodePartitioningProvider partitioningProvider = getPartitioningProvider(catalogHandle);

        return partitioningProvider.getBucketNodeMapping(
                partitioningHandle.getTransactionHandle().orElseThrow(),
                session.toConnectorSession(catalogHandle),
                partitioningHandle.getConnectorHandle());
    }

    public ToIntFunction<Split> getSplitToBucket(Session session, PartitioningHandle partitioningHandle, int bucketCount)
    {
        CatalogHandle catalogHandle = requiredCatalogHandle(partitioningHandle);
        ConnectorNodePartitioningProvider partitioningProvider = getPartitioningProvider(catalogHandle);

        ToIntFunction<ConnectorSplit> splitBucketFunction = partitioningProvider.getSplitBucketFunction(
                partitioningHandle.getTransactionHandle().orElseThrow(),
                session.toConnectorSession(catalogHandle),
                partitioningHandle.getConnectorHandle(),
                bucketCount);
        checkArgument(splitBucketFunction != null, "No partitioning %s", partitioningHandle);

        return split -> {
            int bucket;
            if (split.getConnectorSplit() instanceof EmptySplit) {
                bucket = 0;
            }
            else {
                bucket = splitBucketFunction.applyAsInt(split.getConnectorSplit());
            }
            return bucket;
        };
    }

    private ConnectorNodePartitioningProvider getPartitioningProvider(CatalogHandle catalogHandle)
    {
        return partitioningProvider.getService(requireNonNull(catalogHandle, "catalogHandle is null"));
    }

    private static CatalogHandle requiredCatalogHandle(PartitioningHandle partitioningHandle)
    {
        return partitioningHandle.getCatalogHandle().orElseThrow(() ->
                new IllegalStateException("No catalog handle for partitioning handle: " + partitioningHandle));
    }

    private static List<InternalNode> createArbitraryBucketToNode(long seed, List<InternalNode> nodes, int bucketCount)
    {
        requireNonNull(nodes, "nodes is null");
        checkArgument(!nodes.isEmpty(), "nodes is empty");
        checkArgument(bucketCount > 0, "bucketCount must be greater than zero");

        // Assign each bucket to the machine with the highest weight (hash)
        // This is simple Rendezvous Hashing (Highest Random Weight) algorithm
        ImmutableList.Builder<InternalNode> bucketAssignments = ImmutableList.builderWithExpectedSize(bucketCount);
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            long bucketHash = XxHash64.hash(seed, bucket);

            InternalNode bestNode = null;
            long highestWeight = Long.MIN_VALUE;
            for (InternalNode node : nodes) {
                long weight = XxHash64.hash(node.longHashCode(), bucketHash);
                if (weight >= highestWeight) {
                    highestWeight = weight;
                    bestNode = node;
                }
            }

            bucketAssignments.add(requireNonNull(bestNode));
        }

        return bucketAssignments.build();
    }
}
