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
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.scheduler.BucketNodeMap;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSelector;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.operator.BucketPartitionFunction;
import io.trino.operator.PartitionFunction;
import io.trino.operator.RetryPolicy;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.split.EmptySplit;
import io.trino.sql.planner.SystemPartitioningHandle.SystemPartitioning;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionMaxPartitionCount;
import static io.trino.SystemSessionProperties.getMaxHashPartitionCount;
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
    private final TypeOperators typeOperators;
    private final CatalogServiceProvider<ConnectorNodePartitioningProvider> partitioningProvider;

    @Inject
    public NodePartitioningManager(
            NodeScheduler nodeScheduler,
            TypeOperators typeOperators,
            CatalogServiceProvider<ConnectorNodePartitioningProvider> partitioningProvider)
    {
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.partitioningProvider = requireNonNull(partitioningProvider, "partitioningProvider is null");
    }

    public PartitionFunction getPartitionFunction(
            Session session,
            PartitioningScheme partitioningScheme,
            List<Type> partitionChannelTypes)
    {
        int[] bucketToPartition = partitioningScheme.getBucketToPartition()
                .orElseThrow(() -> new IllegalArgumentException("Bucket to partition must be set before a partition function can be created"));

        PartitioningHandle partitioningHandle = partitioningScheme.getPartitioning().getHandle();
        if (partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle) {
            return ((SystemPartitioningHandle) partitioningHandle.getConnectorHandle()).getPartitionFunction(
                    partitionChannelTypes,
                    partitioningScheme.getHashColumn().isPresent(),
                    bucketToPartition,
                    typeOperators);
        }

        if (partitioningHandle.getConnectorHandle() instanceof MergePartitioningHandle handle) {
            return handle.getPartitionFunction(
                    (scheme, types) -> getPartitionFunction(session, scheme, types, bucketToPartition),
                    partitionChannelTypes,
                    bucketToPartition);
        }

        return getPartitionFunction(session, partitioningScheme, partitionChannelTypes, bucketToPartition);
    }

    public PartitionFunction getPartitionFunction(Session session, PartitioningScheme partitioningScheme, List<Type> partitionChannelTypes, int[] bucketToPartition)
    {
        PartitioningHandle partitioningHandle = partitioningScheme.getPartitioning().getHandle();

        if (partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle handle) {
            return handle.getPartitionFunction(
                    partitionChannelTypes,
                    partitioningScheme.getHashColumn().isPresent(),
                    bucketToPartition,
                    typeOperators);
        }

        BucketFunction bucketFunction = getBucketFunction(session, partitioningHandle, partitionChannelTypes, bucketToPartition.length);
        return new BucketPartitionFunction(bucketFunction, bucketToPartition);
    }

    public BucketFunction getBucketFunction(Session session, PartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount)
    {
        CatalogHandle catalogHandle = requiredCatalogHandle(partitioningHandle);
        ConnectorNodePartitioningProvider partitioningProvider = getPartitioningProvider(catalogHandle);

        BucketFunction bucketFunction = partitioningProvider.getBucketFunction(
                partitioningHandle.getTransactionHandle().orElseThrow(),
                session.toConnectorSession(),
                partitioningHandle.getConnectorHandle(),
                partitionChannelTypes,
                bucketCount);
        checkArgument(bucketFunction != null, "No bucket function for partitioning: %s", partitioningHandle);
        return bucketFunction;
    }

    public NodePartitionMap getNodePartitioningMap(Session session, PartitioningHandle partitioningHandle)
    {
        return getNodePartitioningMap(session, partitioningHandle, new HashMap<>(), new AtomicReference<>(), Optional.empty());
    }

    public NodePartitionMap getNodePartitioningMap(Session session, PartitioningHandle partitioningHandle, Optional<Integer> partitionCount)
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
            Optional<Integer> partitionCount)
    {
        requireNonNull(session, "session is null");
        requireNonNull(partitioningHandle, "partitioningHandle is null");

        if (partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle) {
            return systemNodePartitionMap(session, partitioningHandle, systemPartitioningCache, partitionCount);
        }

        if (partitioningHandle.getConnectorHandle() instanceof MergePartitioningHandle mergeHandle) {
            return mergeHandle.getNodePartitioningMap(handle ->
                    getNodePartitioningMap(session, handle, bucketToNodeCache, systemPartitioningCache, partitionCount));
        }

        Optional<ConnectorBucketNodeMap> optionalMap = getConnectorBucketNodeMap(session, partitioningHandle);
        if (optionalMap.isEmpty()) {
            return systemNodePartitionMap(session, FIXED_HASH_DISTRIBUTION, systemPartitioningCache, partitionCount);
        }
        ConnectorBucketNodeMap connectorBucketNodeMap = optionalMap.get();

        // safety check for crazy partitioning
        checkArgument(connectorBucketNodeMap.getBucketCount() < 1_000_000, "Too many buckets in partitioning: %s", connectorBucketNodeMap.getBucketCount());

        List<InternalNode> bucketToNode;
        if (connectorBucketNodeMap.hasFixedMapping()) {
            bucketToNode = getFixedMapping(connectorBucketNodeMap);
        }
        else {
            CatalogHandle catalogHandle = requiredCatalogHandle(partitioningHandle);
            bucketToNode = bucketToNodeCache.computeIfAbsent(
                    connectorBucketNodeMap.getBucketCount(),
                    bucketCount -> createArbitraryBucketToNode(getAllNodes(session, catalogHandle), bucketCount));
        }

        int[] bucketToPartition = new int[connectorBucketNodeMap.getBucketCount()];
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

        return new NodePartitionMap(partitionToNode, bucketToPartition, getSplitToBucket(session, partitioningHandle, connectorBucketNodeMap.getBucketCount()));
    }

    private NodePartitionMap systemNodePartitionMap(
            Session session,
            PartitioningHandle partitioningHandle,
            AtomicReference<List<InternalNode>> nodesCache,
            Optional<Integer> partitionCount)
    {
        SystemPartitioning partitioning = ((SystemPartitioningHandle) partitioningHandle.getConnectorHandle()).getPartitioning();

        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, Optional.empty());

        List<InternalNode> nodes = switch (partitioning) {
            case COORDINATOR_ONLY -> ImmutableList.of(nodeSelector.selectCurrentNode());
            case SINGLE -> nodeSelector.selectRandomNodes(1);
            case FIXED -> {
                List<InternalNode> value = nodesCache.get();
                if (value == null) {
                    value = nodeSelector.selectRandomNodes(partitionCount.orElse(getMaxHashPartitionCount(session)));
                    nodesCache.set(value);
                }
                yield value;
            }
            default -> throw new IllegalArgumentException("Unsupported plan distribution " + partitioning);
        };
        checkCondition(!nodes.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");

        return new NodePartitionMap(nodes, split -> {
            throw new UnsupportedOperationException("System distribution does not support source splits");
        });
    }

    public BucketNodeMap getBucketNodeMap(Session session, PartitioningHandle partitioningHandle)
    {
        Optional<ConnectorBucketNodeMap> bucketNodeMap = getConnectorBucketNodeMap(session, partitioningHandle);
        int bucketCount = bucketNodeMap.map(ConnectorBucketNodeMap::getBucketCount).orElseGet(() -> getDefaultBucketCount(session, partitioningHandle));
        ToIntFunction<Split> splitToBucket = getSplitToBucket(session, partitioningHandle, bucketCount);

        if (bucketNodeMap.map(ConnectorBucketNodeMap::hasFixedMapping).orElse(false)) {
            return new BucketNodeMap(splitToBucket, getFixedMapping(bucketNodeMap.get()));
        }

        List<InternalNode> nodes = getAllNodes(session, requiredCatalogHandle(partitioningHandle));
        return new BucketNodeMap(splitToBucket, createArbitraryBucketToNode(nodes, bucketCount));
    }

    /**
     * Query plans typically divide data into buckets to help split the work among Trino nodes.  How many buckets?  That is dictated by the connector, via
     * {@link #getConnectorBucketNodeMap}.  But when that returns empty, this method should be used to determine how many buckets to create.
     *
     * @return The default bucket count to use when the connector doesn't provide a number.
     */
    private int getDefaultBucketCount(Session session, PartitioningHandle partitioningHandle)
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
            remoteBucketCount = getNodeCount(session, partitioningHandle) * 3;
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

    public int getNodeCount(Session session, PartitioningHandle partitioningHandle)
    {
        return getAllNodes(session, requiredCatalogHandle(partitioningHandle)).size();
    }

    private List<InternalNode> getAllNodes(Session session, CatalogHandle catalogHandle)
    {
        return nodeScheduler.createNodeSelector(session, Optional.of(catalogHandle)).allNodes();
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

    private static List<InternalNode> createArbitraryBucketToNode(List<InternalNode> nodes, int bucketCount)
    {
        return cyclingShuffledStream(nodes)
                .limit(bucketCount)
                .collect(toImmutableList());
    }

    private static <T> Stream<T> cyclingShuffledStream(Collection<T> collection)
    {
        List<T> list = new ArrayList<>(collection);
        Collections.shuffle(list);
        return Stream.generate(() -> list).flatMap(List::stream);
    }
}
