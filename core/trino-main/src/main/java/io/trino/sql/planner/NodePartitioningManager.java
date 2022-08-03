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
import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.scheduler.BucketNodeMap;
import io.trino.execution.scheduler.FixedBucketNodeMap;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSelector;
import io.trino.execution.scheduler.group.DynamicBucketNodeMap;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.operator.BucketPartitionFunction;
import io.trino.operator.PartitionFunction;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.type.Type;
import io.trino.split.EmptySplit;
import io.trino.sql.planner.SystemPartitioningHandle.SystemPartitioning;
import io.trino.type.BlockTypeOperators;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getHashPartitionCount;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.util.Failures.checkCondition;
import static java.util.Objects.requireNonNull;

public class NodePartitioningManager
{
    private final NodeScheduler nodeScheduler;
    private final BlockTypeOperators blockTypeOperators;
    private final CatalogServiceProvider<ConnectorNodePartitioningProvider> partitioningProvider;

    @Inject
    public NodePartitioningManager(
            NodeScheduler nodeScheduler,
            BlockTypeOperators blockTypeOperators,
            CatalogServiceProvider<ConnectorNodePartitioningProvider> partitioningProvider)
    {
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
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
                    blockTypeOperators);
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
        requireNonNull(session, "session is null");
        requireNonNull(partitioningHandle, "partitioningHandle is null");

        if (partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle) {
            return systemNodePartitionMap(session, partitioningHandle);
        }

        Optional<ConnectorBucketNodeMap> optionalMap = getConnectorBucketNodeMap(session, partitioningHandle);
        if (optionalMap.isEmpty()) {
            return systemNodePartitionMap(session, FIXED_HASH_DISTRIBUTION);
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
            bucketToNode = createArbitraryBucketToNode(
                    nodeScheduler.createNodeSelector(session, Optional.of(catalogHandle)).allNodes(),
                    connectorBucketNodeMap.getBucketCount());
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

        return new NodePartitionMap(partitionToNode, bucketToPartition, getSplitToBucket(session, partitioningHandle));
    }

    private NodePartitionMap systemNodePartitionMap(Session session, PartitioningHandle partitioningHandle)
    {
        SystemPartitioning partitioning = ((SystemPartitioningHandle) partitioningHandle.getConnectorHandle()).getPartitioning();

        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, Optional.empty());

        List<InternalNode> nodes = switch (partitioning) {
            case COORDINATOR_ONLY -> ImmutableList.of(nodeSelector.selectCurrentNode());
            case SINGLE -> nodeSelector.selectRandomNodes(1);
            case FIXED -> nodeSelector.selectRandomNodes(getHashPartitionCount(session));
            default -> throw new IllegalArgumentException("Unsupported plan distribution " + partitioning);
        };
        checkCondition(!nodes.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");

        return new NodePartitionMap(nodes, split -> {
            throw new UnsupportedOperationException("System distribution does not support source splits");
        });
    }

    public BucketNodeMap getBucketNodeMap(Session session, PartitioningHandle partitioningHandle, boolean preferDynamic)
    {
        Optional<ConnectorBucketNodeMap> bucketNodeMap = getConnectorBucketNodeMap(session, partitioningHandle);

        ToIntFunction<Split> splitToBucket = getSplitToBucket(session, partitioningHandle);
        if (bucketNodeMap.map(ConnectorBucketNodeMap::hasFixedMapping).orElse(false)) {
            return new FixedBucketNodeMap(splitToBucket, getFixedMapping(bucketNodeMap.get()));
        }

        if (preferDynamic) {
            int bucketCount = bucketNodeMap.map(ConnectorBucketNodeMap::getBucketCount)
                    .orElseGet(() -> getNodeCount(session, partitioningHandle));
            return new DynamicBucketNodeMap(splitToBucket, bucketCount);
        }

        List<InternalNode> nodes = getAllNodes(session, requiredCatalogHandle(partitioningHandle));
        int bucketCount = bucketNodeMap.map(ConnectorBucketNodeMap::getBucketCount).orElseGet(nodes::size);
        return new FixedBucketNodeMap(splitToBucket, createArbitraryBucketToNode(nodes, bucketCount));
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

    private ToIntFunction<Split> getSplitToBucket(Session session, PartitioningHandle partitioningHandle)
    {
        CatalogHandle catalogHandle = requiredCatalogHandle(partitioningHandle);
        ConnectorNodePartitioningProvider partitioningProvider = getPartitioningProvider(catalogHandle);

        ToIntFunction<ConnectorSplit> splitBucketFunction = partitioningProvider.getSplitBucketFunction(
                partitioningHandle.getTransactionHandle().orElseThrow(),
                session.toConnectorSession(catalogHandle),
                partitioningHandle.getConnectorHandle());
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
