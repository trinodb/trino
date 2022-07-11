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
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.scheduler.BucketNodeMap;
import io.trino.execution.scheduler.FixedBucketNodeMap;
import io.trino.execution.scheduler.NodeScheduler;
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
        CatalogName catalogName = partitioningHandle.getConnectorId()
                .orElseThrow(() -> new IllegalArgumentException("No connector ID for partitioning handle: " + partitioningHandle));
        ConnectorNodePartitioningProvider partitioningProvider = getPartitioningProvider(catalogName);
        BucketFunction bucketFunction = partitioningProvider.getBucketFunction(
                partitioningHandle.getTransactionHandle().orElseThrow(() -> new IllegalArgumentException("No transactionHandle for partitioning handle: " + partitioningHandle)),
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
            return ((SystemPartitioningHandle) partitioningHandle.getConnectorHandle()).getNodePartitionMap(session, nodeScheduler);
        }

        ConnectorBucketNodeMap connectorBucketNodeMap = getConnectorBucketNodeMap(session, partitioningHandle);
        // safety check for crazy partitioning
        checkArgument(connectorBucketNodeMap.getBucketCount() < 1_000_000, "Too many buckets in partitioning: %s", connectorBucketNodeMap.getBucketCount());

        List<InternalNode> bucketToNode;
        if (connectorBucketNodeMap.hasFixedMapping()) {
            bucketToNode = getFixedMapping(connectorBucketNodeMap);
        }
        else {
            CatalogName catalogName = partitioningHandle.getConnectorId()
                    .orElseThrow(() -> new IllegalArgumentException("No connector ID for partitioning handle: " + partitioningHandle));
            bucketToNode = createArbitraryBucketToNode(
                    nodeScheduler.createNodeSelector(session, Optional.of(catalogName)).allNodes(),
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

    public BucketNodeMap getBucketNodeMap(Session session, PartitioningHandle partitioningHandle, boolean preferDynamic)
    {
        ConnectorBucketNodeMap connectorBucketNodeMap = getConnectorBucketNodeMap(session, partitioningHandle);

        if (connectorBucketNodeMap.hasFixedMapping()) {
            return new FixedBucketNodeMap(getSplitToBucket(session, partitioningHandle), getFixedMapping(connectorBucketNodeMap));
        }

        if (preferDynamic) {
            return new DynamicBucketNodeMap(getSplitToBucket(session, partitioningHandle), connectorBucketNodeMap.getBucketCount());
        }

        Optional<CatalogName> catalogName = partitioningHandle.getConnectorId();
        checkArgument(catalogName.isPresent(), "No connector ID for partitioning handle: %s", partitioningHandle);
        return new FixedBucketNodeMap(
                getSplitToBucket(session, partitioningHandle),
                createArbitraryBucketToNode(
                        new ArrayList<>(nodeScheduler.createNodeSelector(session, catalogName).allNodes()),
                        connectorBucketNodeMap.getBucketCount()));
    }

    private static List<InternalNode> getFixedMapping(ConnectorBucketNodeMap connectorBucketNodeMap)
    {
        return connectorBucketNodeMap.getFixedMapping().stream()
                .map(InternalNode.class::cast)
                .collect(toImmutableList());
    }

    public ConnectorBucketNodeMap getConnectorBucketNodeMap(Session session, PartitioningHandle partitioningHandle)
    {
        CatalogName catalogName = partitioningHandle.getConnectorId()
                .orElseThrow(() -> new IllegalArgumentException("No connector ID for partitioning handle: " + partitioningHandle));
        ConnectorNodePartitioningProvider partitioningProvider = getPartitioningProvider(catalogName);
        ConnectorBucketNodeMap connectorBucketNodeMap = partitioningProvider.getBucketNodeMap(
                partitioningHandle.getTransactionHandle().orElseThrow(() -> new IllegalArgumentException("No transactionHandle for partitioning handle: " + partitioningHandle)),
                session.toConnectorSession(catalogName),
                partitioningHandle.getConnectorHandle());
        checkArgument(connectorBucketNodeMap != null, "No partition map %s", partitioningHandle);
        return connectorBucketNodeMap;
    }

    private ToIntFunction<Split> getSplitToBucket(Session session, PartitioningHandle partitioningHandle)
    {
        CatalogName catalogName = partitioningHandle.getConnectorId()
                .orElseThrow(() -> new IllegalArgumentException("No connector ID for partitioning handle: " + partitioningHandle));
        ConnectorNodePartitioningProvider partitioningProvider = getPartitioningProvider(catalogName);

        ToIntFunction<ConnectorSplit> splitBucketFunction = partitioningProvider.getSplitBucketFunction(
                partitioningHandle.getTransactionHandle().orElseThrow(() -> new IllegalArgumentException("No transactionHandle for partitioning handle: " + partitioningHandle)),
                session.toConnectorSession(catalogName),
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

    private ConnectorNodePartitioningProvider getPartitioningProvider(CatalogName catalogName)
    {
        return partitioningProvider.getService(requireNonNull(catalogName, "catalogName is null"));
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
