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
package io.trino.plugin.varada.dispatcher;

import com.google.common.collect.Lists;
import com.google.common.collect.MoreCollectors;
import io.airlift.log.Logger;
import io.trino.plugin.varada.CoordinatorNodeManager;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.connector.TestingConnectorProxiedConnectorTransformer;
import io.trino.plugin.varada.storage.splits.ConnectorSplitConsistentHashNodeDistributor;
import io.trino.plugin.varada.storage.splits.ConnectorSplitNodeDistributor;
import io.trino.plugin.varada.util.NodeUtils;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDispatcherProxiedConnectorTransformer
{
    private static final Logger logger = Logger.get(TestDispatcherProxiedConnectorTransformer.class);

    private final Random random = new Random();
    private GlobalConfiguration globalConfiguration;
    private CoordinatorNodeManager coordinatorNodeManager;
    private ConnectorSplitNodeDistributor connectorSplitNodeDistributor;

    @BeforeEach
    public void before()
    {
        globalConfiguration = new GlobalConfiguration();
        coordinatorNodeManager = mock(CoordinatorNodeManager.class);
    }

    @Test
    public void testGetHostAddressForSplitFirstWorker()
    {
        List<DispatcherSplit> dispatcherSplits = IntStream.range(0, 100)
                .mapToObj(i -> {
                    DispatcherSplit split = mock(DispatcherSplit.class);
                    when(split.getPath()).thenReturn("path/to/randomUUID--" + i);
                    when(split.getStart()).thenReturn(0L);
                    when(split.getLength()).thenReturn((long) random.nextInt(1000));
                    return split;
                })
                .collect(Collectors.toList());

        TestingConnectorProxiedConnectorTransformer connectorTransformer = new TestingConnectorProxiedConnectorTransformer();

        // 3 nodes
        List<Node> workers1 = IntStream.range(15, 18)
                .mapToObj(this::getInternalNode)
                .collect(Collectors.toList());

        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(workers1);
        connectorSplitNodeDistributor = new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager);

        Map<Node, Set<DispatcherSplit>> nodeToHiveSplitsMap1 = getNodeToPathsMap(dispatcherSplits,
                connectorTransformer,
                workers1);

        log(workers1, nodeToHiveSplitsMap1);

        validateNodeMap(dispatcherSplits, workers1, nodeToHiveSplitsMap1);

        // 2 nodes
        List<Node> workers2 = Lists.newArrayList(workers1).subList(1, 3);

        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(workers2);
        connectorSplitNodeDistributor = new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager);

        Map<Node, Set<DispatcherSplit>> nodeToHiveSplitsMap2 = getNodeToPathsMap(dispatcherSplits,
                connectorTransformer,
                workers2);

        log(workers2, nodeToHiveSplitsMap2);

        validateNodeMap(dispatcherSplits, workers2, nodeToHiveSplitsMap2);

        workers2.forEach(node -> {
            Set<DispatcherSplit> hiveSplitSet1 = nodeToHiveSplitsMap1.get(node);
            Set<DispatcherSplit> hiveSplitSet2 = nodeToHiveSplitsMap2.get(node);
            assertThat(hiveSplitSet2.size()).isGreaterThanOrEqualTo(hiveSplitSet1.size());
            assertThat(hiveSplitSet2).containsAll(hiveSplitSet1);
        });

        // 3 nodes
        Node newWorker = getInternalNode(18);
        List<Node> workers3 = Stream.concat(Stream.of(newWorker),
                        workers2.stream())
                .collect(Collectors.toList());

        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(workers3);
        connectorSplitNodeDistributor = new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager);

        Map<Node, Set<DispatcherSplit>> nodeToHiveSplitsMap3 = getNodeToPathsMap(dispatcherSplits,
                connectorTransformer,
                workers3);

        log(workers3, nodeToHiveSplitsMap3);

        validateNodeMap(dispatcherSplits, workers3, nodeToHiveSplitsMap3);

        List<Node> worker3AsList = Lists.newArrayList(workers3);
        //make sure "old" nodes don't exchange splits between them
        assertThat(nodeToHiveSplitsMap3.get(worker3AsList.get(1))).hasSizeGreaterThan(10)
                .doesNotContainAnyElementsOf(nodeToHiveSplitsMap3.get(worker3AsList.get(2)));
        assertThat(nodeToHiveSplitsMap3.get(worker3AsList.get(2))).hasSizeGreaterThan(10)
                .doesNotContainAnyElementsOf(nodeToHiveSplitsMap3.get(worker3AsList.get(1)));
        assertThat(nodeToHiveSplitsMap3.get(worker3AsList.get(0))).hasSizeGreaterThan(10);

        // 4 nodes
        newWorker = getInternalNode(19);
        List<Node> workers4 = Stream.concat(Stream.of(newWorker),
                        workers3.stream())
                .collect(Collectors.toList());

        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(workers4);
        connectorSplitNodeDistributor = new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager);

        Map<Node, Set<DispatcherSplit>> nodeToHiveSplitsMap4 = getNodeToPathsMap(dispatcherSplits,
                connectorTransformer,
                workers4);

        log(workers4, nodeToHiveSplitsMap4);

        assertThat(nodeToHiveSplitsMap4.keySet().size()).isEqualTo(workers4.size());
        nodeToHiveSplitsMap4.values().forEach(pathSet -> assertThat(pathSet.size()).isGreaterThan(0));

        workers3.forEach(node -> {
            Set<DispatcherSplit> hiveSplitSet3 = nodeToHiveSplitsMap3.get(node);
            Set<DispatcherSplit> hiveSplitSet4 = nodeToHiveSplitsMap4.get(node);
            assertThat(hiveSplitSet4.size()).isLessThanOrEqualTo(hiveSplitSet3.size());
            assertThat(hiveSplitSet3).containsAll(hiveSplitSet4);
        });

        // 3 nodes
        List<Node> workers5 = Lists.newArrayList(workers4).subList(1, 4);

        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(workers5);
        connectorSplitNodeDistributor = new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager);

        Map<Node, Set<DispatcherSplit>> nodeToHiveSplitsMap5 = getNodeToPathsMap(dispatcherSplits,
                connectorTransformer,
                workers5);

        log(workers5, nodeToHiveSplitsMap5);

        validateNodeMap(dispatcherSplits, workers5, nodeToHiveSplitsMap5);

        workers5.forEach(node -> {
            Set<DispatcherSplit> splitSet1 = nodeToHiveSplitsMap4.get(node);
            Set<DispatcherSplit> splitSet2 = nodeToHiveSplitsMap5.get(node);
            assertThat(splitSet2.size()).isGreaterThanOrEqualTo(splitSet1.size());
            assertThat(splitSet2).containsAll(splitSet1);
        });

        // 2 nodes
        List<Node> workers6 = Lists.newArrayList(workers5).subList(1, 3);

        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(workers6);
        connectorSplitNodeDistributor = new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager);

        Map<Node, Set<DispatcherSplit>> nodeToHiveSplitsMap6 = getNodeToPathsMap(dispatcherSplits,
                connectorTransformer,
                workers6);

        log(workers6, nodeToHiveSplitsMap6);

        validateNodeMap(dispatcherSplits, workers6, nodeToHiveSplitsMap6);

        workers6.forEach(node -> {
            Set<DispatcherSplit> hiveSplitSet1 = nodeToHiveSplitsMap5.get(node);
            Set<DispatcherSplit> hiveSplitSet2 = nodeToHiveSplitsMap6.get(node);
            assertThat(hiveSplitSet2.size()).isGreaterThanOrEqualTo(hiveSplitSet1.size());
            assertThat(hiveSplitSet2).containsAll(hiveSplitSet1);
        });
    }

    @Test
    public void testGetHostAddressForSplitLastWorker()
    {
        List<DispatcherSplit> splits = IntStream.range(0, 100)
                .mapToObj(i -> {
                    DispatcherSplit split = mock(DispatcherSplit.class);
                    when(split.getPath()).thenReturn(UUID.randomUUID().toString());
                    when(split.getStart()).thenReturn(0L);
                    when(split.getLength()).thenReturn((long) random.nextInt(1000));
                    return split;
                })
                .collect(Collectors.toList());

        TestingConnectorProxiedConnectorTransformer connectorTransformer = new TestingConnectorProxiedConnectorTransformer();

        // 3 nodes
        List<Node> workers1 = IntStream.range(10, 13)
                .mapToObj(this::getInternalNode)
                .collect(Collectors.toList());

        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(workers1);
        connectorSplitNodeDistributor = new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager);

        Map<Node, Set<DispatcherSplit>> nodeToHiveSplitsMap1 = getNodeToPathsMap(splits,
                connectorTransformer,
                workers1);

        log(workers1, nodeToHiveSplitsMap1);

        validateNodeMap(splits, workers1, nodeToHiveSplitsMap1);

        // 2 nodes
        List<Node> workers2 = Lists.newArrayList(workers1).subList(0, 2);

        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(workers2);
        connectorSplitNodeDistributor = new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager);

        Map<Node, Set<DispatcherSplit>> nodeToHiveSplitsMap2 = getNodeToPathsMap(splits,
                connectorTransformer,
                workers2);

        log(workers2, nodeToHiveSplitsMap2);

        validateNodeMap(splits, workers2, nodeToHiveSplitsMap2);

        workers2.forEach(node -> {
            Set<DispatcherSplit> hiveSplitSet1 = nodeToHiveSplitsMap1.get(node);
            Set<DispatcherSplit> hiveSplitSet2 = nodeToHiveSplitsMap2.get(node);
            assertThat(hiveSplitSet2.size()).isGreaterThanOrEqualTo(hiveSplitSet1.size());
            assertThat(hiveSplitSet2).containsAll(hiveSplitSet1);
        });

        // 3 nodes
        Node newWorker = getInternalNode(14);
        List<Node> workers3 = Stream.concat(Stream.of(newWorker),
                        workers2.stream())
                .collect(Collectors.toList());

        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(workers3);
        connectorSplitNodeDistributor = new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager);

        Map<Node, Set<DispatcherSplit>> nodeToHiveSplitsMap3 = getNodeToPathsMap(splits,
                connectorTransformer,
                workers3);

        log(workers3, nodeToHiveSplitsMap3);

        validateNodeMap(splits, workers3, nodeToHiveSplitsMap3);
        List<Node> worker3AsList = Lists.newArrayList(workers3);
        //make sure "old" nodes don't exchange splits between them
        assertThat(nodeToHiveSplitsMap3.get(worker3AsList.get(1))).hasSizeGreaterThan(10)
                .doesNotContainAnyElementsOf(nodeToHiveSplitsMap3.get(worker3AsList.get(2)));
        assertThat(nodeToHiveSplitsMap3.get(worker3AsList.get(2))).hasSizeGreaterThan(10)
                .doesNotContainAnyElementsOf(nodeToHiveSplitsMap3.get(worker3AsList.get(1)));
        assertThat(nodeToHiveSplitsMap3.get(worker3AsList.get(0))).hasSizeGreaterThan(10);

        // 4 nodes
        newWorker = getInternalNode(15);
        List<Node> workers4 = Stream.concat(Stream.of(newWorker),
                        workers3.stream())
                .collect(Collectors.toList());

        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(workers4);
        connectorSplitNodeDistributor = new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager);

        Map<Node, Set<DispatcherSplit>> nodeToHiveSplitsMap4 = getNodeToPathsMap(splits,
                connectorTransformer,
                workers4);

        log(workers4, nodeToHiveSplitsMap4);

        validateNodeMap(splits, workers4, nodeToHiveSplitsMap4);

        workers3.forEach(node -> {
            Set<DispatcherSplit> hiveSplitSet3 = nodeToHiveSplitsMap3.get(node);
            Set<DispatcherSplit> hiveSplitSet4 = nodeToHiveSplitsMap4.get(node);
            assertThat(hiveSplitSet4.size()).isLessThanOrEqualTo(hiveSplitSet3.size());
            assertThat(hiveSplitSet3).containsAll(hiveSplitSet4);
        });
    }

    private Map<Node, Set<DispatcherSplit>> getNodeToPathsMap(List<DispatcherSplit> splits,
            DispatcherProxiedConnectorTransformer connectorTransformer,
            List<Node> nodes)
    {
        return splits.stream()
                .collect(groupingBy(hiveSplit -> {
                    HostAddress hostAddress = connectorTransformer.getHostAddressForSplit(
                                    connectorTransformer.getSplitKey(hiveSplit.getPath(), hiveSplit.getStart(), hiveSplit.getLength()),
                                    connectorSplitNodeDistributor)
                            .stream()
                            .collect(MoreCollectors.onlyElement());
                    return nodes.stream()
                            .filter(node -> hostAddress.equals(node.getHostAndPort()))
                            .findFirst()
                            .orElseThrow();
                }, Collectors.toSet()));
    }

    private void log(List<Node> workers,
            Map<Node, Set<DispatcherSplit>> nodeHiveSplitMap)
    {
        logger.info("%s", workers);
        logger.info("%s", nodeHiveSplitMap.entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> entry.getKey().getNodeIdentifier(), entry -> entry.getValue().size())));
    }

    private Node getInternalNode(int i)
    {
        return NodeUtils.node(i, true);
    }

    private void validateNodeMap(List<DispatcherSplit> splits,
            List<Node> nodes,
            Map<Node, Set<DispatcherSplit>> nodeToSplitsMap)
    {
        assertThat(nodeToSplitsMap.keySet().size()).isEqualTo(nodes.size());
        assertThat(nodeToSplitsMap.values().stream().map(Collection::size).reduce(0, Integer::sum))
                .isEqualTo(splits.size());
        nodeToSplitsMap.values().forEach(pathSet -> assertThat(pathSet.size()).isGreaterThan(0));
    }
}
