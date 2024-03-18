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
package io.trino.plugin.varada.storage.splits;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.plugin.varada.CoordinatorNodeManager;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.util.NodeUtils;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.plugin.varada.storage.splits.ConnectorSplitSessionNodeDistributor.RANDOM_HASHING;
import static java.lang.String.format;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("UnstableApiUsage")
public class ConnectorSplitNodeDistributorTest
{
    private static final Logger logger = Logger.get(ConnectorSplitNodeDistributorTest.class);

    private final List<String> keys = IntStream.range(0, 100)
            .mapToObj(i -> format("%d/Integer/toString/%d", i, i))
            .toList();

    private CoordinatorNodeManager coordinatorNodeManager;
    private GlobalConfiguration globalConfiguration;
    private ConnectorSplitNodeDistributor connectorSplitNodeDistributor;

    @BeforeEach
    @SuppressWarnings("MockNotUsedInProduction")
    public void before()
    {
        coordinatorNodeManager = mock(CoordinatorNodeManager.class);
        globalConfiguration = new GlobalConfiguration();
    }

    @Test
    public void testGetNodeBuckets()
    {
        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(ImmutableList.of());
        connectorSplitNodeDistributor = new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager);

        List<String> nodeIds = IntStream.range(1, 6)
                .mapToObj(Integer::toString)
                .collect(toCollection(ArrayList::new));
        Map<String, List<Integer>> origMap = nodeIds.stream()
                .collect(Collectors.toMap(Function.identity(), nodeId -> IntStream.range(0, globalConfiguration.getConsistentSplitBucketsPerWorker())
                        .mapToObj(i -> ((ConnectorSplitConsistentHashNodeDistributor) connectorSplitNodeDistributor).getNodeBucket(nodeId, i))
                        .collect(Collectors.toList())));

        origMap.forEach((key, value) -> logger.debug("%s=%s", key, value.stream().sorted().collect(Collectors.toList())));

        IntStream.range(0, 10).forEach(index -> {
            Collections.shuffle(nodeIds);
            Map<String, List<Integer>> currentMap = nodeIds.stream()
                    .collect(Collectors.toMap(Function.identity(), nodeId -> IntStream.range(0, globalConfiguration.getConsistentSplitBucketsPerWorker())
                            .mapToObj(i -> ((ConnectorSplitConsistentHashNodeDistributor) connectorSplitNodeDistributor).getNodeBucket(nodeId, i))
                            .collect(Collectors.toList())));
            assertThat(currentMap).isEqualTo(origMap);
        });
    }

    @Test
    public void testIdentitiesHashValidation()
    {
        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(ImmutableList.of());
        connectorSplitNodeDistributor = new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager);

        assertThatThrownBy(() -> connectorSplitNodeDistributor.getNode(UUID.randomUUID().toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("no available nodes");
    }

    @Test
    public void testConsistentHash()
    {
        Map<String, Node> nodesMap = IntStream.range(5, 10)
                .boxed()
                .map(this::createNode)
                .collect(Collectors.toMap(Node::getNodeIdentifier, Function.identity()));

        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(new ArrayList<>(nodesMap.values()));
        connectorSplitNodeDistributor = new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager);

        Map<String, String> keyToNodeMap1 = keys.stream()
                .collect(Collectors.toMap(Function.identity(), key -> connectorSplitNodeDistributor.getNode(key).getNodeIdentifier()));
        keyToNodeMap1.values().forEach(value -> assertThat(nodesMap).containsKey(value));
        IntStream.range(0, 10).forEach(i -> {
            Map<String, String> keyToNodeMap2 = keys.stream()
                    .collect(Collectors.toMap(Function.identity(), key -> connectorSplitNodeDistributor.getNode(key).getNodeIdentifier()));
            keyToNodeMap2.values().forEach(value -> assertThat(nodesMap).containsKey(value));
            assertThat(keyToNodeMap2).isEqualTo(keyToNodeMap1);
        });
    }

    @Disabled // this test is too long and should be used manually
    @Test
    public void testDistribution()
    {
        globalConfiguration.setConsistentSplitBucketsPerWorker(2048);
        PearsonsCorrelation pearsonsCorrelation = new PearsonsCorrelation();
        int numKeysToTest = 10240;
        int[] buckets = new int[numKeysToTest];
        for (int round = 0; round < 10; round++) {
            for (int i = 0; i < numKeysToTest; i++) {
                buckets[i] = (int) Math.round(Math.random() * 100000000);
            }
            for (int numNodes = 2; numNodes < 64; numNodes++) {
                Map<String, Node> nodesMap = IntStream.range(5, 5 + numNodes)
                        .boxed()
                        .map(this::createNode)
                        .collect(Collectors.toMap(Node::getNodeIdentifier, Function.identity()));
                when(coordinatorNodeManager.getWorkerNodes()).thenReturn(new ArrayList<>(nodesMap.values()));
                connectorSplitNodeDistributor = new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager);
                Map<String, Integer> nodeToCountMap = new HashMap<>(numKeysToTest);
                Map<String, Integer> nodeToRandMap = new HashMap<>(numKeysToTest);
                for (int i = 0; i < numKeysToTest; i++) {
                    String key = format("%d/Integer/toString/%d", buckets[i], buckets[i]);
                    String node = connectorSplitNodeDistributor.getNode(key).getNodeIdentifier();
                    nodeToCountMap.compute(node, (k, v) -> Objects.nonNull(v) ? v + 1 : 1);
                    key = new ArrayList<>(nodesMap.values()).get(buckets[i] % nodesMap.size()).getNodeIdentifier();
                    if (nodeToRandMap.containsKey(key)) {
                        nodeToRandMap.put(key, nodeToRandMap.get(key) + 1);
                    }
                    else {
                        nodeToRandMap.put(key, 1);
                    }
                }
                double[] hashArr = new double[nodeToCountMap.size()];
                double[] randArr = new double[nodeToCountMap.size()];
                int curr = 0;
                for (String key : nodeToCountMap.keySet()) {
                    hashArr[curr] = nodeToCountMap.get(key);
                    randArr[curr] = nodeToRandMap.get(key);
                    curr++;
                }
                Arrays.sort(hashArr);
                Arrays.sort(randArr);
                double score = pearsonsCorrelation.correlation(hashArr, randArr);
                if (score < 0.9) {
                    logger.info("NumNodes %d,  score %f", numNodes, score);
                }
            }
        }
    }

    @Test
    public void testConsistentHashRemoveAddSameNode()
    {
        Map<String, Node> nodesMap = IntStream.range(5, 10)
                .boxed()
                .map(this::createNode)
                .collect(Collectors.toMap(Node::getNodeIdentifier, Function.identity()));

        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(new ArrayList<>(nodesMap.values()));
        connectorSplitNodeDistributor = new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager);

        Map<String, String> keyToNodeMap1 = keys.stream()
                .collect(Collectors.toMap(Function.identity(), key -> connectorSplitNodeDistributor.getNode(key).getNodeIdentifier()));
        keyToNodeMap1.values().forEach(value -> assertThat(nodesMap).containsKey(value));

        Map<String, Set<String>> nodeToKeysMap1 = keyToNodeMap1.keySet()
                .stream()
                .collect(groupingBy(keyToNodeMap1::get, Collectors.toSet()));

        //remove one node (4 left)
        Node removedNode = nodesMap.remove(Integer.toString(5));

        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(new ArrayList<>(nodesMap.values()));
        connectorSplitNodeDistributor.updateNodeBucketsIfNeeded();

        Map<String, String> keyToNodeMap2 = keys.stream()
                .collect(Collectors.toMap(Function.identity(), key -> connectorSplitNodeDistributor.getNode(key).getNodeIdentifier()));
        keyToNodeMap2.values().forEach(value -> assertThat(nodesMap).containsKey(value));

        Map<String, Set<String>> nodeToKeysMap2 = keyToNodeMap2.keySet()
                .stream()
                .collect(groupingBy(keyToNodeMap1::get, Collectors.toSet()));

        nodeToKeysMap2.forEach((nodeIdentifier, keySet) -> assertThat(keySet).containsAll(nodeToKeysMap1.get(nodeIdentifier)));

        //add one node (5 left)
        nodesMap.put(removedNode.getNodeIdentifier(), removedNode);

        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(new ArrayList<>(nodesMap.values()));
        connectorSplitNodeDistributor.updateNodeBucketsIfNeeded();

        Map<String, String> keyToNodeMap3 = keys.stream()
                .collect(Collectors.toMap(Function.identity(), key -> connectorSplitNodeDistributor.getNode(key).getNodeIdentifier()));
        keyToNodeMap3.values().forEach(value -> assertThat(nodesMap).containsKey(value));

        Map<String, Set<String>> nodeToKeysMap3 = keyToNodeMap3.keySet()
                .stream()
                .collect(groupingBy(keyToNodeMap1::get, Collectors.toSet()));

        nodeToKeysMap3.forEach((nodeIdentifier, keySet) -> assertThat(keySet).isEqualTo(nodeToKeysMap1.get(nodeIdentifier)));
    }

    @Test
    public void testConsistentHashOrderedPyramid()
    {
        Map<String, Node> nodesMap = new HashMap<>();

        connectorSplitNodeDistributor = new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager);

        //add one node each time
        Map<String, Set<String>> nodeToKeysMap1 = assertAddNode(nodesMap, "a", Map.of());
        Map<String, Set<String>> nodeToKeysMap2 = assertAddNode(nodesMap, "b", nodeToKeysMap1);
        Map<String, Set<String>> nodeToKeysMap3 = assertAddNode(nodesMap, "c", nodeToKeysMap2);
        Map<String, Set<String>> nodeToKeysMap4 = assertAddNode(nodesMap, "d", nodeToKeysMap3);
        Map<String, Set<String>> nodeToKeysMap5 = assertAddNode(nodesMap, "e", nodeToKeysMap4);

        //remove one node each time
        assertThat(assertRemoveNode(nodesMap, "e", nodeToKeysMap5)).isEqualTo(nodeToKeysMap4);
        assertThat(assertRemoveNode(nodesMap, "d", nodeToKeysMap4)).isEqualTo(nodeToKeysMap3);
        assertThat(assertRemoveNode(nodesMap, "c", nodeToKeysMap3)).isEqualTo(nodeToKeysMap2);
        assertThat(assertRemoveNode(nodesMap, "b", nodeToKeysMap2)).isEqualTo(nodeToKeysMap1);
    }

    @Test
    public void testConsistentHashRandomPyramid()
    {
        List<String> nodeIds = new ArrayList<>(List.of("a", "c", "e", "b", "d")); // when starting from 0 we get duplicates in hashes
        Map<String, Node> nodesMap = new HashMap<>();

        connectorSplitNodeDistributor = new ConnectorSplitConsistentHashNodeDistributor(globalConfiguration, coordinatorNodeManager);

        //add one node each time
        Map<String, Set<String>> nodeToKeysMap1 = assertAddNode(nodesMap, nodeIds.remove(0), Map.of());
        Map<String, Set<String>> nodeToKeysMap2 = assertAddNode(nodesMap, nodeIds.remove(0), nodeToKeysMap1);
        Map<String, Set<String>> nodeToKeysMap3 = assertAddNode(nodesMap, nodeIds.remove(0), nodeToKeysMap2);
        Map<String, Set<String>> nodeToKeysMap4 = assertAddNode(nodesMap, nodeIds.remove(0), nodeToKeysMap3);
        Map<String, Set<String>> nodeToKeysMap5 = assertAddNode(nodesMap, nodeIds.remove(0), nodeToKeysMap4);

        //remove one node each time
        nodeIds = new ArrayList<>(List.of("c", "e", "a", "b", "d"));

        Map<String, Set<String>> nodeToKeysMap6 = assertRemoveNode(nodesMap, nodeIds.remove(0), nodeToKeysMap5);
        nodeToKeysMap6.forEach((nodeIdentifier, keySet) -> assertThat(keySet).containsAll(nodeToKeysMap5.get(nodeIdentifier)));

        Map<String, Set<String>> nodeToKeysMap7 = assertRemoveNode(nodesMap, nodeIds.remove(0), nodeToKeysMap6);
        nodeToKeysMap7.forEach((nodeIdentifier, keySet) -> assertThat(keySet).containsAll(nodeToKeysMap6.get(nodeIdentifier)));

        Map<String, Set<String>> nodeToKeysMap8 = assertRemoveNode(nodesMap, nodeIds.remove(0), nodeToKeysMap7);
        nodeToKeysMap8.forEach((nodeIdentifier, keySet) -> assertThat(keySet).containsAll(nodeToKeysMap7.get(nodeIdentifier)));

        Map<String, Set<String>> nodeToKeysMap9 = assertRemoveNode(nodesMap, nodeIds.remove(0), nodeToKeysMap8);
        nodeToKeysMap9.forEach((nodeIdentifier, keySet) -> assertThat(keySet).containsAll(nodeToKeysMap8.get(nodeIdentifier)));
    }

    @Test
    public void testConnectorSplitSessionHashTestSession()
    {
        String nodeIdGenericFlag = "-someId";
        Map<String, Node> nodesMap = IntStream.range(0, 5)
                .boxed()
                .map(x -> createNode(x + nodeIdGenericFlag))
                .collect(Collectors.toMap(Node::getNodeIdentifier, Function.identity()));

        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(new ArrayList<>(nodesMap.values()));
        NodeManager nodeManager = mock(NodeManager.class);
        when(nodeManager.getWorkerNodes()).thenReturn(new HashSet<>(nodesMap.values()));

        int expectedNodeNumber = 1;
        //test choose worker by position in list
        connectorSplitNodeDistributor = new ConnectorSplitSessionNodeDistributor(nodeManager, String.valueOf(expectedNodeNumber));
        String ignore = "someKey";
        Node node = connectorSplitNodeDistributor.getNode(ignore);
        assertThat(node).isNotNull();

        //test random hashing with -1 RANDOM_HASHING flag
        connectorSplitNodeDistributor = new ConnectorSplitSessionNodeDistributor(nodeManager, String.valueOf(RANDOM_HASHING));
        Node nodeResult = connectorSplitNodeDistributor.getNode(ignore);
        String prevValue = nodeResult.getNodeIdentifier();
        boolean isRandomWorking = false;
        for (int i = 0; i < 100; i++) {
            nodeResult = connectorSplitNodeDistributor.getNode(ignore);
            if (!nodeResult.getNodeIdentifier().equals(prevValue)) {
                isRandomWorking = true;
            }
        }
        assertThat(isRandomWorking).isTrue();

        //testByNodeIdentifier
        String expectedNodeId = "2" + nodeIdGenericFlag;
        connectorSplitNodeDistributor = new ConnectorSplitSessionNodeDistributor(nodeManager, expectedNodeId);
        nodeResult = connectorSplitNodeDistributor.getNode(ignore);
        assertThat(nodeResult.getNodeIdentifier()).isEqualTo(expectedNodeId);

        //test invalid number
        int invalidNumber = 20;
        connectorSplitNodeDistributor = new ConnectorSplitSessionNodeDistributor(nodeManager, String.valueOf(invalidNumber));
        Assertions.assertThrows(IndexOutOfBoundsException.class, () -> connectorSplitNodeDistributor.getNode(ignore));
    }

    private Map<String, Set<String>> assertRemoveNode(Map<String, Node> nodesMap,
            String nodeId,
            Map<String, Set<String>> previousNodeToKeysMap)
    {
        nodesMap.remove(nodeId);

        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(new ArrayList<>(nodesMap.values()));
        connectorSplitNodeDistributor.updateNodeBucketsIfNeeded();

        Map<String, String> keyToNodeMap = keys.stream()
                .collect(Collectors.toMap(Function.identity(), key -> connectorSplitNodeDistributor.getNode(key).getNodeIdentifier()));

        Map<String, Set<String>> currentNodeToKeysMap = keyToNodeMap.keySet()
                .stream()
                .collect(groupingBy(keyToNodeMap::get, Collectors.toSet()));

        String logMessage = currentNodeToKeysMap.entrySet()
                .stream()
                .map(entry -> format("%s=%d", entry.getKey(), entry.getValue().size()))
                .collect(Collectors.joining(","));
        logger.debug("assertRemoveNode->currentNodeToKeysMap:\n%s", logMessage);

        assertThat(currentNodeToKeysMap.keySet()).containsAll(nodesMap.keySet());

        currentNodeToKeysMap.entrySet()
                .stream()
                .filter(entry -> previousNodeToKeysMap.containsKey(entry.getKey()))
                .forEach(entry -> {
                    Set<String> previousNodeToKeys = previousNodeToKeysMap.get(entry.getKey());
                    assertThat(entry.getValue()).containsAll(previousNodeToKeys);
                });

        return currentNodeToKeysMap;
    }

    private Map<String, Set<String>> assertAddNode(Map<String, Node> nodesMap,
            String nodeId,
            Map<String, Set<String>> previousNodeToKeysMap)
    {
        Node addedNode = createNode(nodeId);
        nodesMap.put(addedNode.getNodeIdentifier(), addedNode);

        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(new ArrayList<>(nodesMap.values()));
        connectorSplitNodeDistributor.updateNodeBucketsIfNeeded();

        Map<String, String> keyToNodeMap = keys.stream()
                .collect(Collectors.toMap(Function.identity(), key -> connectorSplitNodeDistributor.getNode(key).getNodeIdentifier()));

        Map<String, Set<String>> currentNodeToKeysMap = keyToNodeMap.keySet()
                .stream()
                .collect(groupingBy(keyToNodeMap::get, Collectors.toSet()));

        logger.debug("nodesMap=%s", nodesMap.keySet());
        logger.debug("currentNodeToKeysMap=%s", currentNodeToKeysMap.keySet());
        assertThat(currentNodeToKeysMap.keySet()).containsAll(nodesMap.keySet());

        currentNodeToKeysMap.entrySet()
                .stream()
                .filter(entry -> previousNodeToKeysMap.containsKey(entry.getKey()))
                .forEach(entry -> {
                    Set<String> previousNodeToKeys = previousNodeToKeysMap.get(entry.getKey());
                    assertThat(previousNodeToKeys).containsAll(entry.getValue());
                });

        String logMessage = currentNodeToKeysMap.entrySet()
                .stream()
                .map(entry -> format("%s=%d", entry.getKey(), entry.getValue().size()))
                .collect(Collectors.joining(","));
        logger.debug("assertAddNode->currentNodeToKeysMap: node %s\n%s", nodeId, logMessage);

        return currentNodeToKeysMap;
    }

    private Node createNode(String id)
    {
        return NodeUtils.node(id, false);
    }

    private Node createNode(Integer i)
    {
        return NodeUtils.node(i, false);
    }
}
