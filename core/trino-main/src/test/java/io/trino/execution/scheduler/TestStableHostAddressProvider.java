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

import com.google.common.collect.ImmutableSet;
import io.trino.node.InternalNode;
import io.trino.node.TestingInternalNodeManager;
import io.trino.spi.HostAddress;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.NodeVersion.UNKNOWN;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStableHostAddressProvider
{
    private static final int KEY_COUNT = 10_000;
    private static final int PREFERRED_HOSTS = 2;

    @Test
    public void testEmptyWhenNoWorkerNodes()
    {
        StableHostAddressProvider provider = createProvider(TestingInternalNodeManager.createDefault());
        assertThat(provider.getHosts("s3://bucket/data/file.parquet:0:1024")).isEmpty();
    }

    @Test
    public void testDeterministic()
    {
        StableHostAddressProvider provider = createProvider(10);
        for (String key : keys()) {
            assertThat(provider.getHosts(key)).isEqualTo(provider.getHosts(key));
        }
    }

    @Test
    public void testHostCountCappedByNodeCount()
    {
        StableHostAddressProvider singleNode = createProvider(1);
        assertThat(singleNode.getHosts("s3://bucket/data/file.parquet:0:1024")).hasSize(1);

        StableHostAddressProvider manyNodes = createProvider(10);
        for (String key : keys()) {
            assertThat(manyNodes.getHosts(key)).hasSize(PREFERRED_HOSTS);
        }
    }

    @Test
    public void testAllNodesUsed()
    {
        int nodeCount = 10;
        StableHostAddressProvider provider = createProvider(nodeCount);
        long distinctHosts = keys().stream()
                .flatMap(key -> provider.getHosts(key).stream())
                .distinct()
                .count();
        assertThat(distinctHosts).isEqualTo(nodeCount);
    }

    @Test
    public void testRoughlyUniformDistribution()
    {
        int nodeCount = 10;
        StableHostAddressProvider provider = createProvider(nodeCount);
        Map<HostAddress, Long> perHost = keys().stream()
                .flatMap(key -> provider.getHosts(key).stream())
                .collect(groupingBy(host -> host, counting()));
        assertThat(perHost).hasSize(nodeCount);
        long max = perHost.values().stream().mapToLong(Long::longValue).max().orElseThrow();
        long min = perHost.values().stream().mapToLong(Long::longValue).min().orElseThrow();
        // rendezvous hashing spreads keys evenly; no node should absorb a disproportionate share
        assertThat((double) max / min).isLessThan(1.2);
    }

    @Test
    public void testAddingNodeReassignsFewKeys()
    {
        TestingInternalNodeManager nodeManager = createNodeManager(20);
        StableHostAddressProvider provider = createProvider(nodeManager);
        Map<String, List<HostAddress>> before = snapshotAssignments(provider);

        nodeManager.addNodes(node(20));
        provider.refreshSnapshot();

        assertReassignedFraction(before, snapshotAssignments(provider));
    }

    @Test
    public void testRemovingNodeReassignsFewKeys()
    {
        TestingInternalNodeManager nodeManager = createNodeManager(20);
        StableHostAddressProvider provider = createProvider(nodeManager);
        Map<String, List<HostAddress>> before = snapshotAssignments(provider);

        nodeManager.removeNode(node(0));
        provider.refreshSnapshot();

        assertReassignedFraction(before, snapshotAssignments(provider));
    }

    // Rendezvous hashing must move only keys touching the added/removed node, not reshuffle everything.
    private static void assertReassignedFraction(Map<String, List<HostAddress>> before, Map<String, List<HostAddress>> after)
    {
        long changed = before.keySet().stream()
                .filter(key -> !before.get(key).equals(after.get(key)))
                .count();
        double fraction = (double) changed / before.size();
        assertThat(changed).isPositive();
        assertThat(fraction).isLessThan(0.25);
    }

    private static Map<String, List<HostAddress>> snapshotAssignments(StableHostAddressProvider provider)
    {
        return keys().stream()
                .collect(toImmutableMap(key -> key, provider::getHosts));
    }

    private static StableHostAddressProvider createProvider(int nodeCount)
    {
        return createProvider(createNodeManager(nodeCount));
    }

    private static StableHostAddressProvider createProvider(TestingInternalNodeManager nodeManager)
    {
        return new StableHostAddressProvider(
                nodeManager,
                new StableHostAddressProviderConfig().setPreferredHostsCount(PREFERRED_HOSTS));
    }

    private static TestingInternalNodeManager createNodeManager(int nodeCount)
    {
        ImmutableSet.Builder<InternalNode> nodes = ImmutableSet.builder();
        for (int i = 0; i < nodeCount; i++) {
            nodes.add(node(i));
        }
        return TestingInternalNodeManager.createDefault(nodes.build());
    }

    private static InternalNode node(int index)
    {
        return new InternalNode("node" + index, URI.create("http://node" + index + ":8080"), UNKNOWN, false);
    }

    private static List<String> keys()
    {
        return range(0, KEY_COUNT)
                .mapToObj(i -> "s3://bucket/schema/table/data/file_" + i + ".parquet:" + ((long) i * 128 * 1024 * 1024) + ":" + (128 * 1024 * 1024))
                .collect(toImmutableList());
    }
}
