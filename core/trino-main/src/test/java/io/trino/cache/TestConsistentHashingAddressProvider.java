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
package io.trino.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.trino.client.NodeVersion;
import io.trino.metadata.InternalNode;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.type.Type;
import io.trino.testing.TestingNodeManager;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.cache.PlanSignature.canonicalizePlanSignature;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConsistentHashingAddressProvider
{
    private static final CacheColumnId COLUMN1 = new CacheColumnId("col1");
    private static final CacheColumnId COLUMN2 = new CacheColumnId("col2");
    private static final CacheSplitId SPLIT1 = new CacheSplitId("split1");
    private static final CacheSplitId SPLIT2 = new CacheSplitId("split2");

    @Test
    public void testAddressProvider()
    {
        TestingNodeManager nodeManager = new TestingNodeManager();
        nodeManager.addNode(node("node1"));
        nodeManager.addNode(node("node2"));
        nodeManager.addNode(node("node3"));
        nodeManager.addNode(node("node4"));

        ConsistentHashingAddressProvider addressProvider = new ConsistentHashingAddressProvider(nodeManager);
        String signature1 = canonicalizePlanSignature(createPlanSignature("signature1", COLUMN1)).toString();
        String signature2 = canonicalizePlanSignature(createPlanSignature("signature2", COLUMN1)).toString();
        String signature3 = canonicalizePlanSignature(createPlanSignature("signature1", COLUMN2)).toString();

        // assert that both different signature or split id affects preferred address
        assertThat(getPreferredAddress(addressProvider, signature1, SPLIT1))
                .isNotEqualTo(getPreferredAddress(addressProvider, signature1, SPLIT2));
        assertThat(getPreferredAddress(addressProvider, signature2, SPLIT1))
                .isNotEqualTo(getPreferredAddress(addressProvider, signature2, SPLIT2));
        assertThat(getPreferredAddress(addressProvider, signature1, SPLIT1))
                .isNotEqualTo(getPreferredAddress(addressProvider, signature2, SPLIT1));

        // make sure that columns don't affect preferred address
        assertThat(getPreferredAddress(addressProvider, signature1, SPLIT1))
                .isEqualTo(getPreferredAddress(addressProvider, signature3, SPLIT1));
        assertThat(getPreferredAddress(addressProvider, signature1, SPLIT2))
                .isEqualTo(getPreferredAddress(addressProvider, signature3, SPLIT2));

        assertFairDistribution(addressProvider, signature1, nodeManager.getWorkerNodes());

        Map<String, Set<Integer>> distribution = getDistribution(addressProvider, signature1);
        nodeManager.removeNode(node("node2"));
        addressProvider.refreshHashRing();
        assertFairDistribution(addressProvider, signature1, nodeManager.getWorkerNodes());
        Map<String, Set<Integer>> removeOne = getDistribution(addressProvider, signature1);
        assertMinimalRedistribution(distribution, removeOne);

        nodeManager.addNode(node("node5"));
        addressProvider.refreshHashRing();
        assertFairDistribution(addressProvider, signature1, nodeManager.getWorkerNodes());
        Map<String, Set<Integer>> addOne = getDistribution(addressProvider, signature1);
        assertMinimalRedistribution(removeOne, addOne);
    }

    private static HostAddress getPreferredAddress(
            ConsistentHashingAddressProvider addressProvider,
            String planSignature,
            CacheSplitId splitId)
    {
        return addressProvider.getPreferredAddress(planSignature + splitId);
    }

    private static void assertFairDistribution(ConsistentHashingAddressProvider addressProvider, String planSignature, Set<Node> nodeNames)
    {
        int totalSplits = 1000;
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < totalSplits; i++) {
            counts.merge(getPreferredAddress(addressProvider, planSignature, new CacheSplitId("split" + i)).getHostText(), 1, Math::addExact);
        }
        assertThat(counts.keySet()).isEqualTo(nodeNames.stream().map(node -> node.getHostAndPort().getHostText()).collect(toImmutableSet()));
        int expectedSplitsPerNode = totalSplits / nodeNames.size();
        counts.values().forEach(splitsPerNode -> assertThat(splitsPerNode).isCloseTo(expectedSplitsPerNode, Percentage.withPercentage(20)));
    }

    private void assertMinimalRedistribution(Map<String, Set<Integer>> oldDistribution, Map<String, Set<Integer>> newDistribution)
    {
        oldDistribution.entrySet().stream().filter(e -> newDistribution.containsKey(e.getKey())).forEach(entry -> {
            Set<Integer> oldNodeBuckets = entry.getValue();
            Set<Integer> newNodeBuckets = newDistribution.get(entry.getKey());
            int redDistributedBucketsCount = Sets.difference(oldNodeBuckets, newNodeBuckets).size();
            int oldClusterSize = oldDistribution.size();
            assertThat(redDistributedBucketsCount).isLessThan(oldNodeBuckets.size() / oldClusterSize);
        });
    }

    private Map<String, Set<Integer>> getDistribution(ConsistentHashingAddressProvider addressProvider, String planSignature)
    {
        int totalSplits = 1000;
        Map<String, Set<Integer>> distribution = new HashMap<>();
        for (int i = 0; i < totalSplits; i++) {
            String host = getPreferredAddress(addressProvider, planSignature, new CacheSplitId("split" + i)).getHostText();
            distribution.computeIfAbsent(host, (key) -> new HashSet<>()).add(i);
        }
        return distribution;
    }

    private static Node node(String nodeName)
    {
        return new InternalNode(nodeName, URI.create("http://" + nodeName + "/"), NodeVersion.UNKNOWN, false);
    }

    private static PlanSignature createPlanSignature(String signature, CacheColumnId... ids)
    {
        return new PlanSignature(
                new SignatureKey(signature),
                Optional.empty(),
                ImmutableList.copyOf(ids),
                Stream.of(ids).map(ignore -> (Type) INTEGER).collect(toImmutableList()));
    }
}
