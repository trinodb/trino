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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.trino.client.NodeVersion;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.split.MockSplitSource;
import io.trino.split.SplitSource;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.split.MockSplitSource.Action.FINISH;
import static io.trino.split.SplitSource.SplitBatch;
import static java.lang.Math.abs;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCacheSplitAdmissionController
{
    @Test
    public void testMinSplitProcessedPerWorkerThreshold()
            throws ExecutionException, InterruptedException
    {
        int minSplitSeparation = 5;
        int availableSplits = 50;
        int minSplitBatchSize = 20;
        MinSeparationSplitAdmissionController controller = new MinSeparationSplitAdmissionController(minSplitSeparation);
        Function<String, Optional<HostAddress>> addressProvider = createAddressProvider(2);
        SplitSource cacheSplitSourceA = createCacheSplitSource(addressProvider, controller, availableSplits, minSplitBatchSize);
        SplitSource cacheSplitSourceB = createCacheSplitSource(addressProvider, controller, availableSplits, minSplitBatchSize);
        SplitSource cacheSplitSourceC = createCacheSplitSource(addressProvider, controller, availableSplits, minSplitBatchSize);

        // Batch 1:
        SplitBatch batchA1 = cacheSplitSourceA.getNextBatch(10).get();
        assertCacheSplitIds(batchA1, ImmutableMap.of(
                "node0", createCacheSplitIds(IntStream.of(1, 3, 5, 7, 9)),
                "node1", createCacheSplitIds(IntStream.of(0, 2, 4, 6, 8))));
        assertThat(batchA1.isLastBatch()).isFalse();

        SplitBatch batchB1 = cacheSplitSourceB.getNextBatch(10).get();
        assertCacheSplitIds(batchB1, ImmutableMap.of(
                "node0", createCacheSplitIds(IntStream.of(10, 12, 14, 16, 18)),
                "node1", createCacheSplitIds(IntStream.of(11, 13, 15, 17, 19))));
        assertThat(batchB1.isLastBatch()).isFalse();

        SplitBatch batchC1 = cacheSplitSourceC.getNextBatch(10).get();
        assertCacheSplitIds(batchC1, ImmutableMap.of(
                "node0", createCacheSplitIds(IntStream.of(21, 23, 25, 27, 29)),
                "node1", createCacheSplitIds(IntStream.of(20, 22, 24, 26, 28))));
        assertThat(batchC1.isLastBatch()).isFalse();

        controller.splitsScheduled(batchA1.getSplits());
        controller.splitsScheduled(batchB1.getSplits());
        controller.splitsScheduled(batchC1.getSplits());

        // Batch 2:
        SplitBatch batchA2 = cacheSplitSourceA.getNextBatch(20).get();
        // Since we have crossed the gap limit for some splits, we will get those splits from queue
        assertCacheSplitIds(batchA2, ImmutableMap.of(
                "node0", createCacheSplitIds(IntStream.of(10, 12, 14, 16, 18, 21, 30, 32, 34, 36)),
                "node1", createCacheSplitIds(IntStream.of(11, 13, 15, 17, 19, 20, 31, 33, 35, 37))));
        assertThat(batchA2.isLastBatch()).isFalse();

        SplitBatch batchB2 = cacheSplitSourceB.getNextBatch(19).get();
        // Since we have crossed the gap limit for some splits, we will get those splits from queue
        assertCacheSplitIds(batchB2, ImmutableMap.of(
                "node0", createCacheSplitIds(IntStream.of(1, 3, 5, 7, 9, 21, 38, 41, 43)),
                "node1", createCacheSplitIds(IntStream.of(0, 2, 4, 6, 8, 20, 39, 40, 42, 44))));
        assertThat(batchB2.isLastBatch()).isFalse();

        SplitBatch batchC2 = cacheSplitSourceC.getNextBatch(20).get();
        // Since we have crossed the gap limit for some splits, we will get those splits from queue
        assertCacheSplitIds(batchC2, ImmutableMap.of(
                "node0", createCacheSplitIds(IntStream.of(1, 3, 5, 7, 9, 10, 12, 14, 16, 18)),
                "node1", createCacheSplitIds(IntStream.of(0, 2, 4, 6, 8, 11, 13, 15, 17, 19))));
        assertThat(batchC2.isLastBatch()).isFalse();

        controller.splitsScheduled(batchA2.getSplits());

        // Batch3
        // Since we have scheduled all the splits, we will get the remaining splits from the queue
        SplitBatch batchA3 = cacheSplitSourceA.getNextBatch(30).get();
        assertCacheSplitIds(batchA3, ImmutableMap.of(
                "node0", createCacheSplitIds(IntStream.of(23, 25, 27, 29, 45, 47, 49, 38, 41, 43)),
                "node1", createCacheSplitIds(IntStream.of(22, 24, 26, 28, 46, 48, 39, 40, 42, 44))));
        assertThat(batchA3.isLastBatch()).isTrue();

        SplitBatch batchB3 = cacheSplitSourceB.getNextBatch(30).get();
        assertCacheSplitIds(batchB3, ImmutableMap.of(
                "node0", createCacheSplitIds(IntStream.of(23, 25, 27, 29, 30, 32, 34, 36, 45, 47, 49)),
                "node1", createCacheSplitIds(IntStream.of(22, 24, 26, 28, 31, 33, 35, 37, 46, 48))));
        assertThat(batchB3.isLastBatch()).isTrue();

        SplitBatch batchC3 = cacheSplitSourceC.getNextBatch(30).get();
        assertCacheSplitIds(batchC3, ImmutableMap.of(
                "node0", createCacheSplitIds(IntStream.of(30, 32, 34, 36, 38, 41, 43, 45, 47, 49)),
                "node1", createCacheSplitIds(IntStream.of(31, 33, 35, 37, 39, 40, 42, 44, 46, 48))));
        assertThat(batchC3.isLastBatch()).isTrue();

        // Assert that all splits are scheduled
        assertThat(batchA1.getSplits().size() + batchA2.getSplits().size() + batchA3.getSplits().size()).isEqualTo(50);
        assertThat(batchB1.getSplits().size() + batchB2.getSplits().size() + batchB3.getSplits().size()).isEqualTo(50);
        assertThat(batchC1.getSplits().size() + batchC2.getSplits().size() + batchC3.getSplits().size()).isEqualTo(50);

        cacheSplitSourceA.close();
        cacheSplitSourceB.close();
        cacheSplitSourceC.close();
    }

    @Test
    public void testSplitsFromQueueAreFairlyDistributedAmongNodes()
            throws ExecutionException, InterruptedException
    {
        int minSplitSeparation = 1;
        int availableSplits = 100;
        int minSplitBatchSize = 40;
        MinSeparationSplitAdmissionController controller = new MinSeparationSplitAdmissionController(minSplitSeparation);
        Function<String, Optional<HostAddress>> addressProvider = createAddressProvider(3);
        SplitSource cacheSplitSourceA = createCacheSplitSource(addressProvider, controller, availableSplits, minSplitBatchSize);
        SplitSource cacheSplitSourceB = createCacheSplitSource(addressProvider, controller, availableSplits, minSplitBatchSize);

        // Batch 1:
        SplitBatch batchA1 = cacheSplitSourceA.getNextBatch(40).get();
        assertCacheSplitIds(batchA1, createCacheSplitIds(IntStream.range(0, 40)));
        assertThat(batchA1.isLastBatch()).isFalse();

        SplitBatch batchB1 = cacheSplitSourceB.getNextBatch(40).get();
        assertCacheSplitIds(batchB1, createCacheSplitIds(IntStream.range(40, 80)));
        assertThat(batchB1.isLastBatch()).isFalse();

        controller.splitsScheduled(batchA1.getSplits());
        controller.splitsScheduled(batchB1.getSplits());

        // Batch 2:
        // This will be mostly from the queue since the minSplitSeparation is quite low, and we have already
        // scheduled 30 splits

        // We will get 5 splits from the queue. However, these 5 splits should belong to diverse nodes such that
        // we don't end up scheduling all the splits from a single node to avoid skewness.
        SplitBatch batchB2 = cacheSplitSourceB.getNextBatch(5).get();
        // splits are diverse and belong to all available nodes
        assertHostAddress(batchB2, ImmutableSet.of("node0", "node1", "node2"));
        assertThat(batchB1.isLastBatch()).isFalse();

        cacheSplitSourceA.close();
        cacheSplitSourceB.close();
    }

    private static List<CacheSplitId> createCacheSplitIds(IntStream splitIds)
    {
        return splitIds.boxed()
                .map(i -> new CacheSplitId("split" + i))
                .collect(toImmutableList());
    }

    private static void assertCacheSplitIds(SplitBatch batch, Map<String, List<CacheSplitId>> expected)
    {
        Map<String, List<CacheSplitId>> actual = new HashMap<>();
        for (Split split : batch.getSplits()) {
            String address = getOnlyElement(split.getAddresses()).toString();
            actual.computeIfAbsent(address, _ -> new ArrayList<>()).add(split.getCacheSplitId().get());
        }
        assertThat(actual).isEqualTo(expected);
    }

    private static void assertCacheSplitIds(SplitBatch batch, List<CacheSplitId> expected)
    {
        List<CacheSplitId> actual = batch.getSplits().stream()
                .map(Split::getCacheSplitId)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    private static void assertHostAddress(SplitBatch batch, Set<String> addresses)
    {
        Set<String> hostAddresses = batch.getSplits().stream()
                .map(Split::getAddresses)
                .map(Iterables::getOnlyElement)
                .map(HostAddress::toString)
                .collect(toImmutableSet());
        assertThat(hostAddresses).containsExactlyInAnyOrderElementsOf(addresses);
    }

    private static CacheSplitSource createCacheSplitSource(
            Function<String, Optional<HostAddress>> addressProvider,
            SplitAdmissionController scheduler,
            int availableSplits,
            int minSplitBatchSize)
    {
        return new CacheSplitSource(
                createPlanSignature("signature1"),
                new TestingSplitManager(),
                createMockSplitSource(availableSplits),
                addressProvider,
                scheduler,
                minSplitBatchSize);
    }

    private static MockSplitSource createMockSplitSource(int availableSplits)
    {
        MockSplitSource mockSplitSource = new MockSplitSource();
        mockSplitSource.setBatchSize(10);
        mockSplitSource.increaseAvailableSplits(availableSplits);
        mockSplitSource.atSplitCompletion(FINISH);
        return mockSplitSource;
    }

    private static Function<String, Optional<HostAddress>> createAddressProvider(int numNodes)
    {
        List<Node> nodes = IntStream.range(0, numNodes)
                .mapToObj(i -> node("node" + i))
                .collect(toImmutableList());
        return value -> Optional.of(nodes.get(abs(value.hashCode()) % numNodes))
                .map(Node::getHostAndPort);
    }

    private static Node node(String nodeName)
    {
        return new InternalNode(nodeName, URI.create("http://" + nodeName + "/"), NodeVersion.UNKNOWN, false);
    }

    private static PlanSignature createPlanSignature(String signature)
    {
        return new PlanSignature(
                new SignatureKey(signature),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of());
    }

    private static class TestingSplitManager
            implements ConnectorSplitManager
    {
        private int splitCount;

        @Override
        public Optional<CacheSplitId> getCacheSplitId(ConnectorSplit split)
        {
            return Optional.of(new CacheSplitId("split" + splitCount++));
        }
    }
}
