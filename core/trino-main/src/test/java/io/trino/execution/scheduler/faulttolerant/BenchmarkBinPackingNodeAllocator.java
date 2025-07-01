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
package io.trino.execution.scheduler.faulttolerant;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.SessionTestUtils;
import io.trino.client.NodeVersion;
import io.trino.execution.TaskId;
import io.trino.jmh.Benchmarks;
import io.trino.memory.MemoryInfo;
import io.trino.node.InternalNode;
import io.trino.node.TestingInternalNodeManager;
import io.trino.spi.HostAddress;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.memory.MemoryPoolInfo;
import io.trino.testing.assertions.Assert;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Ticker.systemTicker;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.execution.scheduler.faulttolerant.TaskExecutionClass.STANDARD;
import static io.trino.testing.TestingHandles.createTestCatalogHandle;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkBinPackingNodeAllocator
{
    private static final int CALLS_COUNT = 100;
    private static final int CATALOGS_COUNT = 20;

    @Benchmark
    @OperationsPerInvocation(CALLS_COUNT)
    public void benchmarkProcessPendingAllocations(BenchmarkData data)
    {
        BinPackingNodeAllocatorService nodeAllocatorService = data.getNodeAllocatorService();
        for (int i = 0; i < CALLS_COUNT; ++i) {
            nodeAllocatorService.processPendingAcquires();
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param("64")
        private int nodeCount;

        @Param({"100", "1000", "10000"})
        private int leasesCount = 1000;

        @Param({"1", "10", "100"})
        private int requestersCount = 10;

        @Param({"false", "true"})
        private boolean preferredNodes;

        @Param({"false", "true"})
        private boolean specificCatalogs;

        private BinPackingNodeAllocatorService nodeAllocatorService;

        @Setup
        public void setup()
        {
            List<InternalNode> nodes = new ArrayList<>();
            ConcurrentHashMap<String, Optional<MemoryInfo>> workerMemoryInfos = new ConcurrentHashMap<>();
            MemoryInfo memoryInfo = buildWorkerMemoryInfo(DataSize.ofBytes(0), ImmutableMap.of());
            for (int i = 0; i < nodeCount; i++) {
                String nodeIdentifier = "node" + i;
                nodes.add(new InternalNode(nodeIdentifier, URI.create("local://127.0.0.1:" + (8000 + i)), NodeVersion.UNKNOWN, false));
                workerMemoryInfos.put(nodeIdentifier, Optional.of(memoryInfo));
            }
            TestingInternalNodeManager nodeManager = TestingInternalNodeManager.createDefault(ImmutableSet.copyOf(nodes));

            nodeAllocatorService = new BinPackingNodeAllocatorService(
                    nodeManager,
                    () -> workerMemoryInfos,
                    false,
                    Duration.of(1, ChronoUnit.MINUTES),
                    Duration.of(1, ChronoUnit.MINUTES),
                    true,
                    DataSize.of(0, BYTE),
                    DataSize.of(10, GIGABYTE), // allow overcommit of 10GB for EAGER_SPECULATIVE tasks
                    systemTicker());
            nodeAllocatorService.start();
            NodeAllocator setupNodeAllocator = nodeAllocatorService.getNodeAllocator(SessionTestUtils.TEST_SESSION);

            // fill the nodes
            for (int i = 0; i < nodeCount; i++) {
                NodeAllocator.NodeLease lease = setupNodeAllocator.acquire(
                        new NodeRequirements(Optional.empty(), Optional.empty(), true),
                        DataSize.of(64, GIGABYTE),
                        STANDARD);
                assertAcquired(lease);
            }

            System.out.println("Creating leases");
            List<NodeAllocator> nodeAllocators = new ArrayList<>();
            for (int i = 0; i < requestersCount; i++) {
                Session session = Session.builder(SessionTestUtils.TEST_SESSION)
                        .setQueryId(QueryId.valueOf("query_" + i))
                        .build();
                nodeAllocators.add(nodeAllocatorService.getNodeAllocator(session));
            }
            for (int i = 0; i < leasesCount; i++) {
                Optional<HostAddress> preferredNode = Optional.empty();
                if (preferredNodes) {
                    preferredNode = Optional.of(nodes.get(i % nodeCount).getHostAndPort());
                }
                Optional<CatalogHandle> catalog = Optional.empty();
                if (specificCatalogs) {
                    catalog = Optional.of(createTestCatalogHandle("catalog" + (i % CATALOGS_COUNT)));
                }

                NodeRequirements requirements = new NodeRequirements(
                        catalog,
                        preferredNode,
                        true);
                NodeAllocator.NodeLease lease = nodeAllocators.get(i % requestersCount).acquire(
                        requirements,
                        DataSize.of(1, GIGABYTE),
                        STANDARD);
                assertNotAcquired(lease);
            }
        }

        public BinPackingNodeAllocatorService getNodeAllocatorService()
        {
            return nodeAllocatorService;
        }

        private MemoryInfo buildWorkerMemoryInfo(DataSize usedMemory, Map<TaskId, DataSize> taskMemoryUsage)
        {
            return new MemoryInfo(
                    4,
                    new MemoryPoolInfo(
                            DataSize.of(64, GIGABYTE).toBytes(),
                            usedMemory.toBytes(),
                            0,
                            ImmutableMap.of(),
                            ImmutableMap.of(),
                            ImmutableMap.of(),
                            taskMemoryUsage.entrySet().stream()
                                    .collect(toImmutableMap(
                                            entry -> entry.getKey().toString(),
                                            entry -> entry.getValue().toBytes())),
                            ImmutableMap.of()));
        }

        private void assertAcquired(NodeAllocator.NodeLease lease)
        {
            assertEventually(() -> {
                assertThat(lease.getNode().isCancelled())
                        .describedAs("node lease cancelled")
                        .isFalse();
                assertThat(lease.getNode().isDone())
                        .describedAs("node lease not acquired")
                        .isTrue();
            });
        }

        private void assertNotAcquired(NodeAllocator.NodeLease lease)
        {
            assertThat(lease.getNode().isCancelled())
                    .describedAs("node lease cancelled")
                    .isFalse();
            assertThat(lease.getNode().isDone())
                    .describedAs("node lease acquired")
                    .isFalse();
            // enforce pending acquires processing and check again
            nodeAllocatorService.processPendingAcquires();
            assertThat(lease.getNode().isCancelled())
                    .describedAs("node lease cancelled")
                    .isFalse();
            assertThat(lease.getNode().isDone())
                    .describedAs("node lease acquired")
                    .isFalse();
        }

        private static void assertEventually(Runnable assertion)
        {
            Assert.assertEventually(
                    new io.airlift.units.Duration(1000, MILLISECONDS),
                    new io.airlift.units.Duration(10, MILLISECONDS),
                    assertion::run);
        }
    }

    @Test
    public void ensureBenchmarkValid()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        BenchmarkBinPackingNodeAllocator benchmark = new BenchmarkBinPackingNodeAllocator();
        benchmark.benchmarkProcessPendingAllocations(data);
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkBinPackingNodeAllocator.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgs("-Xmx4g"))
                .run();
    }
}
