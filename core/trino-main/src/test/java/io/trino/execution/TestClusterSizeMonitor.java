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
package io.trino.execution;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import io.trino.node.InternalNode;
import io.trino.node.TestingInternalNodeManager;
import io.trino.spi.NodeVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestClusterSizeMonitor
{
    private static final Duration WAIT = new Duration(1, MINUTES);

    private static final InternalNode ETL_NODE = new InternalNode("etl1", URI.create("http://10.0.0.1:21"), NodeVersion.UNKNOWN, false, ImmutableSet.of("etl", "shared"));
    private static final InternalNode SECOND_ETL_NODE = new InternalNode("etl2", URI.create("http://10.0.0.1:24"), NodeVersion.UNKNOWN, false, ImmutableSet.of("etl"));
    private static final InternalNode ADHOC_NODE = new InternalNode("adhoc1", URI.create("http://10.0.0.1:22"), NodeVersion.UNKNOWN, false, ImmutableSet.of("adhoc", "shared"));
    private static final InternalNode UNGROUPED_NODE = new InternalNode("plain1", URI.create("http://10.0.0.1:23"), NodeVersion.UNKNOWN, false);

    @Test
    @Timeout(30)
    void testWaitsOnTheNodeGroupRatherThanTheCluster()
            throws Exception
    {
        TestingInternalNodeManager nodeManager = TestingInternalNodeManager.createDefault(ADHOC_NODE, UNGROUPED_NODE);
        ClusterSizeMonitor monitor = new ClusterSizeMonitor(nodeManager, false);
        monitor.start();
        try {
            // two workers are active, but neither is in the "etl" group
            ListenableFuture<Void> etlFuture = monitor.waitForMinimumWorkers(Optional.of("etl"), 1, WAIT);
            assertThat(etlFuture.isDone()).isFalse();

            // an unrestricted query is satisfied by the same two workers
            assertThat(monitor.waitForMinimumWorkers(Optional.empty(), 2, WAIT).isDone()).isTrue();

            nodeManager.addNodes(ETL_NODE);
            etlFuture.get(10, SECONDS);
        }
        finally {
            monitor.stop();
        }
    }

    @Test
    @Timeout(30)
    void testOneNodeSatisfiesWaitersInEachOfItsGroups()
            throws Exception
    {
        TestingInternalNodeManager nodeManager = TestingInternalNodeManager.createDefault();
        ClusterSizeMonitor monitor = new ClusterSizeMonitor(nodeManager, false);
        monitor.start();
        try {
            ListenableFuture<Void> etlFuture = monitor.waitForMinimumWorkers(Optional.of("etl"), 1, WAIT);
            ListenableFuture<Void> sharedFuture = monitor.waitForMinimumWorkers(Optional.of("shared"), 1, WAIT);
            ListenableFuture<Void> anyFuture = monitor.waitForMinimumWorkers(Optional.empty(), 1, WAIT);
            assertThat(etlFuture.isDone()).isFalse();

            // a single node declaring both groups must release the waiters of each of them, not just one
            nodeManager.addNodes(ETL_NODE);
            etlFuture.get(10, SECONDS);
            sharedFuture.get(10, SECONDS);
            anyFuture.get(10, SECONDS);
        }
        finally {
            monitor.stop();
        }
    }

    @Test
    @Timeout(30)
    void testTimeoutNamesTheNodeGroupAndItsCount()
    {
        TestingInternalNodeManager nodeManager = TestingInternalNodeManager.createDefault(ETL_NODE, ADHOC_NODE);
        ClusterSizeMonitor monitor = new ClusterSizeMonitor(nodeManager, false);
        monitor.start();
        try {
            ListenableFuture<Void> future = monitor.waitForMinimumWorkers(Optional.of("etl"), 2, new Duration(1, MILLISECONDS));
            assertThatThrownBy(future::get)
                    .hasMessageContaining("in node group 'etl'")
                    // one node declares "etl", not the two active workers
                    .hasMessageContaining("only 1 workers are active");
        }
        finally {
            monitor.stop();
        }
    }

    @Test
    @Timeout(30)
    void testCoordinatorExclusionIsAppliedWithinTheNodeGroup()
    {
        // the coordinator declares "etl" but cannot run the query, so it must not count
        InternalNode coordinator = new InternalNode("local", URI.create("local://127.0.0.1:8080"), NodeVersion.UNKNOWN, true, ImmutableSet.of("etl"));
        TestingInternalNodeManager nodeManager = TestingInternalNodeManager.createDefault(coordinator);
        ClusterSizeMonitor monitor = new ClusterSizeMonitor(nodeManager, false);
        monitor.start();
        try {
            assertThat(monitor.waitForMinimumWorkers(Optional.of("etl"), 1, WAIT).isDone()).isFalse();
        }
        finally {
            monitor.stop();
        }
    }

    @Test
    @Timeout(30)
    void testRepeatedSnapshotsDoNotDoubleCount()
            throws Exception
    {
        TestingInternalNodeManager nodeManager = TestingInternalNodeManager.createDefault(ETL_NODE);
        ClusterSizeMonitor monitor = new ClusterSizeMonitor(nodeManager, false);
        // start() delivers the snapshot both directly and through the listener
        monitor.start();
        try {
            assertThat(monitor.waitForMinimumWorkers(Optional.of("etl"), 1, WAIT).isDone()).isTrue();
            assertThat(monitor.waitForMinimumWorkers(Optional.of("etl"), 2, WAIT).isDone()).isFalse();
        }
        finally {
            monitor.stop();
        }
    }

    @Test
    @Timeout(30)
    void testOnlyWaitersWhoseThresholdIsMetAreReleased()
            throws Exception
    {
        TestingInternalNodeManager nodeManager = TestingInternalNodeManager.createDefault();
        ClusterSizeMonitor monitor = new ClusterSizeMonitor(nodeManager, false);
        monitor.start();
        try {
            ListenableFuture<Void> needsOne = monitor.waitForMinimumWorkers(Optional.of("etl"), 1, WAIT);
            ListenableFuture<Void> needsTwo = monitor.waitForMinimumWorkers(Optional.of("etl"), 2, WAIT);

            nodeManager.addNodes(ETL_NODE);
            needsOne.get(10, SECONDS);
            assertThat(needsTwo.isDone()).isFalse();

            nodeManager.addNodes(SECOND_ETL_NODE);
            needsTwo.get(10, SECONDS);
        }
        finally {
            monitor.stop();
        }
    }
}
