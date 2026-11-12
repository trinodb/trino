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
package io.trino.plugin.tpch;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.connector.DynamicFilterSnapshot.EMPTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTpchTransactionManager
{
    private static final Node NODE_1 = new TestingNode("node1", HostAddress.fromParts("localhost", 1));
    private static final Node NODE_2 = new TestingNode("node2", HostAddress.fromParts("localhost", 2));
    private static final Node NODE_3 = new TestingNode("node3", HostAddress.fromParts("localhost", 3));
    private static final int SPLITS_PER_NODE = 4;

    @Test
    public void testWorkerChanges()
            throws Exception
    {
        Set<Node> initialNodes = ImmutableSet.of(NODE_3, NODE_1);
        for (Set<Node> updatedNodes : List.of(
                ImmutableSet.of(NODE_3, NODE_2, NODE_1),
                ImmutableSet.of(NODE_1),
                ImmutableSet.of(NODE_2, NODE_1))) {
            for (boolean splitsFirst : List.of(true, false)) {
                TestingNodeManager nodeManager = new TestingNodeManager(initialNodes);
                TpchTransactionManager transactionManager = new TpchTransactionManager(nodeManager);
                TpchSplitManager splitManager = new TpchSplitManager(transactionManager, SPLITS_PER_NODE);
                TpchNodePartitioningProvider partitioningProvider = new TpchNodePartitioningProvider(transactionManager, SPLITS_PER_NODE);
                ConnectorTransactionHandle transaction = transactionManager.beginTransaction();
                TpchPartitioningHandle partitioning = new TpchPartitioningHandle("orders", 15_000);

                List<ConnectorSplit> splits;
                ConnectorBucketNodeMap mapping;
                if (splitsFirst) {
                    splits = getSplits(splitManager, transaction);
                    nodeManager.setNodes(updatedNodes);
                    mapping = partitioningProvider.getBucketNodeMapping(transaction, null, partitioning).orElseThrow();
                }
                else {
                    mapping = partitioningProvider.getBucketNodeMapping(transaction, null, partitioning).orElseThrow();
                    nodeManager.setNodes(updatedNodes);
                    splits = getSplits(splitManager, transaction);
                }

                assertThat(mapping.getFixedMapping()).containsExactly(NODE_1, NODE_1, NODE_1, NODE_1, NODE_3, NODE_3, NODE_3, NODE_3);
                assertThat(splits).hasSize(mapping.getBucketCount());
                var splitBucketFunction = partitioningProvider.getSplitBucketFunction(transaction, null, partitioning, mapping.getBucketCount());
                for (int index = 0; index < splits.size(); index++) {
                    TpchSplit split = (TpchSplit) splits.get(index);
                    assertThat(split.getTotalParts()).isEqualTo(mapping.getBucketCount());
                    assertThat(splitBucketFunction.applyAsInt(split)).isEqualTo(index);
                    assertThat(split.getAddresses()).containsExactly(mapping.getFixedMapping().get(index).getHostAndPort());
                }

                // Other scans and bucket mappings in the transaction must retain the same partitioning.
                assertThat(getSplits(splitManager, transaction)).isEqualTo(splits);
                assertThat(partitioningProvider.getBucketNodeMapping(transaction, null, partitioning).orElseThrow().getFixedMapping())
                        .isEqualTo(mapping.getFixedMapping());

                ConnectorTransactionHandle nextTransaction = transactionManager.beginTransaction();
                assertThat(nextTransaction).isNotEqualTo(transaction);
                assertThat(ImmutableSet.copyOf(transactionManager.getWorkerNodes(nextTransaction))).isEqualTo(updatedNodes);
                assertThat(getSplits(splitManager, nextTransaction)).hasSize(updatedNodes.size() * SPLITS_PER_NODE);
                transactionManager.commit(transaction);
                transactionManager.rollback(nextTransaction);
            }
        }
    }

    @Test
    public void testLazySnapshot()
    {
        TestingNodeManager nodeManager = new TestingNodeManager(Set.of());
        TpchTransactionManager transactionManager = new TpchTransactionManager(nodeManager);
        ConnectorTransactionHandle transaction = transactionManager.beginTransaction();
        nodeManager.setNodes(ImmutableSet.of(NODE_1));
        assertThat(transactionManager.getWorkerNodes(transaction)).containsExactly(NODE_1);
        transactionManager.commit(transaction);
    }

    @Test
    public void testTransactionCleanup()
    {
        TpchTransactionManager transactionManager = new TpchTransactionManager(new TestingNodeManager(ImmutableSet.of(NODE_1)));
        ConnectorTransactionHandle committed = transactionManager.beginTransaction();
        ConnectorTransactionHandle rolledBack = transactionManager.beginTransaction();
        transactionManager.getWorkerNodes(committed);
        transactionManager.getWorkerNodes(rolledBack);
        transactionManager.commit(committed);
        transactionManager.rollback(rolledBack);
        assertThatThrownBy(() -> transactionManager.getWorkerNodes(committed))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("no such transaction:");
        assertThatThrownBy(() -> transactionManager.getWorkerNodes(rolledBack))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("no such transaction:");
    }

    @Test
    public void testTransactionHandleSerialization()
    {
        var codec = jsonCodec(TpchTransactionHandle.class);
        TpchTransactionHandle transaction = new TpchTransactionHandle();
        assertThat(codec.fromJson(codec.toJson(transaction))).isEqualTo(transaction);
    }

    private static List<ConnectorSplit> getSplits(TpchSplitManager splitManager, ConnectorTransactionHandle transaction)
            throws Exception
    {
        try (var splitSource = splitManager.getSplits(transaction, null, new TpchTableHandle("tiny", "orders", 0.01), Set.of(), alwaysTrue())) {
            List<ConnectorSplit> splits = splitSource.getNextBatch(Integer.MAX_VALUE, EMPTY).get();
            assertThat(splitSource.isFinished()).isTrue();
            return splits;
        }
    }

    private static class TestingNodeManager
            implements NodeManager
    {
        private Set<Node> nodes;

        public TestingNodeManager(Set<Node> nodes)
        {
            setNodes(nodes);
        }

        public void setNodes(Set<Node> nodes)
        {
            this.nodes = ImmutableSet.copyOf(nodes);
        }

        @Override
        public Set<Node> getAllNodes()
        {
            return nodes;
        }

        @Override
        public Set<Node> getWorkerNodes()
        {
            return nodes;
        }

        @Override
        public Node getCurrentNode()
        {
            throw new UnsupportedOperationException();
        }
    }

    private record TestingNode(String nodeIdentifier, HostAddress hostAndPort)
            implements Node
    {
        @Override
        public String getNodeIdentifier()
        {
            return nodeIdentifier;
        }

        @Override
        public HostAddress getHostAndPort()
        {
            return hostAndPort;
        }

        @Override
        public String getHost()
        {
            return hostAndPort.getHostText();
        }

        @Override
        public String getVersion()
        {
            return "test";
        }

        @Override
        public boolean isCoordinator()
        {
            return false;
        }
    }
}
