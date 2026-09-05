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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.connector.DefaultNodeManager;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.StableHostAddressProvider;
import io.trino.execution.scheduler.StableHostAddressProviderConfig;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.node.InternalNode;
import io.trino.node.TestingInternalNodeManager;
import io.trino.spi.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;
import io.trino.testing.TestingSession;
import io.trino.testing.TestingTransactionHandle;
import io.trino.util.FinalizerService;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

import static io.trino.node.TestingInternalNodeManager.CURRENT_NODE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestNodePartitioningManager
{
    private static final ConnectorPartitioningHandle FIXED_MAPPING_HANDLE = new ConnectorPartitioningHandle() {};
    private static final ConnectorPartitioningHandle BUCKET_COUNT_HANDLE = new ConnectorPartitioningHandle() {};

    private static final InternalNode ETL_NODE = new InternalNode("etl1", URI.create("http://10.0.0.1:21"), NodeVersion.UNKNOWN, false, ImmutableSet.of("etl"));

    private static final PartitioningHandle FIXED_MAPPING_PARTITIONING = partitioning(FIXED_MAPPING_HANDLE);
    private static final PartitioningHandle BUCKET_COUNT_PARTITIONING = partitioning(BUCKET_COUNT_HANDLE);

    @Test
    void testFixedBucketMappingIsRejectedForNodeGroup()
    {
        NodePartitioningManager manager = createNodePartitioningManager();

        // a connector pins buckets without knowing about node groups, so the query must be refused
        assertThatThrownBy(() -> manager.getNodePartitioningMap(sessionWithNodeGroup("etl"), FIXED_MAPPING_PARTITIONING, 2))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("node group 'etl'")
                .extracting(e -> ((TrinoException) e).getErrorCode())
                .isEqualTo(NOT_SUPPORTED.toErrorCode());

        assertThatThrownBy(() -> manager.getBucketNodeMap(sessionWithNodeGroup("etl"), FIXED_MAPPING_PARTITIONING, 2))
                .isInstanceOf(TrinoException.class)
                .extracting(e -> ((TrinoException) e).getErrorCode())
                .isEqualTo(NOT_SUPPORTED.toErrorCode());
    }

    @Test
    void testFixedBucketMappingIsAllowedWithoutNodeGroup()
    {
        NodePartitioningManager manager = createNodePartitioningManager();
        Session session = TestingSession.testSessionBuilder().build();

        assertThat(manager.getNodePartitioningMap(session, FIXED_MAPPING_PARTITIONING, 2).getPartitionToNode())
                .containsExactly(CURRENT_NODE);
    }

    @Test
    void testBucketCountPartitioningHonoursNodeGroup()
    {
        NodePartitioningManager manager = createNodePartitioningManager();

        // a connector that only declares a bucket count leaves node choice to the engine, so it is restricted
        assertThat(manager.getNodePartitioningMap(sessionWithNodeGroup("etl"), BUCKET_COUNT_PARTITIONING, 2).getPartitionToNode())
                .containsExactly(ETL_NODE);
    }

    private static NodePartitioningManager createNodePartitioningManager()
    {
        TestingInternalNodeManager nodeManager = TestingInternalNodeManager.createDefault(ETL_NODE);
        NodeSchedulerConfig config = new NodeSchedulerConfig().setIncludeCoordinator(false);
        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(
                CURRENT_NODE,
                nodeManager,
                config,
                new NodeTaskMap(new FinalizerService()),
                new StableHostAddressProvider(new DefaultNodeManager(CURRENT_NODE, nodeManager, false), new StableHostAddressProviderConfig())));
        return new NodePartitioningManager(nodeScheduler, CatalogServiceProvider.singleton(TEST_CATALOG_HANDLE, new TestPartitioningProvider()));
    }

    private static Session sessionWithNodeGroup(String nodeGroup)
    {
        return TestingSession.testSessionBuilder()
                .setNodeGroup(Optional.of(nodeGroup))
                .build();
    }

    private static PartitioningHandle partitioning(ConnectorPartitioningHandle connectorHandle)
    {
        return new PartitioningHandle(
                Optional.of(TEST_CATALOG_HANDLE),
                Optional.of(TestingTransactionHandle.create()),
                connectorHandle);
    }

    private static class TestPartitioningProvider
            implements ConnectorNodePartitioningProvider
    {
        @Override
        public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
        {
            if (partitioningHandle.equals(FIXED_MAPPING_HANDLE)) {
                return Optional.of(createBucketNodeMap(ImmutableList.of(CURRENT_NODE)));
            }
            return Optional.of(createBucketNodeMap(4));
        }

        @Override
        public BucketFunction getBucketFunction(
                ConnectorTransactionHandle transactionHandle,
                ConnectorSession session,
                ConnectorPartitioningHandle partitioningHandle,
                List<Type> partitionChannelTypes,
                int bucketCount)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
                ConnectorTransactionHandle transactionHandle,
                ConnectorSession session,
                ConnectorPartitioningHandle partitioningHandle,
                int bucketCount)
        {
            return _ -> 0;
        }
    }
}
