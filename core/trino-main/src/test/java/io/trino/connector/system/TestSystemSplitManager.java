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
package io.trino.connector.system;

import com.google.common.collect.ImmutableSet;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.MetadataUtil.TableMetadataBuilder;
import io.trino.node.InternalNode;
import io.trino.node.TestingInternalNodeManager;
import io.trino.spi.HostAddress;
import io.trino.spi.NodeVersion;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingSession;
import io.trino.testing.TestingTransactionHandle;
import io.trino.transaction.TestingTransactionManager;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.metadata.AbstractMockMetadata.dummyMetadata;
import static io.trino.node.TestingInternalNodeManager.CURRENT_NODE;
import static io.trino.spi.connector.SystemTable.Distribution.ALL_NODES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThat;

final class TestSystemSplitManager
{
    private static final SchemaTableName TABLE_NAME = new SchemaTableName("runtime", "test_table");

    private static final InternalNode ETL_NODE = new InternalNode("etl1", URI.create("http://10.0.0.1:21"), NodeVersion.UNKNOWN, false, ImmutableSet.of("etl"));
    private static final InternalNode ADHOC_NODE = new InternalNode("adhoc1", URI.create("http://10.0.0.1:22"), NodeVersion.UNKNOWN, false, ImmutableSet.of("adhoc"));

    @Test
    void testAllNodesSplitsAreRestrictedToNodeGroup()
    {
        // System splits are not remotely accessible, so a split addressed at a node outside the query's
        // node group could not be scheduled anywhere and the query would fail with NO_NODES_AVAILABLE.
        assertThat(splitAddresses(sessionWithNodeGroup("etl")))
                .containsExactlyInAnyOrder(ETL_NODE.getHostAndPort(), CURRENT_NODE.getHostAndPort());
        assertThat(splitAddresses(sessionWithNodeGroup("adhoc")))
                .containsExactlyInAnyOrder(ADHOC_NODE.getHostAndPort(), CURRENT_NODE.getHostAndPort());
    }

    @Test
    void testAllNodesSplitsCoverEveryNodeWithoutNodeGroup()
    {
        assertThat(splitAddresses(TestingSession.testSessionBuilder().build()))
                .containsExactlyInAnyOrder(ETL_NODE.getHostAndPort(), ADHOC_NODE.getHostAndPort(), CURRENT_NODE.getHostAndPort());
    }

    @Test
    void testUnmatchedNodeGroupStillLeavesTheCoordinator()
    {
        // the coordinator is never excluded by a node group, so there is always something to schedule on
        assertThat(splitAddresses(sessionWithNodeGroup("missing")))
                .containsExactly(CURRENT_NODE.getHostAndPort());
    }

    private static List<HostAddress> splitAddresses(Session session)
    {
        TestingInternalNodeManager nodeManager = TestingInternalNodeManager.createDefault(ETL_NODE, ADHOC_NODE);
        SystemSplitManager splitManager = new SystemSplitManager(
                CURRENT_NODE,
                nodeManager,
                new SystemTablesProvider(new TestingTransactionManager(), dummyMetadata(), TEST_CATALOG_NAME, ImmutableSet.of(new AllNodesSystemTable())));

        ConnectorSession connectorSession = new FullConnectorSession(session, session.getIdentity().toConnectorIdentity());
        ConnectorSplitSource splitSource = splitManager.getSplits(
                new SystemTransactionHandle(TransactionId.create(), TestingTransactionHandle.create()),
                connectorSession,
                new SystemTableHandle(TABLE_NAME.getSchemaName(), TABLE_NAME.getTableName(), TupleDomain.all()),
                Set.<ColumnHandle>of(),
                Constraint.alwaysTrue());

        return splitSource.getNextBatch(1000, DynamicFilterSnapshot.EMPTY).join().stream()
                .flatMap(split -> split.getAddresses().stream())
                .toList();
    }

    private static Session sessionWithNodeGroup(String nodeGroup)
    {
        return TestingSession.testSessionBuilder()
                .setNodeGroup(Optional.of(nodeGroup))
                .build();
    }

    private static class AllNodesSystemTable
            implements SystemTable
    {
        @Override
        public Distribution getDistribution()
        {
            return ALL_NODES;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata()
        {
            return TableMetadataBuilder.tableMetadataBuilder(TABLE_NAME)
                    .column("value", BIGINT)
                    .build();
        }

        @Override
        public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
        {
            throw new UnsupportedOperationException();
        }
    }
}
