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
package io.trino.plugin.raptor.legacy.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.trino.client.NodeVersion;
import io.trino.metadata.InternalNode;
import io.trino.plugin.raptor.legacy.NodeSupplier;
import io.trino.plugin.raptor.legacy.RaptorColumnHandle;
import io.trino.plugin.raptor.legacy.RaptorMetadata;
import io.trino.plugin.raptor.legacy.RaptorSplitManager;
import io.trino.plugin.raptor.legacy.RaptorTableHandle;
import io.trino.plugin.raptor.legacy.RaptorTransactionHandle;
import io.trino.plugin.raptor.legacy.util.DaoSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import io.trino.testing.TestingNodeManager;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Ticker.systemTicker;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.plugin.raptor.legacy.DatabaseTesting.createTestingJdbi;
import static io.trino.plugin.raptor.legacy.metadata.DatabaseShardManager.shardIndexTable;
import static io.trino.plugin.raptor.legacy.metadata.SchemaDaoUtil.createTablesWithRetry;
import static io.trino.plugin.raptor.legacy.metadata.TestDatabaseShardManager.shardInfo;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_METHOD)
@Execution(SAME_THREAD)
public class TestRaptorSplitManager
{
    private static final ConnectorTableMetadata TEST_TABLE = tableMetadataBuilder(new SchemaTableName("demo", "test_table"))
            .column("ds", createVarcharType(10))
            .column("foo", createVarcharType(10))
            .column("bar", BigintType.BIGINT)
            .build();

    private Handle dummyHandle;
    private Path temporary;
    private RaptorMetadata metadata;
    private RaptorSplitManager raptorSplitManager;
    private ConnectorTableHandle tableHandle;
    private ShardManager shardManager;
    private long tableId;

    @BeforeEach
    public void setup()
            throws Exception
    {
        Jdbi dbi = createTestingJdbi();
        dummyHandle = dbi.open();
        createTablesWithRetry(dbi);
        temporary = createTempDirectory(null);
        AssignmentLimiter assignmentLimiter = new AssignmentLimiter(ImmutableSet::of, systemTicker(), new MetadataConfig());
        shardManager = new DatabaseShardManager(dbi, new DaoSupplier<>(dbi, ShardDao.class), ImmutableSet::of, assignmentLimiter, systemTicker(), new Duration(0, MINUTES));
        TestingNodeManager nodeManager = new TestingNodeManager();
        NodeSupplier nodeSupplier = nodeManager::getWorkerNodes;

        String nodeName = UUID.randomUUID().toString();
        nodeManager.addNode(new InternalNode(nodeName, new URI("http://127.0.0.1/"), NodeVersion.UNKNOWN, false));

        CatalogName connectorId = new CatalogName("raptor");
        metadata = new RaptorMetadata(dbi, shardManager);

        metadata.createTable(SESSION, TEST_TABLE, false);
        tableHandle = metadata.getTableHandle(SESSION, TEST_TABLE.getTable());

        List<ShardInfo> shards = ImmutableList.<ShardInfo>builder()
                .add(shardInfo(UUID.randomUUID(), nodeName))
                .add(shardInfo(UUID.randomUUID(), nodeName))
                .add(shardInfo(UUID.randomUUID(), nodeName))
                .add(shardInfo(UUID.randomUUID(), nodeName))
                .build();

        tableId = ((RaptorTableHandle) tableHandle).getTableId();

        List<ColumnInfo> columns = metadata.getColumnHandles(SESSION, tableHandle).values().stream()
                .map(RaptorColumnHandle.class::cast)
                .map(ColumnInfo::fromHandle)
                .collect(toList());

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, shards, Optional.empty(), 0);

        raptorSplitManager = new RaptorSplitManager(connectorId, nodeSupplier, shardManager, false);
    }

    @AfterEach
    public void teardown()
            throws IOException
    {
        dummyHandle.close();
        dummyHandle = null;
        deleteRecursively(temporary, ALLOW_INSECURE);
    }

    @Test
    public void testSanity()
    {
        ConnectorSplitSource splitSource = getSplits(raptorSplitManager, tableHandle);
        int splitCount = 0;
        while (!splitSource.isFinished()) {
            splitCount += getSplits(splitSource, 1000).size();
        }
        assertThat(splitCount).isEqualTo(4);
    }

    @Test
    public void testNoHostForShard()
    {
        assertThatThrownBy(() -> {
            deleteShardNodes();

            ConnectorSplitSource splitSource = getSplits(raptorSplitManager, tableHandle);
            getSplits(splitSource, 1000);
        })
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("No host for shard .* found: \\[\\]");
    }

    @Test
    public void testAssignRandomNodeWhenBackupAvailable()
    {
        TestingNodeManager nodeManager = new TestingNodeManager();
        CatalogName connectorId = new CatalogName("raptor");
        NodeSupplier nodeSupplier = nodeManager::getWorkerNodes;
        InternalNode node = new InternalNode(UUID.randomUUID().toString(), URI.create("http://127.0.0.1/"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(node);
        RaptorSplitManager raptorSplitManagerWithBackup = new RaptorSplitManager(connectorId, nodeSupplier, shardManager, true);

        deleteShardNodes();

        ConnectorSplitSource partitionSplit = getSplits(raptorSplitManagerWithBackup, tableHandle);
        List<ConnectorSplit> batch = getSplits(partitionSplit, 1);
        assertThat(getOnlyElement(getOnlyElement(batch).getAddresses())).isEqualTo(node.getHostAndPort());
    }

    @Test
    public void testNoNodes()
    {
        assertThatThrownBy(() -> {
            deleteShardNodes();

            RaptorSplitManager raptorSplitManagerWithBackup = new RaptorSplitManager(new CatalogName("fbraptor"), ImmutableSet::of, shardManager, true);
            ConnectorSplitSource splitSource = getSplits(raptorSplitManagerWithBackup, tableHandle);
            getSplits(splitSource, 1000);
        })
                .isInstanceOf(TrinoException.class)
                .hasMessage("No nodes available to run query");
    }

    private void deleteShardNodes()
    {
        dummyHandle.execute("DELETE FROM shard_nodes");
        dummyHandle.execute(format("UPDATE %s SET node_ids = ''", shardIndexTable(tableId)));
    }

    private static ConnectorSplitSource getSplits(RaptorSplitManager splitManager, ConnectorTableHandle table)
    {
        ConnectorTransactionHandle transaction = new RaptorTransactionHandle();
        return splitManager.getSplits(transaction, SESSION, table, DynamicFilter.EMPTY, Constraint.alwaysTrue());
    }

    private static List<ConnectorSplit> getSplits(ConnectorSplitSource source, int maxSize)
    {
        return getFutureValue(source.getNextBatch(maxSize)).getSplits();
    }
}
