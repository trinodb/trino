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
package io.prestosql.plugin.raptor.legacy.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.prestosql.client.NodeVersion;
import io.prestosql.metadata.InternalNode;
import io.prestosql.plugin.raptor.legacy.NodeSupplier;
import io.prestosql.plugin.raptor.legacy.RaptorColumnHandle;
import io.prestosql.plugin.raptor.legacy.RaptorConnectorId;
import io.prestosql.plugin.raptor.legacy.RaptorMetadata;
import io.prestosql.plugin.raptor.legacy.RaptorSplitManager;
import io.prestosql.plugin.raptor.legacy.RaptorTableHandle;
import io.prestosql.plugin.raptor.legacy.RaptorTransactionHandle;
import io.prestosql.plugin.raptor.legacy.util.DaoSupplier;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.BigintType;
import io.prestosql.testing.TestingNodeManager;
import io.prestosql.type.InternalTypeManager;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Ticker.systemTicker;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.prestosql.plugin.raptor.legacy.metadata.DatabaseShardManager.shardIndexTable;
import static io.prestosql.plugin.raptor.legacy.metadata.SchemaDaoUtil.createTablesWithRetry;
import static io.prestosql.plugin.raptor.legacy.metadata.TestDatabaseShardManager.shardInfo;
import static io.prestosql.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestRaptorSplitManager
{
    private static final ConnectorTableMetadata TEST_TABLE = tableMetadataBuilder(new SchemaTableName("demo", "test_table"))
            .column("ds", createVarcharType(10))
            .column("foo", createVarcharType(10))
            .column("bar", BigintType.BIGINT)
            .build();

    private Handle dummyHandle;
    private File temporary;
    private RaptorMetadata metadata;
    private RaptorSplitManager raptorSplitManager;
    private ConnectorTableHandle tableHandle;
    private ShardManager shardManager;
    private long tableId;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        DBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime() + ThreadLocalRandom.current().nextLong());
        dbi.registerMapper(new TableColumn.Mapper(new InternalTypeManager(createTestMetadataManager())));
        dummyHandle = dbi.open();
        createTablesWithRetry(dbi);
        temporary = createTempDir();
        AssignmentLimiter assignmentLimiter = new AssignmentLimiter(ImmutableSet::of, systemTicker(), new MetadataConfig());
        shardManager = new DatabaseShardManager(dbi, new DaoSupplier<>(dbi, ShardDao.class), ImmutableSet::of, assignmentLimiter, systemTicker(), new Duration(0, MINUTES));
        TestingNodeManager nodeManager = new TestingNodeManager();
        NodeSupplier nodeSupplier = nodeManager::getWorkerNodes;

        String nodeName = UUID.randomUUID().toString();
        nodeManager.addNode(new InternalNode(nodeName, new URI("http://127.0.0.1/"), NodeVersion.UNKNOWN, false));

        RaptorConnectorId connectorId = new RaptorConnectorId("raptor");
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

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws IOException
    {
        dummyHandle.close();
        deleteRecursively(temporary.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testSanity()
    {
        ConnectorSplitSource splitSource = getSplits(raptorSplitManager, tableHandle);
        int splitCount = 0;
        while (!splitSource.isFinished()) {
            splitCount += getSplits(splitSource, 1000).size();
        }
        assertEquals(splitCount, 4);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "No host for shard .* found: \\[\\]")
    public void testNoHostForShard()
    {
        deleteShardNodes();

        ConnectorSplitSource splitSource = getSplits(raptorSplitManager, tableHandle);
        getSplits(splitSource, 1000);
    }

    @Test
    public void testAssignRandomNodeWhenBackupAvailable()
            throws URISyntaxException
    {
        TestingNodeManager nodeManager = new TestingNodeManager();
        RaptorConnectorId connectorId = new RaptorConnectorId("raptor");
        NodeSupplier nodeSupplier = nodeManager::getWorkerNodes;
        InternalNode node = new InternalNode(UUID.randomUUID().toString(), new URI("http://127.0.0.1/"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(node);
        RaptorSplitManager raptorSplitManagerWithBackup = new RaptorSplitManager(connectorId, nodeSupplier, shardManager, true);

        deleteShardNodes();

        ConnectorSplitSource partitionSplit = getSplits(raptorSplitManagerWithBackup, tableHandle);
        List<ConnectorSplit> batch = getSplits(partitionSplit, 1);
        assertEquals(getOnlyElement(getOnlyElement(batch).getAddresses()), node.getHostAndPort());
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "No nodes available to run query")
    public void testNoNodes()
    {
        deleteShardNodes();

        RaptorSplitManager raptorSplitManagerWithBackup = new RaptorSplitManager(new RaptorConnectorId("fbraptor"), ImmutableSet::of, shardManager, true);
        ConnectorSplitSource splitSource = getSplits(raptorSplitManagerWithBackup, tableHandle);
        getSplits(splitSource, 1000);
    }

    private void deleteShardNodes()
    {
        dummyHandle.execute("DELETE FROM shard_nodes");
        dummyHandle.execute(format("UPDATE %s SET node_ids = ''", shardIndexTable(tableId)));
    }

    private static ConnectorSplitSource getSplits(RaptorSplitManager splitManager, ConnectorTableHandle table)
    {
        ConnectorTransactionHandle transaction = new RaptorTransactionHandle();
        return splitManager.getSplits(transaction, SESSION, table, UNGROUPED_SCHEDULING, DynamicFilter.EMPTY);
    }

    private static List<ConnectorSplit> getSplits(ConnectorSplitSource source, int maxSize)
    {
        return getFutureValue(source.getNextBatch(NOT_PARTITIONED, maxSize)).getSplits();
    }
}
