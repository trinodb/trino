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
package io.trino.plugin.raptor.legacy.storage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.trino.client.NodeVersion;
import io.trino.metadata.InternalNode;
import io.trino.plugin.raptor.legacy.backup.BackupStore;
import io.trino.plugin.raptor.legacy.metadata.ColumnInfo;
import io.trino.plugin.raptor.legacy.metadata.MetadataDao;
import io.trino.plugin.raptor.legacy.metadata.ShardInfo;
import io.trino.plugin.raptor.legacy.metadata.ShardManager;
import io.trino.plugin.raptor.legacy.metadata.ShardMetadata;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingNodeManager;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.raptor.legacy.DatabaseTesting.createTestingJdbi;
import static io.trino.plugin.raptor.legacy.metadata.SchemaDaoUtil.createTablesWithRetry;
import static io.trino.plugin.raptor.legacy.metadata.TestDatabaseShardManager.createShardManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.nio.file.Files.createTempDirectory;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestShardEjector
{
    private Jdbi dbi;
    private Handle dummyHandle;
    private ShardManager shardManager;
    private Path dataDir;
    private StorageService storageService;

    @BeforeMethod
    public void setup()
            throws IOException
    {
        dbi = createTestingJdbi();
        dummyHandle = dbi.open();
        createTablesWithRetry(dbi);
        shardManager = createShardManager(dbi);

        dataDir = createTempDirectory(null);
        storageService = new FileStorageService(dataDir.toFile());
        storageService.start();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        if (dummyHandle != null) {
            dummyHandle.close();
            dummyHandle = null;
        }
        if (dataDir != null) {
            deleteRecursively(dataDir, ALLOW_INSECURE);
        }
    }

    @Test(invocationCount = 20)
    public void testEjector()
            throws Exception
    {
        NodeManager nodeManager = createNodeManager("node1", "node2", "node3", "node4", "node5");

        ShardEjector ejector = new ShardEjector(
                nodeManager.getCurrentNode().getNodeIdentifier(),
                nodeManager::getWorkerNodes,
                shardManager,
                storageService,
                new Duration(1, HOURS),
                Optional.of(new TestingBackupStore()),
                "test");

        List<ShardInfo> shards = ImmutableList.<ShardInfo>builder()
                .add(shardInfo("node1", 14))
                .add(shardInfo("node1", 13))
                .add(shardInfo("node1", 12))
                .add(shardInfo("node1", 11))
                .add(shardInfo("node1", 10))
                .add(shardInfo("node1", 10))
                .add(shardInfo("node1", 10))
                .add(shardInfo("node1", 10))
                .add(shardInfo("node2", 5))
                .add(shardInfo("node2", 5))
                .add(shardInfo("node3", 10))
                .add(shardInfo("node4", 10))
                .add(shardInfo("node5", 10))
                .add(shardInfo("node6", 200))
                .build();

        long tableId = createTable("test");
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));

        shardManager.createTable(tableId, columns, false, OptionalLong.empty());

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, shards, Optional.empty(), 0);

        for (ShardInfo shard : shards.subList(0, 8)) {
            File file = storageService.getStorageFile(shard.getShardUuid());
            storageService.createParents(file);
            assertTrue(file.createNewFile());
        }

        ejector.process();

        shardManager.getShardNodes(tableId, TupleDomain.all());

        Set<UUID> ejectedShards = shards.subList(0, 4).stream()
                .map(ShardInfo::getShardUuid)
                .collect(toSet());
        Set<UUID> keptShards = shards.subList(4, 8).stream()
                .map(ShardInfo::getShardUuid)
                .collect(toSet());

        Set<UUID> remaining = uuids(shardManager.getNodeShards("node1"));

        for (UUID uuid : ejectedShards) {
            assertFalse(remaining.contains(uuid));
            assertFalse(storageService.getStorageFile(uuid).exists());
        }

        assertEquals(remaining, keptShards);
        for (UUID uuid : keptShards) {
            assertTrue(storageService.getStorageFile(uuid).exists());
        }

        Set<UUID> others = ImmutableSet.<UUID>builder()
                .addAll(uuids(shardManager.getNodeShards("node2")))
                .addAll(uuids(shardManager.getNodeShards("node3")))
                .addAll(uuids(shardManager.getNodeShards("node4")))
                .addAll(uuids(shardManager.getNodeShards("node5")))
                .build();

        assertTrue(others.containsAll(ejectedShards));
    }

    private long createTable(String name)
    {
        return dbi.onDemand(MetadataDao.class).insertTable("test", name, false, false, null, 0);
    }

    private static Set<UUID> uuids(Set<ShardMetadata> metadata)
    {
        return metadata.stream()
                .map(ShardMetadata::getShardUuid)
                .collect(toSet());
    }

    private static ShardInfo shardInfo(String node, long size)
    {
        return new ShardInfo(randomUUID(), OptionalInt.empty(), ImmutableSet.of(node), ImmutableList.of(), 1, size, size * 2, 0);
    }

    private static NodeManager createNodeManager(String current, String... others)
    {
        Node currentNode = createTestingNode(current);
        TestingNodeManager nodeManager = new TestingNodeManager(currentNode);
        for (String other : others) {
            nodeManager.addNode(createTestingNode(other));
        }
        return nodeManager;
    }

    private static Node createTestingNode(String identifier)
    {
        return new InternalNode(identifier, URI.create("http://test"), NodeVersion.UNKNOWN, false);
    }

    private static class TestingBackupStore
            implements BackupStore
    {
        @Override
        public void backupShard(UUID uuid, File source)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void restoreShard(UUID uuid, File target)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean deleteShard(UUID uuid)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean shardExists(UUID uuid)
        {
            return true;
        }
    }
}
