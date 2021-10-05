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
package io.trino.plugin.raptor.legacy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.slice.Slice;
import io.trino.PagesIndexPageSorter;
import io.trino.operator.PagesIndex;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.security.AllowAllAccessControl;
import io.trino.plugin.raptor.legacy.metadata.MetadataDao;
import io.trino.plugin.raptor.legacy.metadata.ShardManager;
import io.trino.plugin.raptor.legacy.metadata.TableColumn;
import io.trino.plugin.raptor.legacy.storage.StorageManager;
import io.trino.plugin.raptor.legacy.storage.StorageManagerConfig;
import io.trino.spi.NodeManager;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingNodeManager;
import io.trino.type.InternalTypeManager;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.operator.scalar.timestamp.VarcharToTimestampCast.castToShortTimestamp;
import static io.trino.plugin.raptor.legacy.RaptorTableProperties.TEMPORAL_COLUMN_PROPERTY;
import static io.trino.plugin.raptor.legacy.metadata.SchemaDaoUtil.createTablesWithRetry;
import static io.trino.plugin.raptor.legacy.metadata.TestDatabaseShardManager.createShardManager;
import static io.trino.plugin.raptor.legacy.storage.TestRaptorStorageManager.createRaptorStorageManager;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.util.DateTimeUtils.parseDate;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestRaptorConnector
{
    private Handle dummyHandle;
    private MetadataDao metadataDao;
    private File dataDir;
    private RaptorConnector connector;

    @BeforeMethod
    public void setup()
    {
        TypeManager typeManager = new InternalTypeManager(createTestMetadataManager(), new TypeOperators());
        DBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime() + ThreadLocalRandom.current().nextLong());
        dbi.registerMapper(new TableColumn.Mapper(typeManager));
        dummyHandle = dbi.open();
        metadataDao = dbi.onDemand(MetadataDao.class);
        createTablesWithRetry(dbi);
        dataDir = Files.createTempDir();

        CatalogName connectorId = new CatalogName("test");
        NodeManager nodeManager = new TestingNodeManager();
        NodeSupplier nodeSupplier = nodeManager::getWorkerNodes;
        ShardManager shardManager = createShardManager(dbi);
        StorageManager storageManager = createRaptorStorageManager(dbi, dataDir);
        StorageManagerConfig config = new StorageManagerConfig();
        connector = new RaptorConnector(
                new LifeCycleManager(ImmutableList.of(), null),
                new TestingNodeManager(),
                new RaptorMetadataFactory(dbi, shardManager),
                new RaptorSplitManager(connectorId, nodeSupplier, shardManager, false),
                new RaptorPageSourceProvider(storageManager),
                new RaptorPageSinkProvider(storageManager,
                        new PagesIndexPageSorter(new PagesIndex.TestingFactory(false)),
                        config),
                new RaptorNodePartitioningProvider(nodeSupplier),
                new RaptorSessionProperties(config),
                new RaptorTableProperties(typeManager),
                ImmutableSet.of(),
                new AllowAllAccessControl(),
                dbi);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        dummyHandle.close();
        deleteRecursively(dataDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testMaintenanceBlocked()
    {
        long tableId1 = createTable("test1");
        long tableId2 = createTable("test2");

        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId1));
        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId2));

        // begin delete for table1
        ConnectorTransactionHandle txn1 = connector.beginTransaction(READ_COMMITTED, false);
        ConnectorTableHandle handle1 = getTableHandle(connector.getMetadata(txn1), "test1");
        connector.getMetadata(txn1).beginDelete(SESSION, handle1);

        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId1));
        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId2));

        // begin delete for table2
        ConnectorTransactionHandle txn2 = connector.beginTransaction(READ_COMMITTED, false);
        ConnectorTableHandle handle2 = getTableHandle(connector.getMetadata(txn2), "test2");
        connector.getMetadata(txn2).beginDelete(SESSION, handle2);

        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId1));
        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId2));

        // begin another delete for table1
        ConnectorTransactionHandle txn3 = connector.beginTransaction(READ_COMMITTED, false);
        ConnectorTableHandle handle3 = getTableHandle(connector.getMetadata(txn3), "test1");
        connector.getMetadata(txn3).beginDelete(SESSION, handle3);

        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId1));
        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId2));

        // commit first delete for table1
        connector.commit(txn1);

        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId1));
        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId2));

        // rollback second delete for table1
        connector.rollback(txn3);

        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId1));
        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId2));

        // commit delete for table2
        connector.commit(txn2);

        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId1));
        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId2));
    }

    @Test
    public void testMaintenanceUnblockedOnStart()
    {
        long tableId = createTable("test");

        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId));
        metadataDao.blockMaintenance(tableId);
        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId));

        connector.start();

        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId));
    }

    @Test
    public void testTemporalShardSplit()
            throws Exception
    {
        // Same date should be in same split
        assertSplitShard(DATE, "2001-08-22", "2001-08-22", 1);

        // Same date should be in different splits
        assertSplitShard(DATE, "2001-08-22", "2001-08-23", 2);

        // Same timestamp should be in same split
        assertSplitShard(TIMESTAMP_MILLIS, "2001-08-22 00:00:01.000", "2001-08-22 23:59:01.000", 1);

        // Same timestamp should be in different splits
        assertSplitShard(TIMESTAMP_MILLIS, "2001-08-22 23:59:01.000", "2001-08-23 00:00:01.000", 2);
    }

    private void assertSplitShard(Type temporalType, String min, String max, int expectedSplits)
            throws Exception
    {
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new RaptorSessionProperties(new StorageManagerConfig()).getSessionProperties())
                .build();

        ConnectorTransactionHandle transaction = connector.beginTransaction(READ_COMMITTED, false);
        connector.getMetadata(transaction).createTable(
                SESSION,
                new ConnectorTableMetadata(
                        new SchemaTableName("test", "test"),
                        ImmutableList.of(new ColumnMetadata("id", BIGINT), new ColumnMetadata("time", temporalType)),
                        ImmutableMap.of(TEMPORAL_COLUMN_PROPERTY, "time")),
                false);
        connector.commit(transaction);

        ConnectorTransactionHandle txn1 = connector.beginTransaction(READ_COMMITTED, false);
        ConnectorTableHandle handle1 = getTableHandle(connector.getMetadata(txn1), "test");
        ConnectorInsertTableHandle insertTableHandle = connector.getMetadata(txn1).beginInsert(session, handle1);
        ConnectorPageSink raptorPageSink = connector.getPageSinkProvider().createPageSink(txn1, session, insertTableHandle);

        Object timestamp1 = null;
        Object timestamp2 = null;
        if (temporalType.equals(TIMESTAMP_MILLIS)) {
            timestamp1 = SqlTimestamp.newInstance(3, castToShortTimestamp(TIMESTAMP_MILLIS.getPrecision(), min), 0);
            timestamp2 = SqlTimestamp.newInstance(3, castToShortTimestamp(TIMESTAMP_MILLIS.getPrecision(), max), 0);
        }
        else if (temporalType.equals(DATE)) {
            timestamp1 = new SqlDate(parseDate(min));
            timestamp2 = new SqlDate(parseDate(max));
        }

        Page inputPage = MaterializedResult.resultBuilder(session, ImmutableList.of(BIGINT, temporalType))
                .row(1L, timestamp1)
                .row(2L, timestamp2)
                .build()
                .toPage();

        raptorPageSink.appendPage(inputPage);

        Collection<Slice> shards = raptorPageSink.finish().get();
        assertEquals(shards.size(), expectedSplits);
        connector.getMetadata(txn1).dropTable(session, handle1);
        connector.commit(txn1);
    }

    private long createTable(String name)
    {
        ConnectorTransactionHandle transaction = connector.beginTransaction(READ_COMMITTED, false);
        connector.getMetadata(transaction).createTable(
                SESSION,
                new ConnectorTableMetadata(
                        new SchemaTableName("test", name),
                        ImmutableList.of(new ColumnMetadata("id", BIGINT))),
                false);
        connector.commit(transaction);

        transaction = connector.beginTransaction(READ_COMMITTED, false);
        ConnectorTableHandle tableHandle = getTableHandle(connector.getMetadata(transaction), name);
        connector.commit(transaction);
        return ((RaptorTableHandle) tableHandle).getTableId();
    }

    private static ConnectorTableHandle getTableHandle(ConnectorMetadata metadata, String name)
    {
        return metadata.getTableHandle(SESSION, new SchemaTableName("test", name));
    }
}
