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
package io.prestosql.transaction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.prestosql.connector.CatalogName;
import io.prestosql.connector.informationschema.InformationSchemaConnector;
import io.prestosql.connector.system.SystemConnector;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.testing.TestingConnectorContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.connector.CatalogName.createInformationSchemaCatalogName;
import static io.prestosql.connector.CatalogName.createSystemTablesCatalogName;
import static io.prestosql.spi.StandardErrorCode.TRANSACTION_ALREADY_ABORTED;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTransactionManager
{
    private static final String CATALOG = "test_catalog";
    private static final CatalogName CATALOG_NAME = new CatalogName(CATALOG);
    private static final CatalogName SYSTEM_TABLES_ID = createSystemTablesCatalogName(CATALOG_NAME);
    private static final CatalogName INFORMATION_SCHEMA_ID = createInformationSchemaCatalogName(CATALOG_NAME);
    private final ExecutorService finishingExecutor = newCachedThreadPool(daemonThreadsNamed("transaction-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        finishingExecutor.shutdownNow();
    }

    @Test
    public void testTransactionWorkflow()
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            CatalogManager catalogManager = new CatalogManager();
            TransactionManager transactionManager = InMemoryTransactionManager.create(new TransactionManagerConfig(), executor.getExecutor(), catalogManager, finishingExecutor);

            Connector c1 = new TpchConnectorFactory().create(CATALOG, ImmutableMap.of(), new TestingConnectorContext());
            registerConnector(catalogManager, transactionManager, CATALOG, CATALOG_NAME, c1);

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertFalse(transactionInfo.isAutoCommitContext());
            assertTrue(transactionInfo.getCatalogNames().isEmpty());
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            ConnectorMetadata metadata = transactionManager.getOptionalCatalogMetadata(transactionId, CATALOG).get().getMetadata();
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession(CATALOG_NAME));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertEquals(transactionInfo.getCatalogNames(), ImmutableList.of(CATALOG_NAME, INFORMATION_SCHEMA_ID, SYSTEM_TABLES_ID));
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            getFutureValue(transactionManager.asyncCommit(transactionId));

            assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
        }
    }

    @Test
    public void testAbortedTransactionWorkflow()
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            CatalogManager catalogManager = new CatalogManager();
            TransactionManager transactionManager = InMemoryTransactionManager.create(new TransactionManagerConfig(), executor.getExecutor(), catalogManager, finishingExecutor);

            Connector c1 = new TpchConnectorFactory().create(CATALOG, ImmutableMap.of(), new TestingConnectorContext());
            registerConnector(catalogManager, transactionManager, CATALOG, CATALOG_NAME, c1);

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertFalse(transactionInfo.isAutoCommitContext());
            assertTrue(transactionInfo.getCatalogNames().isEmpty());
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            ConnectorMetadata metadata = transactionManager.getOptionalCatalogMetadata(transactionId, CATALOG).get().getMetadata();
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession(CATALOG_NAME));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertEquals(transactionInfo.getCatalogNames(), ImmutableList.of(CATALOG_NAME, INFORMATION_SCHEMA_ID, SYSTEM_TABLES_ID));
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            getFutureValue(transactionManager.asyncAbort(transactionId));

            assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
        }
    }

    @Test
    public void testFailedTransactionWorkflow()
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            CatalogManager catalogManager = new CatalogManager();
            TransactionManager transactionManager = InMemoryTransactionManager.create(new TransactionManagerConfig(), executor.getExecutor(), catalogManager, finishingExecutor);

            Connector c1 = new TpchConnectorFactory().create(CATALOG, ImmutableMap.of(), new TestingConnectorContext());
            registerConnector(catalogManager, transactionManager, CATALOG, CATALOG_NAME, c1);

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertFalse(transactionInfo.isAutoCommitContext());
            assertTrue(transactionInfo.getCatalogNames().isEmpty());
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            ConnectorMetadata metadata = transactionManager.getOptionalCatalogMetadata(transactionId, CATALOG).get().getMetadata();
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession(CATALOG_NAME));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertEquals(transactionInfo.getCatalogNames(), ImmutableList.of(CATALOG_NAME, INFORMATION_SCHEMA_ID, SYSTEM_TABLES_ID));
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            transactionManager.fail(transactionId);
            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

            try {
                transactionManager.getCatalogMetadata(transactionId, CATALOG_NAME);
                fail();
            }
            catch (PrestoException e) {
                assertEquals(e.getErrorCode(), TRANSACTION_ALREADY_ABORTED.toErrorCode());
            }
            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

            getFutureValue(transactionManager.asyncAbort(transactionId));

            assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
        }
    }

    @Test
    public void testExpiration()
            throws Exception
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            TransactionManager transactionManager = InMemoryTransactionManager.create(
                    new TransactionManagerConfig()
                            .setIdleTimeout(new Duration(1, TimeUnit.MILLISECONDS))
                            .setIdleCheckInterval(new Duration(5, TimeUnit.MILLISECONDS)),
                    executor.getExecutor(),
                    new CatalogManager(),
                    finishingExecutor);

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertFalse(transactionInfo.isAutoCommitContext());
            assertTrue(transactionInfo.getCatalogNames().isEmpty());
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            transactionManager.trySetInactive(transactionId);
            TimeUnit.MILLISECONDS.sleep(100);

            assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
        }
    }

    private static void registerConnector(
            CatalogManager catalogManager,
            TransactionManager transactionManager,
            String catalogName,
            CatalogName catalog,
            Connector connector)
    {
        CatalogName systemId = createSystemTablesCatalogName(catalog);
        InternalNodeManager nodeManager = new InMemoryNodeManager();
        MetadataManager metadata = MetadataManager.createTestMetadataManager(catalogManager);

        catalogManager.registerCatalog(new Catalog(
                catalogName,
                catalog,
                connector,
                createInformationSchemaCatalogName(catalog),
                new InformationSchemaConnector(catalogName, nodeManager, metadata, new AllowAllAccessControl()),
                systemId,
                new SystemConnector(
                        systemId,
                        nodeManager,
                        connector.getSystemTables(),
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, catalog))));
    }

    private static class IdleCheckExecutor
            implements Closeable
    {
        private final ScheduledExecutorService executorService = newSingleThreadScheduledExecutor(daemonThreadsNamed("idle-check"));

        public ScheduledExecutorService getExecutor()
        {
            return executorService;
        }

        @Override
        public void close()
        {
            executorService.shutdownNow();
        }
    }
}
