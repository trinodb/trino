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
package io.trino.transaction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.connector.CatalogName;
import io.trino.metadata.CatalogManager;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.connector.CatalogName.createInformationSchemaCatalogName;
import static io.trino.connector.CatalogName.createSystemTablesCatalogName;
import static io.trino.spi.StandardErrorCode.TRANSACTION_ALREADY_ABORTED;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
        try (LocalQueryRunner queryRunner = LocalQueryRunner.create(TEST_SESSION)) {
            TransactionManager transactionManager = queryRunner.getTransactionManager();

            queryRunner.createCatalog(CATALOG, new TpchConnectorFactory(), ImmutableMap.of());

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertFalse(transactionInfo.isAutoCommitContext());
            assertTrue(transactionInfo.getCatalogNames().isEmpty());
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            ConnectorMetadata metadata = transactionManager.getOptionalCatalogMetadata(transactionId, CATALOG).get().getMetadata(TEST_SESSION);
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
        try (LocalQueryRunner queryRunner = LocalQueryRunner.create(TEST_SESSION)) {
            TransactionManager transactionManager = queryRunner.getTransactionManager();

            queryRunner.createCatalog(CATALOG, new TpchConnectorFactory(), ImmutableMap.of());

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertFalse(transactionInfo.isAutoCommitContext());
            assertTrue(transactionInfo.getCatalogNames().isEmpty());
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            ConnectorMetadata metadata = transactionManager.getOptionalCatalogMetadata(transactionId, CATALOG).get().getMetadata(TEST_SESSION);
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
        try (LocalQueryRunner queryRunner = LocalQueryRunner.create(TEST_SESSION)) {
            TransactionManager transactionManager = queryRunner.getTransactionManager();

            queryRunner.createCatalog(CATALOG, new TpchConnectorFactory(), ImmutableMap.of());

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertFalse(transactionInfo.isAutoCommitContext());
            assertTrue(transactionInfo.getCatalogNames().isEmpty());
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            ConnectorMetadata metadata = transactionManager.getOptionalCatalogMetadata(transactionId, CATALOG).get().getMetadata(TEST_SESSION);
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession(CATALOG_NAME));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertEquals(transactionInfo.getCatalogNames(), ImmutableList.of(CATALOG_NAME, INFORMATION_SCHEMA_ID, SYSTEM_TABLES_ID));
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            transactionManager.fail(transactionId);
            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

            assertTrinoExceptionThrownBy(() -> transactionManager.getCatalogMetadata(transactionId, CATALOG_NAME))
                    .hasErrorCode(TRANSACTION_ALREADY_ABORTED);

            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

            getFutureValue(transactionManager.asyncAbort(transactionId));

            assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
        }
    }

    @Test
    public void testExpiration()
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
            assertEventually(new Duration(10, SECONDS), () -> assertTrue(transactionManager.getAllTransactionInfos().isEmpty()));
        }
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
