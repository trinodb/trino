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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
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
import static io.trino.metadata.CatalogManager.NO_CATALOGS;
import static io.trino.spi.StandardErrorCode.TRANSACTION_ALREADY_ABORTED;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTransactionManager
{
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

            queryRunner.createCatalog(TEST_CATALOG_NAME, new TpchConnectorFactory(), ImmutableMap.of());

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertThat(transactionManager.getAllTransactionInfos()).hasSize(1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertThat(transactionInfo.isAutoCommitContext()).isFalse();
            assertThat(transactionInfo.getCatalogNames()).isEmpty();
            assertThat(transactionInfo.getWrittenCatalogName()).isEmpty();

            ConnectorMetadata metadata = transactionManager.getOptionalCatalogMetadata(transactionId, TEST_CATALOG_NAME).get().getMetadata(TEST_SESSION);
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession(TEST_CATALOG_HANDLE));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertThat(transactionInfo.getCatalogNames()).containsExactly(TEST_CATALOG_NAME);
            assertThat(transactionInfo.getWrittenCatalogName()).isEmpty();

            getFutureValue(transactionManager.asyncCommit(transactionId));

            assertThat(transactionManager.getAllTransactionInfos()).isEmpty();
        }
    }

    @Test
    public void testAbortedTransactionWorkflow()
    {
        try (LocalQueryRunner queryRunner = LocalQueryRunner.create(TEST_SESSION)) {
            TransactionManager transactionManager = queryRunner.getTransactionManager();

            queryRunner.createCatalog(TEST_CATALOG_NAME, new TpchConnectorFactory(), ImmutableMap.of());

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertThat(transactionManager.getAllTransactionInfos()).hasSize(1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertThat(transactionInfo.isAutoCommitContext()).isFalse();
            assertThat(transactionInfo.getCatalogNames()).isEmpty();
            assertThat(transactionInfo.getWrittenCatalogName()).isEmpty();

            ConnectorMetadata metadata = transactionManager.getOptionalCatalogMetadata(transactionId, TEST_CATALOG_NAME).get().getMetadata(TEST_SESSION);
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession(TEST_CATALOG_HANDLE));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertThat(transactionInfo.getCatalogNames()).containsExactly(TEST_CATALOG_NAME);
            assertThat(transactionInfo.getWrittenCatalogName()).isEmpty();

            getFutureValue(transactionManager.asyncAbort(transactionId));

            assertThat(transactionManager.getAllTransactionInfos()).isEmpty();
        }
    }

    @Test
    public void testFailedTransactionWorkflow()
    {
        try (LocalQueryRunner queryRunner = LocalQueryRunner.create(TEST_SESSION)) {
            TransactionManager transactionManager = queryRunner.getTransactionManager();

            queryRunner.createCatalog(TEST_CATALOG_NAME, new TpchConnectorFactory(), ImmutableMap.of());

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertThat(transactionManager.getAllTransactionInfos()).hasSize(1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertThat(transactionInfo.isAutoCommitContext()).isFalse();
            assertThat(transactionInfo.getCatalogNames()).isEmpty();
            assertThat(transactionInfo.getWrittenCatalogName()).isEmpty();

            ConnectorMetadata metadata = transactionManager.getOptionalCatalogMetadata(transactionId, TEST_CATALOG_NAME).get().getMetadata(TEST_SESSION);
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession(TEST_CATALOG_HANDLE));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertThat(transactionInfo.getCatalogNames()).containsExactly(TEST_CATALOG_NAME);
            assertThat(transactionInfo.getWrittenCatalogName()).isEmpty();

            transactionManager.fail(transactionId);
            assertThat(transactionManager.getAllTransactionInfos()).hasSize(1);

            assertTrinoExceptionThrownBy(() -> transactionManager.getCatalogMetadata(transactionId, TEST_CATALOG_HANDLE))
                    .hasErrorCode(TRANSACTION_ALREADY_ABORTED);

            assertThat(transactionManager.getAllTransactionInfos()).hasSize(1);

            getFutureValue(transactionManager.asyncAbort(transactionId));

            assertThat(transactionManager.getAllTransactionInfos()).isEmpty();
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
                    NO_CATALOGS,
                    finishingExecutor);

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertThat(transactionManager.getAllTransactionInfos()).hasSize(1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertThat(transactionInfo.isAutoCommitContext()).isFalse();
            assertThat(transactionInfo.getCatalogNames()).isEmpty();
            assertThat(transactionInfo.getWrittenCatalogName()).isEmpty();

            transactionManager.trySetInactive(transactionId);
            assertEventually(new Duration(10, SECONDS), () -> assertThat(transactionManager.getAllTransactionInfos().isEmpty()).isTrue());
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
