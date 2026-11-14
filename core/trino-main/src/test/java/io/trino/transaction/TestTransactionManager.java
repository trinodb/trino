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
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.trino.NotInTransactionException;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.connector.MockConnectorTransactionHandle.INSTANCE;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestTransactionManager
{
    private final ExecutorService finishingExecutor = newCachedThreadPool(daemonThreadsNamed("transaction-%s"));

    @AfterAll
    public void tearDown()
    {
        finishingExecutor.shutdownNow();
    }

    @Test
    public void testTransactionWorkflow()
    {
        try (QueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION)) {
            TransactionManager transactionManager = queryRunner.getTransactionManager();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog(TEST_CATALOG_NAME, "tpch", ImmutableMap.of());

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertThat(transactionManager.getAllTransactionInfos()).hasSize(1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertThat(transactionInfo.isAutoCommitContext()).isFalse();
            assertThat(transactionInfo.getCatalogNames()).isEmpty();
            assertThat(transactionInfo.getWrittenCatalogName()).isEmpty();

            ConnectorMetadata metadata = transactionManager.getOptionalCatalogMetadata(transactionId, TEST_CATALOG_NAME).get().getMetadata(TEST_SESSION);
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession(TEST_CATALOG_HANDLE));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertThat(transactionInfo.getCatalogNames()).isEqualTo(ImmutableList.of(TEST_CATALOG_NAME));
            assertThat(transactionInfo.getWrittenCatalogName()).isEmpty();

            getFutureValue(transactionManager.asyncCommit(transactionId));

            assertThat(transactionManager.getAllTransactionInfos()).isEmpty();
        }
    }

    @Test
    public void testCommitPreservesTracingContext()
    {
        ContextKey<String> query = ContextKey.named("query");
        Map<String, String> commitContexts = new ConcurrentHashMap<>();
        ConnectorFactory factory = new ConnectorFactory()
        {
            @Override
            public String getName()
            {
                return "commit_context";
            }

            @Override
            public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
            {
                return new Connector()
                {
                    @Override
                    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
                    {
                        return INSTANCE;
                    }

                    @Override
                    public void commit(ConnectorTransactionHandle transactionHandle)
                    {
                        String queryId = Context.current().get(query);
                        if (queryId != null) {
                            commitContexts.put(catalogName, queryId);
                        }
                    }

                    @Override
                    public void shutdown() {}
                };
            }
        };
        try (QueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION)) {
            queryRunner.installPlugin(new Plugin()
            {
                @Override
                public Iterable<ConnectorFactory> getConnectorFactories()
                {
                    return List.of(factory);
                }
            });
            queryRunner.createCatalog("write_catalog", factory.getName(), Map.of());
            queryRunner.createCatalog("read_catalog", factory.getName(), Map.of());
            TransactionManager transactionManager = queryRunner.getTransactionManager();
            TransactionId transactionId = transactionManager.beginTransaction(true);
            transactionManager.getCatalogMetadataForWrite(transactionId, "write_catalog");
            transactionManager.getOptionalCatalogMetadata(transactionId, "read_catalog").orElseThrow();

            try (var _ = Context.current().with(query, "query-123").makeCurrent()) {
                getFutureValue(transactionManager.asyncCommit(transactionId));
            }
            assertThat(commitContexts).isEqualTo(Map.of("write_catalog", "query-123", "read_catalog", "query-123"));
        }
    }

    @Test
    public void testAbortedTransactionWorkflow()
    {
        try (QueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION)) {
            TransactionManager transactionManager = queryRunner.getTransactionManager();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog(TEST_CATALOG_NAME, "tpch", ImmutableMap.of());

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertThat(transactionManager.getAllTransactionInfos()).hasSize(1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertThat(transactionInfo.isAutoCommitContext()).isFalse();
            assertThat(transactionInfo.getCatalogNames()).isEmpty();
            assertThat(transactionInfo.getWrittenCatalogName()).isEmpty();

            ConnectorMetadata metadata = transactionManager.getOptionalCatalogMetadata(transactionId, TEST_CATALOG_NAME).get().getMetadata(TEST_SESSION);
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession(TEST_CATALOG_HANDLE));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertThat(transactionInfo.getCatalogNames()).isEqualTo(ImmutableList.of(TEST_CATALOG_NAME));
            assertThat(transactionInfo.getWrittenCatalogName()).isEmpty();

            getFutureValue(transactionManager.asyncAbort(transactionId));

            assertThat(transactionManager.getAllTransactionInfos()).isEmpty();
        }
    }

    @Test
    public void testFailedTransactionWorkflow()
    {
        try (QueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION)) {
            TransactionManager transactionManager = queryRunner.getTransactionManager();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog(TEST_CATALOG_NAME, "tpch", ImmutableMap.of());

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertThat(transactionManager.getAllTransactionInfos()).hasSize(1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertThat(transactionInfo.isAutoCommitContext()).isFalse();
            assertThat(transactionInfo.getCatalogNames()).isEmpty();
            assertThat(transactionInfo.getWrittenCatalogName()).isEmpty();

            ConnectorMetadata metadata = transactionManager.getOptionalCatalogMetadata(transactionId, TEST_CATALOG_NAME).get().getMetadata(TEST_SESSION);
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession(TEST_CATALOG_HANDLE));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertThat(transactionInfo.getCatalogNames()).isEqualTo(ImmutableList.of(TEST_CATALOG_NAME));
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
    public void testMetricsCollectionAfterTransactionFinishes()
    {
        try (QueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION)) {
            TransactionManager transactionManager = queryRunner.getTransactionManager();
            Metadata metadata = queryRunner.getPlannerContext().getMetadata();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog(TEST_CATALOG_NAME, "tpch", ImmutableMap.of());

            TransactionId transactionId = transactionManager.beginTransaction(false);
            Session session = TEST_SESSION.beginTransactionId(transactionId, transactionManager, queryRunner.getAccessControl());

            // register the catalog as active in the transaction, materializing the transactional ConnectorMetadata
            transactionManager.getOptionalCatalogMetadata(transactionId, TEST_CATALOG_NAME).orElseThrow().getMetadata(session);

            getFutureValue(transactionManager.asyncCommit(transactionId));

            // metrics collection races with query completion: the transaction is already finished, so
            // getMetrics will throw NotInTransactionException that is then handled by
            assertThatThrownBy(() -> metadata.getMetrics(session, TEST_CATALOG_NAME))
                    .isInstanceOf(NotInTransactionException.class);
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
            assertEventually(new Duration(10, SECONDS), () -> assertThat(transactionManager.getAllTransactionInfos()).isEmpty());
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
