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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.Session.SessionBuilder;
import io.trino.client.NodeVersion;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.sql.tree.Isolation;
import io.trino.sql.tree.StartTransaction;
import io.trino.sql.tree.TransactionAccessMode;
import io.trino.transaction.InMemoryTransactionManager;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionInfo;
import io.trino.transaction.TransactionManager;
import io.trino.transaction.TransactionManagerConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.metadata.CatalogManager.NO_CATALOGS;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.StandardErrorCode.INCOMPATIBLE_CLIENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestStartTransactionTask
{
    private final Metadata metadata = createTestMetadataManager();
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private final ScheduledExecutorService scheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testNonTransactionalClient()
    {
        Session session = sessionBuilder().build();
        TransactionManager transactionManager = createTestTransactionManager();
        QueryStateMachine stateMachine = createQueryStateMachine("START TRANSACTION", session, transactionManager);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        assertTrinoExceptionThrownBy(
                () -> getFutureValue(new StartTransactionTask(transactionManager)
                        .execute(new StartTransaction(ImmutableList.of()), stateMachine, emptyList(), WarningCollector.NOOP)))
                .hasErrorCode(INCOMPATIBLE_CLIENT);

        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());

        assertFalse(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId());
        assertFalse(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent());
    }

    @Test
    public void testNestedTransaction()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Session session = sessionBuilder()
                .setTransactionId(TransactionId.create())
                .setClientTransactionSupport()
                .build();
        QueryStateMachine stateMachine = createQueryStateMachine("START TRANSACTION", session, transactionManager);

        assertTrinoExceptionThrownBy(
                () -> getFutureValue(new StartTransactionTask(transactionManager)
                        .execute(new StartTransaction(ImmutableList.of()), stateMachine, emptyList(), WarningCollector.NOOP)))
                .hasErrorCode(NOT_SUPPORTED);

        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());

        assertFalse(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId());
        assertFalse(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent());
    }

    @Test
    public void testStartTransaction()
    {
        Session session = sessionBuilder()
                .setClientTransactionSupport()
                .build();
        TransactionManager transactionManager = createTestTransactionManager();
        QueryStateMachine stateMachine = createQueryStateMachine("START TRANSACTION", session, transactionManager);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        getFutureValue(new StartTransactionTask(transactionManager).execute(new StartTransaction(ImmutableList.of()), stateMachine, emptyList(), WarningCollector.NOOP));
        assertFalse(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId());
        assertTrue(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent());
        assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

        TransactionInfo transactionInfo = transactionManager.getTransactionInfo(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().get());
        assertFalse(transactionInfo.isAutoCommitContext());
    }

    @Test
    public void testStartTransactionExplicitModes()
    {
        Session session = sessionBuilder()
                .setClientTransactionSupport()
                .build();
        TransactionManager transactionManager = createTestTransactionManager();
        QueryStateMachine stateMachine = createQueryStateMachine("START TRANSACTION", session, transactionManager);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        getFutureValue(new StartTransactionTask(transactionManager).execute(
                new StartTransaction(ImmutableList.of(new Isolation(Isolation.Level.SERIALIZABLE), new TransactionAccessMode(true))),
                stateMachine,
                emptyList(),
                WarningCollector.NOOP));
        assertFalse(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId());
        assertTrue(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent());
        assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

        TransactionInfo transactionInfo = transactionManager.getTransactionInfo(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().get());
        assertEquals(transactionInfo.getIsolationLevel(), IsolationLevel.SERIALIZABLE);
        assertTrue(transactionInfo.isReadOnly());
        assertFalse(transactionInfo.isAutoCommitContext());
    }

    @Test
    public void testStartTransactionTooManyIsolationLevels()
    {
        Session session = sessionBuilder()
                .setClientTransactionSupport()
                .build();
        TransactionManager transactionManager = createTestTransactionManager();
        QueryStateMachine stateMachine = createQueryStateMachine("START TRANSACTION", session, transactionManager);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        assertTrinoExceptionThrownBy(() ->
                getFutureValue(new StartTransactionTask(transactionManager).execute(
                        new StartTransaction(ImmutableList.of(new Isolation(Isolation.Level.READ_COMMITTED), new Isolation(Isolation.Level.READ_COMMITTED))),
                        stateMachine,
                        emptyList(),
                        WarningCollector.NOOP)))
                .hasErrorCode(SYNTAX_ERROR);

        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());

        assertFalse(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId());
        assertFalse(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent());
    }

    @Test
    public void testStartTransactionTooManyAccessModes()
    {
        Session session = sessionBuilder()
                .setClientTransactionSupport()
                .build();
        TransactionManager transactionManager = createTestTransactionManager();
        QueryStateMachine stateMachine = createQueryStateMachine("START TRANSACTION", session, transactionManager);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        assertTrinoExceptionThrownBy(() ->
                getFutureValue(new StartTransactionTask(transactionManager).execute(
                        new StartTransaction(ImmutableList.of(new TransactionAccessMode(true), new TransactionAccessMode(true))),
                        stateMachine,
                        emptyList(),
                        WarningCollector.NOOP)))
                .hasErrorCode(SYNTAX_ERROR);

        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());

        assertFalse(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId());
        assertFalse(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent());
    }

    @Test
    public void testStartTransactionIdleExpiration()
            throws Exception
    {
        Session session = sessionBuilder()
                .setClientTransactionSupport()
                .build();
        TransactionManager transactionManager = InMemoryTransactionManager.create(
                new TransactionManagerConfig()
                        .setIdleTimeout(new Duration(1, TimeUnit.MICROSECONDS)) // Fast idle timeout
                        .setIdleCheckInterval(new Duration(10, TimeUnit.MILLISECONDS)),
                scheduledExecutor,
                NO_CATALOGS,
                executor);
        QueryStateMachine stateMachine = createQueryStateMachine("START TRANSACTION", session, transactionManager);
        assertFalse(stateMachine.getSession().getTransactionId().isPresent());

        getFutureValue(new StartTransactionTask(transactionManager).execute(
                new StartTransaction(ImmutableList.of()),
                stateMachine,
                emptyList(),
                WarningCollector.NOOP));
        assertFalse(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId());
        assertTrue(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent());

        long start = System.nanoTime();
        while (!transactionManager.getAllTransactionInfos().isEmpty()) {
            if (Duration.nanosSince(start).toMillis() > 10_000) {
                fail("Transaction did not expire in the allotted time");
            }
            TimeUnit.MILLISECONDS.sleep(10);
        }
    }

    private QueryStateMachine createQueryStateMachine(String query, Session session, TransactionManager transactionManager)
    {
        return QueryStateMachine.begin(
                Optional.empty(),
                query,
                Optional.empty(),
                session,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                true,
                transactionManager,
                new AccessControlManager(transactionManager, emptyEventListenerManager(), new AccessControlConfig(), DefaultSystemAccessControl.NAME),
                executor,
                metadata,
                WarningCollector.NOOP,
                Optional.empty(),
                true,
                new NodeVersion("test"));
    }

    private static SessionBuilder sessionBuilder()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME);
    }
}
