
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

import io.trino.Session;
import io.trino.Session.SessionBuilder;
import io.trino.client.NodeVersion;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.Rollback;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.StandardErrorCode.NOT_IN_TRANSACTION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestRollbackTask
{
    private final Metadata metadata = createTestMetadataManager();
    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
    }

    @Test
    public void testRollback()
    {
        TransactionManager transactionManager = createTestTransactionManager();

        Session session = sessionBuilder()
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();
        QueryStateMachine stateMachine = createQueryStateMachine("ROLLBACK", session, transactionManager);
        assertTrue(stateMachine.getSession().getTransactionId().isPresent());
        assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

        getFutureValue(new RollbackTask(transactionManager).execute(new Rollback(), stateMachine, emptyList(), WarningCollector.NOOP));
        assertTrue(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId());
        assertFalse(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent());

        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
    }

    @Test
    public void testNoTransactionRollback()
    {
        TransactionManager transactionManager = createTestTransactionManager();

        Session session = sessionBuilder()
                .build();
        QueryStateMachine stateMachine = createQueryStateMachine("ROLLBACK", session, transactionManager);

        assertTrinoExceptionThrownBy(
                () -> getFutureValue((Future<?>) new RollbackTask(transactionManager).execute(new Rollback(), stateMachine, emptyList(), WarningCollector.NOOP)))
                .hasErrorCode(NOT_IN_TRANSACTION);

        assertFalse(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId());
        assertFalse(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent());

        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
    }

    @Test
    public void testUnknownTransactionRollback()
    {
        TransactionManager transactionManager = createTestTransactionManager();

        Session session = sessionBuilder()
                .setTransactionId(TransactionId.create()) // Use a random transaction ID that is unknown to the system
                .build();
        QueryStateMachine stateMachine = createQueryStateMachine("ROLLBACK", session, transactionManager);

        getFutureValue(new RollbackTask(transactionManager).execute(new Rollback(), stateMachine, emptyList(), WarningCollector.NOOP));
        assertTrue(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId()); // Still issue clear signal
        assertFalse(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent());

        assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
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
                new AllowAllAccessControl(),
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
