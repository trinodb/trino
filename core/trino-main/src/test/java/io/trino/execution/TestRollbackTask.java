
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
import io.trino.sql.RedactedQuery;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Rollback;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.metadata.TestMetadataManager.createTestMetadataManager;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.StandardErrorCode.NOT_IN_TRANSACTION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestRollbackTask
{
    private final Metadata metadata = createTestMetadataManager();
    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));

    @AfterAll
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
        assertThat(stateMachine.getSession().getTransactionId()).isPresent();
        assertThat(transactionManager.getAllTransactionInfos()).hasSize(1);

        getFutureValue(new RollbackTask(transactionManager).execute(new Rollback(new NodeLocation(1, 1)), stateMachine, emptyList(), WarningCollector.NOOP));
        assertThat(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId()).isTrue();
        assertThat(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId()).isEmpty();

        assertThat(transactionManager.getAllTransactionInfos()).isEmpty();
    }

    @Test
    public void testNoTransactionRollback()
    {
        TransactionManager transactionManager = createTestTransactionManager();

        Session session = sessionBuilder()
                .build();
        QueryStateMachine stateMachine = createQueryStateMachine("ROLLBACK", session, transactionManager);

        assertTrinoExceptionThrownBy(
                () -> getFutureValue((Future<?>) new RollbackTask(transactionManager).execute(new Rollback(new NodeLocation(1, 1)), stateMachine, emptyList(), WarningCollector.NOOP)))
                .hasErrorCode(NOT_IN_TRANSACTION);

        assertThat(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId()).isFalse();
        assertThat(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId()).isEmpty();

        assertThat(transactionManager.getAllTransactionInfos()).isEmpty();
    }

    @Test
    public void testUnknownTransactionRollback()
    {
        TransactionManager transactionManager = createTestTransactionManager();

        Session session = sessionBuilder()
                .setTransactionId(TransactionId.create()) // Use a random transaction ID that is unknown to the system
                .build();
        QueryStateMachine stateMachine = createQueryStateMachine("ROLLBACK", session, transactionManager);

        getFutureValue(new RollbackTask(transactionManager).execute(new Rollback(new NodeLocation(1, 1)), stateMachine, emptyList(), WarningCollector.NOOP));
        assertThat(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId()).isTrue(); // Still issue clear signal
        assertThat(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId()).isEmpty();

        assertThat(transactionManager.getAllTransactionInfos()).isEmpty();
    }

    private QueryStateMachine createQueryStateMachine(String query, Session session, TransactionManager transactionManager)
    {
        return QueryStateMachine.begin(
                Optional.empty(),
                _ -> new RedactedQuery(query, Optional.empty()),
                session,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                true,
                transactionManager,
                new AllowAllAccessControl(),
                executor,
                metadata,
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector(),
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
