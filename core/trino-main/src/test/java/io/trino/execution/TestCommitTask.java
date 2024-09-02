
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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.secrets.SecretsResolver;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.Session;
import io.trino.Session.SessionBuilder;
import io.trino.client.NodeVersion;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.Commit;
import io.trino.sql.tree.NodeLocation;
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
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.StandardErrorCode.NOT_IN_TRANSACTION;
import static io.trino.spi.StandardErrorCode.UNKNOWN_TRANSACTION;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
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
public class TestCommitTask
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
    public void testCommit()
    {
        TransactionManager transactionManager = createTestTransactionManager();

        Session session = sessionBuilder()
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();
        QueryStateMachine stateMachine = createQueryStateMachine("COMMIT", session, transactionManager);
        assertThat(stateMachine.getSession().getTransactionId().isPresent()).isTrue();
        assertThat(transactionManager.getAllTransactionInfos().size()).isEqualTo(1);

        getFutureValue(new CommitTask(transactionManager).execute(new Commit(new NodeLocation(1, 1)), stateMachine, emptyList(), WarningCollector.NOOP));
        assertThat(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId()).isTrue();
        assertThat(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent()).isFalse();

        assertThat(transactionManager.getAllTransactionInfos().isEmpty()).isTrue();
    }

    @Test
    public void testNoTransactionCommit()
    {
        TransactionManager transactionManager = createTestTransactionManager();

        Session session = sessionBuilder()
                .build();
        QueryStateMachine stateMachine = createQueryStateMachine("COMMIT", session, transactionManager);

        assertTrinoExceptionThrownBy(
                () -> getFutureValue(new CommitTask(transactionManager).execute(new Commit(new NodeLocation(1, 1)), stateMachine, emptyList(), WarningCollector.NOOP)))
                .hasErrorCode(NOT_IN_TRANSACTION);

        assertThat(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId()).isFalse();
        assertThat(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent()).isFalse();

        assertThat(transactionManager.getAllTransactionInfos().isEmpty()).isTrue();
    }

    @Test
    public void testUnknownTransactionCommit()
    {
        TransactionManager transactionManager = createTestTransactionManager();

        Session session = sessionBuilder()
                .setTransactionId(TransactionId.create()) // Use a random transaction ID that is unknown to the system
                .build();
        QueryStateMachine stateMachine = createQueryStateMachine("COMMIT", session, transactionManager);

        Future<?> future = new CommitTask(transactionManager).execute(new Commit(new NodeLocation(1, 1)), stateMachine, emptyList(), WarningCollector.NOOP);
        assertTrinoExceptionThrownBy(() -> getFutureValue(future))
                .hasErrorCode(UNKNOWN_TRANSACTION);

        assertThat(stateMachine.getQueryInfo(Optional.empty()).isClearTransactionId()).isTrue(); // Still issue clear signal
        assertThat(stateMachine.getQueryInfo(Optional.empty()).getStartedTransactionId().isPresent()).isFalse();

        assertThat(transactionManager.getAllTransactionInfos().isEmpty()).isTrue();
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
                new AccessControlManager(NodeVersion.UNKNOWN, transactionManager, emptyEventListenerManager(), new AccessControlConfig(), OpenTelemetry.noop(), new SecretsResolver(ImmutableMap.of()), DefaultSystemAccessControl.NAME),
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
