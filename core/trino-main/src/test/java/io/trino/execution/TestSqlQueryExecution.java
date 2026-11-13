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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.trino.Session;
import io.trino.exchange.ExchangeMetricsCollector;
import io.trino.execution.QueryExecution.QueryExecutionFactory;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.execution.scheduler.faulttolerant.TaskDescriptorStorage;
import io.trino.execution.warnings.WarningCollector;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.protocol.Slug;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.NodeVersion;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.SystemSessionProperties.RETRY_POLICY;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.PLANNING;
import static io.trino.execution.QueryState.STARTING;
import static io.trino.execution.TestSqlQueryExecution.Ending.CANCEL;
import static io.trino.execution.TestSqlQueryExecution.Ending.FAIL;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestSqlQueryExecution
{
    private static final String QUERY = "SELECT count(*) FROM tpch.tiny.nation";

    private TestingTrinoServer server;
    private QueryExecutionFactory<?> executionFactory;

    @BeforeAll
    void setUp()
    {
        server = TestingTrinoServer.create();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
        server.installPlugin(new FileSystemExchangePlugin());
        server.loadExchangeManager("filesystem", ImmutableMap.of(
                "exchange.base-directories", System.getProperty("java.io.tmpdir") + "/trino-local-file-system-exchange-manager"));
        executionFactory = server.getInstance(Key.get(new TypeLiteral<Map<Class<? extends Statement>, QueryExecutionFactory<?>>>() {})).get(Query.class);
    }

    @AfterAll
    void tearDown()
            throws Exception
    {
        server.close();
        server = null;
        executionFactory = null;
    }

    @Test
    void testCancelWhilePlanningPublishesFinalQueryInfo()
    {
        assertFinalQueryInfoWhenQueryEnds(session(), PLANNING, CANCEL);
    }

    @Test
    void testCancelWhileStartingPublishesFinalQueryInfo()
    {
        assertFinalQueryInfoWhenQueryEnds(session(), STARTING, CANCEL);
    }

    @Test
    void testFailWhileStartingPublishesFinalQueryInfo()
    {
        assertFinalQueryInfoWhenQueryEnds(session(), STARTING, FAIL);
    }

    @Test
    void testCancelWhileStartingPublishesFinalQueryInfoWithTaskRetries()
    {
        Session session = testSessionBuilder(server.getSessionPropertyManager())
                .setSystemProperty(RETRY_POLICY, "TASK")
                .build();
        assertFinalQueryInfoWhenQueryEnds(session, STARTING, CANCEL);
    }

    @Test
    void testFailWhileStartingTheSchedulerPublishesFinalQueryInfoWithTaskRetries()
    {
        Session session = testSessionBuilder(server.getSessionPropertyManager())
                .setSystemProperty(RETRY_POLICY, "TASK")
                .build();
        QueryStateMachine stateMachine = createStateMachine(session);
        // a storage already initialized for the query makes the first step of the scheduler throw
        TaskDescriptorStorage taskDescriptorStorage = server.getInstance(Key.get(TaskDescriptorStorage.class));
        taskDescriptorStorage.initialize(stateMachine.getQueryId());
        try {
            assertFinalQueryInfoPublished(stateMachine);
        }
        finally {
            taskDescriptorStorage.destroy(stateMachine.getQueryId());
        }
    }

    private void assertFinalQueryInfoWhenQueryEnds(Session session, QueryState endAt, Ending ending)
    {
        QueryStateMachine stateMachine = createStateMachine(session);
        stateMachine.addStateChangeListener(state -> {
            if (state == endAt) {
                switch (ending) {
                    case CANCEL -> stateMachine.transitionToCanceled();
                    case FAIL -> stateMachine.transitionToFailed(new RuntimeException("query failed"));
                }
            }
        });
        assertFinalQueryInfoPublished(stateMachine);
    }

    private QueryStateMachine createStateMachine(Session session)
    {
        return QueryStateMachine.begin(
                Optional.empty(),
                QUERY,
                Optional.empty(),
                session,
                URI.create("http://localhost/query"),
                new ResourceGroupId("test"),
                false,
                server.getTransactionManager(),
                server.getAccessControl(),
                // listeners run inline, so a query ended by a listener ends inside the transition, before the next step of start()
                directExecutor(),
                server.getPlannerContext().getMetadata(),
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector(),
                new ExchangeMetricsCollector(ImmutableList::of, Duration.ofMillis(1)),
                Optional.of(QueryType.SELECT),
                false,
                Optional.empty(),
                new NodeVersion("test"));
    }

    private void assertFinalQueryInfoPublished(QueryStateMachine stateMachine)
    {
        PreparedQuery preparedQuery = new QueryPreparer(new SqlParser()).prepareQuery(stateMachine.getSession(), QUERY);
        QueryExecution execution = executionFactory.createQueryExecution(
                preparedQuery,
                stateMachine,
                Slug.createNew(),
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector());
        SettableFuture<QueryInfo> finalQueryInfo = SettableFuture.create();
        execution.addFinalQueryInfoListener(finalQueryInfo::set);

        execution.start();

        assertThat(execution.getState()).isEqualTo(FAILED);
        // every listener ran inline, so the final query info is published by the time start() returns
        assertThat(finalQueryInfo.isDone()).isTrue();
        assertThat(finalQueryInfo.resultNow().isFinalQueryInfo()).isTrue();
    }

    private Session session()
    {
        return testSessionBuilder(server.getSessionPropertyManager()).build();
    }

    enum Ending
    {
        CANCEL,
        FAIL,
    }
}
