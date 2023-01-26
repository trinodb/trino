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

import io.trino.client.NodeVersion;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.SetSessionAuthorization;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.metadata.MetadataManager.testMetadataManagerBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestSetSessionAuthorizationTask
{
    private TransactionManager transactionManager;
    private AccessControl accessControl;
    private Metadata metadata;
    private SqlParser parser;
    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));

    @BeforeClass
    public void setUp()
    {
        transactionManager = createTestTransactionManager();
        accessControl = new AllowAllAccessControl();
        metadata = testMetadataManagerBuilder()
                .withTransactionManager(transactionManager)
                .build();
        parser = new SqlParser();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
        transactionManager = null;
        accessControl = null;
        metadata = null;
    }

    @Test
    public void testSetSessionAuthorization()
    {
        assertSetSessionAuthorization("SET SESSION AUTHORIZATION otheruser", Optional.of("otheruser"));
        assertSetSessionAuthorization("SET SESSION AUTHORIZATION 'otheruser'", Optional.of("otheruser"));
        assertSetSessionAuthorization("SET SESSION AUTHORIZATION \"otheruser\"", Optional.of("otheruser"));
    }

    @Test
    public void testSetSessionAuthorizationInTransaction()
    {
        String query = "SET SESSION AUTHORIZATION user";
        SetSessionAuthorization statement = (SetSessionAuthorization) parser.createStatement(query, new ParsingOptions());
        TransactionId transactionId = transactionManager.beginTransaction(false);
        QueryStateMachine stateMachine = createStateMachine(Optional.of(transactionId), query);
        assertThatThrownBy(() -> new SetSessionAuthorizationTask(accessControl, transactionManager).execute(statement, stateMachine, emptyList(), WarningCollector.NOOP))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Can't set authorization user in the middle of a transaction");
    }

    private void assertSetSessionAuthorization(String query, Optional<String> expected)
    {
        SetSessionAuthorization statement = (SetSessionAuthorization) parser.createStatement(query, new ParsingOptions());
        QueryStateMachine stateMachine = createStateMachine(Optional.empty(), query);
        new SetSessionAuthorizationTask(accessControl, transactionManager).execute(statement, stateMachine, emptyList(), WarningCollector.NOOP);
        QueryInfo queryInfo = stateMachine.getQueryInfo(Optional.empty());
        assertEquals(queryInfo.getSetAuthorizationUser(), expected);
    }

    private QueryStateMachine createStateMachine(Optional<TransactionId> transactionId, String query)
    {
        QueryStateMachine stateMachine = QueryStateMachine.begin(
                transactionId,
                query,
                Optional.empty(),
                testSessionBuilder().build(),
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                transactionManager,
                accessControl,
                executor,
                metadata,
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector(),
                Optional.empty(),
                true,
                new NodeVersion("test"));
        return stateMachine;
    }
}
