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
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.secrets.SecretsResolver;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.metadata.TestMetadataManager.createTestMetadataManager;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
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
public class TestDeallocateTask
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
    public void testDeallocate()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT bar, baz FROM foo")
                .build();
        Set<String> statements = executeDeallocate("my_query", "DEALLOCATE PREPARE my_query", session);
        assertThat(statements).isEqualTo(ImmutableSet.of("my_query"));
    }

    @Test
    public void testDeallocateNoSuchStatement()
    {
        assertTrinoExceptionThrownBy(() -> executeDeallocate("my_query", "DEALLOCATE PREPARE my_query", TEST_SESSION))
                .hasErrorCode(NOT_FOUND)
                .hasMessage("Prepared statement not found: my_query");
    }

    private Set<String> executeDeallocate(String statementName, String sqlString, Session session)
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControl = new AccessControlManager(
                NodeVersion.UNKNOWN,
                transactionManager,
                emptyEventListenerManager(),
                new AccessControlConfig(),
                OpenTelemetry.noop(),
                new SecretsResolver(ImmutableMap.of()),
                DefaultSystemAccessControl.NAME);
        accessControl.setSystemAccessControls(List.of(AllowAllSystemAccessControl.INSTANCE));
        QueryStateMachine stateMachine = QueryStateMachine.begin(
                Optional.empty(),
                sqlString,
                Optional.empty(),
                session,
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
                Optional.empty(),
                new NodeVersion("test"));
        Deallocate deallocate = new Deallocate(new NodeLocation(1, 1), new Identifier(statementName));
        new DeallocateTask().execute(deallocate, stateMachine, emptyList(), WarningCollector.NOOP);
        return stateMachine.getDeallocatedPreparedStatements();
    }
}
