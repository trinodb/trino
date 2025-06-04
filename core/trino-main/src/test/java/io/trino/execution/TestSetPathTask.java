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
import io.trino.client.NodeVersion;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.TestMetadataManager;
import io.trino.security.AccessControl;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.PathElement;
import io.trino.sql.tree.PathSpecification;
import io.trino.sql.tree.SetPath;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestSetPathTask
{
    private TransactionManager transactionManager;
    private AccessControl accessControl;
    private Metadata metadata;

    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));

    @BeforeAll
    public void setUp()
    {
        transactionManager = createTestTransactionManager();
        accessControl = new AllowAllAccessControl();

        metadata = TestMetadataManager.builder()
                .withTransactionManager(transactionManager)
                .build();
    }

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
        transactionManager = null;
        accessControl = null;
        metadata = null;
    }

    @Test
    public void testSetPath()
    {
        PathSpecification pathSpecification = new PathSpecification(new NodeLocation(1, 10), ImmutableList.of(
                new PathElement(new NodeLocation(1, 1), Optional.empty(), new Identifier("foo"))));

        QueryStateMachine stateMachine = createQueryStateMachine("SET PATH foo");
        executeSetPathTask(pathSpecification, stateMachine);

        assertThat(stateMachine.getSetPath()).isEqualTo("foo");
    }

    @Test
    public void testSetPathInvalidCatalog()
    {
        PathSpecification invalidPathSpecification = new PathSpecification(new NodeLocation(1, 10), ImmutableList.of(
                new PathElement(new NodeLocation(1, 1), Optional.of(new Identifier("invalidCatalog")), new Identifier("thisDoesNotMatter"))));

        QueryStateMachine stateMachine = createQueryStateMachine("SET PATH invalidCatalog.thisDoesNotMatter");

        assertThatThrownBy(() -> executeSetPathTask(invalidPathSpecification, stateMachine))
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching(".* Catalog '.*' not found");
    }

    private QueryStateMachine createQueryStateMachine(String query)
    {
        return QueryStateMachine.begin(
                Optional.empty(),
                query,
                Optional.empty(),
                testSession(),
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
    }

    private void executeSetPathTask(PathSpecification pathSpecification, QueryStateMachine stateMachine)
    {
        getFutureValue(new SetPathTask(metadata).execute(
                new SetPath(new NodeLocation(1, 1), pathSpecification),
                stateMachine,
                emptyList(),
                WarningCollector.NOOP));
    }
}
