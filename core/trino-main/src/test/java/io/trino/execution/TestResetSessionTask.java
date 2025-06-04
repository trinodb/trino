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
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AccessControl;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.ResetSession;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestResetSessionTask
{
    private static final String CATALOG_NAME = "my_catalog";
    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private QueryRunner queryRunner;
    private TransactionManager transactionManager;
    private AccessControl accessControl;
    private Metadata metadata;
    private SessionPropertyManager sessionPropertyManager;

    @BeforeAll
    public void setUp()
    {
        queryRunner = new StandaloneQueryRunner(TEST_SESSION);
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withSessionProperty(stringProperty(
                        "baz",
                        "test property",
                        null,
                        false))
                .build()));
        queryRunner.createCatalog(CATALOG_NAME, "mock", ImmutableMap.of());

        transactionManager = queryRunner.getTransactionManager();
        accessControl = queryRunner.getAccessControl();
        metadata = queryRunner.getPlannerContext().getMetadata();
        sessionPropertyManager = queryRunner.getSessionPropertyManager();
    }

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
        queryRunner.close();
        queryRunner = null;
        transactionManager = null;
        accessControl = null;
        metadata = null;
        sessionPropertyManager = null;
    }

    @Test
    public void test()
    {
        Session session = testSessionBuilder(sessionPropertyManager)
                .setCatalogSessionProperty(CATALOG_NAME, "baz", "blah")
                .build();

        QueryStateMachine stateMachine = QueryStateMachine.begin(
                Optional.empty(),
                "reset foo",
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

        getFutureValue(new ResetSessionTask(metadata, sessionPropertyManager).execute(
                new ResetSession(new NodeLocation(1, 1), QualifiedName.of(CATALOG_NAME, "baz")),
                stateMachine,
                emptyList(),
                WarningCollector.NOOP));

        Set<String> sessionProperties = stateMachine.getResetSessionProperties();
        assertThat(sessionProperties).isEqualTo(ImmutableSet.of(CATALOG_NAME + ".baz"));
    }
}
