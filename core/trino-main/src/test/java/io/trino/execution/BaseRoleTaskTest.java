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
import com.google.common.collect.ImmutableSet;
import io.trino.client.NodeVersion;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.Identity;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * Base class for various *RoleTask test classes.  Distinct from {@link BaseDataDefinitionTaskTest} due to different catalogs and metadata.
 */
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class BaseRoleTaskTest
{
    protected static final String CATALOG_NAME = "foo";
    protected static final String SYSTEM_ROLE_CATALOG_NAME = "system_role";
    protected static final String ROLE_NAME = "bar";
    private static final String USER_NAME = "user";
    protected AccessControl accessControl;
    protected Metadata metadata;
    private QueryRunner queryRunner;
    private TransactionManager transactionManager;
    private ExecutorService executor;
    private SqlParser parser;

    @BeforeAll
    public void setUp()
    {
        queryRunner = new StandaloneQueryRunner(TEST_SESSION);
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withListRoleGrants((connectorSession, roles, grantees, limit) -> ImmutableSet.of(new RoleGrant(new TrinoPrincipal(USER, USER_NAME), ROLE_NAME, false)))
                .build()));
        queryRunner.createCatalog(CATALOG_NAME, "mock", ImmutableMap.of());

        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withName("system_role_connector")
                .build()));
        queryRunner.createCatalog(SYSTEM_ROLE_CATALOG_NAME, "system_role_connector", ImmutableMap.of());

        transactionManager = queryRunner.getTransactionManager();
        accessControl = queryRunner.getAccessControl();
        metadata = queryRunner.getPlannerContext().getMetadata();
        parser = new SqlParser();
        executor = newCachedThreadPool(daemonThreadsNamed("test-set-role-task-executor-%s"));
    }

    @AfterAll
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
        executor.shutdownNow();
        executor = null;
        metadata = null;
        accessControl = null;
        transactionManager = null;
        parser = null;
    }

    protected <T extends Statement> QueryStateMachine execute(String statement, DataDefinitionTask<T> task)
    {
        QueryStateMachine stateMachine = QueryStateMachine.begin(
                Optional.empty(),
                statement,
                Optional.empty(),
                testSessionBuilder()
                        .setIdentity(Identity.ofUser(USER_NAME))
                        .build(),
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
        task.execute((T) parser.createStatement(statement), stateMachine, ImmutableList.of(), WarningCollector.NOOP);
        return stateMachine;
    }
}
