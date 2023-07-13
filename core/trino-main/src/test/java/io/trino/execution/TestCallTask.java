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
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogServiceProvider;
import io.trino.connector.MockConnectorFactory;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.CatalogProcedures;
import io.trino.metadata.Metadata;
import io.trino.metadata.ProcedureRegistry;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.security.AccessControl;
import io.trino.security.AllowAllAccessControl;
import io.trino.security.DenyAllAccessControl;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.AccessDeniedException;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Call;
import io.trino.sql.tree.QualifiedName;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingAccessControlManager;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.util.Reflection.methodHandle;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestCallTask
{
    private ExecutorService executor;

    private static boolean invoked;
    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void init()
    {
        queryRunner = LocalQueryRunner.builder(TEST_SESSION).build();
        queryRunner.createCatalog(TEST_CATALOG_NAME, MockConnectorFactory.create(), ImmutableMap.of());
        executor = newCachedThreadPool(daemonThreadsNamed("call-task-test-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void close()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
        executor.shutdownNow();
        executor = null;
    }

    @BeforeMethod
    public void cleanup()
    {
        invoked = false;
    }

    @Test
    public void testExecute()
    {
        executeCallTask(methodHandle(TestCallTask.class, "testingMethod"), transactionManager -> new AllowAllAccessControl());
        assertThat(invoked).isTrue();
    }

    @Test
    public void testExecuteNoPermission()
    {
        assertThatThrownBy(
                () -> executeCallTask(methodHandle(TestCallTask.class, "testingMethod"), transactionManager -> new DenyAllAccessControl()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot execute procedure test-catalog.test.testing_procedure");

        assertThat(invoked).isFalse();
    }

    @Test
    public void testExecuteNoPermissionOnInsert()
    {
        assertThatThrownBy(
                () -> executeCallTask(
                        methodHandle(TestingProcedure.class, "testingMethod", ConnectorAccessControl.class),
                        transactionManager -> {
                            TestingAccessControlManager accessControl = new TestingAccessControlManager(transactionManager, emptyEventListenerManager());
                            accessControl.loadSystemAccessControl(AllowAllSystemAccessControl.NAME, ImmutableMap.of());
                            accessControl.deny(privilege("testing_table", INSERT_TABLE));
                            return accessControl;
                        }))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot insert into table test-catalog.test.testing_table");
    }

    private void executeCallTask(MethodHandle methodHandle, Function<TransactionManager, AccessControl> accessControlProvider)
    {
        TransactionManager transactionManager = queryRunner.getTransactionManager();
        ProcedureRegistry procedureRegistry = new ProcedureRegistry(CatalogServiceProvider.singleton(
                queryRunner.getCatalogHandle(TEST_CATALOG_NAME),
                new CatalogProcedures(ImmutableList.of(new Procedure(
                        "test",
                        "testing_procedure",
                        ImmutableList.of(),
                        methodHandle)))));
        AccessControl accessControl = accessControlProvider.apply(transactionManager);

        PlannerContext plannerContext = plannerContextBuilder()
                .withTransactionManager(transactionManager)
                .build();
        new CallTask(transactionManager, plannerContext, accessControl, procedureRegistry)
                .execute(
                        new Call(QualifiedName.of("testing_procedure"), ImmutableList.of()),
                        stateMachine(transactionManager, plannerContext.getMetadata(), accessControl),
                        ImmutableList.of(),
                        WarningCollector.NOOP);
    }

    private QueryStateMachine stateMachine(TransactionManager transactionManager, Metadata metadata, AccessControl accessControl)
    {
        return QueryStateMachine.begin(
                Optional.empty(),
                "CALL testing_procedure()",
                Optional.empty(),
                testSessionBuilder()
                        .setCatalog(TEST_CATALOG_NAME)
                        .setSchema("test")
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
    }

    public static void testingMethod()
    {
        invoked = true;
    }

    public static class TestingProcedure
    {
        public static void testingMethod(ConnectorAccessControl connectorAccessControl)
        {
            connectorAccessControl.checkCanInsertIntoTable(null, new SchemaTableName("test", "testing_table"));
        }
    }
}
