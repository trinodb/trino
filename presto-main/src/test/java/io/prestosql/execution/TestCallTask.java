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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.plugin.base.security.AllowAllSystemAccessControl;
import io.prestosql.security.AccessControl;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.security.DenyAllAccessControl;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.tree.Call;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.testing.TestingAccessControlManager;
import io.prestosql.transaction.TransactionManager;
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
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.block.MethodHandleUtil.methodHandle;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.prestosql.testing.TestingAccessControlManager.privilege;
import static io.prestosql.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.prestosql.testing.TestingSession.createBogusTestingCatalog;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestCallTask
{
    private ExecutorService executor;

    private static boolean invoked;

    @BeforeClass
    public void init()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("call-task-test-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void close()
    {
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
                .hasMessage("Access Denied: Cannot execute procedure test.test.testing_procedure");

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
                .hasMessage("Access Denied: Cannot insert into table test.test.testing_table");
    }

    private void executeCallTask(MethodHandle methodHandle, Function<TransactionManager, AccessControl> accessControlProvider)
    {
        TransactionManager transactionManager = createTransactionManager();
        MetadataManager metadata = createMetadataManager(
                transactionManager,
                new Procedure(
                        "test",
                        "testing_procedure",
                        ImmutableList.of(),
                        methodHandle));
        AccessControl accessControl = accessControlProvider.apply(transactionManager);

        new CallTask()
                .execute(
                        new Call(QualifiedName.of("testing_procedure"), ImmutableList.of()),
                        transactionManager,
                        metadata,
                        accessControl,
                        stateMachine(transactionManager, metadata, accessControl),
                        ImmutableList.of());
    }

    private TransactionManager createTransactionManager()
    {
        CatalogManager catalogManager = new CatalogManager();
        catalogManager.registerCatalog(createBogusTestingCatalog("test"));
        return createTestTransactionManager(catalogManager);
    }

    private MetadataManager createMetadataManager(TransactionManager transactionManager, Procedure procedure)
    {
        MetadataManager metadata = createTestMetadataManager(transactionManager, new FeaturesConfig());
        metadata.getProcedureRegistry().addProcedures(new CatalogName("test"), ImmutableList.of(procedure));
        return metadata;
    }

    private QueryStateMachine stateMachine(TransactionManager transactionManager, MetadataManager metadata, AccessControl accessControl)
    {
        return QueryStateMachine.begin(
                "CALL testing_procedure()",
                Optional.empty(),
                testSession(transactionManager),
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                transactionManager,
                accessControl,
                executor,
                metadata,
                WarningCollector.NOOP,
                Optional.empty());
    }

    private Session testSession(TransactionManager transactionManager)
    {
        return testSessionBuilder()
                .setCatalog("test")
                .setSchema("test")
                .setTransactionId(transactionManager.beginTransaction(true))
                .build();
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
