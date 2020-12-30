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
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.SetSessionAuthorization;
import io.prestosql.transaction.TransactionId;
import io.prestosql.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.testing.TestingSession.createBogusTestingCatalog;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestSetSessionAuthorizationTask
{
    private static final String CATALOG_NAME = "foo";

    private TransactionManager transactionManager;
    private AccessControl accessControl;
    private Metadata metadata;
    private ExecutorService executor;
    private SqlParser parser;

    @BeforeClass
    public void setUp()
    {
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new AllowAllAccessControl();

        metadata = createTestMetadataManager(transactionManager, new FeaturesConfig());

        catalogManager.registerCatalog(createBogusTestingCatalog(CATALOG_NAME));
        executor = newCachedThreadPool(daemonThreadsNamed("test-set-session-authorization-task-executor-%s"));
        parser = new SqlParser();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
        metadata = null;
        accessControl = null;
        transactionManager = null;
        parser = null;
    }

    @Test
    public void testSetSessionAuthorization()
    {
        assertSetSessionAuthorization("SET SESSION AUTHORIZATION user", Optional.of("user"));
        assertSetSessionAuthorization("SET SESSION AUTHORIZATION 'user'", Optional.of("user"));
        assertSetSessionAuthorization("SET SESSION AUTHORIZATION \"user\"", Optional.of("user"));
    }

    private void assertSetSessionAuthorization(String statement, Optional<String> expected)
    {
        SetSessionAuthorization setSessionAuthorization = (SetSessionAuthorization) parser.createStatement(statement, new ParsingOptions());
        QueryStateMachine stateMachine = createStateMachineWithSession(statement, testSessionBuilder().build());
        new SetSessionAuthorizationTask().execute(setSessionAuthorization, transactionManager, metadata, accessControl, stateMachine, ImmutableList.of());
        QueryInfo queryInfo = stateMachine.getQueryInfo(Optional.empty());
        assertEquals(queryInfo.getSetAuthorizationUser(), expected);

        TransactionId transactionId = transactionManager.beginTransaction(false);
        stateMachine = createStateMachineWithSession(statement, testSessionBuilder().setTransactionId(transactionId).build());
        try {
            new SetSessionAuthorizationTask().execute(setSessionAuthorization, transactionManager, metadata, accessControl, stateMachine, ImmutableList.of());
            fail("Set session authorization should have failed as a transaction is in progress");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), GENERIC_USER_ERROR.toErrorCode());
            assertEquals(e.getMessage(), "Can't set authorization user in the middle of a transaction");
        }
    }

    private QueryStateMachine createStateMachineWithSession(String statement, Session session)
    {
        QueryStateMachine stateMachine = QueryStateMachine.begin(
                statement,
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
                Optional.empty());
        return stateMachine;
    }
}
