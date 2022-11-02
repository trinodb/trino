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
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.Execute;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.sql.QueryUtil.identifier;
import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.QueryUtil.table;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestPrepareTask
{
    private final Metadata metadata = createTestMetadataManager();
    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
    }

    @Test
    public void testPrepare()
    {
        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo")));
        String sqlString = "PREPARE my_query FROM SELECT * FROM foo";
        Map<String, String> statements = executePrepare("my_query", query, sqlString, TEST_SESSION);
        assertEquals(statements, ImmutableMap.of("my_query", "SELECT *\nFROM\n  foo\n"));
    }

    @Test
    public void testPrepareNameExists()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT bar, baz from foo")
                .build();

        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo")));
        String sqlString = "PREPARE my_query FROM SELECT * FROM foo";
        Map<String, String> statements = executePrepare("my_query", query, sqlString, session);
        assertEquals(statements, ImmutableMap.of("my_query", "SELECT *\nFROM\n  foo\n"));
    }

    @Test
    public void testPrepareInvalidStatement()
    {
        Statement statement = new Execute(identifier("foo"), emptyList());
        String sqlString = "PREPARE my_query FROM EXECUTE foo";
        assertTrinoExceptionThrownBy(() -> executePrepare("my_query", statement, sqlString, TEST_SESSION))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("Invalid statement type for prepared statement: EXECUTE");
    }

    private Map<String, String> executePrepare(String statementName, Statement statement, String sqlString, Session session)
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControl = new AccessControlManager(transactionManager, emptyEventListenerManager(), new AccessControlConfig(), DefaultSystemAccessControl.NAME);
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
                Optional.empty());
        Prepare prepare = new Prepare(identifier(statementName), statement);
        new PrepareTask(new SqlParser()).execute(prepare, stateMachine, emptyList(), WarningCollector.NOOP);
        return stateMachine.getAddedPreparedStatements();
    }
}
