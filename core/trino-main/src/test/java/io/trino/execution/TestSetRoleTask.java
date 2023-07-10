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
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.Identity;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.SetRole;
import io.trino.testing.LocalQueryRunner;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.ROLE_NOT_FOUND;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestSetRoleTask
{
    private static final String CATALOG_NAME = "foo";
    private static final String SYSTEM_ROLE_CATALOG_NAME = "system_role";
    private static final String USER_NAME = "user";
    private static final String ROLE_NAME = "bar";

    private LocalQueryRunner queryRunner;
    private TransactionManager transactionManager;
    private AccessControl accessControl;
    private Metadata metadata;
    private ExecutorService executor;
    private SqlParser parser;

    @BeforeClass
    public void setUp()
    {
        queryRunner = LocalQueryRunner.create(TEST_SESSION);
        MockConnectorFactory mockConnectorFactory = MockConnectorFactory.builder()
                .withListRoleGrants((connectorSession, roles, grantees, limit) -> ImmutableSet.of(new RoleGrant(new TrinoPrincipal(USER, USER_NAME), ROLE_NAME, false)))
                .build();
        queryRunner.createCatalog(CATALOG_NAME, mockConnectorFactory, ImmutableMap.of());

        MockConnectorFactory systemConnectorFactory = MockConnectorFactory.builder()
                .withName("system_role_connector")
                .build();
        queryRunner.createCatalog(SYSTEM_ROLE_CATALOG_NAME, systemConnectorFactory, ImmutableMap.of());

        transactionManager = queryRunner.getTransactionManager();
        accessControl = queryRunner.getAccessControl();
        metadata = queryRunner.getMetadata();
        parser = queryRunner.getSqlParser();
        executor = newCachedThreadPool(daemonThreadsNamed("test-set-role-task-executor-%s"));
    }

    @AfterClass(alwaysRun = true)
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

    @Test
    public void testSetRole()
    {
        assertSetRole("SET ROLE ALL IN " + CATALOG_NAME, ImmutableMap.of(CATALOG_NAME, new SelectedRole(SelectedRole.Type.ALL, Optional.empty())));
        assertSetRole("SET ROLE NONE IN " + CATALOG_NAME, ImmutableMap.of(CATALOG_NAME, new SelectedRole(SelectedRole.Type.NONE, Optional.empty())));
        assertSetRole("SET ROLE " + ROLE_NAME + " IN " + CATALOG_NAME, ImmutableMap.of(CATALOG_NAME, new SelectedRole(SelectedRole.Type.ROLE, Optional.of(ROLE_NAME))));

        assertSetRole("SET ROLE ALL", ImmutableMap.of("system", new SelectedRole(SelectedRole.Type.ALL, Optional.empty())));
        assertSetRole("SET ROLE NONE", ImmutableMap.of("system", new SelectedRole(SelectedRole.Type.NONE, Optional.empty())));
    }

    @Test
    public void testSetRoleInvalidRole()
    {
        assertTrinoExceptionThrownBy(() -> executeSetRole("SET ROLE unknown IN " + CATALOG_NAME))
                .hasErrorCode(ROLE_NOT_FOUND)
                .hasMessage("line 1:1: Role 'unknown' does not exist");
    }

    @Test
    public void testSetRoleInvalidCatalog()
    {
        assertTrinoExceptionThrownBy(() -> executeSetRole("SET ROLE foo IN invalid"))
                .hasErrorCode(CATALOG_NOT_FOUND)
                .hasMessage("line 1:1: Catalog 'invalid' does not exist");
    }

    @Test
    public void testSetCatalogRoleInCatalogWithSystemSecurity()
    {
        assertTrinoExceptionThrownBy(() -> executeSetRole("SET ROLE foo IN " + SYSTEM_ROLE_CATALOG_NAME))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Catalog '" + SYSTEM_ROLE_CATALOG_NAME + "' does not support role management");
    }

    private void assertSetRole(String statement, Map<String, SelectedRole> expected)
    {
        QueryStateMachine stateMachine = executeSetRole(statement);
        QueryInfo queryInfo = stateMachine.getQueryInfo(Optional.empty());
        assertEquals(queryInfo.getSetRoles(), expected);
    }

    private QueryStateMachine executeSetRole(String statement)
    {
        SetRole setRole = (SetRole) parser.createStatement(statement, new ParsingOptions());
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
        new SetRoleTask(metadata, accessControl).execute(setRole, stateMachine, ImmutableList.of(), WarningCollector.NOOP);
        return stateMachine;
    }
}
