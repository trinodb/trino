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
import io.trino.connector.CatalogName;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.system.StaticSystemTablesProvider;
import io.trino.connector.system.SystemTablesMetadata;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Catalog;
import io.trino.metadata.Catalog.SecurityManagement;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.Identity;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.sql.analyzer.FeaturesConfig;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.SetRole;
import io.trino.testing.TestingConnectorContext;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.connector.CatalogName.createInformationSchemaCatalogName;
import static io.trino.connector.CatalogName.createSystemTablesCatalogName;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.ROLE_NOT_FOUND;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestSetRoleTask
{
    private static final String CATALOG_NAME = "foo";
    private static final String SYSTEM_ROLE_CATALOG_NAME = "system_role";
    private static final String USER_NAME = "user";
    private static final String ROLE_NAME = "bar";

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

        MockConnectorFactory mockConnectorFactory = MockConnectorFactory.builder()
                .withListRoleGrants((connectorSession, roles, grantees, limit) -> ImmutableSet.of(new RoleGrant(new TrinoPrincipal(USER, USER_NAME), ROLE_NAME, false)))
                .build();
        Connector testConnector = mockConnectorFactory.create(CATALOG_NAME, ImmutableMap.of(), new TestingConnectorContext());
        CatalogName catalogName = new CatalogName(CATALOG_NAME);
        catalogManager.registerCatalog(new Catalog(
                CATALOG_NAME,
                catalogName,
                testConnector,
                SecurityManagement.CONNECTOR,
                createInformationSchemaCatalogName(catalogName),
                testConnector,
                createSystemTablesCatalogName(catalogName),
                testConnector));

        CatalogName systemRoleCatalog = new CatalogName(SYSTEM_ROLE_CATALOG_NAME);
        Connector systemRoleConnector = new Connector()
        {
            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
            {
                return new ConnectorTransactionHandle() {};
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
            {
                return new SystemTablesMetadata(new StaticSystemTablesProvider(ImmutableSet.of()));
            }
        };
        catalogManager.registerCatalog(new Catalog(
                SYSTEM_ROLE_CATALOG_NAME,
                systemRoleCatalog,
                systemRoleConnector,
                SecurityManagement.SYSTEM,
                createInformationSchemaCatalogName(systemRoleCatalog),
                systemRoleConnector,
                createSystemTablesCatalogName(systemRoleCatalog),
                systemRoleConnector));

        executor = newCachedThreadPool(daemonThreadsNamed("test-set-role-task-executor-%s"));
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
                Optional.empty());
        new SetRoleTask().execute(setRole, transactionManager, metadata, accessControl, stateMachine, ImmutableList.of(), WarningCollector.NOOP);
        return stateMachine;
    }
}
