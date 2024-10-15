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
package io.trino.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.secrets.SecretsResolver;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogServiceProvider;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.eventlistener.EventListenerManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.base.security.AllowAllAccessControl;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.plugin.base.security.ReadOnlySystemAccessControl;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemAccessControlFactory;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.testing.TestingEventListenerManager;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.security.AccessDeniedException.denySelectTable;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAccessControlManager
{
    private static final Principal PRINCIPAL = new BasicPrincipal("principal");
    private static final String USER_NAME = "user_name";
    private static final QueryId queryId = new QueryId("query_id");
    private static final Instant queryStart = Instant.now();

    @Test
    public void testInitializing()
    {
        AccessControlManager accessControlManager = createAccessControlManager(createTestTransactionManager());
        assertThatThrownBy(() -> accessControlManager.checkCanImpersonateUser(Identity.forUser("test").build(), "foo"))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Trino server is still initializing");
    }

    @Test
    public void testNoneSystemAccessControl()
    {
        AccessControlManager accessControlManager = createAccessControlManager(createTestTransactionManager());
        accessControlManager.loadSystemAccessControl(AllowAllSystemAccessControl.NAME, ImmutableMap.of());
        accessControlManager.checkCanImpersonateUser(Identity.forUser("test").build(), "foo");
    }

    @Test
    public void testReadOnlySystemAccessControl()
    {
        Identity identity = Identity.forUser(USER_NAME).withPrincipal(PRINCIPAL).build();
        QualifiedObjectName tableName = new QualifiedObjectName(TEST_CATALOG_NAME, "schema", "table");

        assertAccessControl(new ReadOnlySystemAccessControl(), new AllowAllAccessControl(), (accessControlManager, securityContext) ->
        {
            accessControlManager.checkCanSetUser(Optional.of(PRINCIPAL), USER_NAME);
            accessControlManager.checkCanSetSystemSessionProperty(identity, queryId, "property");
            accessControlManager.checkCanSetCatalogSessionProperty(securityContext, TEST_CATALOG_NAME, "property");
            accessControlManager.checkCanShowSchemas(securityContext, TEST_CATALOG_NAME);
            accessControlManager.checkCanShowTables(securityContext, new CatalogSchemaName(TEST_CATALOG_NAME, "schema"));
            accessControlManager.checkCanSelectFromColumns(securityContext, tableName, ImmutableSet.of("column"));
            accessControlManager.checkCanCreateViewWithSelectFromColumns(securityContext, tableName, ImmutableSet.of("column"));
            Set<String> catalogs = ImmutableSet.of(TEST_CATALOG_NAME);
            assertThat(accessControlManager.filterCatalogs(securityContext, catalogs)).isEqualTo(catalogs);
            Set<String> schemas = ImmutableSet.of("schema");
            assertThat(accessControlManager.filterSchemas(securityContext, TEST_CATALOG_NAME, schemas)).isEqualTo(schemas);
            Set<SchemaTableName> tableNames = ImmutableSet.of(new SchemaTableName("schema", "table"));
            assertThat(accessControlManager.filterTables(securityContext, TEST_CATALOG_NAME, tableNames)).isEqualTo(tableNames);

            assertThatThrownBy(() -> accessControlManager.checkCanInsertIntoTable(securityContext, tableName))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessage("Access Denied: Cannot insert into table test_catalog.schema.table");
        });
    }

    @Test
    public void testNoCatalogAccessControl()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = createAccessControlManager(transactionManager);

        accessControlManager.setSystemAccessControls(ImmutableList.of(new TestSystemAccessControl()));

        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanSelectFromColumns(context(transactionId), new QualifiedObjectName(TEST_CATALOG_NAME, "schema", "table"), ImmutableSet.of("column"));
                });
    }

    @Test
    public void testDenyCatalogAccessControl()
    {
        assertAccessControl(new AllowAllSystemAccessControl(), new DenyConnectorAccessControl(), (accessControlManager, securityContext) ->
                assertThatThrownBy(
                        () -> accessControlManager.checkCanSelectFromColumns(securityContext, new QualifiedObjectName(TEST_CATALOG_NAME, "schema", "table"), ImmutableSet.of("column")))
                        .isInstanceOf(AccessDeniedException.class)
                        .hasMessage("Access Denied: Cannot select from columns [column] in table or view schema.table"));
    }

    @Test
    public void testDenySystemAccessControl()
    {
        assertAccessControl(new TestSystemAccessControl(), new AllowAllAccessControl(), (accessControlManager, securityContext) ->
                assertThatThrownBy(
                        () -> accessControlManager.checkCanSelectFromColumns(securityContext, new QualifiedObjectName("secured_catalog", "schema", "table"), ImmutableSet.of("column")))
                        .isInstanceOf(AccessDeniedException.class)
                        .hasMessage("Access Denied: Cannot select from table secured_catalog.schema.table"));
    }

    @Test
    public void testDenyExecuteProcedureBySystem()
    {
        assertAccessControl(new TestSystemAccessControl(), new AllowAllAccessControl(), (accessControlManager, securityContext) ->
                assertThatThrownBy(
                        () -> accessControlManager.checkCanExecuteProcedure(securityContext, new QualifiedObjectName(TEST_CATALOG_NAME, "schema", "procedure")))
                        .isInstanceOf(AccessDeniedException.class)
                        .hasMessage("Access Denied: Cannot execute procedure test_catalog.schema.procedure"));
    }

    @Test
    public void testDenyExecuteProcedureByConnector()
    {
        assertAccessControl(new AllowAllSystemAccessControl(), new DenyConnectorAccessControl(), (accessControlManager, securityContext) ->
                assertThatThrownBy(
                        () -> accessControlManager.checkCanExecuteProcedure(securityContext, new QualifiedObjectName(TEST_CATALOG_NAME, "schema", "procedure")))
                        .isInstanceOf(AccessDeniedException.class)
                        .hasMessage("Access Denied: Cannot execute procedure schema.procedure"));
    }

    @Test
    public void testAllowExecuteProcedure()
    {
        assertAccessControl(new AllowAllSystemAccessControl(), new AllowAllAccessControl(), (accessControlManager, securityContext) ->
                accessControlManager.checkCanExecuteProcedure(securityContext, new QualifiedObjectName(TEST_CATALOG_NAME, "schema", "procedure")));
    }

    @Test
    public void testRegisterSingleEventListenerForDefaultAccessControl()
    {
        EventListener expectedListener = new EventListener() {};

        String defaultAccessControlName = "event-listening-default-access-control";
        TestingEventListenerManager eventListenerManager = emptyEventListenerManager();
        AccessControlManager accessControlManager = createAccessControlManager(
                eventListenerManager,
                defaultAccessControlName);
        accessControlManager.addSystemAccessControlFactory(
                eventListeningSystemAccessControlFactory(defaultAccessControlName, expectedListener));

        accessControlManager.loadSystemAccessControl();

        assertThat(eventListenerManager.getConfiguredEventListeners())
                .contains(expectedListener);
    }

    @Test
    public void testRegisterMultipleEventListenerForDefaultAccessControl()
    {
        EventListener firstListener = new EventListener() {};
        EventListener secondListener = new EventListener() {};

        String defaultAccessControlName = "event-listening-default-access-control";
        TestingEventListenerManager eventListenerManager = emptyEventListenerManager();
        AccessControlManager accessControlManager = createAccessControlManager(
                eventListenerManager,
                defaultAccessControlName);
        accessControlManager.addSystemAccessControlFactory(
                eventListeningSystemAccessControlFactory(defaultAccessControlName, firstListener, secondListener));

        accessControlManager.loadSystemAccessControl();

        assertThat(eventListenerManager.getConfiguredEventListeners())
                .contains(firstListener, secondListener);
    }

    @Test
    public void testRegisterSingleEventListener()
            throws IOException
    {
        EventListener expectedListener = new EventListener() {};

        String systemAccessControlName = "event-listening-sac";
        TestingEventListenerManager eventListenerManager = emptyEventListenerManager();
        AccessControlManager accessControlManager = createAccessControlManager(eventListenerManager, ImmutableList.of("access-control.name=" + systemAccessControlName));
        accessControlManager.addSystemAccessControlFactory(
                eventListeningSystemAccessControlFactory(systemAccessControlName, expectedListener));

        accessControlManager.loadSystemAccessControl();

        assertThat(eventListenerManager.getConfiguredEventListeners())
                .contains(expectedListener);
    }

    @Test
    public void testRegisterMultipleEventListeners()
            throws IOException
    {
        EventListener firstListener = new EventListener() {};
        EventListener secondListener = new EventListener() {};

        String systemAccessControlName = "event-listening-sac";
        TestingEventListenerManager eventListenerManager = emptyEventListenerManager();
        AccessControlManager accessControlManager = createAccessControlManager(eventListenerManager, ImmutableList.of("access-control.name=" + systemAccessControlName));
        accessControlManager.addSystemAccessControlFactory(
                eventListeningSystemAccessControlFactory(systemAccessControlName, firstListener, secondListener));

        accessControlManager.loadSystemAccessControl();

        assertThat(eventListenerManager.getConfiguredEventListeners())
                .contains(firstListener, secondListener);
    }

    @Test
    public void testDenyExecuteFunctionBySystemAccessControl()
    {
        QualifiedObjectName functionName = new QualifiedObjectName(TEST_CATALOG_NAME, "schema", "executed_function");
        assertAccessControl(new TestSystemAccessControl(), new AllowAllAccessControl(), (accessControlManager, securityContext) ->
        {
            assertThat(accessControlManager.canExecuteFunction(securityContext, functionName)).isFalse();
            assertThat(accessControlManager.canCreateViewWithExecuteFunction(securityContext, functionName)).isFalse();
        });
    }

    @Test
    public void testAllowExecuteFunction()
    {
        QualifiedObjectName functionName = new QualifiedObjectName(TEST_CATALOG_NAME, "schema", "executed_function");
        assertAccessControl(new AllowAllSystemAccessControl(), new AllowAllAccessControl(), (accessControlManager, securityContext) ->
        {
            assertThat(accessControlManager.canExecuteFunction(securityContext, functionName)).isTrue();
            assertThat(accessControlManager.canCreateViewWithExecuteFunction(securityContext, functionName)).isTrue();
        });
    }

    @Test
    public void testAllowExecuteTableFunction()
    {
        QualifiedObjectName functionName = new QualifiedObjectName(TEST_CATALOG_NAME, "schema", "executed_function");
        assertAccessControl(new AllowAllSystemAccessControl(), new AllowAllAccessControl(), (accessControlManager, securityContext) ->
        {
            assertThat(accessControlManager.canExecuteFunction(securityContext, functionName)).isTrue();
            assertThat(accessControlManager.canCreateViewWithExecuteFunction(securityContext, functionName)).isTrue();
        });
    }

    @Test
    public void testRemovedMethodsCannotBeDeclared()
    {
        try (QueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION)) {
            TransactionManager transactionManager = queryRunner.getTransactionManager();
            AccessControlManager accessControlManager = createAccessControlManager(transactionManager);

            assertThatThrownBy(() ->
                    accessControlManager.setSystemAccessControls(ImmutableList.of(new AllowAllSystemAccessControl()
                    {
                        @SuppressWarnings("unused")
                        public void checkCanAccessCatalog(SystemSecurityContext context, String catalogName) {}
                    })))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageMatching("Access control .* must not implement removed method checkCanAccessCatalog\\(.*\\)");

            assertThatThrownBy(() ->
                    accessControlManager.setSystemAccessControls(ImmutableList.of(new AllowAllSystemAccessControl()
                    {
                        @SuppressWarnings("unused")
                        public void checkCanGrantExecuteFunctionPrivilege(SystemSecurityContext context, String functionName, TrinoPrincipal grantee, boolean grantOption) {}
                    })))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageMatching("Access control .* must not implement removed method checkCanGrantExecuteFunctionPrivilege\\(.*\\)");

            assertThatThrownBy(() ->
                    accessControlManager.setSystemAccessControls(ImmutableList.of(new AllowAllSystemAccessControl()
                    {
                        @SuppressWarnings("unused")
                        public void checkCanExecuteFunction(SystemSecurityContext systemSecurityContext, String functionName) {}
                    })))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageMatching("Access control .* must not implement removed method checkCanExecuteFunction\\(.*\\)");

            assertThatThrownBy(() ->
                    accessControlManager.setSystemAccessControls(ImmutableList.of(new AllowAllSystemAccessControl()
                    {
                        @SuppressWarnings("unused")
                        public void checkCanExecuteFunction(SystemSecurityContext systemSecurityContext, FunctionKind functionKind, CatalogSchemaRoutineName functionName) {}
                    })))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageMatching("Access control .* must not implement removed method checkCanExecuteFunction\\(.*\\)");

            assertThatThrownBy(() ->
                    accessControlManager.setSystemAccessControls(ImmutableList.of(new AllowAllSystemAccessControl()
                    {
                        @SuppressWarnings("unused")
                        public void checkCanGrantExecuteFunctionPrivilege(SystemSecurityContext context, FunctionKind functionKind, CatalogSchemaRoutineName functionName, TrinoPrincipal grantee, boolean grantOption) {}
                    })))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageMatching("Access control .* must not implement removed method checkCanGrantExecuteFunctionPrivilege\\(.*\\)");

            accessControlManager.setSystemAccessControls(ImmutableList.of(new AllowAllSystemAccessControl()));
        }
    }

    private static void assertAccessControl(SystemAccessControl systemAccessControl, ConnectorAccessControl accessControl, BiConsumer<AccessControlManager, SecurityContext> test)
    {
        try (QueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION)) {
            TransactionManager transactionManager = queryRunner.getTransactionManager();
            AccessControlManager accessControlManager = createAccessControlManager(transactionManager);
            accessControlManager.setSystemAccessControls(ImmutableList.of(systemAccessControl));

            queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.create()));
            queryRunner.createCatalog(TEST_CATALOG_NAME, "mock", ImmutableMap.of());

            queryRunner.inTransaction(transactionSession -> {
                CatalogHandle catalogHandle = queryRunner.getPlannerContext().getMetadata().getCatalogHandle(transactionSession, TEST_CATALOG_NAME).orElseThrow();
                accessControlManager.setConnectorAccessControlProvider(CatalogServiceProvider.singleton(catalogHandle, Optional.of(accessControl)));
                test.accept(accessControlManager, context(transactionSession.getRequiredTransactionId()));
                return null;
            });
        }
    }

    private static SecurityContext context(TransactionId transactionId)
    {
        Identity identity = Identity.forUser(USER_NAME).withPrincipal(PRINCIPAL).build();
        return new SecurityContext(transactionId, identity, queryId, queryStart);
    }

    private static AccessControlManager createAccessControlManager(TestingEventListenerManager eventListenerManager, List<String> systemAccessControlProperties)
            throws IOException
    {
        Path systemAccessControlConfig = createTempFile("access-control-config-file", ".properties");
        Files.write(systemAccessControlConfig, systemAccessControlProperties, TRUNCATE_EXISTING, CREATE, WRITE);
        String accessControlConfigPath = systemAccessControlConfig
                .toFile()
                .getAbsolutePath();

        return createAccessControlManager(
                eventListenerManager,
                new AccessControlConfig().setAccessControlFiles(ImmutableList.of(accessControlConfigPath)));
    }

    private static AccessControlManager createAccessControlManager(TransactionManager testTransactionManager)
    {
        return new AccessControlManager(NodeVersion.UNKNOWN, testTransactionManager, emptyEventListenerManager(), new AccessControlConfig(), OpenTelemetry.noop(), new SecretsResolver(ImmutableMap.of()), DefaultSystemAccessControl.NAME);
    }

    private static AccessControlManager createAccessControlManager(EventListenerManager eventListenerManager, AccessControlConfig config)
    {
        return new AccessControlManager(NodeVersion.UNKNOWN, createTestTransactionManager(), eventListenerManager, config, OpenTelemetry.noop(), new SecretsResolver(ImmutableMap.of()), DefaultSystemAccessControl.NAME);
    }

    private static AccessControlManager createAccessControlManager(EventListenerManager eventListenerManager, String defaultAccessControlName)
    {
        return new AccessControlManager(NodeVersion.UNKNOWN, createTestTransactionManager(), eventListenerManager, new AccessControlConfig(), OpenTelemetry.noop(), new SecretsResolver(ImmutableMap.of()), defaultAccessControlName);
    }

    private static SystemAccessControlFactory eventListeningSystemAccessControlFactory(String name, EventListener... eventListeners)
    {
        return new SystemAccessControlFactory()
        {
            @Override
            public String getName()
            {
                return name;
            }

            @Override
            public SystemAccessControl create(Map<String, String> config)
            {
                return new SystemAccessControl()
                {
                    @Override
                    public void checkCanSetSystemSessionProperty(Identity identity, QueryId queryId, String propertyName)
                    {
                    }

                    @Override
                    public Iterable<EventListener> getEventListeners()
                    {
                        return ImmutableSet.copyOf(eventListeners);
                    }
                };
            }
        };
    }

    private static class TestSystemAccessControl
            implements SystemAccessControl
    {
        @Override
        public boolean canAccessCatalog(SystemSecurityContext context, String catalogName)
        {
            return true;
        }

        @Override
        public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
        {
            if (table.getCatalogName().equals("secured_catalog")) {
                denySelectTable(table.toString());
            }
        }

        @Override
        public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
        {
            return catalogs;
        }
    }

    private static class DenyConnectorAccessControl
            implements ConnectorAccessControl {}
}
