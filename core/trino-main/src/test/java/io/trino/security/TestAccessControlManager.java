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
import io.opentelemetry.api.OpenTelemetry;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogServiceProvider;
import io.trino.connector.MockConnectorFactory;
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
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
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
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;
import io.trino.testing.LocalQueryRunner;
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

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.security.AccessDeniedException.denySelectTable;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

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
        assertThatThrownBy(() -> accessControlManager.checkCanSetUser(Optional.empty(), "foo"))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Trino server is still initializing");
    }

    @Test
    public void testNoneSystemAccessControl()
    {
        AccessControlManager accessControlManager = createAccessControlManager(createTestTransactionManager());
        accessControlManager.loadSystemAccessControl(AllowAllSystemAccessControl.NAME, ImmutableMap.of());
        accessControlManager.checkCanSetUser(Optional.empty(), USER_NAME);
    }

    @Test
    public void testReadOnlySystemAccessControl()
    {
        Identity identity = Identity.forUser(USER_NAME).withPrincipal(PRINCIPAL).build();
        QualifiedObjectName tableName = new QualifiedObjectName(TEST_CATALOG_NAME, "schema", "table");
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = createAccessControlManager(transactionManager);

        accessControlManager.loadSystemAccessControl(ReadOnlySystemAccessControl.NAME, ImmutableMap.of());
        accessControlManager.checkCanSetUser(Optional.of(PRINCIPAL), USER_NAME);
        accessControlManager.checkCanSetSystemSessionProperty(identity, "property");

        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    SecurityContext context = new SecurityContext(transactionId, identity, queryId, queryStart);
                    accessControlManager.checkCanSetCatalogSessionProperty(context, TEST_CATALOG_NAME, "property");
                    accessControlManager.checkCanShowSchemas(context, TEST_CATALOG_NAME);
                    accessControlManager.checkCanShowTables(context, new CatalogSchemaName(TEST_CATALOG_NAME, "schema"));
                    accessControlManager.checkCanSelectFromColumns(context, tableName, ImmutableSet.of("column"));
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(context, tableName, ImmutableSet.of("column"));
                    Set<String> catalogs = ImmutableSet.of(TEST_CATALOG_NAME);
                    assertEquals(accessControlManager.filterCatalogs(context, catalogs), catalogs);
                    Set<String> schemas = ImmutableSet.of("schema");
                    assertEquals(accessControlManager.filterSchemas(context, TEST_CATALOG_NAME, schemas), schemas);
                    Set<SchemaTableName> tableNames = ImmutableSet.of(new SchemaTableName("schema", "table"));
                    assertEquals(accessControlManager.filterTables(context, TEST_CATALOG_NAME, tableNames), tableNames);
                });

        assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanInsertIntoTable(new SecurityContext(transactionId, identity, queryId, queryStart), tableName);
                }))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot insert into table test_catalog.schema.table");
    }

    @Test
    public void testSetAccessControl()
    {
        AccessControlManager accessControlManager = createAccessControlManager(createTestTransactionManager());

        TestSystemAccessControlFactory accessControlFactory = new TestSystemAccessControlFactory("test");
        accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        accessControlManager.loadSystemAccessControl("test", ImmutableMap.of());

        accessControlManager.checkCanSetUser(Optional.of(PRINCIPAL), USER_NAME);
        assertEquals(accessControlFactory.getCheckedUserName(), USER_NAME);
        assertEquals(accessControlFactory.getCheckedPrincipal(), Optional.of(PRINCIPAL));
    }

    @Test
    public void testNoCatalogAccessControl()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = createAccessControlManager(transactionManager);

        TestSystemAccessControlFactory accessControlFactory = new TestSystemAccessControlFactory("test");
        accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        accessControlManager.loadSystemAccessControl("test", ImmutableMap.of());

        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanSelectFromColumns(context(transactionId), new QualifiedObjectName(TEST_CATALOG_NAME, "schema", "table"), ImmutableSet.of("column"));
                });
    }

    @Test
    public void testDenyCatalogAccessControl()
    {
        try (LocalQueryRunner queryRunner = LocalQueryRunner.create(TEST_SESSION)) {
            TransactionManager transactionManager = queryRunner.getTransactionManager();
            Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
            AccessControlManager accessControlManager = createAccessControlManager(transactionManager);

            TestSystemAccessControlFactory accessControlFactory = new TestSystemAccessControlFactory("test");
            accessControlManager.addSystemAccessControlFactory(accessControlFactory);
            accessControlManager.loadSystemAccessControl("test", ImmutableMap.of());

            queryRunner.createCatalog(TEST_CATALOG_NAME, MockConnectorFactory.create(), ImmutableMap.of());
            accessControlManager.setConnectorAccessControlProvider(CatalogServiceProvider.singleton(queryRunner.getCatalogHandle(TEST_CATALOG_NAME), Optional.of(new DenyConnectorAccessControl())));

            assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager)
                    .execute(transactionId -> {
                        accessControlManager.checkCanSelectFromColumns(context(transactionId), new QualifiedObjectName(TEST_CATALOG_NAME, "schema", "table"), ImmutableSet.of("column"));
                    }))
                    .isInstanceOf(TrinoException.class)
                    .hasMessageMatching("Access Denied: Cannot select from columns \\[column\\] in table or view schema.table");
        }
    }

    @Test
    public void testDenyTableFunctionCatalogAccessControl()
    {
        try (LocalQueryRunner queryRunner = LocalQueryRunner.create(TEST_SESSION)) {
            TransactionManager transactionManager = queryRunner.getTransactionManager();
            AccessControlManager accessControlManager = createAccessControlManager(transactionManager);

            TestSystemAccessControlFactory accessControlFactory = new TestSystemAccessControlFactory("test");
            accessControlManager.addSystemAccessControlFactory(accessControlFactory);
            accessControlManager.loadSystemAccessControl("allow-all", ImmutableMap.of());

            queryRunner.createCatalog(TEST_CATALOG_NAME, MockConnectorFactory.create(), ImmutableMap.of());
            accessControlManager.setConnectorAccessControlProvider(CatalogServiceProvider.singleton(queryRunner.getCatalogHandle(TEST_CATALOG_NAME), Optional.of(new DenyConnectorAccessControl())));
        }
    }

    @Test
    public void testColumnMaskOrdering()
    {
        try (LocalQueryRunner queryRunner = LocalQueryRunner.create(TEST_SESSION)) {
            TransactionManager transactionManager = queryRunner.getTransactionManager();
            AccessControlManager accessControlManager = createAccessControlManager(transactionManager);

            accessControlManager.addSystemAccessControlFactory(new SystemAccessControlFactory()
            {
                @Override
                public String getName()
                {
                    return "test";
                }

                @Override
                public SystemAccessControl create(Map<String, String> config)
                {
                    return new SystemAccessControl()
                    {
                        @Override
                        public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, CatalogSchemaTableName tableName, String column, Type type)
                        {
                            return Optional.of(ViewExpression.builder()
                                    .identity("user")
                                    .expression("system mask")
                                    .build());
                        }

                        @Override
                        public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
                        {
                        }
                    };
                }
            });
            accessControlManager.loadSystemAccessControl("test", ImmutableMap.of());

            queryRunner.createCatalog(TEST_CATALOG_NAME, MockConnectorFactory.create(), ImmutableMap.of());
            accessControlManager.setConnectorAccessControlProvider(CatalogServiceProvider.singleton(queryRunner.getCatalogHandle(TEST_CATALOG_NAME), Optional.of(new ConnectorAccessControl()
            {
                @Override
                public Optional<ViewExpression> getColumnMask(ConnectorSecurityContext context, SchemaTableName tableName, String column, Type type)
                {
                    return Optional.of(ViewExpression.builder()
                            .identity("user").expression("connector mask")
                            .build());
                }

                @Override
                public void checkCanShowCreateTable(ConnectorSecurityContext context, SchemaTableName tableName)
                {
                }
            })));
        }
    }

    private static SecurityContext context(TransactionId transactionId)
    {
        Identity identity = Identity.forUser(USER_NAME).withPrincipal(PRINCIPAL).build();
        return new SecurityContext(transactionId, identity, queryId, queryStart);
    }

    @Test
    public void testDenySystemAccessControl()
    {
        try (LocalQueryRunner queryRunner = LocalQueryRunner.create(TEST_SESSION)) {
            TransactionManager transactionManager = queryRunner.getTransactionManager();
            Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
            AccessControlManager accessControlManager = createAccessControlManager(transactionManager);

            TestSystemAccessControlFactory accessControlFactory = new TestSystemAccessControlFactory("test");
            accessControlManager.addSystemAccessControlFactory(accessControlFactory);
            accessControlManager.loadSystemAccessControl("test", ImmutableMap.of());

            queryRunner.createCatalog(TEST_CATALOG_NAME, MockConnectorFactory.create(), ImmutableMap.of());
            accessControlManager.setConnectorAccessControlProvider(CatalogServiceProvider.singleton(queryRunner.getCatalogHandle(TEST_CATALOG_NAME), Optional.of(new DenyConnectorAccessControl())));

            assertThatThrownBy(() -> transaction(transactionManager, metadata, accessControlManager)
                    .execute(transactionId -> {
                        accessControlManager.checkCanSelectFromColumns(
                                context(transactionId),
                                new QualifiedObjectName("secured_catalog", "schema", "table"),
                                ImmutableSet.of("column"));
                    }))
                    .isInstanceOf(TrinoException.class)
                    .hasMessageMatching("Access Denied: Cannot select from table secured_catalog.schema.table");
        }
    }

    @Test
    public void testDenyExecuteProcedureBySystem()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = createAccessControlManager(transactionManager);

        TestSystemAccessControlFactory accessControlFactory = new TestSystemAccessControlFactory("deny-all");
        accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        accessControlManager.loadSystemAccessControl("deny-all", ImmutableMap.of());

        assertDenyExecuteProcedure(transactionManager, metadata, accessControlManager, "Access Denied: Cannot execute procedure test_catalog.schema.procedure");
    }

    @Test
    public void testDenyExecuteProcedureByConnector()
    {
        try (LocalQueryRunner queryRunner = LocalQueryRunner.create(TEST_SESSION)) {
            TransactionManager transactionManager = queryRunner.getTransactionManager();
            Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
            AccessControlManager accessControlManager = createAccessControlManager(transactionManager);
            accessControlManager.loadSystemAccessControl("allow-all", ImmutableMap.of());

            queryRunner.createCatalog(TEST_CATALOG_NAME, MockConnectorFactory.create(), ImmutableMap.of());
            accessControlManager.setConnectorAccessControlProvider(CatalogServiceProvider.singleton(queryRunner.getCatalogHandle(TEST_CATALOG_NAME), Optional.of(new DenyConnectorAccessControl())));

            assertDenyExecuteProcedure(transactionManager, metadata, accessControlManager, "Access Denied: Cannot execute procedure schema.procedure");
        }
    }

    @Test
    public void testAllowExecuteProcedure()
    {
        try (LocalQueryRunner queryRunner = LocalQueryRunner.create(TEST_SESSION)) {
            TransactionManager transactionManager = queryRunner.getTransactionManager();
            Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
            AccessControlManager accessControlManager = createAccessControlManager(transactionManager);
            accessControlManager.loadSystemAccessControl("allow-all", ImmutableMap.of());

            queryRunner.createCatalog(TEST_CATALOG_NAME, MockConnectorFactory.create(), ImmutableMap.of());
            accessControlManager.setConnectorAccessControlProvider(CatalogServiceProvider.singleton(queryRunner.getCatalogHandle(TEST_CATALOG_NAME), Optional.of(new AllowAllAccessControl())));

            transaction(transactionManager, metadata, accessControlManager)
                    .execute(transactionId -> {
                        accessControlManager.checkCanExecuteProcedure(context(transactionId), new QualifiedObjectName(TEST_CATALOG_NAME, "schema", "procedure"));
                    });
        }
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

    private void assertDenyExecuteProcedure(TransactionManager transactionManager, Metadata metadata, AccessControlManager accessControlManager, String s)
    {
        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    assertThatThrownBy(
                            () -> accessControlManager.checkCanExecuteProcedure(context(transactionId), new QualifiedObjectName(TEST_CATALOG_NAME, "schema", "procedure")))
                            .isInstanceOf(AccessDeniedException.class)
                            .hasMessage(s);
                });
    }

    @Test
    public void testDenyExecuteFunctionBySystemAccessControl()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = createAccessControlManager(transactionManager);

        TestSystemAccessControlFactory accessControlFactory = new TestSystemAccessControlFactory("deny-all");
        accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        accessControlManager.loadSystemAccessControl("deny-all", ImmutableMap.of());

        QualifiedObjectName functionName = new QualifiedObjectName(TEST_CATALOG_NAME, "schema", "executed_function");
        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    assertThat(accessControlManager.canExecuteFunction(context(transactionId), functionName)).isFalse();
                    assertThat(accessControlManager.canCreateViewWithExecuteFunction(context(transactionId), functionName)).isFalse();
                });
    }

    @Test
    public void testAllowExecuteFunction()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = createAccessControlManager(transactionManager);
        accessControlManager.loadSystemAccessControl("allow-all", ImmutableMap.of());

        QualifiedObjectName functionName = new QualifiedObjectName(TEST_CATALOG_NAME, "schema", "executed_function");
        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    assertThat(accessControlManager.canExecuteFunction(context(transactionId), functionName)).isTrue();
                    assertThat(accessControlManager.canCreateViewWithExecuteFunction(context(transactionId), functionName)).isTrue();
                });
    }

    @Test
    public void testAllowExecuteTableFunction()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
        AccessControlManager accessControlManager = createAccessControlManager(transactionManager);
        accessControlManager.loadSystemAccessControl("allow-all", ImmutableMap.of());

        QualifiedObjectName functionName = new QualifiedObjectName(TEST_CATALOG_NAME, "schema", "executed_function");
        transaction(transactionManager, metadata, accessControlManager)
                .execute(transactionId -> {
                    assertThat(accessControlManager.canExecuteFunction(context(transactionId), functionName)).isTrue();
                    assertThat(accessControlManager.canCreateViewWithExecuteFunction(context(transactionId), functionName)).isTrue();
                });
    }

    @Test
    public void testRemovedMethodsCannotBeDeclared()
    {
        try (LocalQueryRunner queryRunner = LocalQueryRunner.create(TEST_SESSION)) {
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

    private AccessControlManager createAccessControlManager(TestingEventListenerManager eventListenerManager, List<String> systemAccessControlProperties)
            throws IOException
    {
        Path systemAccessControlConfig = createTempFile("access-control-config-file", ".properties");
        Files.write(systemAccessControlConfig, systemAccessControlProperties, TRUNCATE_EXISTING, CREATE, WRITE);
        String accessControlConfigPath = systemAccessControlConfig
                .toFile()
                .getAbsolutePath();

        return createAccessControlManager(
                eventListenerManager,
                new AccessControlConfig().setAccessControlFiles(accessControlConfigPath));
    }

    private AccessControlManager createAccessControlManager(TransactionManager testTransactionManager)
    {
        return new AccessControlManager(NodeVersion.UNKNOWN, testTransactionManager, emptyEventListenerManager(), new AccessControlConfig(), OpenTelemetry.noop(), DefaultSystemAccessControl.NAME);
    }

    private AccessControlManager createAccessControlManager(EventListenerManager eventListenerManager, AccessControlConfig config)
    {
        return new AccessControlManager(NodeVersion.UNKNOWN, createTestTransactionManager(), eventListenerManager, config, OpenTelemetry.noop(), DefaultSystemAccessControl.NAME);
    }

    private AccessControlManager createAccessControlManager(EventListenerManager eventListenerManager, String defaultAccessControlName)
    {
        return new AccessControlManager(NodeVersion.UNKNOWN, createTestTransactionManager(), eventListenerManager, new AccessControlConfig(), OpenTelemetry.noop(), defaultAccessControlName);
    }

    private SystemAccessControlFactory eventListeningSystemAccessControlFactory(String name, EventListener... eventListeners)
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
                    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
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

    private static class TestSystemAccessControlFactory
            implements SystemAccessControlFactory
    {
        private final String name;
        private Map<String, String> config;

        private Optional<Principal> checkedPrincipal;
        private String checkedUserName;

        public TestSystemAccessControlFactory(String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        public Map<String, String> getConfig()
        {
            return config;
        }

        public Optional<Principal> getCheckedPrincipal()
        {
            return checkedPrincipal;
        }

        public String getCheckedUserName()
        {
            return checkedUserName;
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public SystemAccessControl create(Map<String, String> config)
        {
            this.config = config;
            return new SystemAccessControl()
            {
                @Override
                public void checkCanSetUser(Optional<Principal> principal, String userName)
                {
                    checkedPrincipal = principal;
                    checkedUserName = userName;
                }

                @Override
                public boolean canAccessCatalog(SystemSecurityContext context, String catalogName)
                {
                    return true;
                }

                @Override
                public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
                {
                    throw new UnsupportedOperationException();
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
            };
        }
    }

    private static class DenyConnectorAccessControl
            implements ConnectorAccessControl
    {
    }
}
