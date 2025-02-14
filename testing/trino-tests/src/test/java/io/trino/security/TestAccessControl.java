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
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.plugin.base.security.ForwardingSystemAccessControl;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.jdbc.JdbcPlugin;
import io.trino.plugin.jdbc.TestingH2JdbcModule;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.function.Signature;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.sql.SqlPath;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingAccessControlManager;
import io.trino.testing.TestingAccessControlManager.TestingPrivilege;
import io.trino.testing.TestingGroupProvider;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.ADD_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.ALTER_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.COMMENT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.COMMENT_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_MATERIALIZED_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW_WITH_SELECT_COLUMNS;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.EXECUTE_FUNCTION;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.EXECUTE_QUERY;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.GRANT_EXECUTE_FUNCTION;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SET_SESSION;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SET_TABLE_PROPERTIES;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SET_USER;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SHOW_COLUMNS;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SHOW_CREATE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.TRUNCATE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.UPDATE_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestAccessControl
        extends AbstractTestQueryFramework
{
    private static final String DEFAULT_SCHEMA = "default";
    private static final String REDIRECTED_SOURCE = "redirected_source";
    private static final String REDIRECTED_TARGET = "redirected_target";
    private final AtomicReference<SystemAccessControl> systemAccessControl = new AtomicReference<>(new DefaultSystemAccessControl());
    private final TestingGroupProvider groupProvider = new TestingGroupProvider();
    private TestingSystemSecurityMetadata systemSecurityMetadata;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setSource("test")
                .setCatalog("blackhole")
                .setSchema("default")
                .setPath(SqlPath.buildPath("mock.function", Optional.empty()))
                .build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setAdditionalModule(binder -> {
                    newOptionalBinder(binder, SystemSecurityMetadata.class)
                            .setBinding()
                            .to(TestingSystemSecurityMetadata.class)
                            .in(Scopes.SINGLETON);
                })
                .setWorkerCount(0)
                .setSystemAccessControl(new ForwardingSystemAccessControl()
                {
                    @Override
                    protected SystemAccessControl delegate()
                    {
                        return systemAccessControl.get();
                    }
                })
                .build();
        queryRunner.getGroupProvider().setConfiguredGroupProvider(groupProvider);
        queryRunner.installPlugin(new BlackHolePlugin());
        queryRunner.createCatalog("blackhole", "blackhole");
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory", Map.of());
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withGetTableHandle((session1, schemaTableName) -> {
                    if (schemaTableName.getTableName().startsWith("new")) {
                        return null;
                    }
                    return new MockConnectorTableHandle(schemaTableName);
                })
                .withListSchemaNames(connectorSession -> ImmutableList.of(DEFAULT_SCHEMA))
                .withListTables((connectorSession, schemaName) -> {
                    if (schemaName.equals(DEFAULT_SCHEMA)) {
                        return ImmutableList.of(REDIRECTED_SOURCE);
                    }
                    return ImmutableList.of();
                })
                .withGetViews((connectorSession, prefix) -> {
                    ConnectorViewDefinition definitionRunAsDefiner = new ConnectorViewDefinition(
                            "SELECT 1 AS test",
                            Optional.of("mock"),
                            Optional.of("default"),
                            ImmutableList.of(new ConnectorViewDefinition.ViewColumn("test", BIGINT.getTypeId(), Optional.empty())),
                            Optional.of("comment"),
                            Optional.of("admin"),
                            false,
                            ImmutableList.of());
                    ConnectorViewDefinition definitionRunAsInvoker = new ConnectorViewDefinition(
                            "SELECT 1 AS test",
                            Optional.of("mock"),
                            Optional.of("default"),
                            ImmutableList.of(new ConnectorViewDefinition.ViewColumn("test", BIGINT.getTypeId(), Optional.empty())),
                            Optional.of("comment"),
                            Optional.empty(),
                            true,
                            ImmutableList.of());
                    return ImmutableMap.of(
                            new SchemaTableName("default", "test_view_definer"), definitionRunAsDefiner,
                            new SchemaTableName("default", "test_view_invoker"), definitionRunAsInvoker);
                })
                .withGetMaterializedViews(new BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorMaterializedViewDefinition>>()
                {
                    @Override
                    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> apply(ConnectorSession session, SchemaTablePrefix schemaTablePrefix)
                    {
                        ConnectorMaterializedViewDefinition materializedViewDefinition = new ConnectorMaterializedViewDefinition(
                                "SELECT 1 AS test",
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(new ConnectorMaterializedViewDefinition.Column("test", BIGINT.getTypeId(), Optional.empty())),
                                Optional.of(Duration.ZERO),
                                Optional.of("comment"),
                                Optional.of("owner"),
                                ImmutableList.of());
                        return ImmutableMap.of(
                                new SchemaTableName("default", "test_materialized_view"), materializedViewDefinition);
                    }
                })
                .withListRoleGrants((connectorSession, roles, grantees, limit) -> ImmutableSet.of(new RoleGrant(new TrinoPrincipal(USER, "alice"), "alice_role", false)))
                .withAnalyzeProperties(() -> ImmutableList.of(
                        integerProperty("another_property", "description", 0, false),
                        integerProperty("integer_analyze_property", "description", 0, false)))
                .withGetMaterializedViewProperties(() -> ImmutableList.of(
                        integerProperty("another_property", "description", 0, false),
                        stringProperty("string_materialized_view_property", "description", "", false)))
                .withSchemaProperties(() -> ImmutableList.of(
                        integerProperty("another_property", "description", 0, false),
                        booleanProperty("boolean_schema_property", "description", false, false)))
                .withTableProperties(() -> ImmutableList.of(
                        integerProperty("another_property", "description", 0, false),
                        doubleProperty("double_table_property", "description", 0d, false)))
                .withColumnProperties(() -> ImmutableList.of(
                        integerProperty("another_property", "description", 0, false),
                        stringProperty("string_column_property", "description", "", false)))
                .withRedirectTable((connectorSession, schemaTableName) -> {
                    if (schemaTableName.equals(SchemaTableName.schemaTableName(DEFAULT_SCHEMA, REDIRECTED_SOURCE))) {
                        return Optional.of(
                                new CatalogSchemaTableName("mock", SchemaTableName.schemaTableName(DEFAULT_SCHEMA, REDIRECTED_TARGET)));
                    }
                    return Optional.empty();
                })
                .withGetComment(schemaTableName -> {
                    if (schemaTableName.getTableName().equals(REDIRECTED_TARGET)) {
                        return Optional.of("this is a redirected table");
                    }
                    return Optional.empty();
                })
                .withFunctions(ImmutableList.<FunctionMetadata>builder()
                        .add(FunctionMetadata.scalarBuilder("my_function")
                                .signature(Signature.builder().argumentType(BIGINT).returnType(BIGINT).build())
                                .noDescription()
                                .build())
                        .add(FunctionMetadata.scalarBuilder("other_function")
                                .signature(Signature.builder().argumentType(BIGINT).returnType(BIGINT).build())
                                .noDescription()
                                .build())
                        .build())
                .withFunctionProvider(Optional.of(new FunctionProvider()
                {
                    @Override
                    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies, InvocationConvention invocationConvention)
                    {
                        return ScalarFunctionImplementation.builder()
                                .methodHandle(MethodHandles.identity(long.class))
                                .build();
                    }
                }))
                .build()));
        queryRunner.createCatalog("mock", "mock");
        queryRunner.installPlugin(new JdbcPlugin("base_jdbc", TestingH2JdbcModule::new));
        queryRunner.createCatalog("jdbc", "base_jdbc", TestingH2JdbcModule.createProperties());
        for (String tableName : ImmutableList.of("orders", "nation", "region", "lineitem")) {
            queryRunner.execute(format("CREATE TABLE %1$s AS SELECT * FROM tpch.tiny.%1$s WITH NO DATA", tableName));
        }
        systemSecurityMetadata = (TestingSystemSecurityMetadata) queryRunner.getCoordinator().getInstance(Key.get(SystemSecurityMetadata.class));
        return queryRunner;
    }

    private void reset()
    {
        systemAccessControl.set(new DefaultSystemAccessControl());
        requireNonNull(systemSecurityMetadata, "systemSecurityMetadata is null")
                .reset();
        getQueryRunner().getAccessControl().reset();
        groupProvider.reset();
    }

    @Test
    public void testAccessControl()
    {
        reset();

        assertAccessDenied("SELECT * FROM orders", "Cannot execute query", privilege("query", EXECUTE_QUERY));
        assertAccessDenied("INSERT INTO orders SELECT * FROM orders", "Cannot insert into table .*.orders.*", privilege("orders", INSERT_TABLE));
        assertAccessDenied("DELETE FROM orders", "Cannot delete from table .*.orders.*", privilege("orders", DELETE_TABLE));
        assertAccessDenied("TRUNCATE TABLE orders", "Cannot truncate table .*.orders.*", privilege("orders", TRUNCATE_TABLE));
        assertAccessDenied("CREATE TABLE foo AS SELECT * FROM orders", "Cannot create table .*.foo.*", privilege("foo", CREATE_TABLE));
        assertAccessDenied("ALTER TABLE orders SET PROPERTIES field_length = 32", "Cannot set table properties to .*.orders.*", privilege("orders", SET_TABLE_PROPERTIES));
        assertAccessDenied("ALTER TABLE orders ALTER COLUMN orderkey SET DATA TYPE char(100)", "Cannot alter a column for table .*.orders.*", privilege("orders", ALTER_COLUMN));
        assertAccessDenied("SELECT * FROM nation", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table .*.nation.*", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT * FROM (SELECT * FROM nation)", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table .*.nation.*", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT name FROM (SELECT * FROM nation)", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table .*.nation.*", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessAllowed("SELECT name FROM nation", privilege("nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT n1.nationkey, n2.regionkey FROM nation n1, nation n2", "Cannot select from columns \\[nationkey, regionkey] in table .*.nation.*", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT count(name) as c FROM nation where comment > 'abc' GROUP BY regionkey having max(nationkey) > 10", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table .*.nation.*", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT 1 FROM region, nation where region.regionkey = nation.nationkey", "Cannot select from columns \\[nationkey] in table .*.nation.*", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT count(*) FROM nation", "Cannot select from columns \\[] in table .*.nation.*", privilege("nation", SELECT_COLUMN));
        assertAccessDenied("WITH t1 AS (SELECT * FROM nation) SELECT * FROM t1", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table .*.nation.*", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessAllowed("SELECT name AS my_alias FROM nation", privilege("my_alias", SELECT_COLUMN));
        assertAccessAllowed("SELECT my_alias from (SELECT name AS my_alias FROM nation)", privilege("my_alias", SELECT_COLUMN));
        assertAccessDenied("SELECT name AS my_alias FROM nation", "Cannot select from columns \\[name] in table .*.nation.*", privilege("nation.name", SELECT_COLUMN));
        assertAccessAllowed("SELECT 1 FROM mock.default.test_materialized_view");
        assertAccessDenied("SELECT 1 FROM mock.default.test_materialized_view", "Cannot select from columns.*", privilege("test_materialized_view", SELECT_COLUMN));
        assertAccessAllowed("SELECT * FROM mock.default.test_materialized_view");
        assertAccessDenied("SELECT * FROM mock.default.test_materialized_view", "Cannot select from columns.*", privilege("test_materialized_view", SELECT_COLUMN));
        assertAccessAllowed("SELECT 1 FROM mock.default.test_view_definer");
        assertAccessDenied("SELECT 1 FROM mock.default.test_view_definer", "Cannot select from columns.*", privilege("test_view_definer", SELECT_COLUMN));
        assertAccessAllowed("SELECT * FROM mock.default.test_view_definer");
        assertAccessDenied("SELECT * FROM mock.default.test_view_definer", "Cannot select from columns.*", privilege("test_view_definer", SELECT_COLUMN));
        assertAccessAllowed("SELECT 1 FROM mock.default.test_view_invoker");
        assertAccessDenied("SELECT 1 FROM mock.default.test_view_invoker", "Cannot select from columns.*", privilege("test_view_invoker", SELECT_COLUMN));
        assertAccessAllowed("SELECT * FROM mock.default.test_view_invoker");
        assertAccessDenied("SELECT * FROM mock.default.test_view_invoker", "Cannot select from columns.*", privilege("test_view_invoker", SELECT_COLUMN));
        // with current implementation this next block of checks is redundant to `SELECT 1 FROM ..`, but it is not obvious unless details of how
        // semantics analyzer works are known
        assertAccessAllowed("SELECT count(*) FROM mock.default.test_materialized_view");
        assertAccessDenied("SELECT count(*) FROM mock.default.test_materialized_view", "Cannot select from columns.*", privilege("test_materialized_view", SELECT_COLUMN));
        assertAccessAllowed("SELECT count(*) FROM mock.default.test_view_invoker");
        assertAccessDenied("SELECT count(*) FROM mock.default.test_view_invoker", "Cannot select from columns.*", privilege("test_view_invoker", SELECT_COLUMN));
        assertAccessAllowed("SELECT count(*) FROM mock.default.test_view_definer");
        assertAccessDenied("SELECT count(*) FROM mock.default.test_view_definer", "Cannot select from columns.*", privilege("test_view_definer", SELECT_COLUMN));

        assertAccessDenied(
                "SELECT orders.custkey, lineitem.quantity FROM orders JOIN lineitem USING (orderkey)",
                "Cannot select from columns \\[orderkey, custkey] in table .*",
                privilege("orders.orderkey", SELECT_COLUMN));

        assertAccessDenied(
                "SELECT orders.custkey, lineitem.quantity FROM orders JOIN lineitem USING (orderkey)",
                "Cannot select from columns \\[orderkey, quantity] in table .*",
                privilege("lineitem.orderkey", SELECT_COLUMN));

        assertAccessDenied("SHOW CREATE TABLE orders", "Cannot show create table for .*.orders.*", privilege("orders", SHOW_CREATE_TABLE));
        assertAccessAllowed("SHOW CREATE TABLE lineitem", privilege("orders", SHOW_CREATE_TABLE));
        assertAccessDenied("SELECT my_function(1)", "Cannot execute function my_function", privilege("mock.function.my_function", EXECUTE_FUNCTION));
        assertAccessAllowed("SELECT my_function(1)", privilege("max", EXECUTE_FUNCTION));
        assertAccessAllowed("SELECT abs(-10)", privilege("abs", EXECUTE_FUNCTION));
        assertAccessAllowed("SELECT abs(-10)", privilege("system.builtin.abs", EXECUTE_FUNCTION));
        assertAccessAllowed("SHOW STATS FOR lineitem");
        assertAccessAllowed("SHOW STATS FOR lineitem", privilege("orders", SELECT_COLUMN));
        assertAccessAllowed("SHOW STATS FOR (SELECT * FROM lineitem)");
        assertAccessAllowed("SHOW STATS FOR (SELECT * FROM lineitem)", privilege("orders", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT * FROM nation)", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table or view .*.nation", privilege("nation", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT nationkey FROM nation)", "Cannot select from columns \\[nationkey] in table or view .*.nation", privilege("nation", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT nationkey FROM nation)", "Cannot select from columns \\[nationkey] in table or view .*.nation", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT *, nationkey FROM nation)", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table or view .*.nation", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT *, * FROM nation)", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table or view .*.nation", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT linenumber, orderkey FROM lineitem)", "Cannot select from columns \\[linenumber, orderkey] in table or view .*.lineitem.*", privilege("lineitem", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT linenumber, orderkey, quantity FROM lineitem)", "Cannot select from columns \\[linenumber, orderkey, quantity] in table or view .*.lineitem.*", privilege("lineitem.linenumber", SELECT_COLUMN), privilege("lineitem.orderkey", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT nationkey FROM nation)", "Cannot select from columns \\[nationkey] in table or view .*.nation.*", privilege("nation", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT * FROM nation)", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table or view .*.nation.*", privilege("nation", SELECT_COLUMN));
    }

    @Test
    public void testViewColumnAccessControl()
    {
        reset();

        Session viewOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(Identity.ofUser("test_view_access_owner"))
                .setCatalog(getSession().getCatalog())
                .setSchema(getSession().getSchema())
                .build();

        String columnAccessViewName = "test_view_column_access_" + randomNameSuffix();

        // TEST COLUMN-LEVEL PRIVILEGES
        // view creation permissions are only checked at query time, not at creation
        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW " + columnAccessViewName + " AS SELECT * FROM orders",
                privilege("orders", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify selecting from a view over a table requires the view owner to have special view creation privileges for the table
        assertAccessDenied(
                "SELECT * FROM " + columnAccessViewName,
                "View owner does not have sufficient privileges: View owner 'test_view_access_owner' cannot create view that selects from \\w+.\\w+.orders\\w*",
                privilege(viewOwnerSession.getUser(), "orders", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify the view owner can select from the view even without special view creation privileges
        assertAccessAllowed(
                viewOwnerSession,
                "SELECT * FROM " + columnAccessViewName,
                privilege(viewOwnerSession.getUser(), "orders", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify selecting from a view over a table does not require the session user to have SELECT privileges on the underlying table
        assertAccessAllowed(
                "SELECT * FROM " + columnAccessViewName,
                privilege(getSession().getUser(), "orders", CREATE_VIEW_WITH_SELECT_COLUMNS));
        assertAccessAllowed(
                "SELECT * FROM " + columnAccessViewName,
                privilege(getSession().getUser(), "orders", SELECT_COLUMN));

        Session nestedViewOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(Identity.ofUser("test_nested_view_access_owner"))
                .setCatalog(getSession().getCatalog())
                .setSchema(getSession().getSchema())
                .build();

        String nestedViewName = "test_nested_view_column_access_" + randomNameSuffix();
        // view creation permissions are only checked at query time, not at creation
        assertAccessAllowed(
                nestedViewOwnerSession,
                "CREATE VIEW " + nestedViewName + " AS SELECT * FROM " + columnAccessViewName,
                privilege(columnAccessViewName, CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify selecting from a view over a view requires the view owner of the outer view to have special view creation privileges for the inner view
        assertAccessDenied(
                "SELECT * FROM " + nestedViewName,
                "View owner does not have sufficient privileges: View owner 'test_nested_view_access_owner' cannot create view that selects from \\w+.\\w+.test_view_column_access\\w*",
                privilege(nestedViewOwnerSession.getUser(), columnAccessViewName, CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify selecting from a view over a view does not require the session user to have SELECT privileges for the inner view
        assertAccessAllowed(
                "SELECT * FROM " + nestedViewName,
                privilege(getSession().getUser(), columnAccessViewName, CREATE_VIEW_WITH_SELECT_COLUMNS));
        assertAccessAllowed(
                "SELECT * FROM " + nestedViewName,
                privilege(getSession().getUser(), columnAccessViewName, SELECT_COLUMN));

        // verify that INVOKER security runs as session user
        String invokerViewName = "test_invoker_view_column_access_" + randomNameSuffix();
        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW " + invokerViewName + " SECURITY INVOKER AS SELECT * FROM orders",
                privilege("orders", CREATE_VIEW_WITH_SELECT_COLUMNS));
        assertAccessAllowed(
                "SELECT * FROM " + invokerViewName,
                privilege(viewOwnerSession.getUser(), "orders", SELECT_COLUMN));
        assertAccessDenied(
                "SELECT * FROM " + invokerViewName,
                "Cannot select from columns \\[.*] in table .*.orders.*",
                privilege(getSession().getUser(), "orders", SELECT_COLUMN));

        // verify that groups are set inside access control
        // require view owner to be in a group to access table
        getQueryRunner().getAccessControl().denyIdentityTable((identity, table) -> identity.getGroups().contains("testgroup") || !"orders".equals(table));
        assertThatThrownBy(() -> getQueryRunner().execute(getSession(), "SELECT * FROM " + columnAccessViewName))
                .hasMessageMatching("Access Denied: View owner does not have sufficient privileges: View owner 'test_view_access_owner' cannot create view that selects from \\w+.\\w+.orders");

        // verify view can be queried when owner is in group
        groupProvider.setUserGroups(ImmutableMap.of(viewOwnerSession.getUser(), ImmutableSet.of("testgroup")));
        getQueryRunner().execute(getSession(), "SELECT * FROM " + columnAccessViewName);

        // change access denied exception to view
        assertAccessDenied("SHOW CREATE VIEW " + nestedViewName, "Cannot show create table for .*test_nested_view_column_access.*", privilege(nestedViewName, SHOW_CREATE_TABLE));
        assertAccessAllowed("SHOW CREATE VIEW " + nestedViewName, privilege("test_denied_access_view", SHOW_CREATE_TABLE));

        assertAccessAllowed(nestedViewOwnerSession, "DROP VIEW " + nestedViewName);
        assertAccessAllowed(viewOwnerSession, "DROP VIEW " + columnAccessViewName);
        assertAccessAllowed(viewOwnerSession, "DROP VIEW " + invokerViewName);
    }

    @Test
    public void testViewOwnersRoleGrants()
    {
        reset();

        String viewOwner = "view_owner";
        TrinoPrincipal viewOwnerPrincipal = new TrinoPrincipal(USER, viewOwner);
        String viewName = "test_view_column_access_" + randomNameSuffix();

        systemSecurityMetadata.grantRoles(getSession(), Set.of("view_owner_role"), Set.of(viewOwnerPrincipal), false, Optional.empty());
        systemSecurityMetadata.setViewOwner(
                getSession(),
                new CatalogSchemaTableName("blackhole", "default", viewName),
                viewOwnerPrincipal);

        Session viewOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(Identity.forUser(viewOwner)
                        .withEnabledRoles(Set.of("view_owner_role"))
                        .build())
                .setCatalog(getSession().getCatalog())
                .setSchema(getSession().getSchema())
                .build();
        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW " + viewName + " AS SELECT * FROM orders",
                privilege("orders", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // whenever view_owner_role_without_access is granted to test_view_access_owner, the view cannot be queried
        getQueryRunner().getAccessControl()
                .denyIdentityTable((identity, table) -> !(identity.getEnabledRoles().contains("view_owner_role_without_access") && "orders".equals(table)));

        systemSecurityMetadata.grantRoles(getSession(), Set.of("view_owner_role_without_access"), Set.of(viewOwnerPrincipal), false, Optional.empty());
        assertThatThrownBy(() -> getQueryRunner().execute(viewOwnerSession,
                "SELECT * FROM " + viewName))
                .hasMessageMatching("Access Denied: Cannot select from columns \\[.*] in table or view \\w+\\.\\w+\\.orders");

        systemSecurityMetadata.revokeRoles(getSession(), Set.of("view_owner_role_without_access"), Set.of(viewOwnerPrincipal), false, Optional.empty());
        getQueryRunner().execute(viewOwnerSession, "SELECT * FROM " + viewName);

        assertAccessAllowed(viewOwnerSession, "DROP VIEW " + viewName);
    }

    @Test
    public void testJoinBaseTableWithView()
    {
        reset();

        String viewOwner = "view_owner";
        TrinoPrincipal viewOwnerPrincipal = new TrinoPrincipal(USER, viewOwner);
        String viewName = "test_join_base_table_with_view_" + randomNameSuffix();

        systemSecurityMetadata.grantRoles(getSession(), Set.of("view_owner_role"), Set.of(viewOwnerPrincipal), false, Optional.empty());
        systemSecurityMetadata.setViewOwner(
                getSession(),
                new CatalogSchemaTableName("blackhole", "default", viewName),
                viewOwnerPrincipal);

        Session viewOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(Identity.forUser(viewOwner)
                        .withEnabledRoles(Set.of("view_owner_role"))
                        .build())
                .setCatalog(getSession().getCatalog())
                .setSchema(getSession().getSchema())
                .build();
        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW " + viewName + " AS SELECT * FROM orders",
                privilege("orders", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // view_owner_role_without_access is granted to view_owner, that role deny access to the base table
        getQueryRunner().getAccessControl()
                .denyIdentityTable((identity, table) -> !(identity.getEnabledRoles().contains("view_owner_role_without_access") && "orders".equals(table)));
        systemSecurityMetadata.grantRoles(getSession(), Set.of("view_owner_role_without_access"), Set.of(viewOwnerPrincipal), false, Optional.empty());
        String errorMessage = "Access Denied: Cannot select from columns \\[.*] in table or view \\w+\\.\\w+\\.orders";

        getQueryRunner().execute(viewOwnerSession, "SELECT * FROM orders");
        assertThatThrownBy(() -> getQueryRunner().execute(viewOwnerSession, "SELECT * FROM " + viewName))
                .hasMessageMatching(errorMessage);
        assertThatThrownBy(() -> getQueryRunner().execute(viewOwnerSession, "SELECT * FROM orders, " + viewName))
                .hasMessageMatching(errorMessage);
        assertThatThrownBy(() -> getQueryRunner().execute(viewOwnerSession, "SELECT * FROM %s, orders".formatted(viewName)))
                .hasMessageMatching(errorMessage);

        assertAccessAllowed(viewOwnerSession, "DROP VIEW " + viewName);
    }

    @Test
    public void testAllowCallFunction()
    {
        reset();

        String functionOwner = "function_owner";
        CatalogSchemaFunctionName outerFunction = new CatalogSchemaFunctionName("memory", new SchemaFunctionName("default", "function_allow_outer"));
        CatalogSchemaFunctionName innerFunction = new CatalogSchemaFunctionName("memory", new SchemaFunctionName("default", "function_allow_inner"));

        Session functionOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(Identity.ofUser(functionOwner))
                .setCatalog(getSession().getCatalog())
                .setSchema(getSession().getSchema())
                .build();

        // simply create a function
        assertAccessAllowed(
                functionOwnerSession,
                "CREATE FUNCTION memory.default.function_allow_inner (x integer) RETURNS bigint RETURN x + 42");
        assertThat(systemSecurityMetadata.getFunctionOwner(innerFunction)).isEqualTo(functionOwner);

        // assert that function can be called for both definer session and default session
        assertAccessAllowed(
                functionOwnerSession,
                "SELECT memory.default.function_allow_inner(2)");
        assertAccessAllowed("SELECT memory.default.function_allow_inner(2)");

        // simply create another function, internally calls the first function
        assertAccessAllowed(
                functionOwnerSession,
                "CREATE FUNCTION memory.default.function_allow_outer (x integer) RETURNS bigint RETURN x + memory.default.function_allow_inner(58)");
        assertThat(systemSecurityMetadata.getFunctionOwner(outerFunction)).isEqualTo(functionOwner);

        // assert that THE outer function can be called for both definer session and default session
        assertAccessAllowed(
                functionOwnerSession,
                "SELECT memory.default.function_allow_outer(2)");
        assertAccessAllowed("SELECT memory.default.function_allow_outer(2)");

        // assert that lack of privileges to execute inner function doesn't block calling it through outer one
        assertAccessAllowed(
                "SELECT memory.default.function_allow_outer(2)",
                privilege(getSession().getUser(), "memory.default.function_allow_inner", EXECUTE_FUNCTION));

        assertAccessDenied(
                "SELECT memory.default.function_allow_inner(2)",
                "Cannot execute function memory.default.function_allow_inner",
                privilege(getSession().getUser(), "memory.default.function_allow_inner", EXECUTE_FUNCTION));
    }

    @Test
    public void testAllowCallFunctionWithRoleGrant()
    {
        reset();

        String functionOwner = "function_owner";
        CatalogSchemaFunctionName outerFunction = new CatalogSchemaFunctionName("memory", new SchemaFunctionName("default", "function_deny_outer"));
        CatalogSchemaFunctionName innerFunction = new CatalogSchemaFunctionName("memory", new SchemaFunctionName("default", "function_deny_inner"));
        TrinoPrincipal functionOwnerPrincipal = new TrinoPrincipal(USER, functionOwner);
        systemSecurityMetadata.grantRoles(getSession(), ImmutableSet.of("function_owner_role"), ImmutableSet.of(functionOwnerPrincipal), false, Optional.empty());

        Session functionOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(Identity.forUser(functionOwner)
                        .withEnabledRoles(Set.of("function_owner_role"))
                        .build())
                .setCatalog(getSession().getCatalog())
                .setSchema(getSession().getSchema())
                .build();

        // simply create a function
        assertAccessAllowed(
                functionOwnerSession,
                "CREATE FUNCTION memory.default.function_deny_inner (x integer) RETURNS bigint RETURN x + 42");
        assertThat(systemSecurityMetadata.getFunctionOwner(innerFunction)).isEqualTo(functionOwner);

        // simply create another function that internally calls the first function
        assertAccessAllowed(
                functionOwnerSession,
                "CREATE FUNCTION memory.default.function_deny_outer (x integer) RETURNS bigint RETURN x + memory.default.function_deny_inner(58)");
        assertThat(systemSecurityMetadata.getFunctionOwner(outerFunction)).isEqualTo(functionOwner);

        // assert that outer function can be called for both definer session and default session
        assertAccessAllowed(
                functionOwnerSession,
                "SELECT memory.default.function_deny_outer(2)");
        assertAccessAllowed("SELECT memory.default.function_deny_outer(2)");

        // block role function_owner_role_without_access from calling inner function
        getQueryRunner().getAccessControl()
                .denyIdentityFunction((identity, function) -> !(identity.getEnabledRoles().contains("function_owner_role_without_access") && "default.function_deny_inner".equals(function)));
        // assign function_owner_role_without_access to function definer
        systemSecurityMetadata.grantRoles(getSession(), ImmutableSet.of("function_owner_role_without_access"), ImmutableSet.of(functionOwnerPrincipal), false, Optional.empty());

        // assert that because definer has function_owner_role_without_access role assigned it is impossible to call outer function
        assertAccessDenied(
                functionOwnerSession,
                "SELECT memory.default.function_deny_outer(2)",
                "Cannot execute function memory.default.function_deny_inner");
        assertAccessDenied(
                "SELECT memory.default.function_deny_outer(2)",
                "Cannot execute function memory.default.function_deny_inner");

        systemSecurityMetadata.revokeRoles(getSession(), ImmutableSet.of("function_owner_role_without_access"), ImmutableSet.of(functionOwnerPrincipal), false, Optional.empty());

        // assert that after revoking function_owner_role_without_access from definer function can be called once more
        assertAccessAllowed(
                functionOwnerSession,
                "SELECT memory.default.function_deny_outer(2)");
        assertAccessAllowed("SELECT memory.default.function_deny_outer(2)");
    }

    @Test
    public void testFunctionOwnerWhenDroppingFunction()
    {
        reset();

        String functionOwner1 = "function_owner1";
        String functionOwner2 = "function_owner2";
        CatalogSchemaFunctionName functionName = new CatalogSchemaFunctionName("memory", new SchemaFunctionName("default", "my_function"));

        Session functionOwnerSession1 = TestingSession.testSessionBuilder()
                .setIdentity(Identity.ofUser(functionOwner1))
                .setCatalog(getSession().getCatalog())
                .setSchema(getSession().getSchema())
                .build();
        Session functionOwnerSession2 = TestingSession.testSessionBuilder()
                .setIdentity(Identity.ofUser(functionOwner2))
                .setCatalog(getSession().getCatalog())
                .setSchema(getSession().getSchema())
                .build();

        assertAccessAllowed(
                functionOwnerSession1,
                "CREATE FUNCTION memory.default.my_function (x integer) RETURNS bigint RETURN x + 42");
        assertThat(systemSecurityMetadata.getFunctionOwner(functionName)).isEqualTo(functionOwner1);

        assertAccessAllowed(
                functionOwnerSession1,
                "DROP FUNCTION memory.default.my_function(integer)");

        assertAccessAllowed(
                functionOwnerSession2,
                "CREATE FUNCTION memory.default.my_function (x integer) RETURNS bigint RETURN x + 42");
        assertThat(systemSecurityMetadata.getFunctionOwner(functionName)).isEqualTo(functionOwner2);
    }

    @Test
    public void testFunctionOwnerWhenReplacingFunction()
    {
        reset();

        String functionOwner1 = "function_owner1";
        String functionOwner2 = "function_owner2";
        CatalogSchemaFunctionName functionName = new CatalogSchemaFunctionName("memory", new SchemaFunctionName("default", "my_replace_function"));

        Session functionOwnerSession1 = TestingSession.testSessionBuilder()
                .setIdentity(Identity.ofUser(functionOwner1))
                .setCatalog(getSession().getCatalog())
                .setSchema(getSession().getSchema())
                .build();
        Session functionOwnerSession2 = TestingSession.testSessionBuilder()
                .setIdentity(Identity.ofUser(functionOwner2))
                .setCatalog(getSession().getCatalog())
                .setSchema(getSession().getSchema())
                .build();

        assertAccessAllowed(
                functionOwnerSession1,
                "CREATE FUNCTION memory.default.my_replace_function (x integer) RETURNS bigint RETURN x + 42");
        assertThat(systemSecurityMetadata.getFunctionOwner(functionName)).isEqualTo(functionOwner1);

        assertAccessAllowed(
                functionOwnerSession2,
                "CREATE OR REPLACE FUNCTION memory.default.my_replace_function (x integer) RETURNS bigint RETURN x + 42");
        assertThat(systemSecurityMetadata.getFunctionOwner(functionName)).isEqualTo(functionOwner2);
    }

    @Test
    public void testViewFunctionAccessControl()
    {
        reset();

        Session viewOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(Identity.ofUser("test_view_access_owner"))
                .setCatalog(getSession().getCatalog())
                .setSchema(getSession().getSchema())
                .setPath(SqlPath.buildPath("mock.function", Optional.empty()))
                .build();

        // TEST FUNCTION PRIVILEGES
        // view creation permissions are only checked at query time, not at creation
        String functionAccessViewName = "test_view_function_access_" + randomNameSuffix();
        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW " + functionAccessViewName + " AS SELECT my_function(1) AS c",
                privilege("mock.function.my_function", GRANT_EXECUTE_FUNCTION));

        assertAccessDenied(
                "SELECT * FROM " + functionAccessViewName,
                "Cannot execute function my_function",
                privilege(viewOwnerSession.getUser(), "mock.function.my_function", GRANT_EXECUTE_FUNCTION));

        // verify executing from a view over a function does not require the session user to have execute privileges on the underlying function
        assertAccessAllowed(
                "SELECT * FROM " + functionAccessViewName,
                privilege(getSession().getUser(), "mock.function.my_function", EXECUTE_FUNCTION));

        // TEST SECURITY INVOKER
        // view creation permissions are only checked at query time, not at creation
        String invokerFunctionAccessViewName = "test_invoker_view_function_access_" + randomNameSuffix();
        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW " + invokerFunctionAccessViewName + " SECURITY INVOKER AS SELECT my_function(1) AS c",
                privilege("mock.function.my_function", GRANT_EXECUTE_FUNCTION));
        assertAccessAllowed(
                "SELECT * FROM " + invokerFunctionAccessViewName,
                privilege(viewOwnerSession.getUser(), "mock.function.my_function", EXECUTE_FUNCTION));
        assertAccessDenied(
                "SELECT * FROM " + invokerFunctionAccessViewName,
                "Cannot execute function my_function",
                privilege(getSession().getUser(), "mock.function.my_function", EXECUTE_FUNCTION));

        assertAccessAllowed(viewOwnerSession, "DROP VIEW " + functionAccessViewName);
        assertAccessAllowed(viewOwnerSession, "DROP VIEW " + invokerFunctionAccessViewName);
    }

    @Test
    public void testFunctionAccessControl()
    {
        reset();

        assertAccessDenied(
                "SELECT my_function(42)",
                "Cannot execute function my_function",
                new TestingPrivilege(Optional.empty(), "mock.function.my_function", EXECUTE_FUNCTION));

        // inline and builtin functions are always allowed, and there are no security checks
        TestingPrivilege denyAllFunctionCalls = new TestingPrivilege(Optional.empty(), name -> true, EXECUTE_FUNCTION);
        assertAccessAllowed("SELECT abs(42)", denyAllFunctionCalls);
        assertAccessAllowed("WITH FUNCTION foo() RETURNS int RETURN 42 SELECT foo()", denyAllFunctionCalls);
        assertAccessDenied("SELECT my_function(42)", "Cannot execute function my_function", denyAllFunctionCalls);

        TestingPrivilege denyNonMyFunctionCalls = new TestingPrivilege(Optional.empty(), name -> !name.equals("mock.function.my_function"), EXECUTE_FUNCTION);
        assertAccessAllowed("SELECT my_function(42)", denyNonMyFunctionCalls);
        assertAccessDenied("SELECT other_function(42)", "Cannot execute function other_function", denyNonMyFunctionCalls);
    }

    @Test
    public void testTableFunctionRequiredColumns()
    {
        reset();

        assertAccessDenied(
                "SELECT * FROM TABLE(exclude_columns(TABLE(nation), descriptor(regionkey, comment)))",
                "Cannot select from columns \\[nationkey, name] in table .*.nation.*",
                privilege("nation.nationkey", SELECT_COLUMN));
    }

    @Test
    public void testAnalyzeAccessControl()
    {
        reset();

        assertAccessAllowed("ANALYZE nation");
        assertAccessDenied("ANALYZE nation", "Cannot ANALYZE \\(missing insert privilege\\) table .*.nation.*", privilege("nation", INSERT_TABLE));
        assertAccessDenied("ANALYZE nation", "Cannot select from columns \\[.*] in table or view .*.nation", privilege("nation", SELECT_COLUMN));
        assertAccessDenied("ANALYZE nation", "Cannot select from columns \\[.*nationkey.*] in table or view .*.nation", privilege("nation.nationkey", SELECT_COLUMN));
    }

    @Test
    public void testMetadataFilterColumns()
    {
        reset();

        getQueryRunner().getAccessControl().deny(privilege("nation.regionkey", SELECT_COLUMN));

        assertThat(query("SELECT column_name FROM information_schema.columns WHERE table_catalog = CURRENT_CATALOG AND table_schema = CURRENT_SCHEMA and table_name = 'nation'"))
                .matches("VALUES VARCHAR 'nationkey', 'name', 'comment'");

        assertThat(query("SELECT column_name FROM system.jdbc.columns WHERE table_cat = CURRENT_CATALOG AND table_schem = CURRENT_SCHEMA and table_name = 'nation'"))
                .matches("VALUES VARCHAR 'nationkey', 'name', 'comment'");
    }

    @Test
    public void testCommentView()
    {
        reset();

        String viewName = "comment_view" + randomNameSuffix();
        assertUpdate("CREATE VIEW " + viewName + " COMMENT 'old comment' AS SELECT * FROM orders");
        assertAccessDenied("COMMENT ON VIEW " + viewName + " IS 'new comment'", "Cannot comment view to .*", privilege(viewName, COMMENT_VIEW));
        assertAccessAllowed("COMMENT ON VIEW " + viewName + " IS 'new comment'");
    }

    @Test
    public void testCommentOnRedirectedTable()
    {
        reset();

        String query = "SELECT * FROM system.metadata.table_comments WHERE catalog_name = 'mock' AND schema_name = 'default' AND table_name LIKE 'redirected%'";
        assertQuery(query, "VALUES ('mock', 'default', 'redirected_source', 'this is a redirected table')");
        getQueryRunner().getAccessControl().denyTables(schemaTableName -> !schemaTableName.getTableName().equals("redirected_target"));
        assertQueryReturnsEmptyResult(query);
    }

    @Test
    public void testViewWithTableFunction()
    {
        reset();

        for (boolean securityDefiner : Arrays.asList(true, false)) {
            Session viewOwner = getSession();
            Session otherUser = Session.builder(getSession())
                    .setIdentity(Identity.ofUser(getSession().getUser() + "-someone-else"))
                    .build();

            String viewName = "memory.default.definer_view_with_ptf";
            assertUpdate(viewOwner, "CREATE VIEW " + viewName + " SECURITY " + (securityDefiner ? "DEFINER" : "INVOKER") + " AS SELECT * FROM TABLE (jdbc.system.query('SELECT ''from h2'', monthname(CAST(''2005-09-10'' AS date))'))");
            String viewValues = "VALUES ('from h2', 'September') ";

            assertThat(query(viewOwner, "TABLE " + viewName)).matches(viewValues);
            assertThat(query(otherUser, "TABLE " + viewName)).matches(viewValues);

            TestingPrivilege grantExecute = TestingAccessControlManager.privilege("jdbc.system.query", GRANT_EXECUTE_FUNCTION);
            assertAccessAllowed(viewOwner, "TABLE " + viewName, grantExecute);
            if (securityDefiner) {
                assertAccessDenied(
                        otherUser,
                        "TABLE " + viewName,
                        "Cannot execute function jdbc.system.query",
                        grantExecute);
            }
            else {
                assertAccessAllowed(otherUser, "TABLE " + viewName, grantExecute);
            }

            assertUpdate("DROP VIEW " + viewName);
        }
    }

    @Test
    public void testCommentColumnView()
    {
        reset();

        String viewName = "comment_view" + randomNameSuffix();
        assertUpdate("CREATE VIEW " + viewName + " AS SELECT * FROM orders");
        assertAccessDenied("COMMENT ON COLUMN " + viewName + ".orderkey IS 'new order key comment'", "Cannot comment column to .*", privilege(viewName, COMMENT_COLUMN));
        assertUpdate(getSession(), "COMMENT ON COLUMN " + viewName + ".orderkey IS 'new comment'");
    }

    @Test
    public void testCommentColumnMaterializedView()
    {
        reset();

        String viewName = "comment_materialized_view" + randomNameSuffix();
        assertUpdate("CREATE MATERIALIZED VIEW mock.default." + viewName + " AS SELECT * FROM orders");
        assertAccessDenied("COMMENT ON COLUMN mock.default." + viewName + ".column_0 IS 'new comment'", "Cannot comment column to .*", privilege(viewName, COMMENT_COLUMN));
        assertUpdate(getSession(), "COMMENT ON COLUMN mock.default." + viewName + ".column_0 IS 'new comment'");
    }

    @Test
    public void testAddColumn()
    {
        reset();

        String tableName = "test_add_column" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM orders", 0);

        assertAccessDenied("ALTER TABLE " + tableName + " ADD COLUMN new_col char(100)", "Cannot add a column to table .*." + tableName + ".*", privilege(tableName, ADD_COLUMN));
        assertAccessAllowed("ALTER TABLE " + tableName + " ADD COLUMN new_col char(100)", privilege(tableName + ".orderkey", ADD_COLUMN));
    }

    @Test
    public void testDropColumn()
    {
        reset();

        String tableName = "test_drop_column" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM orders", 0);

        assertAccessDenied("ALTER TABLE " + tableName + " DROP COLUMN orderkey", "Cannot drop a column from table .*." + tableName + ".*", privilege(tableName, DROP_COLUMN));
        assertAccessAllowed("ALTER TABLE " + tableName + " DROP COLUMN orderkey", privilege(tableName + ".orderkey", DROP_COLUMN));
    }

    @Test
    public void testRenameColumn()
    {
        reset();

        String tableName = "test_rename_column" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM orders", 0);

        assertAccessDenied("ALTER TABLE " + tableName + " RENAME COLUMN orderkey TO renamed", "Cannot rename a column in table .*." + tableName + ".*", privilege(tableName, RENAME_COLUMN));
        assertAccessAllowed("ALTER TABLE " + tableName + " RENAME COLUMN orderkey TO renamed", privilege(tableName + ".orderkey", RENAME_COLUMN));
    }

    @Test
    public void testSetColumnType()
    {
        reset();

        String tableName = "test_set_colun_type" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM orders", 0);

        assertAccessDenied("ALTER TABLE " + tableName + " ALTER COLUMN orderkey SET DATA TYPE char(100)", "Cannot alter a column for table .*." + tableName + ".*", privilege(tableName, ALTER_COLUMN));
        assertAccessAllowed("ALTER TABLE " + tableName + " ALTER COLUMN orderkey SET DATA TYPE char(100)", privilege(tableName + ".orderkey", ALTER_COLUMN));
    }

    @Test
    public void testDropNotNullConstraint()
    {
        reset();

        String tableName = "test_drop_not_null" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM orders", 0);

        assertAccessDenied("ALTER TABLE " + tableName + " ALTER COLUMN orderkey DROP NOT NULL", "Cannot alter a column for table .*." + tableName + ".*", privilege(tableName, ALTER_COLUMN));
        assertThatThrownBy(() -> getQueryRunner().execute(getSession(), "ALTER TABLE " + tableName + " ALTER COLUMN orderkey DROP NOT NULL"))
                .hasMessageContaining("Column is already nullable"); // Update this test once Black Hole connector supports a not null constraint
    }

    @Test
    public void testSetTableProperties()
    {
        reset();

        assertAccessDenied("ALTER TABLE orders SET PROPERTIES field_length = 32", "Cannot set table properties to .*.orders.*", privilege("orders", SET_TABLE_PROPERTIES));
        assertThatThrownBy(() -> getQueryRunner().execute(getSession(), "ALTER TABLE orders SET PROPERTIES field_length = 32"))
                .hasMessageContaining("This connector does not support setting table properties");
    }

    @Test
    public void testDeleteAccessControl()
    {
        reset();

        assertAccessDenied("DELETE FROM orders WHERE orderkey < 12", "Cannot select from columns \\[orderkey] in table or view .*.orders.*", privilege("orders.orderkey", SELECT_COLUMN));
        assertAccessAllowed("DELETE FROM orders WHERE orderkey < 12", privilege("orders" + ".orderdate", SELECT_COLUMN));
        assertAccessAllowed("DELETE FROM orders", privilege("orders", SELECT_COLUMN));
    }

    @Test
    public void testTruncateAccessControl()
    {
        reset();

        assertAccessAllowed("TRUNCATE TABLE orders", privilege("orders", SELECT_COLUMN));
    }

    @Test
    public void testUpdateAccessControl()
    {
        reset();

        assertAccessDenied("UPDATE orders SET orderkey=123", "Cannot update columns \\[orderkey] in table .*", privilege("orders", UPDATE_TABLE));
        assertAccessDenied("UPDATE orders SET orderkey=123 WHERE custkey < 12", "Cannot select from columns \\[custkey] in table or view .*.default.orders", privilege("orders.custkey", SELECT_COLUMN));
        assertAccessAllowed("UPDATE orders SET orderkey=123", privilege("orders", SELECT_COLUMN));
    }

    @Test
    public void testMergeAccessControl()
    {
        reset();

        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();

        String targetTable = "merge_nation_target_" + randomNameSuffix();
        String targetName = format("%s.%s.%s", catalogName, schemaName, targetTable);
        String sourceTable = "merge_nation_source_" + randomNameSuffix();
        String sourceName = format("%s.%s.%s", catalogName, schemaName, sourceTable);

        assertUpdate(format("CREATE TABLE %s (nation_name VARCHAR, region_name VARCHAR)", targetTable));

        assertUpdate(format("CREATE TABLE %s (nation_name VARCHAR, region_name VARCHAR)", sourceTable));

        String baseMergeSql = format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                "    ON (t.nation_name = s.nation_name)";
        String deleteCase = "" +
                "    WHEN MATCHED AND t.nation_name > (SELECT name FROM tpch.tiny.region WHERE name = t.region_name AND name LIKE ('A%'))" +
                "        THEN DELETE";
        String updateCase = "" +
                "    WHEN MATCHED AND t.nation_name = 'GERMANY'" +
                "        THEN UPDATE SET nation_name = concat(s.nation_name, '_foo')";
        String insertCase = "" +
                "    WHEN NOT MATCHED AND s.region_name = 'EUROPE'" +
                "        THEN INSERT VALUES(s.nation_name, (SELECT 'EUROPE'))";

        // Show that without SELECT on the source table, the MERGE fails regardless of which case is included
        for (String mergeCase : ImmutableList.of(deleteCase, updateCase, insertCase)) {
            assertAccessDenied(baseMergeSql + mergeCase, "Cannot select from columns .* in table or view " + sourceName, privilege(sourceTable, SELECT_COLUMN));
        }

        // Show that without SELECT on the target table, the MERGE fails regardless of which case is included
        for (String mergeCase : ImmutableList.of(deleteCase, updateCase, insertCase)) {
            assertAccessDenied(baseMergeSql + mergeCase, "Cannot select from columns .* in table or view " + targetName, privilege(targetTable, SELECT_COLUMN));
        }

        // Show that without INSERT on the target table, the MERGE fails
        assertAccessDenied(baseMergeSql + insertCase, "Cannot insert into table " + targetName, privilege(targetTable, INSERT_TABLE));

        // Show that without DELETE on the target table, the MERGE fails
        assertAccessDenied(baseMergeSql + deleteCase, "Cannot delete from table " + targetName, privilege(targetTable, DELETE_TABLE));

        // Show that without UPDATE on the target table, the MERGE fails
        assertAccessDenied(baseMergeSql + updateCase, "Cannot update columns \\[nation_name] in table " + targetName, privilege(targetTable, UPDATE_TABLE));

        assertAccessAllowed(
                """
                MERGE INTO orders o USING region r ON (o.orderkey = r.regionkey)
                WHEN MATCHED AND o.orderkey % 2 = 0 THEN DELETE
                WHEN MATCHED AND o.orderkey % 2 = 1 THEN UPDATE SET orderkey = null
                WHEN NOT MATCHED THEN INSERT VALUES (null, null, null, null, null, null, null, null, null)
                """);

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testNonQueryAccessControl()
    {
        reset();

        assertAccessDenied("SET SESSION " + QUERY_MAX_MEMORY + " = '10MB'",
                "Cannot set system session property " + QUERY_MAX_MEMORY,
                privilege(QUERY_MAX_MEMORY, SET_SESSION));

        assertAccessDenied("CREATE TABLE foo (pk bigint)", "Cannot create table .*.foo.*", privilege("foo", CREATE_TABLE));
        assertAccessDenied("DROP TABLE orders", "Cannot drop table .*.orders.*", privilege("orders", DROP_TABLE));
        assertAccessDenied("ALTER TABLE orders RENAME TO foo", "Cannot rename table .*.orders.* to .*.foo.*", privilege("orders", RENAME_TABLE));
        assertAccessDenied("ALTER TABLE orders SET PROPERTIES field_length = 32", "Cannot set table properties to .*.orders.*", privilege("orders", SET_TABLE_PROPERTIES));
        assertAccessDenied("ALTER TABLE orders ADD COLUMN foo bigint", "Cannot add a column to table .*.orders.*", privilege("orders", ADD_COLUMN));
        assertAccessDenied("ALTER TABLE orders DROP COLUMN foo", "Cannot drop a column from table .*.orders.*", privilege("orders", DROP_COLUMN));
        assertAccessDenied("ALTER TABLE orders RENAME COLUMN orderkey TO foo", "Cannot rename a column in table .*.orders.*", privilege("orders", RENAME_COLUMN));
        assertAccessDenied("CREATE VIEW foo as SELECT * FROM orders", "Cannot create view .*.foo.*", privilege("foo", CREATE_VIEW));
        assertAccessDenied("CREATE MATERIALIZED VIEW foo as SELECT * FROM orders", "Cannot create materialized view .*.foo.*", privilege("foo", CREATE_MATERIALIZED_VIEW));
        // todo add DROP VIEW test... not all connectors have view support

        try {
            assertAccessDenied("SELECT 1", "Principal .* cannot become user " + getSession().getUser() + ".*", privilege(getSession().getUser(), SET_USER));
        }
        catch (AssertionError e) {
            // There is no clean exception message for authorization failure.  We simply get a 403
            assertThat(e).hasMessageContaining("statusCode=403");
        }
    }

    @Test
    public void testDescribe()
    {
        reset();

        assertAccessDenied("DESCRIBE orders", "Cannot show columns of table default.orders", privilege("orders", SHOW_COLUMNS));
        getQueryRunner().getAccessControl().deny(privilege("orders.orderkey", SELECT_COLUMN));
        assertQuery(
                "DESCRIBE orders",
                "VALUES " +
                        // orderkey column is filtered
                        "('custkey', 'bigint', '', '')," +
                        "('orderstatus', 'varchar(1)', '', '')," +
                        "('totalprice', 'double', '', '')," +
                        "('orderdate', 'date', '', '')," +
                        "('orderpriority', 'varchar(15)', '', '')," +
                        "('clerk', 'varchar(15)', '', '')," +
                        "('shippriority', 'integer', '', '')," +
                        "('comment', 'varchar(79)', '', '')");
    }

    @Test
    public void testDescribeForViews()
    {
        reset();

        String viewName = "describe_orders_view" + randomNameSuffix();
        assertUpdate("CREATE VIEW " + viewName + " AS SELECT * FROM orders");
        assertAccessDenied("DESCRIBE " + viewName, "Cannot show columns of table default.*", privilege(viewName, SHOW_COLUMNS));
        getQueryRunner().getAccessControl().deny(privilege(viewName + ".orderkey", SELECT_COLUMN));
        assertQuery(
                "DESCRIBE " + viewName,
                "VALUES " +
                        // orderkey column is filtered
                        "('custkey', 'bigint', '', '')," +
                        "('orderstatus', 'varchar(1)', '', '')," +
                        "('totalprice', 'double', '', '')," +
                        "('orderdate', 'date', '', '')," +
                        "('orderpriority', 'varchar(15)', '', '')," +
                        "('clerk', 'varchar(15)', '', '')," +
                        "('shippriority', 'integer', '', '')," +
                        "('comment', 'varchar(79)', '', '')");
        assertUpdate("DROP VIEW " + viewName);
    }

    @Test
    public void testNoCatalogIsNeededInSessionForShowRoles()
    {
        reset();

        Session session = testSessionBuilder()
                .setIdentity(Identity.forUser("alice")
                        .withConnectorRoles(ImmutableMap.of("mock", new SelectedRole(ROLE, Optional.of("alice_role"))))
                        .build())
                .build();
        assertQuery(session, "SHOW ROLES IN mock", "VALUES 'alice_role'");
        assertQuery(session, "SHOW ROLE GRANTS IN mock", "VALUES 'alice_role'");
        assertQuery(session, "SHOW CURRENT ROLES FROM mock", "VALUES 'alice_role'");
        assertQuery(session, "SELECT * FROM mock.information_schema.applicable_roles", "SELECT 'alice', 'USER', 'alice_role', 'NO'");
    }

    @Test
    public void testShowRolesWithLegacyCatalogRoles()
    {
        reset();

        Session session = testSessionBuilder()
                .setCatalog("mock")
                .setIdentity(Identity.forUser("alice")
                        .withConnectorRoles(ImmutableMap.of("mock", new SelectedRole(ROLE, Optional.of("alice_role"))))
                        .build())
                .setSystemProperty("legacy_catalog_roles", "true")
                .build();
        assertQuery(session, "SHOW ROLES", "VALUES 'alice_role'");
        assertQuery(session, "SHOW ROLE GRANTS", "VALUES 'alice_role'");
        assertQuery(session, "SHOW CURRENT ROLES", "VALUES 'alice_role'");
        assertQuery(session, "SELECT * FROM mock.information_schema.applicable_roles", "SELECT 'alice', 'USER', 'alice_role', 'NO'");
    }

    @Test
    public void testEmptyRoles()
    {
        reset();

        assertQueryReturnsEmptyResult("SHOW ROLES");
        assertQueryReturnsEmptyResult("SHOW ROLE GRANTS");
        assertQueryReturnsEmptyResult("SHOW CURRENT ROLES");
        assertQueryReturnsEmptyResult("SELECT * FROM information_schema.applicable_roles");
    }

    @Test
    public void testSetViewAuthorizationWithSecurityDefiner()
    {
        reset();

        assertQuerySucceeds("ALTER VIEW mock.default.test_view_definer SET AUTHORIZATION some_other_user");
    }

    @Test
    public void testSetViewAuthorizationWithSecurityInvoker()
    {
        reset();

        assertQuerySucceeds("ALTER VIEW mock.default.test_view_invoker SET AUTHORIZATION some_other_user");
    }

    @Test
    public void testSystemMetadataAnalyzePropertiesFilteringValues()
    {
        reset();

        getQueryRunner().getAccessControl().denyCatalogs(catalog -> !catalog.equals("mock"));
        assertQueryReturnsEmptyResult("SELECT * FROM system.metadata.analyze_properties");
    }

    @Test
    public void testSystemMetadataMaterializedViewPropertiesFilteringValues()
    {
        reset();

        getQueryRunner().getAccessControl().denyCatalogs(catalog -> !catalog.equals("mock"));
        assertQueryReturnsEmptyResult("SELECT * FROM system.metadata.materialized_view_properties");
    }

    @Test
    public void testSystemMetadataSchemaPropertiesFilteringValues()
    {
        reset();

        getQueryRunner().getAccessControl().denyCatalogs(catalog -> !catalog.equals("mock"));
        assertQueryReturnsEmptyResult("SELECT * FROM system.metadata.schema_properties");
    }

    @Test
    public void testSystemMetadataTablePropertiesFilteringValues()
    {
        reset();

        getQueryRunner().getAccessControl().denyCatalogs(catalog -> !catalog.equals("blackhole") && !catalog.equals("mock"));
        assertQueryReturnsEmptyResult("SELECT * FROM system.metadata.table_properties");
    }

    @Test
    public void testSystemMetadataColumnPropertiesFilteringValues()
    {
        reset();

        getQueryRunner().getAccessControl().denyCatalogs(catalog -> !catalog.equals("mock"));
        assertQueryReturnsEmptyResult("SELECT * FROM system.metadata.column_properties");
    }

    @Test
    public void testUseStatementAccessControl()
    {
        reset();

        Session session = testSessionBuilder()
                .setCatalog(Optional.empty())
                .setSchema(Optional.empty())
                .build();
        getQueryRunner().execute(session, "USE tpch.tiny");
        assertThatThrownBy(() -> getQueryRunner().execute("USE not_exists_catalog.tiny"))
                .hasMessageMatching("Catalog 'not_exists_catalog' not found");
        assertThatThrownBy(() -> getQueryRunner().execute("USE tpch.not_exists_schema"))
                .hasMessageMatching("Schema does not exist: tpch.not_exists_schema");
    }

    @Test
    public void testUseStatementAccessControlWithDeniedCatalog()
    {
        reset();

        getQueryRunner().getAccessControl().denyCatalogs(catalog -> !catalog.equals("tpch"));
        assertThatThrownBy(() -> getQueryRunner().execute("USE tpch.tiny"))
                .hasMessageMatching("Access Denied: Cannot access catalog tpch");
        assertThatThrownBy(() -> getQueryRunner().execute("USE tpch.not_exists_schema"))
                .hasMessageMatching("Access Denied: Cannot access catalog tpch");
    }

    @Test
    public void testUseStatementAccessControlWithDeniedSchema()
    {
        reset();

        getQueryRunner().getAccessControl().denySchemas(schema -> !schema.equals("tiny"));
        assertThatThrownBy(() -> getQueryRunner().execute("USE tpch.tiny"))
                .hasMessageMatching("Access Denied: Cannot access schema: tpch.tiny");
    }

    @Test
    public void testPropertiesAccessControl()
    {
        reset();

        systemAccessControl.set(new DenySetPropertiesSystemAccessControl());
        assertAccessDenied(
                "CREATE TABLE mock.default.new_table (pk bigint) WITH (double_table_property = 0.0)", // default value
                "Cannot access properties: \\[double_table_property]");
        assertAccessDenied(
                "CREATE TABLE mock.default.new_table (pk bigint) WITH (double_table_property = 1.0)",
                "Cannot access properties: \\[double_table_property]");
        assertAccessDenied(
                "CREATE TABLE mock.default.new_table (pk bigint) WITH (double_table_property = 1.0, another_property = 1)",
                "Cannot access properties: \\[another_property, double_table_property]");
        assertAccessDenied(
                "CREATE TABLE mock.default.new_table (pk bigint) WITH (DOUBLE_TABLE_PROPERTY = 1.0)",
                "Cannot access properties: \\[double_table_property]");
        assertAccessDenied(
                "CREATE TABLE mock.default.new_table WITH (double_table_property = 0.0) AS SELECT 1 pk", // default value
                "Cannot access properties: \\[double_table_property]");
        assertAccessDenied(
                "CREATE TABLE mock.default.new_table WITH (double_table_property = 1.0) AS SELECT 1 pk",
                "Cannot access properties: \\[double_table_property]");
        assertAccessDenied(
                "CREATE TABLE mock.default.new_table WITH (double_table_property = 1.0, another_property = 1) AS SELECT 1 pk",
                "Cannot access properties: \\[another_property, double_table_property]");
        assertAccessDenied(
                "CREATE TABLE mock.default.new_table WITH (DOUBLE_TABLE_PROPERTY = 1.0) AS SELECT 1 pk",
                "Cannot access properties: \\[double_table_property]");
        assertAccessDenied(
                "CREATE SCHEMA mock.new_schema WITH (boolean_schema_property = false)", // default value
                "Cannot access properties: \\[boolean_schema_property]");
        assertAccessDenied(
                "CREATE SCHEMA mock.new_schema WITH (boolean_schema_property = true)",
                "Cannot access properties: \\[boolean_schema_property]");
        assertAccessDenied(
                "CREATE SCHEMA mock.new_schema WITH (another_property = 1, boolean_schema_property = true)",
                "Cannot access properties: \\[another_property, boolean_schema_property]");
        assertAccessDenied(
                "CREATE SCHEMA mock.new_schema WITH (BOOLEAN_SCHEMA_PROPERTY = true)",
                "Cannot access properties: \\[boolean_schema_property]");
        assertAccessDenied(
                "CREATE MATERIALIZED VIEW mock.default.new_materialized_view WITH (string_materialized_view_property = '') AS SELECT 1 a", // default value
                "Cannot access properties: \\[string_materialized_view_property]");
        assertAccessDenied(
                "CREATE MATERIALIZED VIEW mock.default.new_materialized_view WITH (string_materialized_view_property = 'value') AS SELECT 1 a",
                "Cannot access properties: \\[string_materialized_view_property]");
        assertAccessDenied(
                "CREATE MATERIALIZED VIEW mock.default.new_materialized_view WITH (string_materialized_view_property = 'value', another_property = 1) AS SELECT 1 a",
                "Cannot access properties: \\[another_property, string_materialized_view_property]");
        assertAccessDenied(
                "CREATE MATERIALIZED VIEW mock.default.new_materialized_view WITH (STRING_MATERIALIZED_VIEW_PROPERTY = 'value') AS SELECT 1 a",
                "Cannot access properties: \\[string_materialized_view_property]");
        assertAccessDenied(
                "ALTER TABLE mock.default.foo SET PROPERTIES double_table_property = 0.0", // default value
                "Cannot access properties: \\[double_table_property]");
        assertAccessDenied(
                "ALTER TABLE mock.default.foo SET PROPERTIES double_table_property = 1.0, another_property = 1",
                "Cannot access properties: \\[another_property, double_table_property]");
        assertAccessDenied(
                "ALTER TABLE mock.default.foo SET PROPERTIES double_table_property = 1.0",
                "Cannot access properties: \\[double_table_property]");
        assertAccessDenied(
                "ALTER TABLE mock.default.foo SET PROPERTIES DOUBLE_TABLE_PROPERTY = 1.0",
                "Cannot access properties: \\[double_table_property]");
        assertAccessDenied(
                "ALTER TABLE mock.default.foo SET PROPERTIES double_table_property = DEFAULT",
                "Cannot access properties: \\[double_table_property]");
        assertAccessDenied(
                "ALTER MATERIALIZED VIEW mock.default.test_materialized_view SET PROPERTIES string_materialized_view_property = ''", // default value
                "Cannot access properties: \\[string_materialized_view_property]");
        assertAccessDenied(
                "ALTER MATERIALIZED VIEW mock.default.test_materialized_view SET PROPERTIES string_materialized_view_property = 'value'",
                "Cannot access properties: \\[string_materialized_view_property]");
        assertAccessDenied(
                "ALTER MATERIALIZED VIEW mock.default.test_materialized_view SET PROPERTIES string_materialized_view_property = 'value', another_property = 1",
                "Cannot access properties: \\[another_property, string_materialized_view_property]");
        assertAccessDenied(
                "ALTER MATERIALIZED VIEW mock.default.test_materialized_view SET PROPERTIES STRING_MATERIALIZED_VIEW_PROPERTY = 'value'",
                "Cannot access properties: \\[string_materialized_view_property]");
        assertAccessDenied(
                "ALTER MATERIALIZED VIEW mock.default.test_materialized_view SET PROPERTIES string_materialized_view_property = DEFAULT",
                "Cannot access properties: \\[string_materialized_view_property]");
    }

    @Test
    public void testPropertiesAccessControlIsSkippedWhenUsingDefaults()
    {
        reset();

        systemAccessControl.set(new DenySetPropertiesSystemAccessControl());
        systemAccessControl.set(new DenySetPropertiesSystemAccessControl());
        assertAccessAllowed("CREATE TABLE mock.default.new_table (pk bigint)");
        assertAccessAllowed("CREATE SCHEMA mock.new_schema");
        assertAccessAllowed("CREATE MATERIALIZED VIEW mock.default.new_materialized_view AS SELECT 1 a");
    }

    @Test
    public void testAccessControlWithGroupsAndColumnMask()
    {
        reset();

        groupProvider.setUserGroups(ImmutableMap.of(getSession().getUser(), ImmutableSet.of("group")));
        TestingAccessControlManager accessControlManager = getQueryRunner().getAccessControl();
        accessControlManager.denyIdentityTable((identity, table) -> (identity.getGroups().contains("group") && "orders".equals(table)));
        accessControlManager.columnMask(
                new QualifiedObjectName("blackhole", "default", "orders"),
                "comment",
                getSession().getUser(),
                ViewExpression.builder().expression("substr(comment,1,3)").build());

        assertAccessAllowed("SELECT comment FROM orders");
    }

    @Test
    public void testAccessControlWithGroupsAndRowFilter()
    {
        reset();

        groupProvider.setUserGroups(ImmutableMap.of(getSession().getUser(), ImmutableSet.of("group")));
        TestingAccessControlManager accessControlManager = getQueryRunner().getAccessControl();
        accessControlManager.denyIdentityTable((identity, table) -> (identity.getGroups().contains("group") && "nation".equals(table)));
        accessControlManager.rowFilter(
                new QualifiedObjectName("blackhole", "default", "nation"),
                getSession().getUser(),
                ViewExpression.builder().expression("nationkey % 2 = 0").build());

        assertAccessAllowed("SELECT nationkey FROM nation");
    }

    @Test
    public void testAccessControlWithRolesAndColumnMask()
    {
        reset();

        String role = "role";
        String user = "user";
        Session session = Session.builder(getSession())
                .setIdentity(Identity.forUser(user)
                        .withEnabledRoles(ImmutableSet.of(role))
                        .build())
                .build();
        systemSecurityMetadata.grantRoles(getSession(), Set.of(role), Set.of(new TrinoPrincipal(USER, user)), false, Optional.empty());
        TestingAccessControlManager accessControlManager = getQueryRunner().getAccessControl();
        accessControlManager.denyIdentityTable((identity, table) -> (identity.getEnabledRoles().contains(role) && "orders".equals(table)));
        accessControlManager.columnMask(
                new QualifiedObjectName("blackhole", "default", "orders"),
                "comment",
                getSession().getUser(),
                ViewExpression.builder().expression("substr(comment,1,3)").build());

        assertAccessAllowed(session, "SELECT comment FROM orders");
    }

    @Test
    public void testAccessControlWithRolesAndRowFilter()
    {
        reset();

        String role = "role";
        String user = "user";
        Session session = Session.builder(getSession())
                .setIdentity(Identity.forUser(user)
                        .withEnabledRoles(ImmutableSet.of(role))
                        .build())
                .build();
        systemSecurityMetadata.grantRoles(getSession(), Set.of(role), Set.of(new TrinoPrincipal(USER, user)), false, Optional.empty());
        TestingAccessControlManager accessControlManager = getQueryRunner().getAccessControl();
        accessControlManager.denyIdentityTable((identity, table) -> (identity.getEnabledRoles().contains(role) && "nation".equals(table)));
        accessControlManager.rowFilter(
                new QualifiedObjectName("blackhole", "default", "nation"),
                getSession().getUser(),
                ViewExpression.builder().expression("nationkey % 2 = 0").build());

        assertAccessAllowed(session, "SELECT nationkey FROM nation");
    }

    private static final class DenySetPropertiesSystemAccessControl
            extends AllowAllSystemAccessControl
    {
        @Override
        public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema, Map<String, Object> properties)
        {
            checkProperties(properties);
        }

        @Override
        public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Object> properties)
        {
            checkProperties(properties);
        }

        @Override
        public void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Object> properties)
        {
            checkProperties(properties);
        }

        @Override
        public void checkCanSetTableProperties(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Optional<Object>> properties)
        {
            checkProperties(properties);
        }

        @Override
        public void checkCanSetMaterializedViewProperties(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Optional<Object>> properties)
        {
            checkProperties(properties);
        }

        private static void checkProperties(Map<?, ?> properties)
        {
            if (!properties.isEmpty()) {
                List<String> keys = properties.keySet()
                        .stream()
                        .map(Object::toString)
                        .sorted().collect(toList());
                throw new AccessDeniedException("Cannot access properties: " + keys);
            }
        }
    }
}
