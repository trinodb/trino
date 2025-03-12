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
package io.trino.testing;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.client.NodeVersion;
import io.trino.eventlistener.EventListenerManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.security.SecurityContext;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.ViewExpression;
import io.trino.transaction.TransactionManager;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.security.AccessDeniedException.denyAddColumn;
import static io.trino.spi.security.AccessDeniedException.denyAlterColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentTable;
import static io.trino.spi.security.AccessDeniedException.denyCommentView;
import static io.trino.spi.security.AccessDeniedException.denyCreateMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyCreateView;
import static io.trino.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.trino.spi.security.AccessDeniedException.denyDeleteTable;
import static io.trino.spi.security.AccessDeniedException.denyDropColumn;
import static io.trino.spi.security.AccessDeniedException.denyDropMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyDropSchema;
import static io.trino.spi.security.AccessDeniedException.denyDropTable;
import static io.trino.spi.security.AccessDeniedException.denyDropView;
import static io.trino.spi.security.AccessDeniedException.denyExecuteQuery;
import static io.trino.spi.security.AccessDeniedException.denyExecuteTableProcedure;
import static io.trino.spi.security.AccessDeniedException.denyImpersonateUser;
import static io.trino.spi.security.AccessDeniedException.denyInsertTable;
import static io.trino.spi.security.AccessDeniedException.denyKillQuery;
import static io.trino.spi.security.AccessDeniedException.denyRefreshMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameColumn;
import static io.trino.spi.security.AccessDeniedException.denyRenameMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameSchema;
import static io.trino.spi.security.AccessDeniedException.denyRenameTable;
import static io.trino.spi.security.AccessDeniedException.denyRenameView;
import static io.trino.spi.security.AccessDeniedException.denySelectColumns;
import static io.trino.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static io.trino.spi.security.AccessDeniedException.denySetMaterializedViewProperties;
import static io.trino.spi.security.AccessDeniedException.denySetSystemSessionProperty;
import static io.trino.spi.security.AccessDeniedException.denySetTableProperties;
import static io.trino.spi.security.AccessDeniedException.denySetUser;
import static io.trino.spi.security.AccessDeniedException.denyShowColumns;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyTruncateTable;
import static io.trino.spi.security.AccessDeniedException.denyUpdateTableColumns;
import static io.trino.spi.security.AccessDeniedException.denyViewQuery;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.ADD_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.ALTER_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.COMMENT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.COMMENT_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.COMMENT_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_MATERIALIZED_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_SCHEMA;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW_WITH_SELECT_COLUMNS;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_MATERIALIZED_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_SCHEMA;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.EXECUTE_FUNCTION;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.EXECUTE_QUERY;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.EXECUTE_TABLE_PROCEDURE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.GRANT_EXECUTE_FUNCTION;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.IMPERSONATE_USER;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.KILL_QUERY;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.REFRESH_MATERIALIZED_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_MATERIALIZED_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_SCHEMA;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SET_MATERIALIZED_VIEW_PROPERTIES;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SET_SESSION;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SET_TABLE_PROPERTIES;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SET_USER;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SHOW_COLUMNS;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SHOW_CREATE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.TRUNCATE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.UPDATE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.VIEW_QUERY;
import static java.util.Objects.requireNonNull;

public class TestingAccessControlManager
        extends AccessControlManager
{
    private static final BiPredicate<Identity, String> IDENTITY_TABLE_TRUE = (identity, table) -> true;
    private static final BiPredicate<Identity, String> IDENTITY_FUNCTION_TRUE = (identity, function) -> true;

    private final Set<TestingPrivilege> denyPrivileges = new HashSet<>();
    private final Map<RowFilterKey, List<ViewExpression>> rowFilters = new HashMap<>();
    private final Map<ColumnMaskKey, ViewExpression> columnMasks = new HashMap<>();
    private Predicate<String> deniedCatalogs = s -> true;
    private Predicate<String> deniedSchemas = s -> true;
    private Predicate<SchemaTableName> deniedTables = s -> true;
    private BiPredicate<Identity, String> denyIdentityTable = IDENTITY_TABLE_TRUE;
    private BiPredicate<Identity, String> denyIdentityFunction = IDENTITY_FUNCTION_TRUE;

    @Inject
    public TestingAccessControlManager(
            TransactionManager transactionManager,
            EventListenerManager eventListenerManager,
            AccessControlConfig accessControlConfig,
            SecretsResolver secretsResolver,
            OpenTelemetry openTelemetry)
    {
        super(NodeVersion.UNKNOWN, transactionManager, eventListenerManager, accessControlConfig, openTelemetry, secretsResolver, DefaultSystemAccessControl.NAME);
    }

    public TestingAccessControlManager(TransactionManager transactionManager, EventListenerManager eventListenerManager, SecretsResolver secretsResolver)
    {
        this(transactionManager, eventListenerManager, new AccessControlConfig(), secretsResolver, OpenTelemetry.noop());
    }

    public static TestingPrivilege privilege(String entityName, TestingPrivilegeType type)
    {
        return new TestingPrivilege(Optional.empty(), entityName, type);
    }

    public static TestingPrivilege privilege(String actorName, String entityName, TestingPrivilegeType type)
    {
        return new TestingPrivilege(Optional.of(actorName), entityName, type);
    }

    public void deny(TestingPrivilege... deniedPrivileges)
    {
        Collections.addAll(this.denyPrivileges, deniedPrivileges);
    }

    public void rowFilter(QualifiedObjectName table, String identity, ViewExpression filter)
    {
        rowFilters.computeIfAbsent(new RowFilterKey(identity, table), key -> new ArrayList<>())
                .add(filter);
    }

    public void columnMask(QualifiedObjectName table, String column, String identity, ViewExpression mask)
    {
        columnMasks.put(new ColumnMaskKey(identity, table, column), mask);
    }

    public void reset()
    {
        denyPrivileges.clear();
        deniedCatalogs = s -> true;
        deniedSchemas = s -> true;
        deniedTables = s -> true;
        denyIdentityTable = IDENTITY_TABLE_TRUE;
        rowFilters.clear();
        columnMasks.clear();
    }

    public void denyCatalogs(Predicate<String> deniedCatalogs)
    {
        this.deniedCatalogs = this.deniedCatalogs.and(deniedCatalogs);
    }

    public void denySchemas(Predicate<String> deniedSchemas)
    {
        this.deniedSchemas = this.deniedSchemas.and(deniedSchemas);
    }

    public void denyTables(Predicate<SchemaTableName> deniedTables)
    {
        this.deniedTables = this.deniedTables.and(deniedTables);
    }

    public void denyIdentityTable(BiPredicate<Identity, String> denyIdentityTable)
    {
        this.denyIdentityTable = requireNonNull(denyIdentityTable, "denyIdentityTable is null");
    }

    public void denyIdentityFunction(BiPredicate<Identity, String> denyIdentityFunction)
    {
        this.denyIdentityFunction = requireNonNull(denyIdentityFunction, "denyIdentityFunction is null");
    }

    @Override
    public Set<String> filterCatalogs(SecurityContext securityContext, Set<String> catalogs)
    {
        return super.filterCatalogs(
                securityContext,
                catalogs.stream()
                        .filter(this.deniedCatalogs)
                        .collect(toImmutableSet()));
    }

    @Override
    public Set<String> filterSchemas(SecurityContext securityContext, String catalogName, Set<String> schemaNames)
    {
        return super.filterSchemas(
                securityContext,
                catalogName,
                schemaNames.stream()
                        .filter(this.deniedSchemas)
                        .collect(toImmutableSet()));
    }

    @Override
    public Set<SchemaTableName> filterTables(SecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return super.filterTables(
                context,
                catalogName,
                tableNames.stream()
                        .filter(this.deniedTables)
                        .collect(toImmutableSet()));
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        if (shouldDenyPrivilege(userName, userName, IMPERSONATE_USER)) {
            denyImpersonateUser(identity.getUser(), userName);
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanImpersonateUser(identity, userName);
        }
    }

    @Override
    @Deprecated
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        if (shouldDenyPrivilege(principal.map(Principal::getName), userName, SET_USER)) {
            denySetUser(principal, userName);
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSetUser(principal, userName);
        }
    }

    @Override
    public void checkCanExecuteQuery(Identity identity, QueryId queryId)
    {
        if (shouldDenyPrivilege(identity.getUser(), "query", EXECUTE_QUERY)) {
            denyExecuteQuery();
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanExecuteQuery(identity, queryId);
        }
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        if (shouldDenyPrivilege(identity.getUser(), "query", VIEW_QUERY)) {
            denyViewQuery();
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanViewQueryOwnedBy(identity, queryOwner);
        }
    }

    @Override
    public Collection<Identity> filterQueriesOwnedBy(Identity identity, Collection<Identity> owners)
    {
        if (shouldDenyPrivilege(identity.getUser(), "query", VIEW_QUERY)) {
            return ImmutableSet.of();
        }
        if (denyPrivileges.isEmpty()) {
            return super.filterQueriesOwnedBy(identity, owners);
        }
        return owners;
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        if (shouldDenyPrivilege(identity.getUser(), "query", KILL_QUERY)) {
            denyKillQuery();
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanKillQueryOwnedBy(identity, queryOwner);
        }
    }

    @Override
    public void checkCanCreateSchema(SecurityContext context, CatalogSchemaName schemaName, Map<String, Object> properties)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), schemaName.getSchemaName(), CREATE_SCHEMA)) {
            denyCreateSchema(schemaName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanCreateSchema(context, schemaName, properties);
        }
    }

    @Override
    public void checkCanDropSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), schemaName.getSchemaName(), DROP_SCHEMA)) {
            denyDropSchema(schemaName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanDropSchema(context, schemaName);
        }
    }

    @Override
    public void checkCanRenameSchema(SecurityContext context, CatalogSchemaName schemaName, String newSchemaName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), schemaName.getSchemaName(), RENAME_SCHEMA)) {
            denyRenameSchema(schemaName.toString(), newSchemaName);
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanRenameSchema(context, schemaName, newSchemaName);
        }
    }

    @Override
    public void checkCanShowCreateTable(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), SHOW_CREATE_TABLE)) {
            denyShowCreateTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanShowCreateTable(context, tableName);
        }
    }

    @Override
    public void checkCanCreateTable(SecurityContext context, QualifiedObjectName tableName, Map<String, Object> properties)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), CREATE_TABLE)) {
            denyCreateTable(tableName.toString());
        }
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), SET_TABLE_PROPERTIES)) {
            denySetTableProperties(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanCreateTable(context, tableName, properties);
        }
    }

    @Override
    public void checkCanDropTable(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), DROP_TABLE)) {
            denyDropTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanDropTable(context, tableName);
        }
    }

    @Override
    public void checkCanRenameTable(SecurityContext context, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), RENAME_TABLE)) {
            denyRenameTable(tableName.toString(), newTableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanRenameTable(context, tableName, newTableName);
        }
    }

    @Override
    public void checkCanSetTableProperties(SecurityContext context, QualifiedObjectName tableName, Map<String, Optional<Object>> properties)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), SET_TABLE_PROPERTIES)) {
            denySetTableProperties(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSetTableProperties(context, tableName, properties);
        }
    }

    @Override
    public void checkCanSetTableComment(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), COMMENT_TABLE)) {
            denyCommentTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSetTableComment(context, tableName);
        }
    }

    @Override
    public void checkCanSetViewComment(SecurityContext context, QualifiedObjectName viewName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), viewName.objectName(), COMMENT_VIEW)) {
            denyCommentView(viewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSetViewComment(context, viewName);
        }
    }

    @Override
    public void checkCanSetColumnComment(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), COMMENT_COLUMN)) {
            denyCommentColumn(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSetColumnComment(context, tableName);
        }
    }

    @Override
    public void checkCanAddColumns(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), ADD_COLUMN)) {
            denyAddColumn(tableName.toString());
        }
        super.checkCanAddColumns(context, tableName);
    }

    @Override
    public void checkCanDropColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), DROP_COLUMN)) {
            denyDropColumn(tableName.toString());
        }
        super.checkCanDropColumn(context, tableName);
    }

    @Override
    public void checkCanRenameColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), RENAME_COLUMN)) {
            denyRenameColumn(tableName.toString());
        }
        super.checkCanRenameColumn(context, tableName);
    }

    @Override
    public void checkCanAlterColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), ALTER_COLUMN)) {
            denyAlterColumn(tableName.toString());
        }
        super.checkCanAlterColumn(context, tableName);
    }

    @Override
    public void checkCanInsertIntoTable(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), INSERT_TABLE)) {
            denyInsertTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanInsertIntoTable(context, tableName);
        }
    }

    @Override
    public void checkCanDeleteFromTable(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), DELETE_TABLE)) {
            denyDeleteTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanDeleteFromTable(context, tableName);
        }
    }

    @Override
    public void checkCanTruncateTable(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), TRUNCATE_TABLE)) {
            denyTruncateTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanTruncateTable(context, tableName);
        }
    }

    @Override
    public void checkCanUpdateTableColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> updatedColumnNames)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), UPDATE_TABLE)) {
            denyUpdateTableColumns(tableName.toString(), updatedColumnNames);
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanUpdateTableColumns(context, tableName, updatedColumnNames);
        }
    }

    @Override
    public void checkCanCreateView(SecurityContext context, QualifiedObjectName viewName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), viewName.objectName(), CREATE_VIEW)) {
            denyCreateView(viewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanCreateView(context, viewName);
        }
    }

    @Override
    public void checkCanRenameView(SecurityContext context, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), viewName.objectName(), RENAME_VIEW)) {
            denyRenameView(viewName.toString(), newViewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanRenameView(context, viewName, newViewName);
        }
    }

    @Override
    public void checkCanDropView(SecurityContext context, QualifiedObjectName viewName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), viewName.objectName(), DROP_VIEW)) {
            denyDropView(viewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanDropView(context, viewName);
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, QueryId queryId, String propertyName)
    {
        if (shouldDenyPrivilege(identity.getUser(), propertyName, SET_SESSION)) {
            denySetSystemSessionProperty(propertyName);
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSetSystemSessionProperty(identity, queryId, propertyName);
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        if (!denyIdentityTable.test(context.getIdentity(), tableName.objectName())) {
            denyCreateViewWithSelect(tableName.toString(), context.getIdentity());
        }
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), CREATE_VIEW_WITH_SELECT_COLUMNS)) {
            denyCreateViewWithSelect(tableName.toString(), context.getIdentity());
        }
        if (denyPrivileges.isEmpty() && denyIdentityTable.equals(IDENTITY_TABLE_TRUE)) {
            super.checkCanCreateViewWithSelectFromColumns(context, tableName, columnNames);
        }
    }

    @Override
    public void checkCanCreateMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName, Map<String, Object> properties)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), materializedViewName.objectName(), CREATE_MATERIALIZED_VIEW)) {
            denyCreateMaterializedView(materializedViewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanCreateMaterializedView(context, materializedViewName, properties);
        }
    }

    @Override
    public void checkCanRefreshMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), materializedViewName.objectName(), REFRESH_MATERIALIZED_VIEW)) {
            denyRefreshMaterializedView(materializedViewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanRefreshMaterializedView(context, materializedViewName);
        }
    }

    @Override
    public void checkCanDropMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), materializedViewName.objectName(), DROP_MATERIALIZED_VIEW)) {
            denyDropMaterializedView(materializedViewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanDropMaterializedView(context, materializedViewName);
        }
    }

    @Override
    public void checkCanRenameMaterializedView(SecurityContext context, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), viewName.objectName(), RENAME_MATERIALIZED_VIEW)) {
            denyRenameMaterializedView(viewName.toString(), newViewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanRenameMaterializedView(context, viewName, newViewName);
        }
    }

    @Override
    public void checkCanSetMaterializedViewProperties(SecurityContext context, QualifiedObjectName materializedViewName, Map<String, Optional<Object>> properties)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), materializedViewName.objectName(), SET_MATERIALIZED_VIEW_PROPERTIES)) {
            denySetMaterializedViewProperties(materializedViewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSetMaterializedViewProperties(context, materializedViewName, properties);
        }
    }

    @Override
    public void checkCanShowColumns(SecurityContext context, CatalogSchemaTableName table)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), table.getSchemaTableName().getTableName(), SHOW_COLUMNS)) {
            denyShowColumns(table.getSchemaTableName().toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanShowColumns(context, table);
        }
    }

    @Override
    public Map<SchemaTableName, Set<String>> filterColumns(SecurityContext context, String catalogName, Map<SchemaTableName, Set<String>> tableColumns)
    {
        tableColumns = tableColumns.entrySet().stream()
                .collect(toImmutableMap(
                        Entry::getKey,
                        e -> localFilterColumns(context, e.getKey(), e.getValue())));
        return super.filterColumns(context, catalogName, tableColumns);
    }

    private Set<String> localFilterColumns(SecurityContext context, SchemaTableName table, Set<String> columns)
    {
        ImmutableSet.Builder<String> visibleColumns = ImmutableSet.builder();
        for (String column : columns) {
            if (!shouldDenyPrivilege(context.getIdentity().getUser(), table.getTableName() + "." + column, SELECT_COLUMN)) {
                visibleColumns.add(column);
            }
        }
        return visibleColumns.build();
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SecurityContext context, String catalogName, String propertyName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), catalogName + "." + propertyName, SET_SESSION)) {
            denySetCatalogSessionProperty(catalogName, propertyName);
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSetCatalogSessionProperty(context, catalogName, propertyName);
        }
    }

    @Override
    public void checkCanSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columns)
    {
        if (!denyIdentityTable.test(context.getIdentity(), tableName.objectName())) {
            denySelectColumns(tableName.toString(), columns);
        }
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName(), SELECT_COLUMN)) {
            denySelectColumns(tableName.toString(), columns);
        }
        for (String column : columns) {
            if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.objectName() + "." + column, SELECT_COLUMN)) {
                denySelectColumns(tableName.toString(), columns);
            }
        }
        if (denyPrivileges.isEmpty() && denyIdentityTable.equals(IDENTITY_TABLE_TRUE)) {
            super.checkCanSelectFromColumns(context, tableName, columns);
        }
    }

    @Override
    public boolean canExecuteFunction(SecurityContext context, QualifiedObjectName functionName)
    {
        if (!denyIdentityFunction.test(context.getIdentity(), functionName.asSchemaFunctionName().toString())) {
            return false;
        }
        if (shouldDenyPrivilege(context.getIdentity().getUser(), functionName.toString(), EXECUTE_FUNCTION)) {
            return false;
        }
        if (denyPrivileges.isEmpty()) {
            return super.canExecuteFunction(context, functionName);
        }
        return true;
    }

    @Override
    public boolean canCreateViewWithExecuteFunction(SecurityContext context, QualifiedObjectName functionName)
    {
        if (!denyIdentityFunction.test(context.getIdentity(), functionName.asSchemaFunctionName().toString())) {
            return false;
        }
        if (shouldDenyPrivilege(context.getIdentity().getUser(), functionName.toString(), GRANT_EXECUTE_FUNCTION)) {
            return false;
        }
        if (denyPrivileges.isEmpty()) {
            return super.canCreateViewWithExecuteFunction(context, functionName);
        }
        return true;
    }

    @Override
    public void checkCanExecuteTableProcedure(SecurityContext context, QualifiedObjectName table, String procedure)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), table + "." + procedure, EXECUTE_TABLE_PROCEDURE)) {
            denyExecuteTableProcedure(table.toString(), procedure);
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanExecuteTableProcedure(context, table, procedure);
        }
    }

    @Override
    public List<ViewExpression> getRowFilters(SecurityContext context, QualifiedObjectName tableName)
    {
        List<ViewExpression> viewExpressions = rowFilters.get(new RowFilterKey(context.getIdentity().getUser(), tableName));
        if (viewExpressions != null) {
            return viewExpressions;
        }
        return super.getRowFilters(context, tableName);
    }

    @Override
    public Map<ColumnSchema, ViewExpression> getColumnMasks(SecurityContext context, QualifiedObjectName tableName, List<ColumnSchema> columns)
    {
        Map<ColumnSchema, ViewExpression> superResult = super.getColumnMasks(context, tableName, columns);
        return columns.stream()
                .flatMap(column ->
                    Optional.ofNullable(columnMasks.get(new ColumnMaskKey(context.getIdentity().getUser(), tableName, column.getName())))
                            .or(() -> Optional.ofNullable(superResult.get(column)))
                            .map(viewExpression -> Map.entry(column, viewExpression))
                            .stream())
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private boolean shouldDenyPrivilege(String actorName, String entityName, TestingPrivilegeType verb)
    {
        return shouldDenyPrivilege(Optional.of(actorName), entityName, verb);
    }

    private boolean shouldDenyPrivilege(Optional<String> actorName, String entityName, TestingPrivilegeType verb)
    {
        for (TestingPrivilege denyPrivilege : denyPrivileges) {
            if (denyPrivilege.matches(actorName, entityName, verb)) {
                return true;
            }
        }
        return false;
    }

    public enum TestingPrivilegeType
    {
        SET_USER, IMPERSONATE_USER,
        EXECUTE_QUERY, VIEW_QUERY, KILL_QUERY,
        EXECUTE_FUNCTION, EXECUTE_TABLE_PROCEDURE,
        CREATE_SCHEMA, DROP_SCHEMA, RENAME_SCHEMA,
        SHOW_CREATE_TABLE, CREATE_TABLE, DROP_TABLE, RENAME_TABLE, COMMENT_TABLE, COMMENT_VIEW, COMMENT_COLUMN, INSERT_TABLE, DELETE_TABLE, MERGE_TABLE, UPDATE_TABLE, TRUNCATE_TABLE, SET_TABLE_PROPERTIES, SHOW_COLUMNS,
        ADD_COLUMN, DROP_COLUMN, RENAME_COLUMN, ALTER_COLUMN, SELECT_COLUMN,
        CREATE_VIEW, RENAME_VIEW, DROP_VIEW, CREATE_VIEW_WITH_SELECT_COLUMNS,
        CREATE_MATERIALIZED_VIEW, REFRESH_MATERIALIZED_VIEW, DROP_MATERIALIZED_VIEW, RENAME_MATERIALIZED_VIEW, SET_MATERIALIZED_VIEW_PROPERTIES,
        GRANT_EXECUTE_FUNCTION,
        SET_SESSION
    }

    public static class TestingPrivilege
    {
        private final Optional<String> actorName;
        private final Predicate<String> entityPredicate;
        private final TestingPrivilegeType type;

        public TestingPrivilege(Optional<String> actorName, String entityName, TestingPrivilegeType type)
        {
            this(actorName, entityName::equals, type);
        }

        public TestingPrivilege(Optional<String> actorName, Predicate<String> entityPredicate, TestingPrivilegeType type)
        {
            this.actorName = requireNonNull(actorName, "actorName is null");
            this.entityPredicate = requireNonNull(entityPredicate, "entityPredicate is null");
            this.type = requireNonNull(type, "type is null");
        }

        public boolean matches(Optional<String> actorName, String entityName, TestingPrivilegeType type)
        {
            return (this.actorName.isEmpty() || this.actorName.equals(actorName)) &&
                    this.type == type &&
                    this.entityPredicate.test(entityName);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestingPrivilege that = (TestingPrivilege) o;
            return Objects.equals(actorName, that.actorName) &&
                    Objects.equals(entityPredicate, that.entityPredicate) &&
                    type == that.type;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(actorName, entityPredicate, type);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("actorName", actorName)
                    .add("type", type)
                    .toString();
        }
    }

    private record RowFilterKey(String identity, QualifiedObjectName table)
    {
        private RowFilterKey
        {
            requireNonNull(identity, "identity is null");
            requireNonNull(table, "table is null");
        }
    }

    private record ColumnMaskKey(String identity, QualifiedObjectName table, String column)
    {
        private ColumnMaskKey
        {
            requireNonNull(identity, "identity is null");
            requireNonNull(table, "table is null");
            requireNonNull(column, "column is null");
        }
    }
}
