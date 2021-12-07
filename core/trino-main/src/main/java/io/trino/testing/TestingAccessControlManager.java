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
import io.trino.eventlistener.EventListenerManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.security.SecurityContext;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;
import io.trino.transaction.TransactionManager;

import javax.inject.Inject;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.security.AccessDeniedException.denyAddColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentTable;
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
import static io.trino.spi.security.AccessDeniedException.denyExecuteFunction;
import static io.trino.spi.security.AccessDeniedException.denyExecuteQuery;
import static io.trino.spi.security.AccessDeniedException.denyGrantExecuteFunctionPrivilege;
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
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.COMMENT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.COMMENT_TABLE;
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

    private final Set<TestingPrivilege> denyPrivileges = new HashSet<>();
    private final Map<RowFilterKey, List<ViewExpression>> rowFilters = new HashMap<>();
    private final Map<ColumnMaskKey, List<ViewExpression>> columnMasks = new HashMap<>();
    private Predicate<String> deniedCatalogs = s -> true;
    private Predicate<SchemaTableName> deniedTables = s -> true;
    private BiPredicate<Identity, String> denyIdentityTable = IDENTITY_TABLE_TRUE;

    @Inject
    public TestingAccessControlManager(TransactionManager transactionManager, EventListenerManager eventListenerManager, AccessControlConfig accessControlConfig)
    {
        super(transactionManager, eventListenerManager, accessControlConfig, DefaultSystemAccessControl.NAME);
    }

    public TestingAccessControlManager(TransactionManager transactionManager, EventListenerManager eventListenerManager)
    {
        this(transactionManager, eventListenerManager, new AccessControlConfig());
    }

    public void loadSystemAccessControl(String name, Map<String, String> properties)
    {
        setSystemAccessControl(name, properties);
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
        columnMasks.computeIfAbsent(new ColumnMaskKey(identity, table, column), key -> new ArrayList<>())
                .add(mask);
    }

    public void reset()
    {
        denyPrivileges.clear();
        deniedCatalogs = s -> true;
        deniedTables = s -> true;
        denyIdentityTable = IDENTITY_TABLE_TRUE;
        rowFilters.clear();
        columnMasks.clear();
    }

    public void denyCatalogs(Predicate<String> deniedCatalogs)
    {
        this.deniedCatalogs = this.deniedCatalogs.and(deniedCatalogs);
    }

    public void denyTables(Predicate<SchemaTableName> deniedTables)
    {
        this.deniedTables = this.deniedTables.and(deniedTables);
    }

    public void denyIdentityTable(BiPredicate<Identity, String> denyIdentityTable)
    {
        this.denyIdentityTable = requireNonNull(denyIdentityTable, "denyIdentityTable is null");
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
    public void checkCanExecuteQuery(Identity identity)
    {
        if (shouldDenyPrivilege(identity.getUser(), "query", EXECUTE_QUERY)) {
            denyExecuteQuery();
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanExecuteQuery(identity);
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
    public void checkCanCreateSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), schemaName.getSchemaName(), CREATE_SCHEMA)) {
            denyCreateSchema(schemaName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanCreateSchema(context, schemaName);
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
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), SHOW_CREATE_TABLE)) {
            denyShowCreateTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanShowCreateTable(context, tableName);
        }
    }

    @Override
    public void checkCanCreateTable(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), CREATE_TABLE)) {
            denyCreateTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanCreateTable(context, tableName);
        }
    }

    @Override
    public void checkCanCreateTable(SecurityContext context, QualifiedObjectName tableName, Map<String, Object> properties)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), CREATE_TABLE)) {
            denyCreateTable(tableName.toString());
        }
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), SET_TABLE_PROPERTIES)) {
            denySetTableProperties(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanCreateTable(context, tableName, properties);
        }
    }

    @Override
    public void checkCanDropTable(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), DROP_TABLE)) {
            denyDropTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanDropTable(context, tableName);
        }
    }

    @Override
    public void checkCanRenameTable(SecurityContext context, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), RENAME_TABLE)) {
            denyRenameTable(tableName.toString(), newTableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanRenameTable(context, tableName, newTableName);
        }
    }

    @Override
    public void checkCanSetTableProperties(SecurityContext context, QualifiedObjectName tableName, Map<String, Object> nonNullProperties, Set<String> nullPropertyNames)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), SET_TABLE_PROPERTIES)) {
            denySetTableProperties(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSetTableProperties(context, tableName, nonNullProperties, nullPropertyNames);
        }
    }

    @Override
    public void checkCanSetTableComment(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), COMMENT_TABLE)) {
            denyCommentTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSetTableComment(context, tableName);
        }
    }

    @Override
    public void checkCanSetColumnComment(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), COMMENT_COLUMN)) {
            denyCommentColumn(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSetColumnComment(context, tableName);
        }
    }

    @Override
    public void checkCanAddColumns(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), ADD_COLUMN)) {
            denyAddColumn(tableName.toString());
        }
        super.checkCanAddColumns(context, tableName);
    }

    @Override
    public void checkCanDropColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), DROP_COLUMN)) {
            denyDropColumn(tableName.toString());
        }
        super.checkCanDropColumn(context, tableName);
    }

    @Override
    public void checkCanRenameColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), RENAME_COLUMN)) {
            denyRenameColumn(tableName.toString());
        }
        super.checkCanRenameColumn(context, tableName);
    }

    @Override
    public void checkCanInsertIntoTable(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), INSERT_TABLE)) {
            denyInsertTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanInsertIntoTable(context, tableName);
        }
    }

    @Override
    public void checkCanDeleteFromTable(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), DELETE_TABLE)) {
            denyDeleteTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanDeleteFromTable(context, tableName);
        }
    }

    @Override
    public void checkCanTruncateTable(SecurityContext context, QualifiedObjectName tableName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), TRUNCATE_TABLE)) {
            denyTruncateTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanTruncateTable(context, tableName);
        }
    }

    @Override
    public void checkCanUpdateTableColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> updatedColumnNames)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), UPDATE_TABLE)) {
            denyUpdateTableColumns(tableName.toString(), updatedColumnNames);
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanUpdateTableColumns(context, tableName, updatedColumnNames);
        }
    }

    @Override
    public void checkCanCreateView(SecurityContext context, QualifiedObjectName viewName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), viewName.getObjectName(), CREATE_VIEW)) {
            denyCreateView(viewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanCreateView(context, viewName);
        }
    }

    @Override
    public void checkCanRenameView(SecurityContext context, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), viewName.getObjectName(), RENAME_VIEW)) {
            denyRenameView(viewName.toString(), newViewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanRenameView(context, viewName, newViewName);
        }
    }

    @Override
    public void checkCanDropView(SecurityContext context, QualifiedObjectName viewName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), viewName.getObjectName(), DROP_VIEW)) {
            denyDropView(viewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanDropView(context, viewName);
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        if (shouldDenyPrivilege(identity.getUser(), propertyName, SET_SESSION)) {
            denySetSystemSessionProperty(propertyName);
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSetSystemSessionProperty(identity, propertyName);
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        if (!denyIdentityTable.test(context.getIdentity(), tableName.getObjectName())) {
            denyCreateViewWithSelect(tableName.toString(), context.getIdentity());
        }
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), CREATE_VIEW_WITH_SELECT_COLUMNS)) {
            denyCreateViewWithSelect(tableName.toString(), context.getIdentity());
        }
        if (denyPrivileges.isEmpty() && denyIdentityTable.equals(IDENTITY_TABLE_TRUE)) {
            super.checkCanCreateViewWithSelectFromColumns(context, tableName, columnNames);
        }
    }

    @Override
    public void checkCanCreateMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), materializedViewName.getObjectName(), CREATE_MATERIALIZED_VIEW)) {
            denyCreateMaterializedView(materializedViewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanCreateMaterializedView(context, materializedViewName);
        }
    }

    @Override
    public void checkCanCreateMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName, Map<String, Object> properties)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), materializedViewName.getObjectName(), CREATE_MATERIALIZED_VIEW)) {
            denyCreateMaterializedView(materializedViewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanCreateMaterializedView(context, materializedViewName, properties);
        }
    }

    @Override
    public void checkCanRefreshMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), materializedViewName.getObjectName(), REFRESH_MATERIALIZED_VIEW)) {
            denyRefreshMaterializedView(materializedViewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanRefreshMaterializedView(context, materializedViewName);
        }
    }

    @Override
    public void checkCanDropMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), materializedViewName.getObjectName(), DROP_MATERIALIZED_VIEW)) {
            denyDropMaterializedView(materializedViewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanDropMaterializedView(context, materializedViewName);
        }
    }

    @Override
    public void checkCanRenameMaterializedView(SecurityContext context, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), viewName.getObjectName(), RENAME_MATERIALIZED_VIEW)) {
            denyRenameMaterializedView(viewName.toString(), newViewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanRenameMaterializedView(context, viewName, newViewName);
        }
    }

    @Override
    public void checkCanSetMaterializedViewProperties(SecurityContext context, QualifiedObjectName materializedViewName, Map<String, Object> nonNullProperties, Set<String> nullPropertyNames)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), materializedViewName.getObjectName(), SET_MATERIALIZED_VIEW_PROPERTIES)) {
            denySetMaterializedViewProperties(materializedViewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSetMaterializedViewProperties(context, materializedViewName, nonNullProperties, nullPropertyNames);
        }
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SecurityContext context, String functionName, Identity grantee, boolean grantOption)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), functionName, GRANT_EXECUTE_FUNCTION)) {
            denyGrantExecuteFunctionPrivilege(functionName, context.getIdentity(), grantee);
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanGrantExecuteFunctionPrivilege(context, functionName, grantee, grantOption);
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
    public Set<String> filterColumns(SecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        ImmutableSet.Builder<String> visibleColumns = ImmutableSet.builder();
        for (String column : columns) {
            if (!shouldDenyPrivilege(context.getIdentity().getUser(), table.getSchemaTableName().getTableName() + "." + column, SELECT_COLUMN)) {
                visibleColumns.add(column);
            }
        }
        return super.filterColumns(context, table, visibleColumns.build());
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
        if (!denyIdentityTable.test(context.getIdentity(), tableName.getObjectName())) {
            denySelectColumns(tableName.toString(), columns);
        }
        if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName(), SELECT_COLUMN)) {
            denySelectColumns(tableName.toString(), columns);
        }
        for (String column : columns) {
            if (shouldDenyPrivilege(context.getIdentity().getUser(), tableName.getObjectName() + "." + column, SELECT_COLUMN)) {
                denySelectColumns(tableName.toString(), columns);
            }
        }
        if (denyPrivileges.isEmpty() && denyIdentityTable.equals(IDENTITY_TABLE_TRUE)) {
            super.checkCanSelectFromColumns(context, tableName, columns);
        }
    }

    @Override
    public void checkCanExecuteFunction(SecurityContext context, String functionName)
    {
        if (shouldDenyPrivilege(context.getIdentity().getUser(), functionName, EXECUTE_FUNCTION)) {
            denyExecuteFunction(functionName);
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanExecuteFunction(context, functionName);
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
    public List<ViewExpression> getColumnMasks(SecurityContext context, QualifiedObjectName tableName, String column, Type type)
    {
        List<ViewExpression> viewExpressions = columnMasks.get(new ColumnMaskKey(context.getIdentity().getUser(), tableName, column));
        if (viewExpressions != null) {
            return viewExpressions;
        }
        return super.getColumnMasks(context, tableName, column, type);
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
        EXECUTE_FUNCTION,
        CREATE_SCHEMA, DROP_SCHEMA, RENAME_SCHEMA,
        SHOW_CREATE_TABLE, CREATE_TABLE, DROP_TABLE, RENAME_TABLE, COMMENT_TABLE, COMMENT_COLUMN, INSERT_TABLE, DELETE_TABLE, UPDATE_TABLE, TRUNCATE_TABLE, SET_TABLE_PROPERTIES, SHOW_COLUMNS,
        ADD_COLUMN, DROP_COLUMN, RENAME_COLUMN, SELECT_COLUMN,
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
            this(actorName, requireNonNull(entityName, "entitName is null")::equals, type);
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
                    this.entityPredicate.test(entityName) &&
                    this.type == type;
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

    private static class RowFilterKey
    {
        private final String identity;
        private final QualifiedObjectName table;

        public RowFilterKey(String identity, QualifiedObjectName table)
        {
            this.identity = requireNonNull(identity, "identity is null");
            this.table = requireNonNull(table, "table is null");
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
            RowFilterKey that = (RowFilterKey) o;
            return identity.equals(that.identity) &&
                    table.equals(that.table);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(identity, table);
        }
    }

    private static class ColumnMaskKey
    {
        private final String identity;
        private final QualifiedObjectName table;
        private final String column;

        public ColumnMaskKey(String identity, QualifiedObjectName table, String column)
        {
            this.identity = identity;
            this.table = table;
            this.column = column;
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
            ColumnMaskKey that = (ColumnMaskKey) o;
            return identity.equals(that.identity) &&
                    table.equals(that.table) &&
                    column.equals(that.column);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(identity, table, column);
        }
    }
}
