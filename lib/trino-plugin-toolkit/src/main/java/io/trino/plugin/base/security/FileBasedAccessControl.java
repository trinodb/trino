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
package io.trino.plugin.base.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.security.TableAccessControlRule.TablePrivilege;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.SchemaRoutineName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.base.security.TableAccessControlRule.TablePrivilege.DELETE;
import static io.trino.plugin.base.security.TableAccessControlRule.TablePrivilege.GRANT_SELECT;
import static io.trino.plugin.base.security.TableAccessControlRule.TablePrivilege.INSERT;
import static io.trino.plugin.base.security.TableAccessControlRule.TablePrivilege.OWNERSHIP;
import static io.trino.plugin.base.security.TableAccessControlRule.TablePrivilege.SELECT;
import static io.trino.plugin.base.security.TableAccessControlRule.TablePrivilege.UPDATE;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_MASK;
import static io.trino.spi.security.AccessDeniedException.denyAddColumn;
import static io.trino.spi.security.AccessDeniedException.denyAlterColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentTable;
import static io.trino.spi.security.AccessDeniedException.denyCommentView;
import static io.trino.spi.security.AccessDeniedException.denyCreateFunction;
import static io.trino.spi.security.AccessDeniedException.denyCreateMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyCreateRole;
import static io.trino.spi.security.AccessDeniedException.denyCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyCreateView;
import static io.trino.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.trino.spi.security.AccessDeniedException.denyDeleteTable;
import static io.trino.spi.security.AccessDeniedException.denyDenySchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyDenyTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyDropColumn;
import static io.trino.spi.security.AccessDeniedException.denyDropFunction;
import static io.trino.spi.security.AccessDeniedException.denyDropMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyDropRole;
import static io.trino.spi.security.AccessDeniedException.denyDropSchema;
import static io.trino.spi.security.AccessDeniedException.denyDropTable;
import static io.trino.spi.security.AccessDeniedException.denyDropView;
import static io.trino.spi.security.AccessDeniedException.denyExecuteProcedure;
import static io.trino.spi.security.AccessDeniedException.denyGrantRoles;
import static io.trino.spi.security.AccessDeniedException.denyGrantSchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyInsertTable;
import static io.trino.spi.security.AccessDeniedException.denyRefreshMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameColumn;
import static io.trino.spi.security.AccessDeniedException.denyRenameMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameSchema;
import static io.trino.spi.security.AccessDeniedException.denyRenameTable;
import static io.trino.spi.security.AccessDeniedException.denyRenameView;
import static io.trino.spi.security.AccessDeniedException.denyRevokeRoles;
import static io.trino.spi.security.AccessDeniedException.denyRevokeSchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denySelectTable;
import static io.trino.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static io.trino.spi.security.AccessDeniedException.denySetMaterializedViewProperties;
import static io.trino.spi.security.AccessDeniedException.denySetRole;
import static io.trino.spi.security.AccessDeniedException.denySetSchemaAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetTableAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetTableProperties;
import static io.trino.spi.security.AccessDeniedException.denySetViewAuthorization;
import static io.trino.spi.security.AccessDeniedException.denyShowColumns;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateFunction;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyShowFunctions;
import static io.trino.spi.security.AccessDeniedException.denyShowTables;
import static io.trino.spi.security.AccessDeniedException.denyTruncateTable;
import static io.trino.spi.security.AccessDeniedException.denyUpdateTableColumns;
import static java.lang.String.format;

public class FileBasedAccessControl
        implements ConnectorAccessControl
{
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";

    private final String catalogName;
    private final List<SchemaAccessControlRule> schemaRules;
    private final List<TableAccessControlRule> tableRules;
    private final List<SessionPropertyAccessControlRule> sessionPropertyRules;
    private final List<FunctionAccessControlRule> functionRules;
    private final List<ProcedureAccessControlRule> procedureRules;
    private final List<AuthorizationRule> authorizationRules;
    private final Set<AnySchemaPermissionsRule> anySchemaPermissionsRules;

    public FileBasedAccessControl(CatalogName catalogName, AccessControlRules rules)
    {
        this.catalogName = catalogName.toString();

        checkArgument(!rules.hasRoleRules(), "File connector access control does not support role rules");

        this.schemaRules = rules.getSchemaRules();
        this.tableRules = rules.getTableRules();
        this.sessionPropertyRules = rules.getSessionPropertyRules();
        this.functionRules = rules.getFunctionRules();
        this.procedureRules = rules.getProcedureRules();
        this.authorizationRules = rules.getAuthorizationRules();
        ImmutableSet.Builder<AnySchemaPermissionsRule> anySchemaPermissionsRules = ImmutableSet.builder();
        schemaRules.stream()
                .map(SchemaAccessControlRule::toAnySchemaPermissionsRule)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(anySchemaPermissionsRules::add);
        tableRules.stream()
                .map(TableAccessControlRule::toAnySchemaPermissionsRule)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(anySchemaPermissionsRules::add);
        functionRules.stream()
                .map(FunctionAccessControlRule::toAnySchemaPermissionsRule)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(anySchemaPermissionsRules::add);
        procedureRules.stream()
                .map(ProcedureAccessControlRule::toAnySchemaPermissionsRule)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(anySchemaPermissionsRules::add);
        this.anySchemaPermissionsRules = anySchemaPermissionsRules.build();
    }

    @Override
    public void checkCanCreateSchema(ConnectorSecurityContext context, String schemaName, Map<String, Object> properties)
    {
        if (!isSchemaOwner(context, schemaName)) {
            denyCreateSchema(schemaName);
        }
    }

    @Override
    public void checkCanDropSchema(ConnectorSecurityContext context, String schemaName)
    {
        if (!isSchemaOwner(context, schemaName)) {
            denyDropSchema(schemaName);
        }
    }

    @Override
    public void checkCanRenameSchema(ConnectorSecurityContext context, String schemaName, String newSchemaName)
    {
        if (!isSchemaOwner(context, schemaName) || !isSchemaOwner(context, newSchemaName)) {
            denyRenameSchema(schemaName, newSchemaName);
        }
    }

    @Override
    public void checkCanSetSchemaAuthorization(ConnectorSecurityContext context, String schemaName, TrinoPrincipal principal)
    {
        if (!isSchemaOwner(context, schemaName)) {
            denySetSchemaAuthorization(schemaName, principal);
        }
        if (!checkCanSetAuthorization(context, principal)) {
            denySetSchemaAuthorization(schemaName, principal);
        }
    }

    @Override
    public void checkCanShowSchemas(ConnectorSecurityContext context) {}

    @Override
    public Set<String> filterSchemas(ConnectorSecurityContext context, Set<String> schemaNames)
    {
        return schemaNames.stream()
                .filter(schemaName -> checkAnySchemaAccess(context, schemaName))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanShowCreateSchema(ConnectorSecurityContext context, String schemaName)
    {
        if (!isSchemaOwner(context, schemaName)) {
            denyShowCreateSchema(schemaName);
        }
    }

    @Override
    public void checkCanShowCreateTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!checkTablePermission(context, tableName, OWNERSHIP)) {
            denyShowCreateTable(tableName.toString());
        }
    }

    @Override
    public void checkCanCreateTable(ConnectorSecurityContext context, SchemaTableName tableName, Map<String, Object> properties)
    {
        // check if user will be an owner of the table after creation
        if (!checkTablePermission(context, tableName, OWNERSHIP)) {
            denyCreateTable(tableName.toString());
        }
    }

    @Override
    public void checkCanDropTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!checkTablePermission(context, tableName, OWNERSHIP)) {
            denyDropTable(tableName.toString());
        }
    }

    @Override
    public void checkCanShowTables(ConnectorSecurityContext context, String schemaName)
    {
        if (!checkAnySchemaAccess(context, schemaName)) {
            denyShowTables(schemaName);
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorSecurityContext context, Set<SchemaTableName> tableNames)
    {
        return tableNames.stream()
                .filter(tableName -> isSchemaOwner(context, tableName.getSchemaName()) || checkAnyTablePermission(context, tableName))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanShowColumns(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!checkAnyTablePermission(context, tableName)) {
            denyShowColumns(tableName.toString());
        }
    }

    @Override
    public Map<SchemaTableName, Set<String>> filterColumns(ConnectorSecurityContext context, Map<SchemaTableName, Set<String>> tableColumns)
    {
        return tableColumns.entrySet().stream()
                .collect(toImmutableMap(
                        Entry::getKey,
                        entry -> filterColumns(context, entry.getKey(), entry.getValue())));
    }

    private Set<String> filterColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columns)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return columns;
        }

        ConnectorIdentity identity = context.getIdentity();
        TableAccessControlRule rule = tableRules.stream()
                .filter(tableRule -> tableRule.matches(identity.getUser(), identity.getEnabledSystemRoles(), identity.getGroups(), tableName))
                .findFirst()
                .orElse(null);
        if (rule == null || rule.getPrivileges().isEmpty()) {
            return ImmutableSet.of();
        }

        // if user has privileges other than select, show all columns
        if (rule.getPrivileges().stream().anyMatch(privilege -> SELECT != privilege)) {
            return columns;
        }

        Set<String> restrictedColumns = rule.getRestrictedColumns();
        return columns.stream()
                .filter(column -> !restrictedColumns.contains(column))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanRenameTable(ConnectorSecurityContext context, SchemaTableName tableName, SchemaTableName newTableName)
    {
        // check if user owns the existing table, and if they will be an owner of the table after the rename
        if (!checkTablePermission(context, tableName, OWNERSHIP) || !checkTablePermission(context, newTableName, OWNERSHIP)) {
            denyRenameTable(tableName.toString(), newTableName.toString());
        }
    }

    @Override
    public void checkCanSetTableProperties(ConnectorSecurityContext context, SchemaTableName tableName, Map<String, Optional<Object>> properties)
    {
        if (!checkTablePermission(context, tableName, OWNERSHIP)) {
            denySetTableProperties(tableName.toString());
        }
    }

    @Override
    public void checkCanSetTableComment(ConnectorSecurityContext identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(identity, tableName, OWNERSHIP)) {
            denyCommentTable(tableName.toString());
        }
    }

    @Override
    public void checkCanSetViewComment(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        if (!checkTablePermission(context, viewName, OWNERSHIP)) {
            denyCommentView(viewName.toString());
        }
    }

    @Override
    public void checkCanSetColumnComment(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!checkTablePermission(context, tableName, OWNERSHIP)) {
            denyCommentColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanAddColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!checkTablePermission(context, tableName, OWNERSHIP)) {
            denyAddColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanDropColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!checkTablePermission(context, tableName, OWNERSHIP)) {
            denyDropColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanRenameColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!checkTablePermission(context, tableName, OWNERSHIP)) {
            denyRenameColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanAlterColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!checkTablePermission(context, tableName, OWNERSHIP)) {
            denyAlterColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanSetTableAuthorization(ConnectorSecurityContext context, SchemaTableName tableName, TrinoPrincipal principal)
    {
        if (!checkTablePermission(context, tableName, OWNERSHIP)) {
            denySetTableAuthorization(tableName.toString(), principal);
        }
        if (!checkCanSetAuthorization(context, principal)) {
            denySetTableAuthorization(tableName.toString(), principal);
        }
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return;
        }

        ConnectorIdentity identity = context.getIdentity();
        boolean allowed = tableRules.stream()
                .filter(rule -> rule.matches(identity.getUser(), identity.getEnabledSystemRoles(), identity.getGroups(), tableName))
                .map(rule -> rule.canSelectColumns(columnNames))
                .findFirst()
                .orElse(false);
        if (!allowed) {
            denySelectTable(tableName.toString());
        }
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!checkTablePermission(context, tableName, INSERT)) {
            denyInsertTable(tableName.toString());
        }
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!checkTablePermission(context, tableName, DELETE)) {
            denyDeleteTable(tableName.toString());
        }
    }

    @Override
    public void checkCanTruncateTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!checkTablePermission(context, tableName, DELETE)) {
            denyTruncateTable(tableName.toString());
        }
    }

    @Override
    public void checkCanUpdateTableColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> updatedColumns)
    {
        if (!checkTablePermission(context, tableName, UPDATE)) {
            denyUpdateTableColumns(tableName.toString(), updatedColumns);
        }
    }

    @Override
    public void checkCanCreateView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        // check if user will be an owner of the view after creation
        if (!checkTablePermission(context, viewName, OWNERSHIP)) {
            denyCreateView(viewName.toString());
        }
    }

    @Override
    public void checkCanRenameView(ConnectorSecurityContext context, SchemaTableName viewName, SchemaTableName newViewName)
    {
        // check if user owns the existing view, and if they will be an owner of the view after the rename
        if (!checkTablePermission(context, viewName, OWNERSHIP) || !checkTablePermission(context, newViewName, OWNERSHIP)) {
            denyRenameView(viewName.toString(), newViewName.toString());
        }
    }

    @Override
    public void checkCanSetViewAuthorization(ConnectorSecurityContext context, SchemaTableName viewName, TrinoPrincipal principal)
    {
        if (!checkTablePermission(context, viewName, OWNERSHIP)) {
            denySetViewAuthorization(viewName.toString(), principal);
        }
        if (!checkCanSetAuthorization(context, principal)) {
            denySetViewAuthorization(viewName.toString(), principal);
        }
    }

    @Override
    public void checkCanDropView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        if (!checkTablePermission(context, viewName, OWNERSHIP)) {
            denyDropView(viewName.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return;
        }

        ConnectorIdentity identity = context.getIdentity();
        TableAccessControlRule rule = tableRules.stream()
                .filter(tableRule -> tableRule.matches(identity.getUser(), identity.getEnabledSystemRoles(), identity.getGroups(), tableName))
                .findFirst()
                .orElse(null);
        if (rule == null || !rule.canSelectColumns(columnNames)) {
            denySelectTable(tableName.toString());
        }
        if (!rule.getPrivileges().contains(GRANT_SELECT)) {
            denyCreateViewWithSelect(tableName.toString(), context.getIdentity());
        }
    }

    @Override
    public void checkCanCreateMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName, Map<String, Object> properties)
    {
        // check if user will be an owner of the view after creation
        if (!checkTablePermission(context, materializedViewName, OWNERSHIP)) {
            denyCreateMaterializedView(materializedViewName.toString());
        }
    }

    @Override
    public void checkCanRefreshMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName)
    {
        if (!checkTablePermission(context, materializedViewName, UPDATE)) {
            denyRefreshMaterializedView(materializedViewName.toString());
        }
    }

    @Override
    public void checkCanDropMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName)
    {
        if (!checkTablePermission(context, materializedViewName, OWNERSHIP)) {
            denyDropMaterializedView(materializedViewName.toString());
        }
    }

    @Override
    public void checkCanRenameMaterializedView(ConnectorSecurityContext context, SchemaTableName viewName, SchemaTableName newViewName)
    {
        // check if user owns the existing materialized view, and if they will be an owner of the materialized view after the rename
        if (!checkTablePermission(context, viewName, OWNERSHIP) || !checkTablePermission(context, newViewName, OWNERSHIP)) {
            denyRenameMaterializedView(viewName.toString(), newViewName.toString());
        }
    }

    @Override
    public void checkCanSetMaterializedViewProperties(ConnectorSecurityContext context, SchemaTableName materializedViewName, Map<String, Optional<Object>> properties)
    {
        if (!checkTablePermission(context, materializedViewName, OWNERSHIP)) {
            denySetMaterializedViewProperties(materializedViewName.toString());
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorSecurityContext context, String propertyName)
    {
        if (!canSetSessionProperty(context, propertyName)) {
            denySetCatalogSessionProperty(propertyName);
        }
    }

    @Override
    public void checkCanGrantSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal grantee, boolean grantOption)
    {
        if (!isSchemaOwner(context, schemaName)) {
            denyGrantSchemaPrivilege(privilege.name(), schemaName);
        }
    }

    @Override
    public void checkCanDenySchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal grantee)
    {
        if (!isSchemaOwner(context, schemaName)) {
            denyDenySchemaPrivilege(privilege.name(), schemaName);
        }
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal revokee, boolean grantOption)
    {
        if (!isSchemaOwner(context, schemaName)) {
            denyRevokeSchemaPrivilege(privilege.name(), schemaName);
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal grantee, boolean grantOption)
    {
        // file based rules are immutable
        denyGrantTablePrivilege(privilege.toString(), tableName.toString());
    }

    @Override
    public void checkCanDenyTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal grantee)
    {
        // file based rules are immutable
        denyDenyTablePrivilege(privilege.toString(), tableName.toString());
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal revokee, boolean grantOption)
    {
        // file based rules are immutable
        denyRevokeTablePrivilege(privilege.toString(), tableName.toString());
    }

    @Override
    public void checkCanCreateRole(ConnectorSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
        denyCreateRole(role);
    }

    @Override
    public void checkCanDropRole(ConnectorSecurityContext context, String role)
    {
        denyDropRole(role);
    }

    @Override
    public void checkCanGrantRoles(ConnectorSecurityContext context,
            Set<String> roles,
            Set<TrinoPrincipal> grantees,
            boolean adminOption,
            Optional<TrinoPrincipal> grantor)
    {
        denyGrantRoles(roles, grantees);
    }

    @Override
    public void checkCanRevokeRoles(ConnectorSecurityContext context,
            Set<String> roles,
            Set<TrinoPrincipal> grantees,
            boolean adminOption,
            Optional<TrinoPrincipal> grantor)
    {
        denyRevokeRoles(roles, grantees);
    }

    @Override
    public void checkCanSetRole(ConnectorSecurityContext context, String role)
    {
        denySetRole(role);
    }

    @Override
    public void checkCanShowRoles(ConnectorSecurityContext context)
    {
        // allow, no roles are supported so show will always be empty
    }

    @Override
    public void checkCanShowCurrentRoles(ConnectorSecurityContext context)
    {
        // allow, no roles are supported so show will always be empty
    }

    @Override
    public void checkCanShowRoleGrants(ConnectorSecurityContext context)
    {
        // allow, no roles are supported so show will always be empty
    }

    @Override
    public void checkCanExecuteProcedure(ConnectorSecurityContext context, SchemaRoutineName procedure)
    {
        ConnectorIdentity identity = context.getIdentity();
        boolean allowed = procedureRules.stream()
                .filter(rule -> rule.matches(identity.getUser(), identity.getEnabledSystemRoles(), identity.getGroups(), procedure))
                .findFirst()
                .filter(ProcedureAccessControlRule::canExecuteProcedure)
                .isPresent();
        if (!allowed) {
            denyExecuteProcedure(procedure.toString());
        }
    }

    @Override
    public void checkCanExecuteTableProcedure(ConnectorSecurityContext context, SchemaTableName tableName, String procedure) {}

    @Override
    public boolean canExecuteFunction(ConnectorSecurityContext context, SchemaRoutineName function)
    {
        return checkFunctionPermission(context, function, FunctionAccessControlRule::canExecuteFunction);
    }

    @Override
    public boolean canCreateViewWithExecuteFunction(ConnectorSecurityContext context, SchemaRoutineName function)
    {
        return checkFunctionPermission(context, function, FunctionAccessControlRule::canGrantExecuteFunction);
    }

    @Override
    public void checkCanShowFunctions(ConnectorSecurityContext context, String schemaName)
    {
        if (!checkAnySchemaAccess(context, schemaName)) {
            denyShowFunctions(schemaName);
        }
    }

    @Override
    public Set<SchemaFunctionName> filterFunctions(ConnectorSecurityContext context, Set<SchemaFunctionName> functionNames)
    {
        return functionNames.stream()
                .filter(name -> isSchemaOwner(context, name.getSchemaName()) ||
                        checkAnyFunctionPermission(context, new SchemaRoutineName(name.getSchemaName(), name.getFunctionName()), FunctionAccessControlRule::canExecuteFunction))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanCreateFunction(ConnectorSecurityContext context, SchemaRoutineName function)
    {
        if (!checkFunctionPermission(context, function, FunctionAccessControlRule::hasOwnership)) {
            denyCreateFunction(function.toString());
        }
    }

    @Override
    public void checkCanDropFunction(ConnectorSecurityContext context, SchemaRoutineName function)
    {
        if (!checkFunctionPermission(context, function, FunctionAccessControlRule::hasOwnership)) {
            denyDropFunction(function.toString());
        }
    }

    @Override
    public void checkCanShowCreateFunction(ConnectorSecurityContext context, SchemaRoutineName function)
    {
        if (!checkFunctionPermission(context, function, FunctionAccessControlRule::hasOwnership)) {
            denyShowCreateFunction(function.getSchemaName());
        }
    }

    @Override
    public List<ViewExpression> getRowFilters(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return ImmutableList.of();
        }

        ConnectorIdentity identity = context.getIdentity();
        return tableRules.stream()
                .filter(rule -> rule.matches(identity.getUser(), identity.getEnabledSystemRoles(), identity.getGroups(), tableName))
                .map(rule -> rule.getFilter(catalogName, tableName.getSchemaName()))
                // we return the first one we find
                .findFirst()
                .stream()
                .flatMap(Optional::stream)
                .collect(toImmutableList());
    }

    @Override
    public Optional<ViewExpression> getColumnMask(ConnectorSecurityContext context, SchemaTableName tableName, String columnName, Type type)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return Optional.empty();
        }

        ConnectorIdentity identity = context.getIdentity();
        List<ViewExpression> masks = tableRules.stream()
                .filter(rule -> rule.matches(identity.getUser(), identity.getEnabledSystemRoles(), identity.getGroups(), tableName))
                .map(rule -> rule.getColumnMask(catalogName, tableName.getSchemaName(), columnName))
                // we return the first one we find
                .findFirst()
                .stream()
                .flatMap(Optional::stream)
                .toList();

        if (masks.size() > 1) {
            throw new TrinoException(INVALID_COLUMN_MASK, format("Multiple masks defined for %s.%s", tableName, columnName));
        }

        return masks.stream().findFirst();
    }

    @Override
    public Map<ColumnSchema, ViewExpression> getColumnMasks(ConnectorSecurityContext context, SchemaTableName tableName, List<ColumnSchema> columns)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return ImmutableMap.of();
        }

        ConnectorIdentity identity = context.getIdentity();
        try {
            return columns.stream()
                    .flatMap(columnSchema -> tableRules.stream()
                            .filter(rule -> rule.matches(identity.getUser(), identity.getEnabledSystemRoles(), identity.getGroups(), tableName))
                            .map(rule -> rule.getColumnMask(catalogName, tableName.getSchemaName(), columnSchema.getName()))
                            .findFirst()
                            .stream()
                            .flatMap(Optional::stream)
                            .map(viewExpression -> Map.entry(columnSchema, viewExpression)))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        catch (IllegalArgumentException exception) {
            throw new TrinoException(INVALID_COLUMN_MASK, "Multiple column masks defined for the same column", exception);
        }
    }

    private boolean canSetSessionProperty(ConnectorSecurityContext context, String property)
    {
        ConnectorIdentity identity = context.getIdentity();
        for (SessionPropertyAccessControlRule rule : sessionPropertyRules) {
            Optional<Boolean> allowed = rule.match(identity.getUser(), identity.getEnabledSystemRoles(), identity.getGroups(), property);
            if (allowed.isPresent()) {
                return allowed.get();
            }
        }
        return false;
    }

    private boolean checkAnyTablePermission(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        return checkTablePermission(context, tableName, privileges -> !privileges.isEmpty());
    }

    private boolean checkTablePermission(ConnectorSecurityContext context, SchemaTableName tableName, TablePrivilege requiredPrivilege)
    {
        return checkTablePermission(context, tableName, privileges -> privileges.contains(requiredPrivilege));
    }

    private boolean checkTablePermission(ConnectorSecurityContext context, SchemaTableName tableName, Predicate<Set<TablePrivilege>> checkPrivileges)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return true;
        }

        ConnectorIdentity identity = context.getIdentity();
        for (TableAccessControlRule rule : tableRules) {
            if (rule.matches(identity.getUser(), identity.getEnabledSystemRoles(), identity.getGroups(), tableName)) {
                return checkPrivileges.test(rule.getPrivileges());
            }
        }
        return false;
    }

    private boolean checkAnySchemaAccess(ConnectorSecurityContext context, String schemaName)
    {
        ConnectorIdentity identity = context.getIdentity();
        return anySchemaPermissionsRules.stream().anyMatch(rule -> rule.match(identity.getUser(), identity.getEnabledSystemRoles(), identity.getGroups(), schemaName));
    }

    private boolean isSchemaOwner(ConnectorSecurityContext context, String schemaName)
    {
        ConnectorIdentity identity = context.getIdentity();
        for (SchemaAccessControlRule rule : schemaRules) {
            Optional<Boolean> owner = rule.match(identity.getUser(), identity.getEnabledSystemRoles(), identity.getGroups(), schemaName);
            if (owner.isPresent()) {
                return owner.get();
            }
        }
        return false;
    }

    private boolean checkFunctionPermission(ConnectorSecurityContext context, SchemaRoutineName functionName, Predicate<FunctionAccessControlRule> executePredicate)
    {
        ConnectorIdentity identity = context.getIdentity();
        return functionRules.stream()
                .filter(rule -> rule.matches(identity.getUser(), identity.getEnabledSystemRoles(), identity.getGroups(), functionName))
                .findFirst()
                .filter(executePredicate)
                .isPresent();
    }

    private boolean checkAnyFunctionPermission(ConnectorSecurityContext context, SchemaRoutineName functionName, Predicate<FunctionAccessControlRule> executePredicate)
    {
        ConnectorIdentity identity = context.getIdentity();
        return functionRules.stream()
                .filter(rule -> rule.matches(identity.getUser(), identity.getEnabledSystemRoles(), identity.getGroups(), functionName))
                .findFirst()
                .filter(executePredicate)
                .isPresent();
    }

    private boolean checkCanSetAuthorization(ConnectorSecurityContext context, TrinoPrincipal principal)
    {
        ConnectorIdentity identity = context.getIdentity();
        Set<String> roles = identity.getConnectorRole().stream()
                .flatMap(role -> role.getRole().stream())
                .collect(toImmutableSet());
        return authorizationRules.stream()
                .flatMap(rule -> rule.match(identity.getUser(), identity.getGroups(), roles, principal).stream())
                .findFirst()
                .orElse(false);
    }
}
