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
package io.prestosql.plugin.base.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.base.security.TableAccessControlRule.TablePrivilege;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorSecurityContext;
import io.prestosql.spi.connector.SchemaRoutineName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.Type;

import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.plugin.base.security.TableAccessControlRule.TablePrivilege.DELETE;
import static io.prestosql.plugin.base.security.TableAccessControlRule.TablePrivilege.GRANT_SELECT;
import static io.prestosql.plugin.base.security.TableAccessControlRule.TablePrivilege.INSERT;
import static io.prestosql.plugin.base.security.TableAccessControlRule.TablePrivilege.OWNERSHIP;
import static io.prestosql.plugin.base.security.TableAccessControlRule.TablePrivilege.SELECT;
import static io.prestosql.plugin.base.util.JsonUtils.parseJson;
import static io.prestosql.spi.security.AccessDeniedException.denyAddColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyCommentColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyCommentTable;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateRole;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateTable;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateView;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.prestosql.spi.security.AccessDeniedException.denyDeleteTable;
import static io.prestosql.spi.security.AccessDeniedException.denyDropColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyDropRole;
import static io.prestosql.spi.security.AccessDeniedException.denyDropSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyDropTable;
import static io.prestosql.spi.security.AccessDeniedException.denyDropView;
import static io.prestosql.spi.security.AccessDeniedException.denyGrantRoles;
import static io.prestosql.spi.security.AccessDeniedException.denyGrantSchemaPrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denyInsertTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameView;
import static io.prestosql.spi.security.AccessDeniedException.denyRevokeRoles;
import static io.prestosql.spi.security.AccessDeniedException.denyRevokeSchemaPrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denySelectTable;
import static io.prestosql.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static io.prestosql.spi.security.AccessDeniedException.denySetRole;
import static io.prestosql.spi.security.AccessDeniedException.denySetSchemaAuthorization;
import static io.prestosql.spi.security.AccessDeniedException.denySetTableAuthorization;
import static io.prestosql.spi.security.AccessDeniedException.denyShowColumns;
import static io.prestosql.spi.security.AccessDeniedException.denyShowCreateSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyShowCreateTable;
import static io.prestosql.spi.security.AccessDeniedException.denyShowTables;
import static java.util.Objects.requireNonNull;

public class FileBasedAccessControl
        implements ConnectorAccessControl
{
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";

    private final String catalogName;
    private final List<SchemaAccessControlRule> schemaRules;
    private final List<TableAccessControlRule> tableRules;
    private final List<SessionPropertyAccessControlRule> sessionPropertyRules;
    private final Set<AnySchemaPermissionsRule> anySchemaPermissionsRules;

    public FileBasedAccessControl(String catalogName, FileBasedAccessControlConfig config)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");

        AccessControlRules rules = parseJson(Paths.get(config.getConfigFile()), AccessControlRules.class);

        this.schemaRules = rules.getSchemaRules();
        this.tableRules = rules.getTableRules();
        this.sessionPropertyRules = rules.getSessionPropertyRules();
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
        this.anySchemaPermissionsRules = anySchemaPermissionsRules.build();
    }

    @Override
    public void checkCanCreateSchema(ConnectorSecurityContext context, String schemaName)
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
    public void checkCanSetSchemaAuthorization(ConnectorSecurityContext context, String schemaName, PrestoPrincipal principal)
    {
        if (!isSchemaOwner(context, schemaName)) {
            denySetSchemaAuthorization(schemaName, principal);
        }
    }

    @Override
    public void checkCanShowSchemas(ConnectorSecurityContext context)
    {
    }

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
    public void checkCanCreateTable(ConnectorSecurityContext context, SchemaTableName tableName)
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
    public List<ColumnMetadata> filterColumns(ConnectorSecurityContext context, SchemaTableName tableName, List<ColumnMetadata> columns)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return columns;
        }

        ConnectorIdentity identity = context.getIdentity();
        TableAccessControlRule rule = tableRules.stream()
                .filter(tableRule -> tableRule.matches(identity.getUser(), identity.getGroups(), tableName))
                .findFirst()
                .orElse(null);
        if (rule == null || rule.getPrivileges().isEmpty()) {
            return ImmutableList.of();
        }

        // if user has privileges other than select, show all columns
        if (rule.getPrivileges().stream().anyMatch(privilege -> SELECT != privilege)) {
            return columns;
        }

        Set<String> restrictedColumns = rule.getRestrictedColumns();
        return columns.stream()
                .filter(columnMetadata -> !restrictedColumns.contains(columnMetadata.getName()))
                .collect(toImmutableList());
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
    public void checkCanSetTableComment(ConnectorSecurityContext identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(identity, tableName, OWNERSHIP)) {
            denyCommentTable(tableName.toString());
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
    public void checkCanSetTableAuthorization(ConnectorSecurityContext context, SchemaTableName tableName, PrestoPrincipal principal)
    {
        if (!checkTablePermission(context, tableName, OWNERSHIP)) {
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
                .filter(rule -> rule.matches(identity.getUser(), identity.getGroups(), tableName))
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
                .filter(tableRule -> tableRule.matches(identity.getUser(), identity.getGroups(), tableName))
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
    public void checkCanSetCatalogSessionProperty(ConnectorSecurityContext context, String propertyName)
    {
        if (!canSetSessionProperty(context, propertyName)) {
            denySetCatalogSessionProperty(propertyName);
        }
    }

    @Override
    public void checkCanGrantSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, PrestoPrincipal grantee, boolean grantOption)
    {
        if (!isSchemaOwner(context, schemaName)) {
            denyGrantSchemaPrivilege(privilege.name(), schemaName);
        }
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, PrestoPrincipal revokee, boolean grantOption)
    {
        if (!isSchemaOwner(context, schemaName)) {
            denyRevokeSchemaPrivilege(privilege.name(), schemaName);
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, PrestoPrincipal grantee, boolean grantOption)
    {
        // file based rules are immutable
        denyGrantTablePrivilege(privilege.toString(), tableName.toString());
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, PrestoPrincipal revokee, boolean grantOption)
    {
        // file based rules are immutable
        denyRevokeTablePrivilege(privilege.toString(), tableName.toString());
    }

    @Override
    public void checkCanCreateRole(ConnectorSecurityContext context, String role, Optional<PrestoPrincipal> grantor)
    {
        denyCreateRole(role);
    }

    @Override
    public void checkCanDropRole(ConnectorSecurityContext context, String role)
    {
        denyDropRole(role);
    }

    @Override
    public void checkCanGrantRoles(ConnectorSecurityContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOption, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        denyGrantRoles(roles, grantees);
    }

    @Override
    public void checkCanRevokeRoles(ConnectorSecurityContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOption, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        denyRevokeRoles(roles, grantees);
    }

    @Override
    public void checkCanSetRole(ConnectorSecurityContext context, String role, String catalogName)
    {
        denySetRole(role);
    }

    @Override
    public void checkCanShowRoleAuthorizationDescriptors(ConnectorSecurityContext context, String catalogName)
    {
        // allow, no roles are supported so show will always be empty
    }

    @Override
    public void checkCanShowRoles(ConnectorSecurityContext context, String catalogName)
    {
        // allow, no roles are supported so show will always be empty
    }

    @Override
    public void checkCanShowCurrentRoles(ConnectorSecurityContext context, String catalogName)
    {
        // allow, no roles are supported so show will always be empty
    }

    @Override
    public void checkCanShowRoleGrants(ConnectorSecurityContext context, String catalogName)
    {
        // allow, no roles are supported so show will always be empty
    }

    @Override
    public void checkCanExecuteProcedure(ConnectorSecurityContext context, SchemaRoutineName procedure)
    {
    }

    @Override
    public Optional<ViewExpression> getRowFilter(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return Optional.empty();
        }

        ConnectorIdentity identity = context.getIdentity();
        return tableRules.stream()
                .filter(rule -> rule.matches(identity.getUser(), identity.getGroups(), tableName))
                .map(rule -> rule.getFilter(identity.getUser(), catalogName, tableName.getSchemaName()))
                .findFirst()
                .flatMap(Function.identity());
    }

    @Override
    public Optional<ViewExpression> getColumnMask(ConnectorSecurityContext context, SchemaTableName tableName, String columnName, Type type)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return Optional.empty();
        }

        ConnectorIdentity identity = context.getIdentity();
        return tableRules.stream()
                .filter(rule -> rule.matches(identity.getUser(), identity.getGroups(), tableName))
                .map(rule -> rule.getColumnMask(identity.getUser(), catalogName, tableName.getSchemaName(), columnName))
                .findFirst()
                .flatMap(Function.identity());
    }

    private boolean canSetSessionProperty(ConnectorSecurityContext context, String property)
    {
        ConnectorIdentity identity = context.getIdentity();
        for (SessionPropertyAccessControlRule rule : sessionPropertyRules) {
            Optional<Boolean> allowed = rule.match(identity.getUser(), identity.getGroups(), property);
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
            if (rule.matches(identity.getUser(), identity.getGroups(), tableName)) {
                return checkPrivileges.test(rule.getPrivileges());
            }
        }
        return false;
    }

    private boolean checkAnySchemaAccess(ConnectorSecurityContext context, String schemaName)
    {
        ConnectorIdentity identity = context.getIdentity();
        return anySchemaPermissionsRules.stream().anyMatch(rule -> rule.match(identity.getUser(), identity.getGroups(), schemaName));
    }

    private boolean isSchemaOwner(ConnectorSecurityContext context, String schemaName)
    {
        ConnectorIdentity identity = context.getIdentity();
        for (SchemaAccessControlRule rule : schemaRules) {
            Optional<Boolean> owner = rule.match(identity.getUser(), identity.getGroups(), schemaName);
            if (owner.isPresent()) {
                return owner.get();
            }
        }
        return false;
    }
}
