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

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.base.security.TableAccessControlRule.TablePrivilege;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorSecurityContext;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;

import javax.inject.Inject;

import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.plugin.base.JsonUtils.parseJson;
import static io.prestosql.plugin.base.security.TableAccessControlRule.TablePrivilege.DELETE;
import static io.prestosql.plugin.base.security.TableAccessControlRule.TablePrivilege.GRANT_SELECT;
import static io.prestosql.plugin.base.security.TableAccessControlRule.TablePrivilege.INSERT;
import static io.prestosql.plugin.base.security.TableAccessControlRule.TablePrivilege.OWNERSHIP;
import static io.prestosql.plugin.base.security.TableAccessControlRule.TablePrivilege.SELECT;
import static io.prestosql.spi.security.AccessDeniedException.denyAddColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyCommentTable;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateTable;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateView;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.prestosql.spi.security.AccessDeniedException.denyDeleteTable;
import static io.prestosql.spi.security.AccessDeniedException.denyDropColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyDropSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyDropTable;
import static io.prestosql.spi.security.AccessDeniedException.denyDropView;
import static io.prestosql.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denyInsertTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denySelectTable;
import static io.prestosql.spi.security.AccessDeniedException.denySetCatalogSessionProperty;

public class FileBasedAccessControl
        implements ConnectorAccessControl
{
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";

    private final List<SchemaAccessControlRule> schemaRules;
    private final List<TableAccessControlRule> tableRules;
    private final List<SessionPropertyAccessControlRule> sessionPropertyRules;

    @Inject
    public FileBasedAccessControl(FileBasedAccessControlConfig config)
    {
        AccessControlRules rules = parseJson(Paths.get(config.getConfigFile()), AccessControlRules.class);

        this.schemaRules = rules.getSchemaRules();
        this.tableRules = rules.getTableRules();
        this.sessionPropertyRules = rules.getSessionPropertyRules();
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
    public void checkCanShowSchemas(ConnectorSecurityContext context)
    {
    }

    @Override
    public Set<String> filterSchemas(ConnectorSecurityContext context, Set<String> schemaNames)
    {
        return schemaNames.stream()
                .filter(schemaName -> canAccessSchema(context, schemaName, AccessMode.READ_ONLY))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanCreateTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!isSchemaOwner(context, tableName.getSchemaName())) {
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
    public void checkCanShowTablesMetadata(ConnectorSecurityContext context, String schemaName)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorSecurityContext context, Set<SchemaTableName> tableNames)
    {
        // table should be visible to schema owner
        // table should be visible to user with any privilege on that table
        return tableNames.stream()
                .filter(tableName -> isSchemaOwner(context, tableName.getSchemaName()) || checkTableAnyPermission(context, tableName))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanShowColumnsMetadata(ConnectorSecurityContext identity, SchemaTableName tableName)
    {
    }

    @Override
    public List<ColumnMetadata> filterColumns(ConnectorSecurityContext identity, SchemaTableName tableName, List<ColumnMetadata> columns)
    {
        return columns;
    }

    @Override
    public void checkCanRenameTable(ConnectorSecurityContext context, SchemaTableName tableName, SchemaTableName newTableName)
    {
        if (!checkTablePermission(context, tableName, OWNERSHIP)) {
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
    public void checkCanSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        // TODO: Implement column level permissions
        if (!checkTablePermission(context, tableName, SELECT)) {
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
        if (!isSchemaOwner(context, viewName.getSchemaName())) {
            denyCreateView(viewName.toString());
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
        // TODO: implement column level permissions
        if (!checkTablePermission(context, tableName, SELECT)) {
            denySelectTable(tableName.toString());
        }
        if (!checkTablePermission(context, tableName, GRANT_SELECT)) {
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
    public void checkCanGrantTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, PrestoPrincipal grantee, boolean withGrantOption)
    {
        if (!checkTablePermission(context, tableName, OWNERSHIP)) {
            denyGrantTablePrivilege(privilege.name(), tableName.toString());
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, PrestoPrincipal revokee, boolean grantOptionFor)
    {
        if (!checkTablePermission(context, tableName, OWNERSHIP)) {
            denyRevokeTablePrivilege(privilege.name(), tableName.toString());
        }
    }

    @Override
    public void checkCanCreateRole(ConnectorSecurityContext context, String role, Optional<PrestoPrincipal> grantor)
    {
    }

    @Override
    public void checkCanDropRole(ConnectorSecurityContext context, String role)
    {
    }

    @Override
    public void checkCanGrantRoles(ConnectorSecurityContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalogName)
    {
    }

    @Override
    public void checkCanRevokeRoles(ConnectorSecurityContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalogName)
    {
    }

    @Override
    public void checkCanSetRole(ConnectorSecurityContext context, String role, String catalogName)
    {
    }

    @Override
    public void checkCanShowRoles(ConnectorSecurityContext context, String catalogName)
    {
    }

    @Override
    public void checkCanShowCurrentRoles(ConnectorSecurityContext context, String catalogName)
    {
    }

    @Override
    public void checkCanShowRoleGrants(ConnectorSecurityContext context, String catalogName)
    {
    }

    private boolean canSetSessionProperty(ConnectorSecurityContext context, String property)
    {
        for (SessionPropertyAccessControlRule rule : sessionPropertyRules) {
            Optional<Boolean> allowed = rule.match(context.getIdentity().getUser(), property);
            if (allowed.isPresent()) {
                return allowed.get();
            }
        }
        return false;
    }

    private boolean checkTablePermission(ConnectorSecurityContext context, SchemaTableName tableName, TablePrivilege... requiredPrivileges)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return true;
        }
        AccessMode requiredSchemaAccessMode = calculateRequiredSchemaAccessModeFromTablePrivileges(requiredPrivileges);
        if (!canAccessSchema(context, tableName.getSchemaName(), requiredSchemaAccessMode)) {
            return false;
        }
        for (TableAccessControlRule rule : tableRules) {
            Optional<Set<TablePrivilege>> tablePrivileges = rule.match(context.getIdentity().getUser(), tableName);
            if (tablePrivileges.isPresent()) {
                return tablePrivileges.get().containsAll(ImmutableSet.copyOf(requiredPrivileges));
            }
        }
        return false;
    }

    private boolean checkTableAnyPermission(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return true;
        }
        if (!canAccessSchema(context, tableName.getSchemaName(), AccessMode.READ_ONLY)) {
            return false;
        }
        for (TableAccessControlRule rule : tableRules) {
            Optional<Set<TablePrivilege>> tablePrivileges = rule.match(context.getIdentity().getUser(), tableName);
            if (tablePrivileges.isPresent()) {
                return !tablePrivileges.get().isEmpty();
            }
        }
        return false;
    }

    private boolean isSchemaOwner(ConnectorSecurityContext context, String schemaName)
    {
        for (SchemaAccessControlRule rule : schemaRules) {
            Optional<Boolean> owner = rule.isSchemaOwner(context.getIdentity().getUser(), schemaName);
            if (owner.isPresent()) {
                return owner.get();
            }
        }
        return false;
    }

    private boolean canAccessSchema(ConnectorSecurityContext context, String schemaName, AccessMode requiredAccessMode)
    {
        for (SchemaAccessControlRule rule : schemaRules) {
            Optional<AccessMode> accessMode = rule.match(context.getIdentity().getUser(), schemaName);
            if (accessMode.isPresent()) {
                return accessMode.get().implies(requiredAccessMode);
            }
        }
        // if not rule match, we assume schema is at least READ_ONLY to user by default
        return true;
    }

    private static AccessMode calculateRequiredSchemaAccessModeFromTablePrivileges(TablePrivilege... tablePrivileges)
    {
        AccessMode accessMode = AccessMode.NONE;
        for (TablePrivilege tablePrivilege : tablePrivileges) {
            if (tablePrivilege.equals(SELECT) && accessMode.equals(AccessMode.NONE)) {
                accessMode = AccessMode.READ_ONLY;
                continue;
            }
            accessMode = AccessMode.ALL;
        }
        return accessMode;
    }
}
