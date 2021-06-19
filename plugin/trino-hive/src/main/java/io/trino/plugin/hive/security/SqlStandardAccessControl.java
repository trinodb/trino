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
package io.trino.plugin.hive.security;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.SchemaRoutineName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.metastore.Database.DEFAULT_DATABASE_NAME;
import static io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import static io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege.DELETE;
import static io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege.INSERT;
import static io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege.SELECT;
import static io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege.UPDATE;
import static io.trino.plugin.hive.metastore.HivePrivilegeInfo.toHivePrivilege;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.isRoleApplicable;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.isRoleEnabled;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.listApplicableRoles;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.listEnabledPrincipals;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.security.AccessDeniedException.denyAddColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentTable;
import static io.trino.spi.security.AccessDeniedException.denyCreateMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyCreateRole;
import static io.trino.spi.security.AccessDeniedException.denyCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyCreateView;
import static io.trino.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.trino.spi.security.AccessDeniedException.denyDeleteTable;
import static io.trino.spi.security.AccessDeniedException.denyDropColumn;
import static io.trino.spi.security.AccessDeniedException.denyDropMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyDropRole;
import static io.trino.spi.security.AccessDeniedException.denyDropSchema;
import static io.trino.spi.security.AccessDeniedException.denyDropTable;
import static io.trino.spi.security.AccessDeniedException.denyDropView;
import static io.trino.spi.security.AccessDeniedException.denyGrantRoles;
import static io.trino.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyInsertTable;
import static io.trino.spi.security.AccessDeniedException.denyRefreshMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameColumn;
import static io.trino.spi.security.AccessDeniedException.denyRenameSchema;
import static io.trino.spi.security.AccessDeniedException.denyRenameTable;
import static io.trino.spi.security.AccessDeniedException.denyRenameView;
import static io.trino.spi.security.AccessDeniedException.denyRevokeRoles;
import static io.trino.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denySelectTable;
import static io.trino.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static io.trino.spi.security.AccessDeniedException.denySetRole;
import static io.trino.spi.security.AccessDeniedException.denySetSchemaAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetTableAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetViewAuthorization;
import static io.trino.spi.security.AccessDeniedException.denyShowColumns;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyShowRoleAuthorizationDescriptors;
import static io.trino.spi.security.AccessDeniedException.denyShowRoles;
import static io.trino.spi.security.AccessDeniedException.denyUpdateTableColumns;
import static io.trino.spi.security.PrincipalType.ROLE;
import static io.trino.spi.security.PrincipalType.USER;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class SqlStandardAccessControl
        implements ConnectorAccessControl
{
    public static final String ADMIN_ROLE_NAME = "admin";
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";
    private static final SchemaTableName ROLES = new SchemaTableName(INFORMATION_SCHEMA_NAME, "roles");
    private static final SchemaTableName ROLE_AUHTORIZATION_DESCRIPTORS = new SchemaTableName(INFORMATION_SCHEMA_NAME, "role_authorization_descriptors");

    private final String catalogName;
    private final SqlStandardAccessControlMetastore metastore;

    @Inject
    public SqlStandardAccessControl(
            CatalogName catalogName,
            SqlStandardAccessControlMetastore metastore)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null").toString();
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    public void checkCanCreateSchema(ConnectorSecurityContext context, String schemaName)
    {
        if (!isAdmin(context)) {
            denyCreateSchema(schemaName);
        }
    }

    @Override
    public void checkCanDropSchema(ConnectorSecurityContext context, String schemaName)
    {
        if (!isDatabaseOwner(context, schemaName)) {
            denyDropSchema(schemaName);
        }
    }

    @Override
    public void checkCanRenameSchema(ConnectorSecurityContext context, String schemaName, String newSchemaName)
    {
        if (!isDatabaseOwner(context, schemaName)) {
            denyRenameSchema(schemaName, newSchemaName);
        }
    }

    @Override
    public void checkCanSetSchemaAuthorization(ConnectorSecurityContext context, String schemaName, TrinoPrincipal principal)
    {
        if (!isDatabaseOwner(context, schemaName)) {
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
        return schemaNames;
    }

    @Override
    public void checkCanShowCreateTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        // This should really be OWNERSHIP, but Hive uses `SELECT with GRANT`
        if (!checkTablePermission(context, tableName, SELECT, true)) {
            denyShowCreateTable(tableName.toString());
        }
    }

    @Override
    public void checkCanShowCreateSchema(ConnectorSecurityContext context, String schemaName)
    {
        if (!isDatabaseOwner(context, schemaName)) {
            denyShowCreateSchema(schemaName);
        }
    }

    @Override
    public void checkCanCreateTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!isDatabaseOwner(context, tableName.getSchemaName())) {
            denyCreateTable(tableName.toString());
        }
    }

    @Override
    public void checkCanDropTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!isTableOwner(context, tableName)) {
            denyDropTable(tableName.toString());
        }
    }

    @Override
    public void checkCanRenameTable(ConnectorSecurityContext context, SchemaTableName tableName, SchemaTableName newTableName)
    {
        if (!isTableOwner(context, tableName)) {
            denyRenameTable(tableName.toString(), newTableName.toString());
        }
    }

    @Override
    public void checkCanSetTableComment(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!isTableOwner(context, tableName)) {
            denyCommentTable(tableName.toString());
        }
    }

    @Override
    public void checkCanSetColumnComment(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!isTableOwner(context, tableName)) {
            denyCommentColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanShowTables(ConnectorSecurityContext context, String schemaName)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorSecurityContext context, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanShowColumns(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!hasAnyTablePermission(context, tableName)) {
            denyShowColumns(tableName.toString());
        }
    }

    @Override
    public Set<String> filterColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columns)
    {
        if (!hasAnyTablePermission(context, tableName)) {
            return ImmutableSet.of();
        }
        return columns;
    }

    @Override
    public void checkCanAddColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!isTableOwner(context, tableName)) {
            denyAddColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanDropColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!isTableOwner(context, tableName)) {
            denyDropColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanRenameColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!isTableOwner(context, tableName)) {
            denyRenameColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanSetTableAuthorization(ConnectorSecurityContext context, SchemaTableName tableName, TrinoPrincipal principal)
    {
        if (!isTableOwner(context, tableName)) {
            denySetTableAuthorization(tableName.toString(), principal);
        }
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        // TODO: Implement column level access control
        if (!checkTablePermission(context, tableName, SELECT, false)) {
            denySelectTable(tableName.toString());
        }
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!checkTablePermission(context, tableName, INSERT, false)) {
            denyInsertTable(tableName.toString());
        }
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (!checkTablePermission(context, tableName, DELETE, false)) {
            denyDeleteTable(tableName.toString());
        }
    }

    @Override
    public void checkCanUpdateTableColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> updatedColumns)
    {
        if (!checkTablePermission(context, tableName, UPDATE, false)) {
            denyUpdateTableColumns(tableName.toString(), updatedColumns);
        }
    }

    @Override
    public void checkCanCreateView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        if (!isDatabaseOwner(context, viewName.getSchemaName())) {
            denyCreateView(viewName.toString());
        }
    }

    @Override
    public void checkCanRenameView(ConnectorSecurityContext context, SchemaTableName viewName, SchemaTableName newViewName)
    {
        if (!isTableOwner(context, viewName)) {
            denyRenameView(viewName.toString(), newViewName.toString());
        }
    }

    @Override
    public void checkCanSetViewAuthorization(ConnectorSecurityContext context, SchemaTableName viewName, TrinoPrincipal principal)
    {
        if (!isTableOwner(context, viewName)) {
            denySetViewAuthorization(viewName.toString(), principal);
        }
    }

    @Override
    public void checkCanDropView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        if (!isTableOwner(context, viewName)) {
            denyDropView(viewName.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        checkCanSelectFromColumns(context, tableName, columnNames);

        // TODO implement column level access control
        if (!checkTablePermission(context, tableName, SELECT, true)) {
            denyCreateViewWithSelect(tableName.toString(), context.getIdentity());
        }
    }

    @Override
    public void checkCanCreateMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName)
    {
        if (!isDatabaseOwner(context, materializedViewName.getSchemaName())) {
            denyCreateMaterializedView(materializedViewName.toString());
        }
    }

    @Override
    public void checkCanRefreshMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName)
    {
        if (!checkTablePermission(context, materializedViewName, UPDATE, false)) {
            denyRefreshMaterializedView(materializedViewName.toString());
        }
    }

    @Override
    public void checkCanDropMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName)
    {
        if (!isTableOwner(context, materializedViewName)) {
            denyDropMaterializedView(materializedViewName.toString());
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorSecurityContext context, String propertyName)
    {
        if (!isAdmin(context)) {
            denySetCatalogSessionProperty(catalogName, propertyName);
        }
    }

    @Override
    public void checkCanGrantSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support grants on schemas");
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal revokee, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support revokes on schemas");
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal grantee, boolean grantOption)
    {
        if (isTableOwner(context, tableName)) {
            return;
        }

        if (!hasGrantOptionForPrivilege(context, privilege, tableName)) {
            denyGrantTablePrivilege(privilege.name(), tableName.toString());
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal revokee, boolean grantOption)
    {
        if (isTableOwner(context, tableName)) {
            return;
        }

        if (!hasGrantOptionForPrivilege(context, privilege, tableName)) {
            denyRevokeTablePrivilege(privilege.name(), tableName.toString());
        }
    }

    @Override
    public void checkCanCreateRole(ConnectorSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
        // currently specifying grantor is supported by metastore, but it is not supported by Hive itself
        if (grantor.isPresent()) {
            throw new AccessDeniedException("Hive Connector does not support WITH ADMIN statement");
        }
        if (!isAdmin(context)) {
            denyCreateRole(role);
        }
    }

    @Override
    public void checkCanDropRole(ConnectorSecurityContext context, String role)
    {
        if (!isAdmin(context)) {
            denyDropRole(role);
        }
    }

    @Override
    public void checkCanGrantRoles(ConnectorSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, String catalogName)
    {
        // currently specifying grantor is supported by metastore, but it is not supported by Hive itself
        if (grantor.isPresent()) {
            throw new AccessDeniedException("Hive Connector does not support GRANTED BY statement");
        }
        if (!hasAdminOptionForRoles(context, roles)) {
            denyGrantRoles(roles, grantees);
        }
    }

    @Override
    public void checkCanRevokeRoles(ConnectorSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, String catalogName)
    {
        // currently specifying grantor is supported by metastore, but it is not supported by Hive itself
        if (grantor.isPresent()) {
            throw new AccessDeniedException("Hive Connector does not support GRANTED BY statement");
        }
        if (!hasAdminOptionForRoles(context, roles)) {
            denyRevokeRoles(roles, grantees);
        }
    }

    @Override
    public void checkCanSetRole(ConnectorSecurityContext context, String role, String catalogName)
    {
        if (!isRoleApplicable(new HivePrincipal(USER, context.getIdentity().getUser()), role, hivePrincipal -> metastore.listRoleGrants(context, hivePrincipal))) {
            denySetRole(role);
        }
    }

    @Override
    public void checkCanShowRoleAuthorizationDescriptors(ConnectorSecurityContext context, String catalogName)
    {
        if (!isAdmin(context)) {
            denyShowRoleAuthorizationDescriptors(catalogName);
        }
    }

    @Override
    public void checkCanShowRoles(ConnectorSecurityContext context, String catalogName)
    {
        if (!isAdmin(context)) {
            denyShowRoles(catalogName);
        }
    }

    @Override
    public void checkCanShowCurrentRoles(ConnectorSecurityContext context, String catalogName)
    {
    }

    @Override
    public void checkCanShowRoleGrants(ConnectorSecurityContext context, String catalogName)
    {
    }

    @Override
    public void checkCanExecuteProcedure(ConnectorSecurityContext context, SchemaRoutineName procedure)
    {
    }

    @Override
    public Optional<ViewExpression> getRowFilter(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        return Optional.empty();
    }

    @Override
    public Optional<ViewExpression> getColumnMask(ConnectorSecurityContext context, SchemaTableName tableName, String columnName, Type type)
    {
        return Optional.empty();
    }

    private boolean isAdmin(ConnectorSecurityContext context)
    {
        return isRoleEnabled(context.getIdentity(), hivePrincipal -> metastore.listRoleGrants(context, hivePrincipal), ADMIN_ROLE_NAME);
    }

    private boolean isDatabaseOwner(ConnectorSecurityContext context, String databaseName)
    {
        // all users are "owners" of the default database
        if (DEFAULT_DATABASE_NAME.equalsIgnoreCase(databaseName)) {
            return true;
        }

        if (isAdmin(context)) {
            return true;
        }

        Optional<Database> databaseMetadata = metastore.getDatabase(context, databaseName);
        if (databaseMetadata.isEmpty()) {
            return false;
        }

        Database database = databaseMetadata.get();

        // a database can be owned by a user or role
        ConnectorIdentity identity = context.getIdentity();
        if (database.getOwnerType() == USER && identity.getUser().equals(database.getOwnerName())) {
            return true;
        }
        if (database.getOwnerType() == ROLE && isRoleEnabled(identity, hivePrincipal -> metastore.listRoleGrants(context, hivePrincipal), database.getOwnerName())) {
            return true;
        }
        return false;
    }

    private boolean isTableOwner(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        return checkTablePermission(context, tableName, OWNERSHIP, false);
    }

    private boolean checkTablePermission(
            ConnectorSecurityContext context,
            SchemaTableName tableName,
            HivePrivilege requiredPrivilege,
            boolean grantOptionRequired)
    {
        if (isAdmin(context)) {
            return true;
        }

        if (tableName.equals(ROLES) || tableName.equals(ROLE_AUHTORIZATION_DESCRIPTORS)) {
            return false;
        }

        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return true;
        }

        Set<HivePrincipal> allowedPrincipals = metastore.listTablePrivileges(context, new HiveIdentity(context.getIdentity()), tableName.getSchemaName(), tableName.getTableName(), Optional.empty()).stream()
                .filter(privilegeInfo -> privilegeInfo.getHivePrivilege() == requiredPrivilege)
                .filter(privilegeInfo -> !grantOptionRequired || privilegeInfo.isGrantOption())
                .map(HivePrivilegeInfo::getGrantee)
                .collect(toImmutableSet());

        return listEnabledPrincipals(context.getIdentity(), hivePrincipal -> metastore.listRoleGrants(context, hivePrincipal))
                .anyMatch(allowedPrincipals::contains);
    }

    private boolean hasGrantOptionForPrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName)
    {
        if (isAdmin(context)) {
            return true;
        }

        return listApplicableTablePrivileges(
                context,
                tableName.getSchemaName(),
                tableName.getTableName(),
                context.getIdentity())
                .anyMatch(privilegeInfo -> privilegeInfo.getHivePrivilege() == toHivePrivilege(privilege) && privilegeInfo.isGrantOption());
    }

    private Stream<HivePrivilegeInfo> listApplicableTablePrivileges(ConnectorSecurityContext context, String databaseName, String tableName, ConnectorIdentity identity)
    {
        String user = identity.getUser();
        HivePrincipal userPrincipal = new HivePrincipal(USER, user);
        Stream<HivePrincipal> principals = Stream.concat(
                Stream.of(userPrincipal),
                listApplicableRoles(userPrincipal, hivePrincipal -> metastore.listRoleGrants(context, hivePrincipal))
                        .map(role -> new HivePrincipal(ROLE, role.getRoleName())));
        return listTablePrivileges(context, new HiveIdentity(identity), databaseName, tableName, principals);
    }

    private Stream<HivePrivilegeInfo> listTablePrivileges(ConnectorSecurityContext context, HiveIdentity identity, String databaseName, String tableName, Stream<HivePrincipal> principals)
    {
        return principals.flatMap(principal -> metastore.listTablePrivileges(context, identity, databaseName, tableName, Optional.of(principal)).stream());
    }

    private boolean hasAdminOptionForRoles(ConnectorSecurityContext context, Set<String> roles)
    {
        if (isAdmin(context)) {
            return true;
        }

        Set<String> rolesWithGrantOption = listApplicableRoles(new HivePrincipal(USER, context.getIdentity().getUser()), hivePrincipal -> metastore.listRoleGrants(context, hivePrincipal))
                .filter(RoleGrant::isGrantable)
                .map(RoleGrant::getRoleName)
                .collect(toSet());
        return rolesWithGrantOption.containsAll(roles);
    }

    private boolean hasAnyTablePermission(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        if (isAdmin(context)) {
            return true;
        }

        if (tableName.equals(ROLES)) {
            return false;
        }

        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return true;
        }

        Set<HivePrincipal> allowedPrincipals = metastore.listTablePrivileges(context, new HiveIdentity(context.getIdentity()), tableName.getSchemaName(), tableName.getTableName(), Optional.empty()).stream()
                .map(HivePrivilegeInfo::getGrantee)
                .collect(toImmutableSet());

        return listEnabledPrincipals(context.getIdentity(), hivePrincipal -> metastore.listRoleGrants(context, hivePrincipal))
                .anyMatch(allowedPrincipals::contains);
    }
}
