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
package io.prestosql.plugin.hive.security;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.base.CatalogName;
import io.prestosql.plugin.hive.HiveTransactionHandle;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HivePrincipal;
import io.prestosql.plugin.hive.metastore.HivePrivilegeInfo;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorSecurityContext;
import io.prestosql.spi.connector.SchemaRoutineName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.Type;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.plugin.hive.metastore.Database.DEFAULT_DATABASE_NAME;
import static io.prestosql.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import static io.prestosql.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege.DELETE;
import static io.prestosql.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege.INSERT;
import static io.prestosql.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static io.prestosql.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege.SELECT;
import static io.prestosql.plugin.hive.metastore.HivePrivilegeInfo.toHivePrivilege;
import static io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreUtil.isRoleApplicable;
import static io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreUtil.isRoleEnabled;
import static io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreUtil.listApplicableRoles;
import static io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreUtil.listApplicableTablePrivileges;
import static io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreUtil.listEnabledPrincipals;
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
import static io.prestosql.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denyInsertTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameView;
import static io.prestosql.spi.security.AccessDeniedException.denyRevokeRoles;
import static io.prestosql.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denySelectTable;
import static io.prestosql.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static io.prestosql.spi.security.AccessDeniedException.denySetRole;
import static io.prestosql.spi.security.AccessDeniedException.denySetSchemaAuthorization;
import static io.prestosql.spi.security.AccessDeniedException.denyShowColumns;
import static io.prestosql.spi.security.AccessDeniedException.denyShowCreateSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyShowCreateTable;
import static io.prestosql.spi.security.AccessDeniedException.denyShowRoleAuthorizationDescriptors;
import static io.prestosql.spi.security.AccessDeniedException.denyShowRoles;
import static io.prestosql.spi.security.PrincipalType.ROLE;
import static io.prestosql.spi.security.PrincipalType.USER;
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
    private final Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider;

    @Inject
    public SqlStandardAccessControl(
            CatalogName catalogName,
            Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null").toString();
        this.metastoreProvider = requireNonNull(metastoreProvider, "metastoreProvider is null");
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
    public void checkCanSetSchemaAuthorization(ConnectorSecurityContext context, String schemaName, PrestoPrincipal principal)
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
    public List<ColumnMetadata> filterColumns(ConnectorSecurityContext context, SchemaTableName tableName, List<ColumnMetadata> columns)
    {
        if (!hasAnyTablePermission(context, tableName)) {
            return ImmutableList.of();
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
    public void checkCanSetCatalogSessionProperty(ConnectorSecurityContext context, String propertyName)
    {
        if (!isAdmin(context)) {
            denySetCatalogSessionProperty(catalogName, propertyName);
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, PrestoPrincipal grantee, boolean grantOption)
    {
        if (isTableOwner(context, tableName)) {
            return;
        }

        if (!hasGrantOptionForPrivilege(context, privilege, tableName)) {
            denyGrantTablePrivilege(privilege.name(), tableName.toString());
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, PrestoPrincipal revokee, boolean grantOption)
    {
        if (isTableOwner(context, tableName)) {
            return;
        }

        if (!hasGrantOptionForPrivilege(context, privilege, tableName)) {
            denyRevokeTablePrivilege(privilege.name(), tableName.toString());
        }
    }

    @Override
    public void checkCanCreateRole(ConnectorSecurityContext context, String role, Optional<PrestoPrincipal> grantor)
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
    public void checkCanGrantRoles(ConnectorSecurityContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOption, Optional<PrestoPrincipal> grantor, String catalogName)
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
    public void checkCanRevokeRoles(ConnectorSecurityContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOption, Optional<PrestoPrincipal> grantor, String catalogName)
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
        SemiTransactionalHiveMetastore metastore = metastoreProvider.apply(((HiveTransactionHandle) context.getTransactionHandle()));
        if (!isRoleApplicable(metastore, new HivePrincipal(USER, context.getIdentity().getUser()), role)) {
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
        SemiTransactionalHiveMetastore metastore = metastoreProvider.apply(((HiveTransactionHandle) context.getTransactionHandle()));
        return isRoleEnabled(context.getIdentity(), metastore::listRoleGrants, ADMIN_ROLE_NAME);
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

        SemiTransactionalHiveMetastore metastore = metastoreProvider.apply(((HiveTransactionHandle) context.getTransactionHandle()));
        Optional<Database> databaseMetadata = metastore.getDatabase(databaseName);
        if (databaseMetadata.isEmpty()) {
            return false;
        }

        Database database = databaseMetadata.get();

        // a database can be owned by a user or role
        ConnectorIdentity identity = context.getIdentity();
        if (database.getOwnerType() == USER && identity.getUser().equals(database.getOwnerName())) {
            return true;
        }
        if (database.getOwnerType() == ROLE && isRoleEnabled(identity, metastore::listRoleGrants, database.getOwnerName())) {
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

        SemiTransactionalHiveMetastore metastore = metastoreProvider.apply(((HiveTransactionHandle) context.getTransactionHandle()));

        Set<HivePrincipal> allowedPrincipals = metastore.listTablePrivileges(new HiveIdentity(context.getIdentity()), tableName.getSchemaName(), tableName.getTableName(), Optional.empty()).stream()
                .filter(privilegeInfo -> privilegeInfo.getHivePrivilege() == requiredPrivilege)
                .filter(privilegeInfo -> !grantOptionRequired || privilegeInfo.isGrantOption())
                .map(HivePrivilegeInfo::getGrantee)
                .collect(toImmutableSet());

        return listEnabledPrincipals(metastore, context.getIdentity())
                .anyMatch(allowedPrincipals::contains);
    }

    private boolean hasGrantOptionForPrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName)
    {
        if (isAdmin(context)) {
            return true;
        }

        SemiTransactionalHiveMetastore metastore = metastoreProvider.apply(((HiveTransactionHandle) context.getTransactionHandle()));
        return listApplicableTablePrivileges(
                metastore,
                tableName.getSchemaName(),
                tableName.getTableName(),
                context.getIdentity())
                .anyMatch(privilegeInfo -> privilegeInfo.getHivePrivilege() == toHivePrivilege(privilege) && privilegeInfo.isGrantOption());
    }

    private boolean hasAdminOptionForRoles(ConnectorSecurityContext context, Set<String> roles)
    {
        if (isAdmin(context)) {
            return true;
        }

        SemiTransactionalHiveMetastore metastore = metastoreProvider.apply(((HiveTransactionHandle) context.getTransactionHandle()));
        Set<String> rolesWithGrantOption = listApplicableRoles(new HivePrincipal(USER, context.getIdentity().getUser()), metastore::listRoleGrants)
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

        SemiTransactionalHiveMetastore metastore = metastoreProvider.apply(((HiveTransactionHandle) context.getTransactionHandle()));
        Set<HivePrincipal> allowedPrincipals = metastore.listTablePrivileges(new HiveIdentity(context.getIdentity()), tableName.getSchemaName(), tableName.getTableName(), Optional.empty()).stream()
                .map(HivePrivilegeInfo::getGrantee)
                .collect(toImmutableSet());

        return listEnabledPrincipals(metastore, context.getIdentity())
                .anyMatch(allowedPrincipals::contains);
    }
}
