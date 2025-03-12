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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.connector.EntityPrivilege;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemAccessControlFactory;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;

import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;

public class AllowAllSystemAccessControl
        implements SystemAccessControl
{
    public static final String NAME = "allow-all";

    public static final AllowAllSystemAccessControl INSTANCE = new AllowAllSystemAccessControl();

    public static class Factory
            implements SystemAccessControlFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public SystemAccessControl create(Map<String, String> config, SystemAccessControlContext context)
        {
            checkArgument(config.isEmpty(), "This access controller does not support any configuration properties");
            return INSTANCE;
        }
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName) {}

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName) {}

    @Override
    public void checkCanReadSystemInformation(Identity identity) {}

    @Override
    public void checkCanWriteSystemInformation(Identity identity) {}

    @Override
    public void checkCanExecuteQuery(Identity identity, QueryId queryId) {}

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner) {}

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner) {}

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(Identity identity, Collection<Identity> queryOwners)
    {
        return queryOwners;
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, QueryId queryId, String propertyName) {}

    @Override
    public boolean canAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        return true;
    }

    @Override
    public void checkCanCreateCatalog(SystemSecurityContext context, String catalog) {}

    @Override
    public void checkCanDropCatalog(SystemSecurityContext context, String catalog) {}

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        return catalogs;
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema, Map<String, Object> properties) {}

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema) {}

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName) {}

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, TrinoPrincipal principal) {}

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName) {}

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schemaName) {}

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table) {}

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Object> properties) {}

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table) {}

    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable) {}

    @Override
    public void checkCanSetTableProperties(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Optional<Object>> properties) {}

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table) {}

    @Override
    public void checkCanSetViewComment(SystemSecurityContext context, CatalogSchemaTableName view) {}

    @Override
    public void checkCanSetColumnComment(SystemSecurityContext context, CatalogSchemaTableName table) {}

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema) {}

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table) {}

    @Override
    public Set<String> filterColumns(SystemSecurityContext context, CatalogSchemaTableName tableName, Set<String> columns)
    {
        return columns;
    }

    @Override
    public Map<SchemaTableName, Set<String>> filterColumns(SystemSecurityContext context, String catalogName, Map<SchemaTableName, Set<String>> tableColumns)
    {
        return tableColumns;
    }

    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table) {}

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table) {}

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table) {}

    @Override
    public void checkCanAlterColumn(SystemSecurityContext context, CatalogSchemaTableName table) {}

    @Override
    public void checkCanSetTableAuthorization(SystemSecurityContext context, CatalogSchemaTableName table, TrinoPrincipal principal) {}

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns) {}

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table) {}

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table) {}

    @Override
    public void checkCanTruncateTable(SystemSecurityContext context, CatalogSchemaTableName table) {}

    @Override
    public void checkCanUpdateTableColumns(SystemSecurityContext securityContext, CatalogSchemaTableName table, Set<String> updatedColumnNames) {}

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view) {}

    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView) {}

    @Override
    public void checkCanSetViewAuthorization(SystemSecurityContext context, CatalogSchemaTableName view, TrinoPrincipal principal) {}

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view) {}

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns) {}

    @Override
    public void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Object> properties) {}

    @Override
    public void checkCanRefreshMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView) {}

    @Override
    public void checkCanDropMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView) {}

    @Override
    public void checkCanRenameMaterializedView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView) {}

    @Override
    public void checkCanSetMaterializedViewProperties(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Optional<Object>> properties) {}

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName) {}

    @Override
    public void checkCanGrantSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee, boolean grantOption) {}

    @Override
    public void checkCanDenySchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee) {}

    @Override
    public void checkCanRevokeSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal revokee, boolean grantOption) {}

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee, boolean grantOption) {}

    @Override
    public void checkCanDenyTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee) {}

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal revokee, boolean grantOption) {}

    @Override
    public void checkCanGrantEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal grantee, boolean grantOption) {}

    @Override
    public void checkCanDenyEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal grantee) {}

    @Override
    public void checkCanRevokeEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal revokee, boolean grantOption) {}

    @Override
    public void checkCanShowRoles(SystemSecurityContext context) {}

    @Override
    public void checkCanCreateRole(SystemSecurityContext context, String role, Optional<TrinoPrincipal> grantor) {}

    @Override
    public void checkCanDropRole(SystemSecurityContext context, String role) {}

    @Override
    public void checkCanGrantRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor) {}

    @Override
    public void checkCanRevokeRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor) {}

    @Override
    public void checkCanShowCurrentRoles(SystemSecurityContext context) {}

    @Override
    public void checkCanShowRoleGrants(SystemSecurityContext context) {}

    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName procedure) {}

    @Override
    public boolean canExecuteFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        return true;
    }

    @Override
    public boolean canCreateViewWithExecuteFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        return true;
    }

    @Override
    public void checkCanExecuteTableProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaTableName table, String procedure) {}

    @Override
    public void checkCanShowFunctions(SystemSecurityContext context, CatalogSchemaName schema) {}

    @Override
    public Set<SchemaFunctionName> filterFunctions(SystemSecurityContext context, String catalogName, Set<SchemaFunctionName> functionNames)
    {
        return functionNames;
    }

    @Override
    public void checkCanCreateFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName) {}

    @Override
    public void checkCanDropFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName) {}

    @Override
    public void checkCanShowCreateFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName) {}

    @Override
    public Iterable<EventListener> getEventListeners()
    {
        return ImmutableSet.of();
    }

    @Override
    public List<ViewExpression> getRowFilters(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        return emptyList();
    }

    @Override
    public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, CatalogSchemaTableName tableName, String columnName, Type type)
    {
        return Optional.empty();
    }

    @Override
    public Map<ColumnSchema, ViewExpression> getColumnMasks(SystemSecurityContext context, CatalogSchemaTableName tableName, List<ColumnSchema> columns)
    {
        return ImmutableMap.of();
    }

    @Override
    public void shutdown() {}
}
