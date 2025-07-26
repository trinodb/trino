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
package io.trino.tracing;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.metadata.QualifiedObjectName;
import io.trino.security.AccessControl;
import io.trino.security.SecurityContext;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.connector.EntityPrivilege;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;

import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.tracing.ScopedSpan.scopedSpan;
import static java.util.Objects.requireNonNull;

public class TracingAccessControl
        implements AccessControl
{
    private final Tracer tracer;
    private final AccessControl delegate;

    @Inject
    public TracingAccessControl(Tracer tracer, @ForTracing AccessControl delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
    }

    @VisibleForTesting
    public AccessControl getDelegate()
    {
        return delegate;
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        Span span = startSpan("checkCanSetUser");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanSetUser(principal, userName);
        }
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        Span span = startSpan("checkCanImpersonateUser");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanImpersonateUser(identity, userName);
        }
    }

    @Override
    public void checkCanReadSystemInformation(Identity identity)
    {
        Span span = startSpan("checkCanReadSystemInformation");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanReadSystemInformation(identity);
        }
    }

    @Override
    public void checkCanWriteSystemInformation(Identity identity)
    {
        Span span = startSpan("checkCanWriteSystemInformation");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanWriteSystemInformation(identity);
        }
    }

    @Override
    public void checkCanExecuteQuery(Identity identity, QueryId queryId)
    {
        Span span = startSpan("checkCanExecuteQuery");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanExecuteQuery(identity, queryId);
        }
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        Span span = startSpan("checkCanViewQueryOwnedBy");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanViewQueryOwnedBy(identity, queryOwner);
        }
    }

    @Override
    public Collection<Identity> filterQueriesOwnedBy(Identity identity, Collection<Identity> queryOwners)
    {
        Span span = startSpan("filterQueriesOwnedBy");
        try (var _ = scopedSpan(span)) {
            return delegate.filterQueriesOwnedBy(identity, queryOwners);
        }
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        Span span = startSpan("checkCanKillQueryOwnedBy");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanKillQueryOwnedBy(identity, queryOwner);
        }
    }

    @Override
    public void checkCanCreateCatalog(SecurityContext context, String catalog)
    {
        Span span = startSpan("checkCanCreateCatalog");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanCreateCatalog(context, catalog);
        }
    }

    @Override
    public void checkCanDropCatalog(SecurityContext context, String catalog)
    {
        Span span = startSpan("checkCanDropCatalog");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanDropCatalog(context, catalog);
        }
    }

    @Override
    public Set<String> filterCatalogs(SecurityContext context, Set<String> catalogs)
    {
        Span span = startSpan("filterCatalogs");
        try (var _ = scopedSpan(span)) {
            return delegate.filterCatalogs(context, catalogs);
        }
    }

    @Override
    public void checkCanCreateSchema(SecurityContext context, CatalogSchemaName schemaName, Map<String, Object> properties)
    {
        Span span = startSpan("checkCanCreateSchema");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanCreateSchema(context, schemaName, properties);
        }
    }

    @Override
    public void checkCanDropSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
        Span span = startSpan("checkCanDropSchema");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanDropSchema(context, schemaName);
        }
    }

    @Override
    public void checkCanRenameSchema(SecurityContext context, CatalogSchemaName schemaName, String newSchemaName)
    {
        Span span = startSpan("checkCanRenameSchema");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanRenameSchema(context, schemaName, newSchemaName);
        }
    }

    @Override
    public void checkCanShowSchemas(SecurityContext context, String catalogName)
    {
        Span span = startSpan("checkCanShowSchemas");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanShowSchemas(context, catalogName);
        }
    }

    @Override
    public Set<String> filterSchemas(SecurityContext context, String catalogName, Set<String> schemaNames)
    {
        Span span = startSpan("filterSchemas");
        try (var _ = scopedSpan(span)) {
            return delegate.filterSchemas(context, catalogName, schemaNames);
        }
    }

    @Override
    public void checkCanShowCreateSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
        Span span = startSpan("checkCanShowCreateSchema");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanShowCreateSchema(context, schemaName);
        }
    }

    @Override
    public void checkCanShowCreateTable(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanShowCreateTable");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanShowCreateTable(context, tableName);
        }
    }

    @Override
    public void checkCanCreateTable(SecurityContext context, QualifiedObjectName tableName, Map<String, Object> properties)
    {
        Span span = startSpan("checkCanCreateTable");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanCreateTable(context, tableName, properties);
        }
    }

    @Override
    public void checkCanDropTable(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanDropTable");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanDropTable(context, tableName);
        }
    }

    @Override
    public void checkCanRenameTable(SecurityContext context, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        Span span = startSpan("checkCanRenameTable");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanRenameTable(context, tableName, newTableName);
        }
    }

    @Override
    public void checkCanSetTableProperties(SecurityContext context, QualifiedObjectName tableName, Map<String, Optional<Object>> properties)
    {
        Span span = startSpan("checkCanSetTableProperties");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanSetTableProperties(context, tableName, properties);
        }
    }

    @Override
    public void checkCanSetTableComment(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanSetTableComment");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanSetTableComment(context, tableName);
        }
    }

    @Override
    public void checkCanSetViewComment(SecurityContext context, QualifiedObjectName viewName)
    {
        Span span = startSpan("checkCanSetViewComment");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanSetViewComment(context, viewName);
        }
    }

    @Override
    public void checkCanSetColumnComment(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanSetColumnComment");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanSetColumnComment(context, tableName);
        }
    }

    @Override
    public void checkCanShowTables(SecurityContext context, CatalogSchemaName schema)
    {
        Span span = startSpan("checkCanShowTables");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanShowTables(context, schema);
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(SecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        Span span = startSpan("filterTables");
        try (var _ = scopedSpan(span)) {
            return delegate.filterTables(context, catalogName, tableNames);
        }
    }

    @Override
    public void checkCanShowColumns(SecurityContext context, CatalogSchemaTableName table)
    {
        Span span = startSpan("checkCanShowColumns");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanShowColumns(context, table);
        }
    }

    @Override
    public Map<SchemaTableName, Set<String>> filterColumns(SecurityContext context, String catalogName, Map<SchemaTableName, Set<String>> tableColumns)
    {
        Span span = startSpan("filterColumns");
        try (var _ = scopedSpan(span)) {
            return delegate.filterColumns(context, catalogName, tableColumns);
        }
    }

    @Override
    public void checkCanAddColumns(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanAddColumns");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanAddColumns(context, tableName);
        }
    }

    @Override
    public void checkCanDropColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanDropColumn");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanDropColumn(context, tableName);
        }
    }

    @Override
    public void checkCanAlterColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanAlterColumn");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanAlterColumn(context, tableName);
        }
    }

    @Override
    public void checkCanRenameColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanRenameColumn");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanRenameColumn(context, tableName);
        }
    }

    @Override
    public void checkCanInsertIntoTable(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanInsertIntoTable");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanInsertIntoTable(context, tableName);
        }
    }

    @Override
    public void checkCanDeleteFromTable(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanDeleteFromTable");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanDeleteFromTable(context, tableName);
        }
    }

    @Override
    public void checkCanTruncateTable(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanTruncateTable");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanTruncateTable(context, tableName);
        }
    }

    @Override
    public void checkCanUpdateTableColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> updatedColumnNames)
    {
        Span span = startSpan("checkCanUpdateTableColumns");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanUpdateTableColumns(context, tableName, updatedColumnNames);
        }
    }

    @Override
    public void checkCanCreateView(SecurityContext context, QualifiedObjectName viewName)
    {
        Span span = startSpan("checkCanCreateView");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanCreateView(context, viewName);
        }
    }

    @Override
    public void checkCanRenameView(SecurityContext context, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
        Span span = startSpan("checkCanRenameView");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanRenameView(context, viewName, newViewName);
        }
    }

    @Override
    public void checkCanRefreshView(SecurityContext context, QualifiedObjectName viewName)
    {
        Span span = startSpan("checkCanRefreshView");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanRefreshView(context, viewName);
        }
    }

    @Override
    public void checkCanDropView(SecurityContext context, QualifiedObjectName viewName)
    {
        Span span = startSpan("checkCanDropView");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanDropView(context, viewName);
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        Span span = startSpan("checkCanCreateViewWithSelectFromColumns");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanCreateViewWithSelectFromColumns(context, tableName, columnNames);
        }
    }

    @Override
    public void checkCanCreateMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName, Map<String, Object> properties)
    {
        Span span = startSpan("checkCanCreateMaterializedView");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanCreateMaterializedView(context, materializedViewName, properties);
        }
    }

    @Override
    public void checkCanRefreshMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
        Span span = startSpan("checkCanRefreshMaterializedView");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanRefreshMaterializedView(context, materializedViewName);
        }
    }

    @Override
    public void checkCanDropMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
        Span span = startSpan("checkCanDropMaterializedView");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanDropMaterializedView(context, materializedViewName);
        }
    }

    @Override
    public void checkCanRenameMaterializedView(SecurityContext context, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
        Span span = startSpan("checkCanRenameMaterializedView");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanRenameMaterializedView(context, viewName, newViewName);
        }
    }

    @Override
    public void checkCanSetMaterializedViewProperties(SecurityContext context, QualifiedObjectName materializedViewName, Map<String, Optional<Object>> properties)
    {
        Span span = startSpan("checkCanSetMaterializedViewProperties");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanSetMaterializedViewProperties(context, materializedViewName, properties);
        }
    }

    @Override
    public void checkCanGrantSchemaPrivilege(SecurityContext context, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal grantee, boolean grantOption)
    {
        Span span = startSpan("checkCanGrantSchemaPrivilege");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanGrantSchemaPrivilege(context, privilege, schemaName, grantee, grantOption);
        }
    }

    @Override
    public void checkCanDenySchemaPrivilege(SecurityContext context, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal grantee)
    {
        Span span = startSpan("checkCanDenySchemaPrivilege");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanDenySchemaPrivilege(context, privilege, schemaName, grantee);
        }
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(SecurityContext context, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal revokee, boolean grantOption)
    {
        Span span = startSpan("checkCanRevokeSchemaPrivilege");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanRevokeSchemaPrivilege(context, privilege, schemaName, revokee, grantOption);
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal grantee, boolean grantOption)
    {
        Span span = startSpan("checkCanGrantTablePrivilege");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanGrantTablePrivilege(context, privilege, tableName, grantee, grantOption);
        }
    }

    @Override
    public void checkCanDenyTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal grantee)
    {
        Span span = startSpan("checkCanDenyTablePrivilege");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanDenyTablePrivilege(context, privilege, tableName, grantee);
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal revokee, boolean grantOption)
    {
        Span span = startSpan("checkCanRevokeTablePrivilege");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanRevokeTablePrivilege(context, privilege, tableName, revokee, grantOption);
        }
    }

    @Override
    public void checkCanGrantTableBranchPrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, String branchName, TrinoPrincipal grantee, boolean grantOption)
    {
        Span span = startSpan("checkCanGrantTableBranchPrivilege");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanGrantTableBranchPrivilege(context, privilege, tableName, branchName, grantee, grantOption);
        }
    }

    @Override
    public void checkCanDenyTableBranchPrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, String branchName, TrinoPrincipal grantee)
    {
        Span span = startSpan("checkCanDenyTableBranchPrivilege");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanDenyTableBranchPrivilege(context, privilege, tableName, branchName, grantee);
        }
    }

    @Override
    public void checkCanRevokeTableBranchPrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, String branchName, TrinoPrincipal revokee, boolean grantOption)
    {
        Span span = startSpan("checkCanRevokeTableBranchPrivilege");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanRevokeTableBranchPrivilege(context, privilege, tableName, branchName, revokee, grantOption);
        }
    }

    @Override
    public void checkCanGrantEntityPrivilege(SecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal grantee, boolean grantOption)
    {
        Span span = startSpan("checkCanGrantEntityPrivilege");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanGrantEntityPrivilege(context, privilege, entity, grantee, grantOption);
        }
    }

    @Override
    public void checkCanDenyEntityPrivilege(SecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal grantee)
    {
        Span span = startSpan("checkCanDenyEntityPrivilege");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanDenyEntityPrivilege(context, privilege, entity, grantee);
        }
    }

    @Override
    public void checkCanRevokeEntityPrivilege(SecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal revokee, boolean grantOption)
    {
        Span span = startSpan("checkCanRevokeEntityPrivilege");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanRevokeEntityPrivilege(context, privilege, entity, revokee, grantOption);
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, QueryId queryId, String propertyName)
    {
        Span span = startSpan("checkCanSetSystemSessionProperty");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanSetSystemSessionProperty(identity, queryId, propertyName);
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SecurityContext context, String catalogName, String propertyName)
    {
        Span span = startSpan("checkCanSetCatalogSessionProperty");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanSetCatalogSessionProperty(context, catalogName, propertyName);
        }
    }

    @Override
    public void checkCanSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        Span span = startSpan("checkCanSelectFromColumns");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanSelectFromColumns(context, tableName, columnNames);
        }
    }

    @Override
    public void checkCanCreateRole(SecurityContext context, String role, Optional<TrinoPrincipal> grantor, Optional<String> catalogName)
    {
        Span span = startSpan("checkCanCreateRole");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanCreateRole(context, role, grantor, catalogName);
        }
    }

    @Override
    public void checkCanDropRole(SecurityContext context, String role, Optional<String> catalogName)
    {
        Span span = startSpan("checkCanDropRole");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanDropRole(context, role, catalogName);
        }
    }

    @Override
    public void checkCanGrantRoles(SecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalogName)
    {
        Span span = startSpan("checkCanGrantRoles");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanGrantRoles(context, roles, grantees, adminOption, grantor, catalogName);
        }
    }

    @Override
    public void checkCanRevokeRoles(SecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalogName)
    {
        Span span = startSpan("checkCanRevokeRoles");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanRevokeRoles(context, roles, grantees, adminOption, grantor, catalogName);
        }
    }

    @Override
    public void checkCanSetCatalogRole(SecurityContext context, String role, String catalogName)
    {
        Span span = startSpan("checkCanSetCatalogRole");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanSetCatalogRole(context, role, catalogName);
        }
    }

    @Override
    public void checkCanShowRoles(SecurityContext context, Optional<String> catalogName)
    {
        Span span = startSpan("checkCanShowRoles");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanShowRoles(context, catalogName);
        }
    }

    @Override
    public void checkCanShowCurrentRoles(SecurityContext context, Optional<String> catalogName)
    {
        Span span = startSpan("checkCanShowCurrentRoles");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanShowCurrentRoles(context, catalogName);
        }
    }

    @Override
    public void checkCanShowRoleGrants(SecurityContext context, Optional<String> catalogName)
    {
        Span span = startSpan("checkCanShowRoleGrants");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanShowRoleGrants(context, catalogName);
        }
    }

    @Override
    public void checkCanExecuteProcedure(SecurityContext context, QualifiedObjectName procedureName)
    {
        Span span = startSpan("checkCanExecuteProcedure");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanExecuteProcedure(context, procedureName);
        }
    }

    @Override
    public boolean canExecuteFunction(SecurityContext context, QualifiedObjectName functionName)
    {
        Span span = startSpan("canExecuteFunction");
        try (var _ = scopedSpan(span)) {
            return delegate.canExecuteFunction(context, functionName);
        }
    }

    @Override
    public boolean canCreateViewWithExecuteFunction(SecurityContext context, QualifiedObjectName functionName)
    {
        Span span = startSpan("canCreateViewWithExecuteFunction");
        try (var _ = scopedSpan(span)) {
            return delegate.canCreateViewWithExecuteFunction(context, functionName);
        }
    }

    @Override
    public void checkCanExecuteTableProcedure(SecurityContext context, QualifiedObjectName tableName, String procedureName)
    {
        Span span = startSpan("checkCanExecuteTableProcedure");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanExecuteTableProcedure(context, tableName, procedureName);
        }
    }

    @Override
    public void checkCanShowFunctions(SecurityContext context, CatalogSchemaName schema)
    {
        Span span = startSpan("checkCanShowFunctions");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanShowFunctions(context, schema);
        }
    }

    @Override
    public Set<SchemaFunctionName> filterFunctions(SecurityContext context, String catalogName, Set<SchemaFunctionName> functionNames)
    {
        Span span = startSpan("filterFunctions");
        try (var _ = scopedSpan(span)) {
            return delegate.filterFunctions(context, catalogName, functionNames);
        }
    }

    @Override
    public void checkCanCreateFunction(SecurityContext context, QualifiedObjectName functionName)
    {
        Span span = startSpan("checkCanCreateFunction");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanCreateFunction(context, functionName);
        }
    }

    @Override
    public void checkCanDropFunction(SecurityContext context, QualifiedObjectName functionName)
    {
        Span span = startSpan("checkCanDropFunction");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanDropFunction(context, functionName);
        }
    }

    @Override
    public void checkCanShowCreateFunction(SecurityContext context, QualifiedObjectName functionName)
    {
        Span span = startSpan("checkCanShowCreateFunction");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanShowCreateFunction(context, functionName);
        }
    }

    @Override
    public void checkCanShowBranches(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanShowBranches");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanShowBranches(context, tableName);
        }
    }

    @Override
    public void checkCanCreateBranch(SecurityContext context, QualifiedObjectName tableName, String branchName)
    {
        Span span = startSpan("checkCanCreateBranch");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanCreateBranch(context, tableName, branchName);
        }
    }

    @Override
    public void checkCanDropBranch(SecurityContext context, QualifiedObjectName tableName, String branchName)
    {
        Span span = startSpan("checkCanDropBranch");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanDropBranch(context, tableName, branchName);
        }
    }

    @Override
    public void checkCanFastForwardBranch(SecurityContext context, QualifiedObjectName tableName, String sourceBranchName, String targetBranchName)
    {
        Span span = startSpan("checkCanFastForwardBranch");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanFastForwardBranch(context, tableName, sourceBranchName, targetBranchName);
        }
    }

    @Override
    public List<ViewExpression> getRowFilters(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("getRowFilters");
        try (var _ = scopedSpan(span)) {
            return delegate.getRowFilters(context, tableName);
        }
    }

    @Override
    public Map<ColumnSchema, ViewExpression> getColumnMasks(SecurityContext context, QualifiedObjectName tableName, List<ColumnSchema> columns)
    {
        Span span = startSpan("getColumnMasks");
        try (var _ = scopedSpan(span)) {
            return delegate.getColumnMasks(context, tableName, columns);
        }
    }

    @Override
    public void checkCanSetEntityAuthorization(SecurityContext context, EntityKindAndName entityKindAndName, TrinoPrincipal principal)
    {
        Span span = startSpan("checkCanSetEntityAuthorization");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanSetEntityAuthorization(context, entityKindAndName, principal);
        }
    }

    private Span startSpan(String methodName)
    {
        return tracer.spanBuilder("AccessControl." + methodName)
                .startSpan();
    }
}
