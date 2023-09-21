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
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;

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
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanSetUser(principal, userName);
        }
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        Span span = startSpan("checkCanImpersonateUser");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanImpersonateUser(identity, userName);
        }
    }

    @Override
    public void checkCanReadSystemInformation(Identity identity)
    {
        Span span = startSpan("checkCanReadSystemInformation");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanReadSystemInformation(identity);
        }
    }

    @Override
    public void checkCanWriteSystemInformation(Identity identity)
    {
        Span span = startSpan("checkCanWriteSystemInformation");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanWriteSystemInformation(identity);
        }
    }

    @Override
    public void checkCanExecuteQuery(Identity identity)
    {
        Span span = startSpan("checkCanExecuteQuery");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanExecuteQuery(identity);
        }
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        Span span = startSpan("checkCanViewQueryOwnedBy");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanViewQueryOwnedBy(identity, queryOwner);
        }
    }

    @Override
    public Collection<Identity> filterQueriesOwnedBy(Identity identity, Collection<Identity> queryOwners)
    {
        Span span = startSpan("filterQueriesOwnedBy");
        try (var ignored = scopedSpan(span)) {
            return delegate.filterQueriesOwnedBy(identity, queryOwners);
        }
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        Span span = startSpan("checkCanKillQueryOwnedBy");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanKillQueryOwnedBy(identity, queryOwner);
        }
    }

    @Override
    public void checkCanCreateCatalog(SecurityContext context, String catalog)
    {
        Span span = startSpan("checkCanCreateCatalog");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanCreateCatalog(context, catalog);
        }
    }

    @Override
    public void checkCanDropCatalog(SecurityContext context, String catalog)
    {
        Span span = startSpan("checkCanDropCatalog");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanDropCatalog(context, catalog);
        }
    }

    @Override
    public Set<String> filterCatalogs(SecurityContext context, Set<String> catalogs)
    {
        Span span = startSpan("filterCatalogs");
        try (var ignored = scopedSpan(span)) {
            return delegate.filterCatalogs(context, catalogs);
        }
    }

    @Override
    public void checkCanCreateSchema(SecurityContext context, CatalogSchemaName schemaName, Map<String, Object> properties)
    {
        Span span = startSpan("checkCanCreateSchema");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanCreateSchema(context, schemaName, properties);
        }
    }

    @Override
    public void checkCanDropSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
        Span span = startSpan("checkCanDropSchema");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanDropSchema(context, schemaName);
        }
    }

    @Override
    public void checkCanRenameSchema(SecurityContext context, CatalogSchemaName schemaName, String newSchemaName)
    {
        Span span = startSpan("checkCanRenameSchema");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanRenameSchema(context, schemaName, newSchemaName);
        }
    }

    @Override
    public void checkCanSetSchemaAuthorization(SecurityContext context, CatalogSchemaName schemaName, TrinoPrincipal principal)
    {
        Span span = startSpan("checkCanSetSchemaAuthorization");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanSetSchemaAuthorization(context, schemaName, principal);
        }
    }

    @Override
    public void checkCanShowSchemas(SecurityContext context, String catalogName)
    {
        Span span = startSpan("checkCanShowSchemas");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanShowSchemas(context, catalogName);
        }
    }

    @Override
    public Set<String> filterSchemas(SecurityContext context, String catalogName, Set<String> schemaNames)
    {
        Span span = startSpan("filterSchemas");
        try (var ignored = scopedSpan(span)) {
            return delegate.filterSchemas(context, catalogName, schemaNames);
        }
    }

    @Override
    public void checkCanShowCreateSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
        Span span = startSpan("checkCanShowCreateSchema");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanShowCreateSchema(context, schemaName);
        }
    }

    @Override
    public void checkCanShowCreateTable(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanShowCreateTable");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanShowCreateTable(context, tableName);
        }
    }

    @Override
    public void checkCanCreateTable(SecurityContext context, QualifiedObjectName tableName, Map<String, Object> properties)
    {
        Span span = startSpan("checkCanCreateTable");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanCreateTable(context, tableName, properties);
        }
    }

    @Override
    public void checkCanDropTable(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanDropTable");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanDropTable(context, tableName);
        }
    }

    @Override
    public void checkCanRenameTable(SecurityContext context, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        Span span = startSpan("checkCanRenameTable");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanRenameTable(context, tableName, newTableName);
        }
    }

    @Override
    public void checkCanSetTableProperties(SecurityContext context, QualifiedObjectName tableName, Map<String, Optional<Object>> properties)
    {
        Span span = startSpan("checkCanSetTableProperties");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanSetTableProperties(context, tableName, properties);
        }
    }

    @Override
    public void checkCanSetTableComment(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanSetTableComment");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanSetTableComment(context, tableName);
        }
    }

    @Override
    public void checkCanSetViewComment(SecurityContext context, QualifiedObjectName viewName)
    {
        Span span = startSpan("checkCanSetViewComment");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanSetViewComment(context, viewName);
        }
    }

    @Override
    public void checkCanSetColumnComment(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanSetColumnComment");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanSetColumnComment(context, tableName);
        }
    }

    @Override
    public void checkCanShowTables(SecurityContext context, CatalogSchemaName schema)
    {
        Span span = startSpan("checkCanShowTables");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanShowTables(context, schema);
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(SecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        Span span = startSpan("filterTables");
        try (var ignored = scopedSpan(span)) {
            return delegate.filterTables(context, catalogName, tableNames);
        }
    }

    @Override
    public void checkCanShowColumns(SecurityContext context, CatalogSchemaTableName table)
    {
        Span span = startSpan("checkCanShowColumns");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanShowColumns(context, table);
        }
    }

    @Override
    public Set<String> filterColumns(SecurityContext context, CatalogSchemaTableName tableName, Set<String> columns)
    {
        Span span = startSpan("filterColumns");
        try (var ignored = scopedSpan(span)) {
            return delegate.filterColumns(context, tableName, columns);
        }
    }

    @Override
    public void checkCanAddColumns(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanAddColumns");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanAddColumns(context, tableName);
        }
    }

    @Override
    public void checkCanDropColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanDropColumn");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanDropColumn(context, tableName);
        }
    }

    @Override
    public void checkCanAlterColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanAlterColumn");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanAlterColumn(context, tableName);
        }
    }

    @Override
    public void checkCanSetTableAuthorization(SecurityContext context, QualifiedObjectName tableName, TrinoPrincipal principal)
    {
        Span span = startSpan("checkCanSetTableAuthorization");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanSetTableAuthorization(context, tableName, principal);
        }
    }

    @Override
    public void checkCanRenameColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanRenameColumn");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanRenameColumn(context, tableName);
        }
    }

    @Override
    public void checkCanInsertIntoTable(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanInsertIntoTable");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanInsertIntoTable(context, tableName);
        }
    }

    @Override
    public void checkCanDeleteFromTable(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanDeleteFromTable");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanDeleteFromTable(context, tableName);
        }
    }

    @Override
    public void checkCanTruncateTable(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("checkCanTruncateTable");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanTruncateTable(context, tableName);
        }
    }

    @Override
    public void checkCanUpdateTableColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> updatedColumnNames)
    {
        Span span = startSpan("checkCanUpdateTableColumns");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanUpdateTableColumns(context, tableName, updatedColumnNames);
        }
    }

    @Override
    public void checkCanCreateView(SecurityContext context, QualifiedObjectName viewName)
    {
        Span span = startSpan("checkCanCreateView");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanCreateView(context, viewName);
        }
    }

    @Override
    public void checkCanRenameView(SecurityContext context, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
        Span span = startSpan("checkCanRenameView");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanRenameView(context, viewName, newViewName);
        }
    }

    @Override
    public void checkCanSetViewAuthorization(SecurityContext context, QualifiedObjectName view, TrinoPrincipal principal)
    {
        Span span = startSpan("checkCanSetViewAuthorization");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanSetViewAuthorization(context, view, principal);
        }
    }

    @Override
    public void checkCanDropView(SecurityContext context, QualifiedObjectName viewName)
    {
        Span span = startSpan("checkCanDropView");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanDropView(context, viewName);
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        Span span = startSpan("checkCanCreateViewWithSelectFromColumns");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanCreateViewWithSelectFromColumns(context, tableName, columnNames);
        }
    }

    @Override
    public void checkCanCreateMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName, Map<String, Object> properties)
    {
        Span span = startSpan("checkCanCreateMaterializedView");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanCreateMaterializedView(context, materializedViewName, properties);
        }
    }

    @Override
    public void checkCanRefreshMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
        Span span = startSpan("checkCanRefreshMaterializedView");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanRefreshMaterializedView(context, materializedViewName);
        }
    }

    @Override
    public void checkCanDropMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
        Span span = startSpan("checkCanDropMaterializedView");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanDropMaterializedView(context, materializedViewName);
        }
    }

    @Override
    public void checkCanRenameMaterializedView(SecurityContext context, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
        Span span = startSpan("checkCanRenameMaterializedView");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanRenameMaterializedView(context, viewName, newViewName);
        }
    }

    @Override
    public void checkCanSetMaterializedViewProperties(SecurityContext context, QualifiedObjectName materializedViewName, Map<String, Optional<Object>> properties)
    {
        Span span = startSpan("checkCanSetMaterializedViewProperties");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanSetMaterializedViewProperties(context, materializedViewName, properties);
        }
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SecurityContext context, String functionName, Identity grantee, boolean grantOption)
    {
        Span span = startSpan("checkCanGrantExecuteFunctionPrivilege");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanGrantExecuteFunctionPrivilege(context, functionName, grantee, grantOption);
        }
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SecurityContext context, FunctionKind functionKind, QualifiedObjectName functionName, Identity grantee, boolean grantOption)
    {
        Span span = startSpan("checkCanGrantExecuteFunctionPrivilege");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanGrantExecuteFunctionPrivilege(context, functionKind, functionName, grantee, grantOption);
        }
    }

    @Override
    public void checkCanGrantSchemaPrivilege(SecurityContext context, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal grantee, boolean grantOption)
    {
        Span span = startSpan("checkCanGrantSchemaPrivilege");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanGrantSchemaPrivilege(context, privilege, schemaName, grantee, grantOption);
        }
    }

    @Override
    public void checkCanDenySchemaPrivilege(SecurityContext context, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal grantee)
    {
        Span span = startSpan("checkCanDenySchemaPrivilege");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanDenySchemaPrivilege(context, privilege, schemaName, grantee);
        }
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(SecurityContext context, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal revokee, boolean grantOption)
    {
        Span span = startSpan("checkCanRevokeSchemaPrivilege");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanRevokeSchemaPrivilege(context, privilege, schemaName, revokee, grantOption);
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal grantee, boolean grantOption)
    {
        Span span = startSpan("checkCanGrantTablePrivilege");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanGrantTablePrivilege(context, privilege, tableName, grantee, grantOption);
        }
    }

    @Override
    public void checkCanDenyTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal grantee)
    {
        Span span = startSpan("checkCanDenyTablePrivilege");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanDenyTablePrivilege(context, privilege, tableName, grantee);
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal revokee, boolean grantOption)
    {
        Span span = startSpan("checkCanRevokeTablePrivilege");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanRevokeTablePrivilege(context, privilege, tableName, revokee, grantOption);
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        Span span = startSpan("checkCanSetSystemSessionProperty");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanSetSystemSessionProperty(identity, propertyName);
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SecurityContext context, String catalogName, String propertyName)
    {
        Span span = startSpan("checkCanSetCatalogSessionProperty");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanSetCatalogSessionProperty(context, catalogName, propertyName);
        }
    }

    @Override
    public void checkCanSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        Span span = startSpan("checkCanSelectFromColumns");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanSelectFromColumns(context, tableName, columnNames);
        }
    }

    @Override
    public void checkCanCreateRole(SecurityContext context, String role, Optional<TrinoPrincipal> grantor, Optional<String> catalogName)
    {
        Span span = startSpan("checkCanCreateRole");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanCreateRole(context, role, grantor, catalogName);
        }
    }

    @Override
    public void checkCanDropRole(SecurityContext context, String role, Optional<String> catalogName)
    {
        Span span = startSpan("checkCanDropRole");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanDropRole(context, role, catalogName);
        }
    }

    @Override
    public void checkCanGrantRoles(SecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalogName)
    {
        Span span = startSpan("checkCanGrantRoles");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanGrantRoles(context, roles, grantees, adminOption, grantor, catalogName);
        }
    }

    @Override
    public void checkCanRevokeRoles(SecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalogName)
    {
        Span span = startSpan("checkCanRevokeRoles");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanRevokeRoles(context, roles, grantees, adminOption, grantor, catalogName);
        }
    }

    @Override
    public void checkCanSetCatalogRole(SecurityContext context, String role, String catalogName)
    {
        Span span = startSpan("checkCanSetCatalogRole");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanSetCatalogRole(context, role, catalogName);
        }
    }

    @Override
    public void checkCanShowRoles(SecurityContext context, Optional<String> catalogName)
    {
        Span span = startSpan("checkCanShowRoles");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanShowRoles(context, catalogName);
        }
    }

    @Override
    public void checkCanShowCurrentRoles(SecurityContext context, Optional<String> catalogName)
    {
        Span span = startSpan("checkCanShowCurrentRoles");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanShowCurrentRoles(context, catalogName);
        }
    }

    @Override
    public void checkCanShowRoleGrants(SecurityContext context, Optional<String> catalogName)
    {
        Span span = startSpan("checkCanShowRoleGrants");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanShowRoleGrants(context, catalogName);
        }
    }

    @Override
    public void checkCanExecuteProcedure(SecurityContext context, QualifiedObjectName procedureName)
    {
        Span span = startSpan("checkCanExecuteProcedure");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanExecuteProcedure(context, procedureName);
        }
    }

    @Override
    public void checkCanExecuteFunction(SecurityContext context, String functionName)
    {
        Span span = startSpan("checkCanExecuteFunction");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanExecuteFunction(context, functionName);
        }
    }

    @Override
    public void checkCanExecuteFunction(SecurityContext context, FunctionKind functionKind, QualifiedObjectName functionName)
    {
        Span span = startSpan("checkCanExecuteFunction");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanExecuteFunction(context, functionKind, functionName);
        }
    }

    @Override
    public void checkCanExecuteTableProcedure(SecurityContext context, QualifiedObjectName tableName, String procedureName)
    {
        Span span = startSpan("checkCanExecuteTableProcedure");
        try (var ignored = scopedSpan(span)) {
            delegate.checkCanExecuteTableProcedure(context, tableName, procedureName);
        }
    }

    @Override
    public List<ViewExpression> getRowFilters(SecurityContext context, QualifiedObjectName tableName)
    {
        Span span = startSpan("getRowFilters");
        try (var ignored = scopedSpan(span)) {
            return delegate.getRowFilters(context, tableName);
        }
    }

    @Override
    public Optional<ViewExpression> getColumnMask(SecurityContext context, QualifiedObjectName tableName, String columnName, Type type)
    {
        Span span = startSpan("getColumnMask");
        try (var ignored = scopedSpan(span)) {
            return delegate.getColumnMask(context, tableName, columnName, type);
        }
    }

    private Span startSpan(String methodName)
    {
        return tracer.spanBuilder("AccessControl." + methodName)
                .startSpan();
    }
}
