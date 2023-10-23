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
package io.trino.plugin.opa;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.opa.schema.OpaQueryContext;
import io.trino.plugin.opa.schema.OpaQueryInput;
import io.trino.plugin.opa.schema.OpaQueryInputAction;
import io.trino.plugin.opa.schema.OpaQueryInputGrant;
import io.trino.plugin.opa.schema.OpaQueryInputResource;
import io.trino.plugin.opa.schema.TrinoColumn;
import io.trino.plugin.opa.schema.TrinoFunction;
import io.trino.plugin.opa.schema.TrinoGrantPrincipal;
import io.trino.plugin.opa.schema.TrinoSchema;
import io.trino.plugin.opa.schema.TrinoTable;
import io.trino.plugin.opa.schema.TrinoUser;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;

import java.security.Principal;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.opa.OpaHighLevelClient.DenyCallable;
import static io.trino.plugin.opa.OpaHighLevelClient.buildQueryInputForSimpleResource;
import static io.trino.spi.security.AccessDeniedException.denyCreateRole;
import static io.trino.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.trino.spi.security.AccessDeniedException.denyDenySchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyDenyTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyExecuteTableProcedure;
import static io.trino.spi.security.AccessDeniedException.denyGrantRoles;
import static io.trino.spi.security.AccessDeniedException.denyGrantSchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyImpersonateUser;
import static io.trino.spi.security.AccessDeniedException.denyRenameMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameSchema;
import static io.trino.spi.security.AccessDeniedException.denyRenameTable;
import static io.trino.spi.security.AccessDeniedException.denyRenameView;
import static io.trino.spi.security.AccessDeniedException.denyRevokeRoles;
import static io.trino.spi.security.AccessDeniedException.denyRevokeSchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static io.trino.spi.security.AccessDeniedException.denySetSchemaAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetTableAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetViewAuthorization;

public class OpaAccessControl
        implements SystemAccessControl
{
    protected final OpaHighLevelClient opaHighLevelClient;

    @Inject
    public OpaAccessControl(OpaHighLevelClient opaHighLevelClient)
    {
        this.opaHighLevelClient = opaHighLevelClient;
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromIdentity(identity),
                "ImpersonateUser",
                () -> denyImpersonateUser(identity.getUser(), userName),
                OpaQueryInputResource.Builder::user,
                new TrinoUser(userName));
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        // This function is called for every query, asking if the user can become themselves, resulting in e.g.
        // Access Denied: Principal bob cannot become user bob
        //
        // The function is deprecated, so let's no-op
    }

    @Override
    public void checkCanExecuteQuery(Identity identity)
    {
        opaHighLevelClient.queryAndEnforce(OpaQueryContext.fromIdentity(identity), "ExecuteQuery", AccessDeniedException::denyExecuteQuery);
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        opaHighLevelClient.queryAndEnforce(OpaQueryContext.fromIdentity(identity), "ViewQueryOwnedBy", AccessDeniedException::denyViewQuery, queryOwner);
    }

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(Identity identity, Collection<Identity> queryOwners)
    {
        return opaHighLevelClient.parallelFilterFromOpa(
                queryOwners,
                queryOwner -> buildQueryInputForSimpleResource(
                        OpaQueryContext.fromIdentity(identity),
                        "FilterViewQueryOwnedBy",
                        OpaQueryInputResource.builder().user(new TrinoUser(queryOwner)).build()));
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        opaHighLevelClient.queryAndEnforce(OpaQueryContext.fromIdentity(identity), "KillQueryOwnedBy", AccessDeniedException::denyKillQuery, queryOwner);
    }

    @Override
    public void checkCanReadSystemInformation(Identity identity)
    {
        opaHighLevelClient.queryAndEnforce(OpaQueryContext.fromIdentity(identity), "ReadSystemInformation", AccessDeniedException::denyReadSystemInformationAccess);
    }

    @Override
    public void checkCanWriteSystemInformation(Identity identity)
    {
        opaHighLevelClient.queryAndEnforce(OpaQueryContext.fromIdentity(identity), "WriteSystemInformation", AccessDeniedException::denyWriteSystemInformationAccess);
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromIdentity(identity),
                "SetSystemSessionProperty",
                AccessDeniedException::denySetSystemSessionProperty,
                OpaQueryInputResource.Builder::systemSessionProperty,
                propertyName);
    }

    @Override
    public boolean canAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        return opaHighLevelClient.queryOpaWithSimpleResource(
                OpaQueryContext.fromSystemSecurityContext(context),
                "AccessCatalog",
                OpaQueryInputResource.builder().catalog(catalogName).build());
    }

    @Override
    public void checkCanCreateCatalog(SystemSecurityContext context, String catalog)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "CreateCatalog",
                AccessDeniedException::denyCreateCatalog,
                OpaQueryInputResource.Builder::catalog,
                catalog);
    }

    @Override
    public void checkCanDropCatalog(SystemSecurityContext context, String catalog)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "DropCatalog",
                AccessDeniedException::denyDropCatalog,
                OpaQueryInputResource.Builder::catalog,
                catalog);
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        return opaHighLevelClient.parallelFilterFromOpa(
                catalogs,
                catalog -> buildQueryInputForSimpleResource(
                        OpaQueryContext.fromSystemSecurityContext(context),
                        "FilterCatalogs",
                        OpaQueryInputResource.builder().catalog(catalog).build()));
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema, Map<String, Object> properties)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "CreateSchema",
                AccessDeniedException::denyCreateSchema,
                schema,
                properties);
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "DropSchema",
                AccessDeniedException::denyDropSchema,
                schema);
    }

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder().schema(TrinoSchema.fromTrinoCatalogSchema(schema)).build();
        OpaQueryInputResource targetResource = OpaQueryInputResource.builder()
                .schema(TrinoSchema.builder()
                        .catalogName(schema.getCatalogName())
                        .schemaName(newSchemaName)
                        .build())
                .build();

        OpaQueryContext queryContext = OpaQueryContext.fromSystemSecurityContext(context);

        if (!opaHighLevelClient.queryOpaWithSourceAndTargetResource(queryContext, "RenameSchema", resource, targetResource)) {
            denyRenameSchema(schema.toString(), newSchemaName);
        }
    }

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, TrinoPrincipal principal)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder().schema(TrinoSchema.fromTrinoCatalogSchema(schema)).build();
        OpaQueryInputGrant grantee = OpaQueryInputGrant.builder().principal(TrinoGrantPrincipal.fromTrinoPrincipal(principal)).build();
        OpaQueryInputAction action = OpaQueryInputAction.builder()
                .operation("SetSchemaAuthorization")
                .resource(resource)
                .grantee(grantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denySetSchemaAuthorization(schema.toString(), principal);
        }
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "ShowSchemas",
                (DenyCallable) AccessDeniedException::denyShowSchemas,
                OpaQueryInputResource.Builder::catalog,
                catalogName);
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        return opaHighLevelClient.parallelFilterFromOpa(
                schemaNames,
                schema -> buildQueryInputForSimpleResource(
                        OpaQueryContext.fromSystemSecurityContext(context),
                        "FilterSchemas",
                        OpaQueryInputResource.builder()
                                .schema(TrinoSchema.builder()
                                        .catalogName(catalogName)
                                        .schemaName(schema)
                                        .build())
                                .build()));
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schemaName)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "ShowCreateSchema",
                AccessDeniedException::denyShowCreateSchema,
                schemaName);
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "ShowCreateTable",
                AccessDeniedException::denyShowCreateTable,
                table);
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Object> properties)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "CreateTable",
                AccessDeniedException::denyCreateTable,
                table,
                properties);
    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "DropTable",
                AccessDeniedException::denyDropTable,
                table);
    }

    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        OpaQueryInputResource oldResource = OpaQueryInputResource.builder().table(TrinoTable.fromTrinoTable(table)).build();
        OpaQueryInputResource newResource = OpaQueryInputResource.builder().table(TrinoTable.fromTrinoTable(newTable)).build();
        OpaQueryContext queryContext = OpaQueryContext.fromSystemSecurityContext(context);

        if (!opaHighLevelClient.queryOpaWithSourceAndTargetResource(queryContext, "RenameTable", oldResource, newResource)) {
            denyRenameTable(table.toString(), newTable.toString());
        }
    }

    @Override
    public void checkCanSetTableProperties(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Optional<Object>> properties)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "SetTableProperties",
                AccessDeniedException::denySetTableProperties,
                table,
                properties);
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "SetTableComment",
                AccessDeniedException::denyCommentTable,
                table);
    }

    @Override
    public void checkCanSetViewComment(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "SetViewComment",
                AccessDeniedException::denyCommentView,
                view);
    }

    @Override
    public void checkCanSetColumnComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "SetColumnComment",
                AccessDeniedException::denyCommentColumn,
                table);
    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "ShowTables",
                AccessDeniedException::denyShowTables,
                schema);
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return opaHighLevelClient.parallelFilterFromOpa(
                tableNames,
                table -> buildQueryInputForSimpleResource(
                        OpaQueryContext.fromSystemSecurityContext(context),
                        "FilterTables",
                        OpaQueryInputResource.builder()
                                .table(TrinoTable.builder()
                                        .catalogName(catalogName)
                                        .schemaName(table.getSchemaName())
                                        .tableName(table.getTableName())
                                        .build())
                                .build()));
    }

    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "ShowColumns",
                AccessDeniedException::denyShowColumns,
                table);
    }

    @Override
    public Map<SchemaTableName, Set<String>> filterColumns(SystemSecurityContext context, String catalogName, Map<SchemaTableName, Set<String>> tableColumns)
    {
        ImmutableSet.Builder<TrinoColumn> allColumnsBuilder = ImmutableSet.builder();
        Map<SchemaTableName, ImmutableSet.Builder<String>> resultBuilder = new HashMap<>();

        for (Map.Entry<SchemaTableName, Set<String>> oneTableColumns : tableColumns.entrySet()) {
            SchemaTableName schemaTableName = oneTableColumns.getKey();
            resultBuilder.put(schemaTableName, ImmutableSet.builder());
            for (String columnForTable : oneTableColumns.getValue()) {
                allColumnsBuilder.add(new TrinoColumn(schemaTableName, columnForTable));
            }
        }

        Set<TrinoColumn> filteredColumns = opaHighLevelClient.parallelFilterFromOpa(
                allColumnsBuilder.build(),
                tableColumn -> buildQueryInputForSimpleResource(
                        OpaQueryContext.fromSystemSecurityContext(context),
                        "FilterColumns",
                        OpaQueryInputResource.builder()
                                .table(TrinoTable.builder()
                                        .catalogName(catalogName)
                                        .schemaName(tableColumn.schemaTableName().getSchemaName())
                                        .tableName(tableColumn.schemaTableName().getTableName())
                                        .columns(ImmutableSet.of(tableColumn.columnName()))
                                        .build())
                                .build()));

        for (TrinoColumn filteredColumn : filteredColumns) {
            resultBuilder.get(filteredColumn.schemaTableName()).add(filteredColumn.columnName());
        }

        return resultBuilder.entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, (mapEntry) -> mapEntry.getValue().build()));
    }

    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "AddColumn",
                AccessDeniedException::denyAddColumn,
                table);
    }

    @Override
    public void checkCanAlterColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "AlterColumn",
                AccessDeniedException::denyAlterColumn,
                table);
    }

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "DropColumn",
                AccessDeniedException::denyDropColumn,
                table);
    }

    @Override
    public void checkCanSetTableAuthorization(SystemSecurityContext context, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder().table(TrinoTable.fromTrinoTable(table)).build();
        OpaQueryInputGrant grantee = OpaQueryInputGrant.builder().principal(TrinoGrantPrincipal.fromTrinoPrincipal(principal)).build();
        OpaQueryInputAction action = OpaQueryInputAction.builder()
                .operation("SetTableAuthorization")
                .resource(resource)
                .grantee(grantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denySetTableAuthorization(table.toString(), principal);
        }
    }

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "RenameColumn",
                AccessDeniedException::denyRenameColumn,
                table);
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "SelectFromColumns",
                AccessDeniedException::denySelectColumns,
                table,
                columns);
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "InsertIntoTable",
                AccessDeniedException::denyInsertTable,
                table);
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "DeleteFromTable",
                AccessDeniedException::denyDeleteTable,
                table);
    }

    @Override
    public void checkCanTruncateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "TruncateTable",
                AccessDeniedException::denyTruncateTable,
                table);
    }

    @Override
    public void checkCanUpdateTableColumns(SystemSecurityContext securityContext, CatalogSchemaTableName table, Set<String> updatedColumnNames)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(securityContext),
                "UpdateTableColumns",
                AccessDeniedException::denyUpdateTableColumns,
                table,
                updatedColumnNames);
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "CreateView",
                AccessDeniedException::denyCreateView,
                view);
    }

    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        OpaQueryInputResource oldResource = OpaQueryInputResource.builder().table(TrinoTable.fromTrinoTable(view)).build();
        OpaQueryInputResource newResource = OpaQueryInputResource.builder().table(TrinoTable.fromTrinoTable(newView)).build();
        OpaQueryContext queryContext = OpaQueryContext.fromSystemSecurityContext(context);

        if (!opaHighLevelClient.queryOpaWithSourceAndTargetResource(queryContext, "RenameView", oldResource, newResource)) {
            denyRenameView(view.toString(), newView.toString());
        }
    }

    @Override
    public void checkCanSetViewAuthorization(SystemSecurityContext context, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder().table(TrinoTable.fromTrinoTable(view)).build();
        OpaQueryInputGrant grantee = OpaQueryInputGrant.builder().principal(TrinoGrantPrincipal.fromTrinoPrincipal(principal)).build();
        OpaQueryInputAction action = OpaQueryInputAction.builder()
                .operation("SetViewAuthorization")
                .resource(resource)
                .grantee(grantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denySetViewAuthorization(view.toString(), principal);
        }
    }

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "DropView",
                AccessDeniedException::denyDropView,
                view);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "CreateViewWithSelectFromColumns",
                (tableAsString, columnSet) -> denyCreateViewWithSelect(tableAsString, context.getIdentity()),
                table,
                columns);
    }

    @Override
    public void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Object> properties)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "CreateMaterializedView",
                AccessDeniedException::denyCreateMaterializedView,
                materializedView,
                properties);
    }

    @Override
    public void checkCanRefreshMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "RefreshMaterializedView",
                AccessDeniedException::denyRefreshMaterializedView,
                materializedView);
    }

    @Override
    public void checkCanSetMaterializedViewProperties(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Optional<Object>> properties)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "SetMaterializedViewProperties",
                AccessDeniedException::denySetMaterializedViewProperties,
                materializedView,
                properties);
    }

    @Override
    public void checkCanDropMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "DropMaterializedView",
                AccessDeniedException::denyDropMaterializedView,
                materializedView);
    }

    @Override
    public void checkCanRenameMaterializedView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        OpaQueryInputResource oldResource = OpaQueryInputResource.builder().table(TrinoTable.fromTrinoTable(view)).build();
        OpaQueryInputResource newResource = OpaQueryInputResource.builder().table(TrinoTable.fromTrinoTable(newView)).build();
        OpaQueryContext queryContext = OpaQueryContext.fromSystemSecurityContext(context);

        if (!opaHighLevelClient.queryOpaWithSourceAndTargetResource(queryContext, "RenameMaterializedView", oldResource, newResource)) {
            denyRenameMaterializedView(view.toString(), newView.toString());
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder()
                .catalog(catalogName)
                .catalogSessionProperty(propertyName)
                .build();
        OpaQueryContext queryContext = OpaQueryContext.fromSystemSecurityContext(context);

        if (!opaHighLevelClient.queryOpaWithSimpleResource(queryContext, "SetCatalogSessionProperty", resource)) {
            denySetCatalogSessionProperty(propertyName);
        }
    }

    @Override
    public void checkCanGrantSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee, boolean grantOption)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder().schema(TrinoSchema.fromTrinoCatalogSchema(schema)).build();
        OpaQueryInputGrant opaGrantee = OpaQueryInputGrant.builder()
                .principal(TrinoGrantPrincipal.fromTrinoPrincipal(grantee))
                .grantOption(grantOption)
                .privilege(privilege)
                .build();
        OpaQueryInputAction action = OpaQueryInputAction.builder()
                .operation("GrantSchemaPrivilege")
                .resource(resource)
                .grantee(opaGrantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyGrantSchemaPrivilege(privilege.toString(), schema.toString());
        }
    }

    @Override
    public void checkCanDenySchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder().schema(TrinoSchema.fromTrinoCatalogSchema(schema)).build();
        OpaQueryInputGrant opaGrantee = OpaQueryInputGrant.builder()
                .principal(TrinoGrantPrincipal.fromTrinoPrincipal(grantee))
                .privilege(privilege)
                .build();
        OpaQueryInputAction action = OpaQueryInputAction.builder()
                .operation("DenySchemaPrivilege")
                .resource(resource)
                .grantee(opaGrantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyDenySchemaPrivilege(privilege.toString(), schema.toString());
        }
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal revokee, boolean grantOption)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder().schema(TrinoSchema.fromTrinoCatalogSchema(schema)).build();
        OpaQueryInputGrant opaGrantee = OpaQueryInputGrant.builder()
                .principal(TrinoGrantPrincipal.fromTrinoPrincipal(revokee))
                .grantOption(grantOption)
                .privilege(privilege)
                .build();
        OpaQueryInputAction action = OpaQueryInputAction.builder()
                .operation("RevokeSchemaPrivilege")
                .resource(resource)
                .grantee(opaGrantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyRevokeSchemaPrivilege(privilege.toString(), schema.toString());
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee, boolean grantOption)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder().table(TrinoTable.fromTrinoTable(table)).build();
        OpaQueryInputGrant opaGrantee = OpaQueryInputGrant.builder()
                .principal(TrinoGrantPrincipal.fromTrinoPrincipal(grantee))
                .grantOption(grantOption)
                .privilege(privilege)
                .build();
        OpaQueryInputAction action = OpaQueryInputAction.builder()
                .operation("GrantTablePrivilege")
                .resource(resource)
                .grantee(opaGrantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyGrantTablePrivilege(privilege.toString(), table.toString());
        }
    }

    @Override
    public void checkCanDenyTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder().table(TrinoTable.fromTrinoTable(table)).build();
        OpaQueryInputGrant opaGrantee = OpaQueryInputGrant.builder()
                .principal(TrinoGrantPrincipal.fromTrinoPrincipal(grantee))
                .privilege(privilege)
                .build();
        OpaQueryInputAction action = OpaQueryInputAction.builder()
                .operation("DenyTablePrivilege")
                .resource(resource)
                .grantee(opaGrantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyDenyTablePrivilege(privilege.toString(), table.toString());
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal revokee, boolean grantOption)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder().table(TrinoTable.fromTrinoTable(table)).build();
        OpaQueryInputGrant opaRevokee = OpaQueryInputGrant.builder()
                .principal(TrinoGrantPrincipal.fromTrinoPrincipal(revokee))
                .privilege(privilege)
                .grantOption(grantOption)
                .build();
        OpaQueryInputAction action = OpaQueryInputAction.builder()
                .operation("RevokeTablePrivilege")
                .resource(resource)
                .grantee(opaRevokee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyRevokeTablePrivilege(privilege.toString(), table.toString());
        }
    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "ShowRoles",
                AccessDeniedException::denyShowRoles);
    }

    @Override
    public void checkCanCreateRole(SystemSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder().role(role).build();
        OpaQueryInputAction action = OpaQueryInputAction.builder()
                .operation("CreateRole")
                .resource(resource)
                .grantor(TrinoGrantPrincipal.fromTrinoPrincipal(grantor))
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyCreateRole(role);
        }
    }

    @Override
    public void checkCanDropRole(SystemSecurityContext context, String role)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "DropRole",
                AccessDeniedException::denyDropRole,
                OpaQueryInputResource.Builder::role,
                role);
    }

    @Override
    public void checkCanGrantRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder().roles(roles).build();
        OpaQueryInputGrant opaGrantees = OpaQueryInputGrant.builder()
                .grantOption(adminOption)
                .principals(grantees.stream()
                        .map(TrinoGrantPrincipal::fromTrinoPrincipal)
                        .collect(toImmutableSet()))
                .build();
        OpaQueryInputAction action = OpaQueryInputAction.builder()
                .operation("GrantRoles")
                .resource(resource)
                .grantee(opaGrantees)
                .grantor(TrinoGrantPrincipal.fromTrinoPrincipal(grantor))
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyGrantRoles(roles, grantees);
        }
    }

    @Override
    public void checkCanRevokeRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder().roles(roles).build();
        OpaQueryInputGrant opaGrantees = OpaQueryInputGrant.builder()
                .grantOption(adminOption)
                .principals(grantees.stream()
                        .map(TrinoGrantPrincipal::fromTrinoPrincipal)
                        .collect(toImmutableSet()))
                .build();
        OpaQueryInputAction action = OpaQueryInputAction.builder()
                .operation("RevokeRoles")
                .resource(resource)
                .grantee(opaGrantees)
                .grantor(TrinoGrantPrincipal.fromTrinoPrincipal(grantor))
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyRevokeRoles(roles, grantees);
        }
    }

    @Override
    public void checkCanShowCurrentRoles(SystemSecurityContext context)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "ShowCurrentRoles",
                AccessDeniedException::denyShowCurrentRoles);
    }

    @Override
    public void checkCanShowRoleGrants(SystemSecurityContext context)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "ShowRoleGrants",
                AccessDeniedException::denyShowRoleGrants);
    }

    @Override
    public void checkCanShowFunctions(SystemSecurityContext context, CatalogSchemaName schema)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(context),
                "ShowFunctions",
                AccessDeniedException::denyShowFunctions,
                schema);
    }

    @Override
    public Set<SchemaFunctionName> filterFunctions(SystemSecurityContext context, String catalogName, Set<SchemaFunctionName> functionNames)
    {
        return opaHighLevelClient.parallelFilterFromOpa(
                functionNames,
                function -> buildQueryInputForSimpleResource(
                        OpaQueryContext.fromSystemSecurityContext(context),
                        "FilterFunctions",
                        OpaQueryInputResource.builder()
                                .function(new TrinoFunction(
                                        TrinoSchema.builder()
                                                .catalogName(catalogName)
                                                .schemaName(function.getSchemaName())
                                                .build(),
                                        function.getFunctionName()))
                                .build()));
    }

    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName procedure)
    {
        opaHighLevelClient.queryAndEnforce(
                OpaQueryContext.fromSystemSecurityContext(systemSecurityContext),
                "ExecuteProcedure",
                (deniedProcedure) -> AccessDeniedException.denyExecuteProcedure(deniedProcedure.toString()),
                OpaQueryInputResource.Builder::function,
                TrinoFunction.fromTrinoFunction(procedure));
    }

    @Override
    public boolean canExecuteFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        return opaHighLevelClient.queryOpaWithSimpleResource(
                OpaQueryContext.fromSystemSecurityContext(systemSecurityContext),
                "ExecuteFunction",
                OpaQueryInputResource.builder().function(TrinoFunction.fromTrinoFunction(functionName)).build());
    }

    @Override
    public boolean canCreateViewWithExecuteFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        return opaHighLevelClient.queryOpaWithSimpleResource(
                OpaQueryContext.fromSystemSecurityContext(systemSecurityContext),
                "CreateViewWithExecuteFunction",
                OpaQueryInputResource.builder().function(TrinoFunction.fromTrinoFunction(functionName)).build());
    }

    @Override
    public void checkCanExecuteTableProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaTableName table, String procedure)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder()
                .table(TrinoTable.fromTrinoTable(table))
                .function(procedure)
                .build();
        OpaQueryContext queryContext = OpaQueryContext.fromSystemSecurityContext(systemSecurityContext);

        if (!opaHighLevelClient.queryOpaWithSimpleResource(queryContext, "ExecuteTableProcedure", resource)) {
            denyExecuteTableProcedure(table.toString(), procedure);
        }
    }
}
