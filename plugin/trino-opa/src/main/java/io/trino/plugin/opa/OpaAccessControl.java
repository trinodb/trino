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
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimaps;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.opa.schema.OpaPluginContext;
import io.trino.plugin.opa.schema.OpaQueryContext;
import io.trino.plugin.opa.schema.OpaQueryInput;
import io.trino.plugin.opa.schema.OpaQueryInputAction;
import io.trino.plugin.opa.schema.OpaQueryInputResource;
import io.trino.plugin.opa.schema.OpaViewExpression;
import io.trino.plugin.opa.schema.TrinoCatalogSessionProperty;
import io.trino.plugin.opa.schema.TrinoFunction;
import io.trino.plugin.opa.schema.TrinoGrantPrincipal;
import io.trino.plugin.opa.schema.TrinoIdentity;
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
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;

import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.opa.OpaHighLevelClient.buildQueryInputForSimpleResource;
import static io.trino.spi.security.AccessDeniedException.denyCreateCatalog;
import static io.trino.spi.security.AccessDeniedException.denyCreateFunction;
import static io.trino.spi.security.AccessDeniedException.denyCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.trino.spi.security.AccessDeniedException.denyDropCatalog;
import static io.trino.spi.security.AccessDeniedException.denyDropFunction;
import static io.trino.spi.security.AccessDeniedException.denyDropSchema;
import static io.trino.spi.security.AccessDeniedException.denyExecuteProcedure;
import static io.trino.spi.security.AccessDeniedException.denyExecuteTableProcedure;
import static io.trino.spi.security.AccessDeniedException.denyImpersonateUser;
import static io.trino.spi.security.AccessDeniedException.denyRenameMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameSchema;
import static io.trino.spi.security.AccessDeniedException.denyRenameTable;
import static io.trino.spi.security.AccessDeniedException.denyRenameView;
import static io.trino.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static io.trino.spi.security.AccessDeniedException.denySetSchemaAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetSystemSessionProperty;
import static io.trino.spi.security.AccessDeniedException.denySetTableAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetViewAuthorization;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyShowFunctions;
import static io.trino.spi.security.AccessDeniedException.denyShowTables;
import static java.util.Objects.requireNonNull;

public sealed class OpaAccessControl
        implements SystemAccessControl
        permits OpaBatchAccessControl
{
    private final LifeCycleManager lifeCycleManager;
    private final OpaHighLevelClient opaHighLevelClient;
    private final boolean allowPermissionManagementOperations;
    private final OpaPluginContext pluginContext;

    @Inject
    public OpaAccessControl(LifeCycleManager lifeCycleManager, OpaHighLevelClient opaHighLevelClient, OpaConfig config, OpaPluginContext pluginContext)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.opaHighLevelClient = requireNonNull(opaHighLevelClient, "opaHighLevelClient is null");
        this.allowPermissionManagementOperations = config.getAllowPermissionManagementOperations();
        this.pluginContext = requireNonNull(pluginContext, "pluginContext is null");
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(identity),
                "ImpersonateUser",
                () -> denyImpersonateUser(identity.getUser(), userName),
                OpaQueryInputResource.builder().user(new TrinoUser(userName)).build());
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {}

    @Override
    public void checkCanExecuteQuery(Identity identity)
    {
        opaHighLevelClient.queryAndEnforce(buildQueryContext(identity), "ExecuteQuery", AccessDeniedException::denyExecuteQuery);
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        opaHighLevelClient.queryAndEnforce(buildQueryContext(identity), "ViewQueryOwnedBy", AccessDeniedException::denyViewQuery, OpaQueryInputResource.builder().user(new TrinoUser(queryOwner)).build());
    }

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(Identity identity, Collection<Identity> queryOwners)
    {
        return opaHighLevelClient.parallelFilterFromOpa(
                queryOwners,
                queryOwner -> buildQueryInputForSimpleResource(
                        buildQueryContext(identity),
                        "FilterViewQueryOwnedBy",
                        OpaQueryInputResource.builder().user(new TrinoUser(queryOwner)).build()));
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        opaHighLevelClient.queryAndEnforce(buildQueryContext(identity), "KillQueryOwnedBy", AccessDeniedException::denyKillQuery, OpaQueryInputResource.builder().user(new TrinoUser(queryOwner)).build());
    }

    @Override
    public void checkCanReadSystemInformation(Identity identity)
    {
        opaHighLevelClient.queryAndEnforce(buildQueryContext(identity), "ReadSystemInformation", AccessDeniedException::denyReadSystemInformationAccess);
    }

    @Override
    public void checkCanWriteSystemInformation(Identity identity)
    {
        opaHighLevelClient.queryAndEnforce(buildQueryContext(identity), "WriteSystemInformation", AccessDeniedException::denyWriteSystemInformationAccess);
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(identity),
                "SetSystemSessionProperty",
                () -> denySetSystemSessionProperty(propertyName),
                OpaQueryInputResource.builder().systemSessionProperty(propertyName).build());
    }

    @Override
    public boolean canAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        return opaHighLevelClient.queryOpaWithSimpleResource(
                buildQueryContext(context),
                "AccessCatalog",
                OpaQueryInputResource.builder().catalog(catalogName).build());
    }

    @Override
    public void checkCanCreateCatalog(SystemSecurityContext context, String catalog)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(context),
                "CreateCatalog",
                () -> denyCreateCatalog(catalog),
                OpaQueryInputResource.builder().catalog(catalog).build());
    }

    @Override
    public void checkCanDropCatalog(SystemSecurityContext context, String catalog)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(context),
                "DropCatalog",
                () -> denyDropCatalog(catalog),
                OpaQueryInputResource.builder().catalog(catalog).build());
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        return opaHighLevelClient.parallelFilterFromOpa(
                catalogs,
                catalog -> buildQueryInputForSimpleResource(
                        buildQueryContext(context),
                        "FilterCatalogs",
                        OpaQueryInputResource.builder().catalog(catalog).build()));
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema, Map<String, Object> properties)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(context),
                "CreateSchema",
                () -> denyCreateSchema(schema.toString()),
                OpaQueryInputResource.builder().schema(new TrinoSchema(schema).withProperties(convertProperties(properties))).build());
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(context),
                "DropSchema",
                () -> denyDropSchema(schema.toString()),
                OpaQueryInputResource.builder().schema(new TrinoSchema(schema)).build());
    }

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder().schema(new TrinoSchema(schema)).build();
        OpaQueryInputResource targetResource = OpaQueryInputResource.builder().schema(new TrinoSchema(schema.getCatalogName(), newSchemaName)).build();

        OpaQueryContext queryContext = buildQueryContext(context);

        if (!opaHighLevelClient.queryOpaWithSourceAndTargetResource(queryContext, "RenameSchema", resource, targetResource)) {
            denyRenameSchema(schema.toString(), newSchemaName);
        }
    }

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, TrinoPrincipal principal)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder().schema(new TrinoSchema(schema)).build();
        OpaQueryInputAction action = OpaQueryInputAction.builder()
                .operation("SetSchemaAuthorization")
                .resource(resource)
                .grantee(TrinoGrantPrincipal.fromTrinoPrincipal(principal))
                .build();
        OpaQueryInput input = new OpaQueryInput(buildQueryContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denySetSchemaAuthorization(schema.toString(), principal);
        }
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(context),
                "ShowSchemas",
                AccessDeniedException::denyShowSchemas,
                OpaQueryInputResource.builder().catalog(catalogName).build());
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        return opaHighLevelClient.parallelFilterFromOpa(
                schemaNames,
                schema -> buildQueryInputForSimpleResource(
                        buildQueryContext(context),
                        "FilterSchemas",
                        OpaQueryInputResource.builder().schema(new TrinoSchema(catalogName, schema)).build()));
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schemaName)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(context),
                "ShowCreateSchema",
                () -> denyShowCreateSchema(schemaName.toString()),
                OpaQueryInputResource.builder().schema(new TrinoSchema(schemaName)).build());
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkTableOperation(context, "ShowCreateTable", table, AccessDeniedException::denyShowCreateTable);
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Object> properties)
    {
        checkTableAndPropertiesOperation(context, "CreateTable", table, convertProperties(properties), AccessDeniedException::denyCreateTable);
    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkTableOperation(context, "DropTable", table, AccessDeniedException::denyDropTable);
    }

    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        OpaQueryInputResource oldResource = OpaQueryInputResource.builder().table(new TrinoTable(table)).build();
        OpaQueryInputResource newResource = OpaQueryInputResource.builder().table(new TrinoTable(newTable)).build();
        OpaQueryContext queryContext = buildQueryContext(context);

        if (!opaHighLevelClient.queryOpaWithSourceAndTargetResource(queryContext, "RenameTable", oldResource, newResource)) {
            denyRenameTable(table.toString(), newTable.toString());
        }
    }

    @Override
    public void checkCanSetTableProperties(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Optional<Object>> properties)
    {
        checkTableAndPropertiesOperation(context, "SetTableProperties", table, properties, AccessDeniedException::denySetTableProperties);
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkTableOperation(context, "SetTableComment", table, AccessDeniedException::denyCommentTable);
    }

    @Override
    public void checkCanSetViewComment(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        checkTableOperation(context, "SetViewComment", view, AccessDeniedException::denyCommentView);
    }

    @Override
    public void checkCanSetColumnComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkTableOperation(context, "SetColumnComment", table, AccessDeniedException::denyCommentColumn);
    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(context),
                "ShowTables",
                () -> denyShowTables(schema.toString()),
                OpaQueryInputResource.builder().schema(new TrinoSchema(schema)).build());
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return opaHighLevelClient.parallelFilterFromOpa(
                tableNames,
                table -> buildQueryInputForSimpleResource(
                        buildQueryContext(context),
                        "FilterTables",
                        OpaQueryInputResource.builder()
                                .table(new TrinoTable(catalogName, table.getSchemaName(), table.getTableName()))
                                .build()));
    }

    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkTableOperation(context, "ShowColumns", table, AccessDeniedException::denyShowColumns);
    }

    @Override
    public Map<SchemaTableName, Set<String>> filterColumns(SystemSecurityContext context, String catalogName, Map<SchemaTableName, Set<String>> tableColumns)
    {
        ImmutableSet.Builder<TrinoTable> allColumnsBuilder = ImmutableSet.builder();
        for (Map.Entry<SchemaTableName, Set<String>> entry : tableColumns.entrySet()) {
            SchemaTableName schemaTableName = entry.getKey();
            TrinoTable trinoTable = new TrinoTable(catalogName, schemaTableName.getSchemaName(), schemaTableName.getTableName());
            for (String columnName : entry.getValue()) {
                allColumnsBuilder.add(trinoTable.withColumns(ImmutableSet.of(columnName)));
            }
        }
        Set<TrinoTable> filteredColumns = opaHighLevelClient.parallelFilterFromOpa(
                allColumnsBuilder.build(),
                tableColumn -> buildQueryInputForSimpleResource(
                        buildQueryContext(context),
                        "FilterColumns",
                        OpaQueryInputResource.builder().table(tableColumn).build()));

        ImmutableSetMultimap.Builder<SchemaTableName, String> results = ImmutableSetMultimap.builder();
        for (TrinoTable tableColumn : filteredColumns) {
            results.put(new SchemaTableName(tableColumn.schemaName(), tableColumn.tableName()), getOnlyElement(tableColumn.columns()));
        }
        return Multimaps.asMap(results.build());
    }

    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkTableOperation(context, "AddColumn", table, AccessDeniedException::denyAddColumn);
    }

    @Override
    public void checkCanAlterColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkTableOperation(context, "AlterColumn", table, AccessDeniedException::denyAlterColumn);
    }

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkTableOperation(context, "DropColumn", table, AccessDeniedException::denyDropColumn);
    }

    @Override
    public void checkCanSetTableAuthorization(SystemSecurityContext context, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder().table(new TrinoTable(table)).build();
        OpaQueryInputAction action = OpaQueryInputAction.builder()
                .operation("SetTableAuthorization")
                .resource(resource)
                .grantee(TrinoGrantPrincipal.fromTrinoPrincipal(principal))
                .build();
        OpaQueryInput input = new OpaQueryInput(buildQueryContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denySetTableAuthorization(table.toString(), principal);
        }
    }

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkTableOperation(context, "RenameColumn", table, AccessDeniedException::denyRenameColumn);
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        checkTableAndColumnsOperation(context, "SelectFromColumns", table, columns, AccessDeniedException::denySelectColumns);
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkTableOperation(context, "InsertIntoTable", table, AccessDeniedException::denyInsertTable);
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkTableOperation(context, "DeleteFromTable", table, AccessDeniedException::denyDeleteTable);
    }

    @Override
    public void checkCanTruncateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkTableOperation(context, "TruncateTable", table, AccessDeniedException::denyTruncateTable);
    }

    @Override
    public void checkCanUpdateTableColumns(SystemSecurityContext securityContext, CatalogSchemaTableName table, Set<String> updatedColumnNames)
    {
        checkTableAndColumnsOperation(securityContext, "UpdateTableColumns", table, updatedColumnNames, AccessDeniedException::denyUpdateTableColumns);
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        checkTableOperation(context, "CreateView", view, AccessDeniedException::denyCreateView);
    }

    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        OpaQueryInputResource oldResource = OpaQueryInputResource.builder().table(new TrinoTable(view)).build();
        OpaQueryInputResource newResource = OpaQueryInputResource.builder().table(new TrinoTable(newView)).build();
        OpaQueryContext queryContext = buildQueryContext(context);

        if (!opaHighLevelClient.queryOpaWithSourceAndTargetResource(queryContext, "RenameView", oldResource, newResource)) {
            denyRenameView(view.toString(), newView.toString());
        }
    }

    @Override
    public void checkCanSetViewAuthorization(SystemSecurityContext context, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        OpaQueryInputResource resource = OpaQueryInputResource.builder().table(new TrinoTable(view)).build();
        OpaQueryInputAction action = OpaQueryInputAction.builder()
                .operation("SetViewAuthorization")
                .resource(resource)
                .grantee(TrinoGrantPrincipal.fromTrinoPrincipal(principal))
                .build();
        OpaQueryInput input = new OpaQueryInput(buildQueryContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denySetViewAuthorization(view.toString(), principal);
        }
    }

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        checkTableOperation(context, "DropView", view, AccessDeniedException::denyDropView);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        checkTableAndColumnsOperation(context, "CreateViewWithSelectFromColumns", table, columns, (tableAsString, columnSet) -> denyCreateViewWithSelect(tableAsString, context.getIdentity()));
    }

    @Override
    public void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Object> properties)
    {
        checkTableAndPropertiesOperation(context, "CreateMaterializedView", materializedView, convertProperties(properties), AccessDeniedException::denyCreateMaterializedView);
    }

    @Override
    public void checkCanRefreshMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        checkTableOperation(context, "RefreshMaterializedView", materializedView, AccessDeniedException::denyRefreshMaterializedView);
    }

    @Override
    public void checkCanSetMaterializedViewProperties(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Optional<Object>> properties)
    {
        checkTableAndPropertiesOperation(context, "SetMaterializedViewProperties", materializedView, properties, AccessDeniedException::denySetMaterializedViewProperties);
    }

    @Override
    public void checkCanDropMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        checkTableOperation(context, "DropMaterializedView", materializedView, AccessDeniedException::denyDropMaterializedView);
    }

    @Override
    public void checkCanRenameMaterializedView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        OpaQueryInputResource oldResource = OpaQueryInputResource.builder().table(new TrinoTable(view)).build();
        OpaQueryInputResource newResource = OpaQueryInputResource.builder().table(new TrinoTable(newView)).build();
        OpaQueryContext queryContext = buildQueryContext(context);

        if (!opaHighLevelClient.queryOpaWithSourceAndTargetResource(queryContext, "RenameMaterializedView", oldResource, newResource)) {
            denyRenameMaterializedView(view.toString(), newView.toString());
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(context),
                "SetCatalogSessionProperty",
                () -> denySetCatalogSessionProperty(propertyName),
                OpaQueryInputResource.builder().catalogSessionProperty(new TrinoCatalogSessionProperty(catalogName, propertyName)).build());
    }

    @Override
    public void checkCanGrantSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee, boolean grantOption)
    {
        enforcePermissionManagementOperation(AccessDeniedException::denyGrantSchemaPrivilege, privilege.toString(), schema.toString());
    }

    @Override
    public void checkCanDenySchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee)
    {
        enforcePermissionManagementOperation(AccessDeniedException::denyDenySchemaPrivilege, privilege.toString(), schema.toString());
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal revokee, boolean grantOption)
    {
        enforcePermissionManagementOperation(AccessDeniedException::denyRevokeSchemaPrivilege, privilege.toString(), schema.toString());
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee, boolean grantOption)
    {
        enforcePermissionManagementOperation(AccessDeniedException::denyGrantTablePrivilege, privilege.toString(), table.toString());
    }

    @Override
    public void checkCanDenyTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee)
    {
        enforcePermissionManagementOperation(AccessDeniedException::denyDenyTablePrivilege, privilege.toString(), table.toString());
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal revokee, boolean grantOption)
    {
        enforcePermissionManagementOperation(AccessDeniedException::denyRevokeTablePrivilege, privilege.toString(), table.toString());
    }

    @Override
    public void checkCanCreateRole(SystemSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
        enforcePermissionManagementOperation(AccessDeniedException::denyCreateRole, role);
    }

    @Override
    public void checkCanDropRole(SystemSecurityContext context, String role)
    {
        enforcePermissionManagementOperation(AccessDeniedException::denyDropRole, role);
    }

    @Override
    public void checkCanGrantRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        enforcePermissionManagementOperation(AccessDeniedException::denyGrantRoles, roles, grantees);
    }

    @Override
    public void checkCanRevokeRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        enforcePermissionManagementOperation(AccessDeniedException::denyRevokeRoles, roles, grantees);
    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context)
    {
        // We always want to allow users to query their current roles, since OPA does not deal with role information
    }

    @Override
    public void checkCanShowCurrentRoles(SystemSecurityContext context)
    {
        // We always want to allow users to query their current roles, since OPA does not deal with role information
    }

    @Override
    public void checkCanShowRoleGrants(SystemSecurityContext context)
    {
        // We always want to allow users to query their current roles, since OPA does not deal with role information
    }

    @Override
    public void checkCanShowFunctions(SystemSecurityContext context, CatalogSchemaName schema)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(context),
                "ShowFunctions",
                () -> denyShowFunctions(schema.toString()),
                OpaQueryInputResource.builder().schema(new TrinoSchema(schema)).build());
    }

    @Override
    public Set<SchemaFunctionName> filterFunctions(SystemSecurityContext context, String catalogName, Set<SchemaFunctionName> functionNames)
    {
        return opaHighLevelClient.parallelFilterFromOpa(
                functionNames,
                function -> buildQueryInputForSimpleResource(
                        buildQueryContext(context),
                        "FilterFunctions",
                        OpaQueryInputResource.builder()
                                .function(
                                        new TrinoFunction(
                                                new TrinoSchema(catalogName, function.getSchemaName()),
                                                function.getFunctionName()))
                                .build()));
    }

    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName procedure)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(systemSecurityContext),
                "ExecuteProcedure",
                () -> denyExecuteProcedure(procedure.toString()),
                OpaQueryInputResource.builder().function(TrinoFunction.fromTrinoFunction(procedure)).build());
    }

    @Override
    public boolean canExecuteFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        return opaHighLevelClient.queryOpaWithSimpleResource(
                buildQueryContext(systemSecurityContext),
                "ExecuteFunction",
                OpaQueryInputResource.builder().function(TrinoFunction.fromTrinoFunction(functionName)).build());
    }

    @Override
    public boolean canCreateViewWithExecuteFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        return opaHighLevelClient.queryOpaWithSimpleResource(
                buildQueryContext(systemSecurityContext),
                "CreateViewWithExecuteFunction",
                OpaQueryInputResource.builder().function(TrinoFunction.fromTrinoFunction(functionName)).build());
    }

    @Override
    public void checkCanExecuteTableProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaTableName table, String procedure)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(systemSecurityContext),
                "ExecuteTableProcedure",
                () -> denyExecuteTableProcedure(table.toString(), procedure),
                OpaQueryInputResource.builder().table(new TrinoTable(table)).function(procedure).build());
    }

    @Override
    public void checkCanCreateFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(systemSecurityContext),
                "CreateFunction",
                () -> denyCreateFunction(functionName.toString()),
                OpaQueryInputResource.builder().function(TrinoFunction.fromTrinoFunction(functionName)).build());
    }

    @Override
    public void checkCanDropFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(systemSecurityContext),
                "DropFunction",
                () -> denyDropFunction(functionName.toString()),
                OpaQueryInputResource.builder().function(TrinoFunction.fromTrinoFunction(functionName)).build());
    }

    @Override
    public List<ViewExpression> getRowFilters(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        List<OpaViewExpression> rowFilterExpressions = opaHighLevelClient.getRowFilterExpressionsFromOpa(buildQueryContext(context), tableName);
        return rowFilterExpressions.stream()
                .map(expression -> expression.toTrinoViewExpression(tableName.getCatalogName(), tableName.getSchemaTableName().getSchemaName()))
                .collect(toImmutableList());
    }

    @Override
    public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, CatalogSchemaTableName tableName, String columnName, Type type)
    {
        return opaHighLevelClient
                .getColumnMaskFromOpa(buildQueryContext(context), tableName, columnName, type)
                .map(expression -> expression.toTrinoViewExpression(tableName.getCatalogName(), tableName.getSchemaTableName().getSchemaName()));
    }

    @Override
    public void shutdown()
    {
        lifeCycleManager.stop();
    }

    private void checkTableOperation(SystemSecurityContext context, String actionName, CatalogSchemaTableName table, Consumer<String> deny)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(context),
                actionName,
                () -> deny.accept(table.toString()),
                OpaQueryInputResource.builder().table(new TrinoTable(table)).build());
    }

    private void checkTableAndPropertiesOperation(SystemSecurityContext context, String actionName, CatalogSchemaTableName table, Map<String, Optional<Object>> properties, Consumer<String> deny)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(context),
                actionName,
                () -> deny.accept(table.toString()),
                OpaQueryInputResource.builder().table(new TrinoTable(table).withProperties(properties)).build());
    }

    private void checkTableAndColumnsOperation(SystemSecurityContext context, String actionName, CatalogSchemaTableName table, Set<String> columns, BiConsumer<String, Set<String>> deny)
    {
        opaHighLevelClient.queryAndEnforce(
                buildQueryContext(context),
                actionName,
                () -> deny.accept(table.toString(), columns),
                OpaQueryInputResource.builder().table(new TrinoTable(table).withColumns(columns)).build());
    }

    private <T> void enforcePermissionManagementOperation(Consumer<T> deny, T arg)
    {
        if (!allowPermissionManagementOperations) {
            deny.accept(arg);
        }
    }

    private <T, U> void enforcePermissionManagementOperation(BiConsumer<T, U> deny, T arg1, U arg2)
    {
        if (!allowPermissionManagementOperations) {
            deny.accept(arg1, arg2);
        }
    }

    private static Map<String, Optional<Object>> convertProperties(Map<String, Object> properties)
    {
        return properties.entrySet().stream()
                .map(propertiesEntry -> Map.entry(propertiesEntry.getKey(), Optional.ofNullable(propertiesEntry.getValue())))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    OpaQueryContext buildQueryContext(Identity trinoIdentity)
    {
        return new OpaQueryContext(TrinoIdentity.fromTrinoIdentity(trinoIdentity), pluginContext);
    }

    OpaQueryContext buildQueryContext(SystemSecurityContext securityContext)
    {
        return new OpaQueryContext(TrinoIdentity.fromTrinoIdentity(securityContext.getIdentity()), pluginContext);
    }
}
