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
package io.trino.plugin.ranger;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.connector.EntityPrivilege;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.io.File;
import java.net.URL;
import java.security.Principal;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.plugin.ranger.RangerTrinoAccessType.ALTER;
import static io.trino.plugin.ranger.RangerTrinoAccessType.CREATE;
import static io.trino.plugin.ranger.RangerTrinoAccessType.DELETE;
import static io.trino.plugin.ranger.RangerTrinoAccessType.DROP;
import static io.trino.plugin.ranger.RangerTrinoAccessType.EXECUTE;
import static io.trino.plugin.ranger.RangerTrinoAccessType.IMPERSONATE;
import static io.trino.plugin.ranger.RangerTrinoAccessType.INSERT;
import static io.trino.plugin.ranger.RangerTrinoAccessType.READ_SYSINFO;
import static io.trino.plugin.ranger.RangerTrinoAccessType.SELECT;
import static io.trino.plugin.ranger.RangerTrinoAccessType.SHOW;
import static io.trino.plugin.ranger.RangerTrinoAccessType.WRITE_SYSINFO;
import static io.trino.plugin.ranger.RangerTrinoAccessType._ANY;
import static io.trino.spi.security.AccessDeniedException.denyAddColumn;
import static io.trino.spi.security.AccessDeniedException.denyAlterColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentTable;
import static io.trino.spi.security.AccessDeniedException.denyCommentView;
import static io.trino.spi.security.AccessDeniedException.denyCreateCatalog;
import static io.trino.spi.security.AccessDeniedException.denyCreateFunction;
import static io.trino.spi.security.AccessDeniedException.denyCreateMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyCreateView;
import static io.trino.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.trino.spi.security.AccessDeniedException.denyDeleteTable;
import static io.trino.spi.security.AccessDeniedException.denyDropCatalog;
import static io.trino.spi.security.AccessDeniedException.denyDropColumn;
import static io.trino.spi.security.AccessDeniedException.denyDropFunction;
import static io.trino.spi.security.AccessDeniedException.denyDropMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyDropSchema;
import static io.trino.spi.security.AccessDeniedException.denyDropTable;
import static io.trino.spi.security.AccessDeniedException.denyDropView;
import static io.trino.spi.security.AccessDeniedException.denyExecuteProcedure;
import static io.trino.spi.security.AccessDeniedException.denyExecuteQuery;
import static io.trino.spi.security.AccessDeniedException.denyExecuteTableProcedure;
import static io.trino.spi.security.AccessDeniedException.denyImpersonateUser;
import static io.trino.spi.security.AccessDeniedException.denyInsertTable;
import static io.trino.spi.security.AccessDeniedException.denyReadSystemInformationAccess;
import static io.trino.spi.security.AccessDeniedException.denyRefreshMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameColumn;
import static io.trino.spi.security.AccessDeniedException.denyRenameMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameSchema;
import static io.trino.spi.security.AccessDeniedException.denyRenameTable;
import static io.trino.spi.security.AccessDeniedException.denyRenameView;
import static io.trino.spi.security.AccessDeniedException.denySelectColumns;
import static io.trino.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static io.trino.spi.security.AccessDeniedException.denySetMaterializedViewProperties;
import static io.trino.spi.security.AccessDeniedException.denySetSchemaAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetSystemSessionProperty;
import static io.trino.spi.security.AccessDeniedException.denySetTableAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetTableProperties;
import static io.trino.spi.security.AccessDeniedException.denySetUser;
import static io.trino.spi.security.AccessDeniedException.denySetViewAuthorization;
import static io.trino.spi.security.AccessDeniedException.denyShowColumns;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateFunction;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyShowFunctions;
import static io.trino.spi.security.AccessDeniedException.denyShowSchemas;
import static io.trino.spi.security.AccessDeniedException.denyShowTables;
import static io.trino.spi.security.AccessDeniedException.denyTruncateTable;
import static io.trino.spi.security.AccessDeniedException.denyUpdateTableColumns;
import static io.trino.spi.security.AccessDeniedException.denyWriteSystemInformationAccess;
import static java.util.Objects.requireNonNullElse;
import static java.util.function.Predicate.not;

public class RangerSystemAccessControl
        implements SystemAccessControl
{
    private static final Logger LOG = Logger.get(RangerSystemAccessControl.class);

    public static final String RANGER_TRINO_SERVICETYPE = "trino";
    public static final String RANGER_TRINO_APPID = "trino";

    private final RangerBasePlugin rangerPlugin;
    private final RangerTrinoEventListener eventListener = new RangerTrinoEventListener();

    @Inject
    public RangerSystemAccessControl(RangerConfig config)
            throws Exception
    {
        checkArgument(!isNullOrEmpty(config.getServiceName()), "ranger.service.name is not configured");

        Configuration hadoopConf = new Configuration();

        for (File configPath : config.getHadoopConfigResource()) {
            URL url = configPath.toURI().toURL();
            LOG.info("Loading Hadoop config %s from url %s", configPath, url);
            hadoopConf.addResource(url);
        }

        UserGroupInformation.setConfiguration(hadoopConf);
        RangerPluginConfig pluginConfig = new RangerPluginConfig(RANGER_TRINO_SERVICETYPE, config.getServiceName(), RANGER_TRINO_APPID, null, null, config.getPluginConfigResource(), null);

        rangerPlugin = new RangerBasePlugin(pluginConfig);
        rangerPlugin.init();
        rangerPlugin.setResultProcessor(new RangerDefaultAuditHandler());
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        if (!hasPermission(RangerTrinoResource.forUser(userName), identity, null, IMPERSONATE, "ImpersonateUser")) {
            denyImpersonateUser(identity.getUser(), userName);
        }
    }

    @Deprecated
    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        if (!hasPermission(RangerTrinoResource.forUser(userName), principal, null, IMPERSONATE, "SetUser")) {
            denySetUser(principal, userName);
        }
    }

    @Override
    public void checkCanExecuteQuery(Identity identity, QueryId queryId)
    {
        if (!hasPermission(RangerTrinoResource.forQueryId(queryId.getId()), identity, queryId, EXECUTE, "ExecuteQuery")) {
            denyExecuteQuery();
        }
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        if (!hasPermission(RangerTrinoResource.forUser(queryOwner.getUser()), identity, null, IMPERSONATE, "ViewQueryOwnedBy")) {
            denyImpersonateUser(identity.getUser(), queryOwner.getUser());
        }
    }

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(Identity identity, Collection<Identity> queryOwners)
    {
        Set<Identity> toExclude = new HashSet<>();

        for (Identity queryOwner : queryOwners) {
            if (!hasPermissionForFilter(RangerTrinoResource.forUser(queryOwner.getUser()), identity, null, IMPERSONATE, "filterViewQueryOwnedBy")) {
                toExclude.add(queryOwner);
            }
        }

        if (toExclude.isEmpty()) {
            return queryOwners;
        }

        return queryOwners.stream().filter(not(toExclude::contains)).collect(Collectors.toList());
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        if (!hasPermission(RangerTrinoResource.forUser(queryOwner.getUser()), identity, null, IMPERSONATE, "KillQueryOwnedBy")) {
            denyImpersonateUser(identity.getUser(), queryOwner.getUser());
        }
    }

    @Override
    public void checkCanReadSystemInformation(Identity identity)
    {
        if (!hasPermission(RangerTrinoResource.forSystemInformation(), identity, null, READ_SYSINFO, "ReadSystemInformation")) {
            denyReadSystemInformationAccess();
        }
    }

    @Override
    public void checkCanWriteSystemInformation(Identity identity)
    {
        if (!hasPermission(RangerTrinoResource.forSystemInformation(), identity, null, WRITE_SYSINFO, "WriteSystemInformation")) {
            denyWriteSystemInformationAccess();
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, QueryId queryId, String propertyName)
    {
        if (!hasPermission(RangerTrinoResource.forSystemProperty(propertyName), identity, queryId, ALTER, "SetSystemSessionProperty")) {
            denySetSystemSessionProperty(propertyName);
        }
    }

    @Override
    public boolean canAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        return hasPermission(RangerTrinoResource.forCatalog(catalogName), context, _ANY, "AccessCatalog");
    }

    @Override
    public void checkCanCreateCatalog(SystemSecurityContext context, String catalogName)
    {
        if (!hasPermission(RangerTrinoResource.forCatalog(catalogName), context, CREATE, "CreateCatalog")) {
            denyCreateCatalog(catalogName);
        }
    }

    @Override
    public void checkCanDropCatalog(SystemSecurityContext context, String catalogName)
    {
        if (!hasPermission(RangerTrinoResource.forCatalog(catalogName), context, DROP, "DropCatalog")) {
            denyDropCatalog(catalogName);
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
        if (!hasPermission(RangerTrinoResource.forSessionProperty(catalogName, propertyName), context, ALTER, "SetCatalogSessionProperty")) {
            denySetCatalogSessionProperty(catalogName, propertyName);
        }
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        Set<String> toExclude = new HashSet<>();

        for (String catalog : catalogs) {
            if (!hasPermissionForFilter(RangerTrinoResource.forCatalog(catalog), context, _ANY, "filterCatalogs")) {
                toExclude.add(catalog);
            }
        }

        if (toExclude.isEmpty()) {
            return catalogs;
        }

        return catalogs.stream().filter(not(toExclude::contains)).collect(Collectors.toSet());
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema, Map<String, Object> properties)
    {
        if (!hasPermission(RangerTrinoResource.forSchema(schema.getCatalogName(), schema.getSchemaName()), context, CREATE, "CreateSchema")) {
            denyCreateSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!hasPermission(RangerTrinoResource.forSchema(schema.getCatalogName(), schema.getSchemaName()), context, DROP, "DropSchema")) {
            denyDropSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        boolean isAllowed = hasPermission(RangerTrinoResource.forSchema(schema.getCatalogName(), schema.getSchemaName()), context, ALTER, "RenameSchema:source") &&
                hasPermission(RangerTrinoResource.forSchema(schema.getCatalogName(), newSchemaName), context, ALTER, "RenameSchema:target");

        if (!isAllowed) {
            denyRenameSchema(schema.getSchemaName(), newSchemaName);
        }
    }

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, TrinoPrincipal principal)
    {
        if (!hasPermission(RangerTrinoResource.forSchema(schema.getCatalogName(), schema.getSchemaName()), context, ALTER, "SetSchemaAuthorization")) {
            denySetSchemaAuthorization(schema.getSchemaName(), principal);
        }
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
        if (!hasPermission(RangerTrinoResource.forCatalog(catalogName), context, _ANY, "ShowSchemas")) {
            denyShowSchemas(catalogName);
        }
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        Set<String> toExclude = new HashSet<>();

        for (String schemaName : schemaNames) {
            if (!hasPermissionForFilter(RangerTrinoResource.forSchema(catalogName, schemaName), context, _ANY, "filterSchemas")) {
                toExclude.add(schemaName);
            }
        }

        if (toExclude.isEmpty()) {
            return schemaNames;
        }

        return schemaNames.stream().filter(not(toExclude::contains)).collect(Collectors.toSet());
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!hasPermission(RangerTrinoResource.forSchema(schema.getCatalogName(), schema.getSchemaName()), context, SHOW, "ShowCreateSchema")) {
            denyShowCreateSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Object> properties)
    {
        if (!hasPermission(createTableResource(table), context, CREATE, "CreateTable")) {
            denyCreateTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createTableResource(table), context, DROP, "DropTable")) {
            denyDropTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        boolean isAllowed = hasPermission(createTableResource(table), context, ALTER, "RenameTable:source") &&
                hasPermission(createTableResource(newTable), context, ALTER, "RenameTable:target");

        if (!isAllowed) {
            denyRenameTable(table.getSchemaTableName().getTableName(), newTable.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanSetTableProperties(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Optional<Object>> properties)
    {
        if (!hasPermission(createTableResource(table), context, ALTER, "SetTableProperties")) {
            denySetTableProperties(table.toString());
        }
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createTableResource(table), context, ALTER, "SetTableComment")) {
            denyCommentTable(table.toString());
        }
    }

    @Override
    public void checkCanSetTableAuthorization(SystemSecurityContext context, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        if (!hasPermission(createTableResource(table), context, ALTER, "SetTableAuthorization")) {
            denySetTableAuthorization(table.toString(), principal);
        }
    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!hasPermission(RangerTrinoResource.forSchema(schema.getCatalogName(), schema.getSchemaName()), context, _ANY, "ShowTables")) {
            denyShowTables(schema.toString());
        }
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createTableResource(table), context, SHOW, "ShowCreateTable")) {
            denyShowCreateTable(table.toString());
        }
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createTableResource(table), context, INSERT, "InsertIntoTable")) {
            denyInsertTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createTableResource(table), context, DELETE, "DeleteFromTable")) {
            denyDeleteTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanTruncateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createTableResource(table), context, DELETE, "TruncateTable")) {
            denyTruncateTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        Set<SchemaTableName> toExclude = new HashSet<>();

        for (SchemaTableName tableName : tableNames) {
            RangerTrinoResource resource = RangerTrinoResource.forTable(catalogName, tableName.getSchemaName(), tableName.getTableName());

            if (!hasPermissionForFilter(resource, context, _ANY, "filterTables")) {
                toExclude.add(tableName);
            }
        }

        if (toExclude.isEmpty()) {
            return tableNames;
        }

        return tableNames.stream().filter(not(toExclude::contains)).collect(Collectors.toSet());
    }

    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createTableResource(table), context, ALTER, "AddColumn")) {
            denyAddColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanAlterColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createTableResource(table), context, ALTER, "AlterColumn")) {
            denyAlterColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createTableResource(table), context, ALTER, "DropColumn")) {
            denyDropColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createTableResource(table), context, ALTER, "RenameColumn")) {
            denyRenameColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanSetColumnComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createTableResource(table), context, ALTER, "SetColumnComment")) {
            denyCommentColumn(table.toString());
        }
    }

    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createTableResource(table), context, _ANY, "ShowColumns")) {
            denyShowColumns(table.toString());
        }
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        for (RangerTrinoResource resource : RangerTrinoResource.forColumns(table.getCatalogName(), table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName(), columns)) {
            if (!hasPermission(resource, context, SELECT, "SelectFromColumns")) {
                denySelectColumns(table.getSchemaTableName().getTableName(), columns);
            }
        }
    }

    @Override
    public void checkCanUpdateTableColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> updatedColumnNames)
    {
        if (!hasPermission(createTableResource(table), context, INSERT, "UpdateTableColumns")) {
            denyUpdateTableColumns(table.getSchemaTableName().getTableName(), updatedColumnNames);
        }
    }

    @Deprecated
    @Override
    public Set<String> filterColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        Set<String> toExclude = new HashSet<>();
        String catalogName = table.getCatalogName();
        String schemaName = table.getSchemaTableName().getSchemaName();
        String tableName = table.getSchemaTableName().getTableName();

        for (String column : columns) {
            RangerTrinoResource resource = RangerTrinoResource.forColumn(catalogName, schemaName, tableName, column);

            if (!hasPermissionForFilter(resource, context, _ANY, "filterColumns")) {
                toExclude.add(column);
            }
        }

        if (toExclude.isEmpty()) {
            return columns;
        }

        return columns.stream().filter(not(toExclude::contains)).collect(Collectors.toSet());
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!hasPermission(createTableResource(view), context, CREATE, "CreateView")) {
            denyCreateView(view.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!hasPermission(createTableResource(view), context, DROP, "DropView")) {
            denyDropView(view.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        boolean isAllowed = hasPermission(createTableResource(view), context, ALTER, "RenameView:source") &&
                hasPermission(createTableResource(newView), context, ALTER, "RenameView:target");

        if (!isAllowed) {
            denyRenameView(view.toString(), newView.toString());
        }
    }

    @Override
    public void checkCanSetViewAuthorization(SystemSecurityContext context, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        if (!hasPermission(createTableResource(view), context, ALTER, "SetViewAuthorization")) {
            denySetViewAuthorization(view.toString(), principal);
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        for (RangerTrinoResource resource : RangerTrinoResource.forColumns(table.getCatalogName(), table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName(), columns)) {
            if (!hasPermission(resource, context, SELECT, "CreateViewWithSelectFromColumns")) {
                denyCreateViewWithSelect(table.getSchemaTableName().getTableName(), context.getIdentity());
            }
        }
    }

    @Override
    public void checkCanSetViewComment(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!hasPermission(createTableResource(view), context, ALTER, "SetViewComment")) {
            denyCommentView(view.toString());
        }
    }

    @Override
    public void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Object> properties)
    {
        if (!hasPermission(createTableResource(materializedView), context, CREATE, "CreateMaterializedView")) {
            denyCreateMaterializedView(materializedView.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRefreshMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        if (!hasPermission(createTableResource(materializedView), context, ALTER, "RefreshMaterializedView")) {
            denyRefreshMaterializedView(materializedView.toString());
        }
    }

    @Override
    public void checkCanSetMaterializedViewProperties(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Optional<Object>> properties)
    {
        if (!hasPermission(createTableResource(materializedView), context, ALTER, "SetMaterializedViewProperties")) {
            denySetMaterializedViewProperties(materializedView.toString());
        }
    }

    @Override
    public void checkCanDropMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        if (!hasPermission(createTableResource(materializedView), context, DROP, "DropMaterializedView")) {
            denyDropMaterializedView(materializedView.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRenameMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, CatalogSchemaTableName newView)
    {
        boolean isAllowed = hasPermission(createTableResource(materializedView), context, ALTER, "RenameMaterializedView:source") &&
                hasPermission(createTableResource(newView), context, ALTER, "RenameMaterializedView:target");

        if (!isAllowed) {
            denyRenameMaterializedView(materializedView.toString(), newView.toString());
        }
    }

    @Override
    public void checkCanGrantSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee, boolean grantOption)
    {
    }

    @Override
    public void checkCanDenySchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee)
    {
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal revokee, boolean grantOption)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee, boolean withGrantOption)
    {
    }

    @Override
    public void checkCanDenyTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal revokee, boolean grantOptionFor)
    {
    }

    @Override
    public void checkCanGrantEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal grantee, boolean grantOption)
    {
    }

    @Override
    public void checkCanDenyEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal grantee)
    {
    }

    @Override
    public void checkCanRevokeEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal revokee, boolean grantOption)
    {
    }

    @Override
    public void checkCanCreateRole(SystemSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
    }

    @Override
    public void checkCanDropRole(SystemSecurityContext context, String role)
    {
    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context)
    {
    }

    @Override
    public void checkCanGrantRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
    }

    @Override
    public void checkCanRevokeRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
    }

    @Override
    public void checkCanShowCurrentRoles(SystemSecurityContext context)
    {
    }

    @Override
    public void checkCanShowRoleGrants(SystemSecurityContext context)
    {
    }

    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext context, CatalogSchemaRoutineName procedure)
    {
        if (!hasPermission(RangerTrinoResource.forSchemaProcedure(procedure.getCatalogName(), procedure.getSchemaRoutineName().getSchemaName(), procedure.getSchemaRoutineName().getRoutineName()), context, EXECUTE, "ExecuteProcedure")) {
            denyExecuteProcedure(procedure.getSchemaRoutineName().getRoutineName());
        }
    }

    @Override
    public void checkCanExecuteTableProcedure(SystemSecurityContext context, CatalogSchemaTableName catalogSchemaTableName, String procedure)
    {
        if (!hasPermission(createTableResource(catalogSchemaTableName), context, ALTER, "ExecuteTableProcedure")) {
            denyExecuteTableProcedure(catalogSchemaTableName.toString(), procedure);
        }
    }

    @Override
    public void checkCanCreateFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
    {
        if (!hasPermission(RangerTrinoResource.forSchemaFunction(functionName.getCatalogName(), functionName.getSchemaRoutineName().getSchemaName(), functionName.getSchemaRoutineName().getRoutineName()), context, CREATE, "CreateFunction")) {
            denyCreateFunction(functionName.toString());
        }
    }

    @Override
    public void checkCanDropFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
    {
        if (!hasPermission(RangerTrinoResource.forSchemaFunction(functionName.getCatalogName(), functionName.getSchemaRoutineName().getSchemaName(), functionName.getSchemaRoutineName().getRoutineName()), context, DROP, "DropFunction")) {
            denyDropFunction(functionName.toString());
        }
    }

    @Override
    public void checkCanShowCreateFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
    {
        if (!hasPermission(RangerTrinoResource.forSchemaFunction(functionName.getCatalogName(), functionName.getSchemaRoutineName().getSchemaName(), functionName.getSchemaRoutineName().getRoutineName()), context, SHOW, "ShowCreateFunction")) {
            denyShowCreateFunction(functionName.toString());
        }
    }

    @Override
    public void checkCanShowFunctions(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!hasPermission(RangerTrinoResource.forSchema(schema.getCatalogName(), schema.getSchemaName()), context, _ANY, "ShowFunctions")) {
            denyShowFunctions(schema.toString());
        }
    }

    @Override
    public boolean canExecuteFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
    {
        return hasPermission(RangerTrinoResource.forSchemaFunction(functionName.getCatalogName(), functionName.getSchemaRoutineName().getSchemaName(), functionName.getSchemaRoutineName().getRoutineName()), context, EXECUTE, "ExecuteFunction");
    }

    @Override
    public boolean canCreateViewWithExecuteFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
    {
        return hasPermission(RangerTrinoResource.forSchemaFunction(functionName.getCatalogName(), functionName.getSchemaRoutineName().getSchemaName(), functionName.getSchemaRoutineName().getRoutineName()), context, EXECUTE, "CreateViewWithExecuteFunction");
    }

    @Override
    public Set<SchemaFunctionName> filterFunctions(SystemSecurityContext context, String catalogName, Set<SchemaFunctionName> functionNames)
    {
        Set<SchemaFunctionName> toExclude = new HashSet<>();

        for (SchemaFunctionName functionName : functionNames) {
            RangerTrinoResource resource = RangerTrinoResource.forSchemaFunction(catalogName, functionName.getSchemaName(), functionName.getFunctionName());

            if (!hasPermissionForFilter(resource, context, _ANY, "filterFunctions")) {
                toExclude.add(functionName);
            }
        }

        if (toExclude.isEmpty()) {
            return functionNames;
        }
        else {
            return functionNames.stream().filter(not(toExclude::contains)).collect(Collectors.toSet());
        }
    }

    @Override
    public List<ViewExpression> getRowFilters(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        RangerAccessResult result = getRowFilterResult(createAccessRequest(createTableResource(tableName), context, SELECT, "getRowFilters"));

        if (!isRowFilterEnabled(result)) {
            return Collections.emptyList();
        }

        String filter = result.getFilterExpr();
        ViewExpression viewExpression = ViewExpression.builder().identity(context.getIdentity().getUser())
                .catalog(tableName.getCatalogName())
                .schema(tableName.getSchemaTableName().getSchemaName())
                .expression(filter).build();

        return ImmutableList.of(viewExpression);
    }

    @Override
    public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, CatalogSchemaTableName tableName, String columnName, Type type)
    {
        RangerAccessResult result = getDataMaskResult(createAccessRequest(RangerTrinoResource.forColumn(tableName.getCatalogName(), tableName.getSchemaTableName().getSchemaName(), tableName.getSchemaTableName().getTableName(), columnName), context, SELECT, "getColumnMask"));

        if (!isDataMaskEnabled(result)) {
            return Optional.empty();
        }

        String maskType = result.getMaskType();
        RangerServiceDef.RangerDataMaskTypeDef maskTypeDef = result.getMaskTypeDef();
        String transformer = null;

        if (maskTypeDef != null) {
            transformer = maskTypeDef.getTransformer();
        }

        if (RangerPolicy.MASK_TYPE_NULL.equalsIgnoreCase(maskType)) {
            transformer = "NULL";
        }
        else if (RangerPolicy.MASK_TYPE_CUSTOM.equalsIgnoreCase(maskType)) {
            String maskedValue = result.getMaskedValue();

            transformer = requireNonNullElse(maskedValue, "NULL");
        }

        if (transformer == null) {
            return Optional.empty();
        }
        else {
            transformer = transformer.replace("{col}", columnName).replace("{type}", type.getDisplayName());

            return Optional.of(ViewExpression.builder().identity(context.getIdentity().getUser()).catalog(tableName.getCatalogName()).schema(tableName.getSchemaTableName().getSchemaName()).expression(transformer).build());
        }
    }

    @Override
    public Iterable<EventListener> getEventListeners()
    {
        return ImmutableList.of(eventListener);
    }

    @Override
    public void shutdown()
    {
        rangerPlugin.cleanup();
    }

    private RangerAccessResult getDataMaskResult(RangerTrinoAccessRequest request)
    {
        return rangerPlugin.evalDataMaskPolicies(request, rangerPlugin.getResultProcessor());
    }

    private RangerAccessResult getRowFilterResult(RangerTrinoAccessRequest request)
    {
        return rangerPlugin.evalRowFilterPolicies(request, rangerPlugin.getResultProcessor());
    }

    private static boolean isDataMaskEnabled(RangerAccessResult result)
    {
        return result.isMaskEnabled();
    }

    private static boolean isRowFilterEnabled(RangerAccessResult result)
    {
        return result.isRowFilterEnabled();
    }

    private RangerTrinoAccessRequest createAccessRequest(RangerTrinoResource resource, SystemSecurityContext context, RangerTrinoAccessType accessType, String action)
    {
        Set<String> userGroups = context.getIdentity().getGroups();

        return new RangerTrinoAccessRequest(resource, context.getIdentity().getUser(), userGroups, getQueryTime(context), getClientAddress(context), getClientType(context), getQueryText(context), accessType, action);
    }

    private RangerTrinoAccessRequest createAccessRequest(RangerTrinoResource resource, Identity identity, QueryId queryId, RangerTrinoAccessType accessType, String action)
    {
        Set<String> userGroups = identity.getGroups();

        return new RangerTrinoAccessRequest(resource, identity.getUser(), userGroups, getQueryTime(queryId), getClientAddress(queryId), getClientType(queryId), getQueryText(queryId), accessType, action);
    }

    private Optional<String> getClientAddress(QueryId queryId)
    {
        return queryId != null ? eventListener.getClientAddress(queryId.getId()) : Optional.empty();
    }

    private Optional<String> getClientType(QueryId queryId)
    {
        return queryId != null ? eventListener.getClientType(queryId.getId()) : Optional.empty();
    }

    private Optional<String> getQueryText(QueryId queryId)
    {
        return queryId != null ? eventListener.getQueryText(queryId.getId()) : Optional.empty();
    }

    private Optional<Instant> getQueryTime(QueryId queryId)
    {
        return queryId != null ? eventListener.getQueryTime(queryId.getId()) : Optional.empty();
    }

    private Optional<String> getClientAddress(SystemSecurityContext context)
    {
        return context != null ? getClientAddress(context.getQueryId()) : Optional.empty();
    }

    private Optional<String> getClientType(SystemSecurityContext context)
    {
        return context != null ? getClientType(context.getQueryId()) : Optional.empty();
    }

    private Optional<String> getQueryText(SystemSecurityContext context)
    {
        return context != null ? getQueryText(context.getQueryId()) : Optional.empty();
    }

    private Optional<Instant> getQueryTime(SystemSecurityContext context)
    {
        return context != null ? getQueryTime(context.getQueryId()) : Optional.empty();
    }

    private boolean hasPermission(RangerTrinoResource resource, SystemSecurityContext context, RangerTrinoAccessType accessType, String action)
    {
        RangerAccessResult result = rangerPlugin.isAccessAllowed(createAccessRequest(resource, context, accessType, action));

        return result != null && result.getIsAllowed();
    }

    private boolean hasPermissionForFilter(RangerTrinoResource resource, SystemSecurityContext context, RangerTrinoAccessType accessType, String action)
    {
        RangerTrinoAccessRequest request = createAccessRequest(resource, context, accessType, action);

        request.setResourceMatchingScope(RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS);

        RangerAccessResult result = rangerPlugin.isAccessAllowed(request, null);

        return result != null && result.getIsAllowed();
    }

    private boolean hasPermission(RangerTrinoResource resource, Identity identity, QueryId queryId, RangerTrinoAccessType accessType, String action)
    {
        RangerAccessResult result = rangerPlugin.isAccessAllowed(createAccessRequest(resource, identity, queryId, accessType, action));

        return result != null && result.getIsAllowed();
    }

    private boolean hasPermissionForFilter(RangerTrinoResource resource, Identity identity, QueryId queryId, RangerTrinoAccessType accessType, String action)
    {
        RangerTrinoAccessRequest request = createAccessRequest(resource, identity, queryId, accessType, action);

        request.setResourceMatchingScope(RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS);

        RangerAccessResult result = rangerPlugin.isAccessAllowed(request, null);

        return result != null && result.getIsAllowed();
    }

    private boolean hasPermission(RangerTrinoResource resource, Optional<Principal> principal, QueryId queryId, RangerTrinoAccessType accessType, String action)
    {
        RangerAccessResult result = rangerPlugin.isAccessAllowed(createAccessRequest(resource, toIdentity(principal), queryId, accessType, action));

        return result != null && result.getIsAllowed();
    }

    private static RangerTrinoResource createTableResource(CatalogSchemaTableName catalogSchemaTableName)
    {
        return RangerTrinoResource.forTable(catalogSchemaTableName.getCatalogName(), catalogSchemaTableName.getSchemaTableName().getSchemaName(), catalogSchemaTableName.getSchemaTableName().getTableName());
    }

    private static Identity toIdentity(Optional<Principal> principal)
    {
        if (principal.isPresent()) {
            return Identity.ofUser(principal.get().getName());
        }

        return Identity.ofUser("");
    }
}
