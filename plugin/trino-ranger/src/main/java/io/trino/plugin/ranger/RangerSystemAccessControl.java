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
import io.trino.spi.security.AccessDeniedException;
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

import java.net.URL;
import java.security.Principal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.trino.plugin.ranger.RangerTrinoAccessType.ALTER;
import static io.trino.plugin.ranger.RangerTrinoAccessType.CREATE;
import static io.trino.plugin.ranger.RangerTrinoAccessType.DELETE;
import static io.trino.plugin.ranger.RangerTrinoAccessType.DROP;
import static io.trino.plugin.ranger.RangerTrinoAccessType.EXECUTE;
import static io.trino.plugin.ranger.RangerTrinoAccessType.GRANT;
import static io.trino.plugin.ranger.RangerTrinoAccessType.IMPERSONATE;
import static io.trino.plugin.ranger.RangerTrinoAccessType.INSERT;
import static io.trino.plugin.ranger.RangerTrinoAccessType.READ_SYSINFO;
import static io.trino.plugin.ranger.RangerTrinoAccessType.REVOKE;
import static io.trino.plugin.ranger.RangerTrinoAccessType.SELECT;
import static io.trino.plugin.ranger.RangerTrinoAccessType.SHOW;
import static io.trino.plugin.ranger.RangerTrinoAccessType.USE;
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
import static io.trino.spi.security.AccessDeniedException.denyCreateRole;
import static io.trino.spi.security.AccessDeniedException.denyCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyCreateView;
import static io.trino.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.trino.spi.security.AccessDeniedException.denyDeleteTable;
import static io.trino.spi.security.AccessDeniedException.denyDenyEntityPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyDenySchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyDenyTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyDropCatalog;
import static io.trino.spi.security.AccessDeniedException.denyDropColumn;
import static io.trino.spi.security.AccessDeniedException.denyDropFunction;
import static io.trino.spi.security.AccessDeniedException.denyDropMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyDropRole;
import static io.trino.spi.security.AccessDeniedException.denyDropSchema;
import static io.trino.spi.security.AccessDeniedException.denyDropTable;
import static io.trino.spi.security.AccessDeniedException.denyDropView;
import static io.trino.spi.security.AccessDeniedException.denyExecuteProcedure;
import static io.trino.spi.security.AccessDeniedException.denyExecuteQuery;
import static io.trino.spi.security.AccessDeniedException.denyExecuteTableProcedure;
import static io.trino.spi.security.AccessDeniedException.denyGrantEntityPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyGrantRoles;
import static io.trino.spi.security.AccessDeniedException.denyGrantSchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyImpersonateUser;
import static io.trino.spi.security.AccessDeniedException.denyInsertTable;
import static io.trino.spi.security.AccessDeniedException.denyReadSystemInformationAccess;
import static io.trino.spi.security.AccessDeniedException.denyRefreshMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameColumn;
import static io.trino.spi.security.AccessDeniedException.denyRenameMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameSchema;
import static io.trino.spi.security.AccessDeniedException.denyRenameTable;
import static io.trino.spi.security.AccessDeniedException.denyRenameView;
import static io.trino.spi.security.AccessDeniedException.denyRevokeEntityPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyRevokeRoles;
import static io.trino.spi.security.AccessDeniedException.denyRevokeSchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
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
import static io.trino.spi.security.AccessDeniedException.denyShowRoles;
import static io.trino.spi.security.AccessDeniedException.denyShowSchemas;
import static io.trino.spi.security.AccessDeniedException.denyShowTables;
import static io.trino.spi.security.AccessDeniedException.denyTruncateTable;
import static io.trino.spi.security.AccessDeniedException.denyUpdateTableColumns;
import static io.trino.spi.security.AccessDeniedException.denyWriteSystemInformationAccess;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNullElse;

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
    {
        super();

        Configuration hadoopConf = new Configuration();

        for (String configPath : config.getHadoopConfigResource()) {
            URL url = hadoopConf.getResource(configPath);

            LOG.info("Trying to load Hadoop config from %s (can be null)", url);

            if (url == null) {
                LOG.warn("Hadoop config %s not found", configPath);
            }
            else {
                hadoopConf.addResource(url);
            }
        }

        UserGroupInformation.setConfiguration(hadoopConf);

        RangerPluginConfig pluginConfig = new RangerPluginConfig(RANGER_TRINO_SERVICETYPE, config.getServiceName(), RANGER_TRINO_APPID, null, null, null);

        for (String configPath : config.getPluginConfigResource()) {
            pluginConfig.addResourceIfReadable(configPath);
        }

        rangerPlugin = new RangerBasePlugin(pluginConfig);

        rangerPlugin.init();

        rangerPlugin.setResultProcessor(new RangerDefaultAuditHandler());
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        if (!hasPermission(createUserResource(userName), identity, null, IMPERSONATE, "ImpersonateUser")) {
            denyImpersonateUser(identity.getUser(), userName);
        }
    }

    @Deprecated
    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        if (!hasPermission(createUserResource(userName), principal, null, IMPERSONATE, "SetUser")) {
            denySetUser(principal, userName);
        }
    }

    @Override
    public void checkCanExecuteQuery(Identity identity, QueryId queryId)
    {
        if (!hasPermission(createResource(queryId), identity, queryId, EXECUTE, "ExecuteQuery")) {
            denyExecuteQuery();
        }
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        if (!hasPermission(createUserResource(queryOwner.getUser()), identity, null, IMPERSONATE, "ViewQueryOwnedBy")) {
            denyImpersonateUser(identity.getUser(), queryOwner.getUser());
        }
    }

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(Identity identity, Collection<Identity> queryOwners)
    {
        Set<Identity> toExclude = new HashSet<>();

        for (Identity queryOwner : queryOwners) {
            if (!hasPermissionForFilter(createUserResource(queryOwner.getUser()), identity, null, IMPERSONATE, "filterViewQueryOwnedBy")) {
                LOG.debug("filterViewQueryOwnedBy(user=%s): skipping queries owned by %s", identity, queryOwner);

                toExclude.add(queryOwner);
            }
        }

        return toExclude.isEmpty() ? queryOwners : queryOwners.stream().filter(((Predicate<? super Identity>) toExclude::contains).negate()).collect(Collectors.toList());
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        if (!hasPermission(createUserResource(queryOwner.getUser()), identity, null, IMPERSONATE, "KillQueryOwnedBy")) {
            denyImpersonateUser(identity.getUser(), queryOwner.getUser());
        }
    }

    @Override
    public void checkCanReadSystemInformation(Identity identity)
    {
        if (!hasPermission(createSystemInformation(), identity, null, READ_SYSINFO, "ReadSystemInformation")) {
            denyReadSystemInformationAccess();
        }
    }

    @Override
    public void checkCanWriteSystemInformation(Identity identity)
    {
        if (!hasPermission(createSystemInformation(), identity, null, WRITE_SYSINFO, "WriteSystemInformation")) {
            denyWriteSystemInformationAccess();
        }
    }

    @Deprecated
    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        if (!hasPermission(createSystemPropertyResource(propertyName), identity, null, ALTER, "SetSystemSessionProperty")) {
            denySetSystemSessionProperty(propertyName);
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, QueryId queryId, String propertyName)
    {
        if (!hasPermission(createSystemPropertyResource(propertyName), identity, queryId, ALTER, "SetSystemSessionProperty")) {
            denySetSystemSessionProperty(propertyName);
        }
    }

    @Override
    public boolean canAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        return hasPermission(createResource(catalogName), context, USE, "AccessCatalog");
    }

    @Override
    public void checkCanCreateCatalog(SystemSecurityContext context, String catalogName)
    {
        if (!hasPermission(createResource(catalogName), context, CREATE, "CreateCatalog")) {
            denyCreateCatalog(catalogName);
        }
    }

    @Override
    public void checkCanDropCatalog(SystemSecurityContext context, String catalogName)
    {
        if (!hasPermission(createResource(catalogName), context, DROP, "DropCatalog")) {
            denyDropCatalog(catalogName);
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
        if (!hasPermission(createCatalogSessionResource(catalogName, propertyName), context, ALTER, "SetCatalogSessionProperty")) {
            denySetCatalogSessionProperty(catalogName, propertyName);
        }
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        Set<String> toExclude = new HashSet<>();

        for (String catalog : catalogs) {
            if (!hasPermissionForFilter(createResource(catalog), context, _ANY, "filterCatalogs")) {
                LOG.debug("filterCatalogs(user=%s): skipping catalog %s", context.getIdentity(), catalog);

                toExclude.add(catalog);
            }
        }

        return toExclude.isEmpty() ? catalogs : catalogs.stream().filter(((Predicate<? super String>) toExclude::contains).negate()).collect(Collectors.toSet());
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema, Map<String, Object> properties)
    {
        if (!hasPermission(createResource(schema.getCatalogName(), schema.getSchemaName()), context, CREATE, "CreateSchema")) {
            denyCreateSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!hasPermission(createResource(schema.getCatalogName(), schema.getSchemaName()), context, DROP, "DropSchema")) {
            denyDropSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        if (!hasPermission(createResource(schema.getCatalogName(), schema.getSchemaName()), context, ALTER, "RenameSchema")) {
            denyRenameSchema(schema.getSchemaName(), newSchemaName);
        }
    }

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, TrinoPrincipal principal)
    {
        if (!hasPermission(createResource(schema.getCatalogName(), schema.getSchemaName()), context, GRANT, "SetSchemaAuthorization")) {
            denySetSchemaAuthorization(schema.getSchemaName(), principal);
        }
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
        if (!hasPermission(createResource(catalogName), context, SHOW, "ShowSchemas")) {
            denyShowSchemas(catalogName);
        }
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        Set<String> toExclude = new HashSet<>();

        for (String schemaName : schemaNames) {
            if (!hasPermissionForFilter(createResource(catalogName, schemaName), context, _ANY, "filterSchemas")) {
                toExclude.add(schemaName);
            }
        }

        return toExclude.isEmpty() ? schemaNames : schemaNames.stream().filter(((Predicate<? super String>) toExclude::contains).negate()).collect(Collectors.toSet());
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!hasPermission(createResource(schema.getCatalogName(), schema.getSchemaName()), context, SHOW, "ShowCreateSchema")) {
            denyShowCreateSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Object> properties)
    {
        if (!hasPermission(createResource(table), context, CREATE, "CreateTable")) {
            denyCreateTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createResource(table), context, DROP, "DropTable")) {
            denyDropTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        if (!hasPermission(createResource(table), context, ALTER, "RenameTable")) {
            denyRenameTable(table.getSchemaTableName().getTableName(), newTable.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanSetTableProperties(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Optional<Object>> properties)
    {
        if (!hasPermission(createResource(table), context, ALTER, "SetTableProperties")) {
            denySetTableProperties(table.toString());
        }
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createResource(table), context, ALTER, "SetTableComment")) {
            denyCommentTable(table.toString());
        }
    }

    @Override
    public void checkCanSetTableAuthorization(SystemSecurityContext context, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        if (!hasPermission(createResource(table), context, GRANT, "SetTableAuthorization")) {
            denySetTableAuthorization(table.toString(), principal);
        }
    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!hasPermission(createResource(schema), context, SHOW, "ShowTables")) {
            denyShowTables(schema.toString());
        }
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createResource(table), context, SHOW, "ShowCreateTable")) {
            denyShowCreateTable(table.toString());
        }
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        RangerTrinoResource res = createResource(table);

        if (!hasPermission(res, context, INSERT, "InsertIntoTable")) {
            denyInsertTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createResource(table), context, DELETE, "DeleteFromTable")) {
            denyDeleteTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanTruncateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createResource(table), context, DELETE, "TruncateTable")) {
            denyTruncateTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        Set<SchemaTableName> toExclude = new HashSet<>();

        for (SchemaTableName tableName : tableNames) {
            RangerTrinoResource res = createResource(catalogName, tableName.getSchemaName(), tableName.getTableName());

            if (!hasPermissionForFilter(res, context, _ANY, "filterTables")) {
                LOG.debug("filterTables(user=%s): skipping table %s.%s.%s", context.getIdentity(), catalogName, tableName.getSchemaName(), tableName.getTableName());

                toExclude.add(tableName);
            }
        }

        return toExclude.isEmpty() ? tableNames : tableNames.stream().filter(((Predicate<? super SchemaTableName>) toExclude::contains).negate()).collect(Collectors.toSet());
    }

    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        RangerTrinoResource res = createResource(table);

        if (!hasPermission(res, context, ALTER, "AddColumn")) {
            denyAddColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanAlterColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        RangerTrinoResource res = createResource(table);

        if (!hasPermission(res, context, ALTER, "AlterColumn")) {
            denyAlterColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createResource(table), context, ALTER, "DropColumn")) {
            denyDropColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        RangerTrinoResource res = createResource(table);

        if (!hasPermission(res, context, ALTER, "RenameColumn")) {
            denyRenameColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanSetColumnComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createResource(table), context, ALTER, "SetColumnComment")) {
            denyCommentColumn(table.toString());
        }
    }

    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createResource(table), context, SHOW, "ShowColumns")) {
            denyShowColumns(table.toString());
        }
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        for (RangerTrinoResource res : createResource(table, columns)) {
            if (!hasPermission(res, context, SELECT, "SelectFromColumns")) {
                denySelectColumns(table.getSchemaTableName().getTableName(), columns);
            }
        }
    }

    @Override
    public void checkCanUpdateTableColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> updatedColumnNames)
    {
        if (!hasPermission(createResource(table), context, INSERT, "UpdateTableColumns")) {
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
            RangerTrinoResource res = createResource(catalogName, schemaName, tableName, column);

            if (!hasPermissionForFilter(res, context, _ANY, "filterColumns")) {
                toExclude.add(column);
            }
        }

        return toExclude.isEmpty() ? columns : columns.stream().filter(((Predicate<? super String>) toExclude::contains).negate()).collect(Collectors.toSet());
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!hasPermission(createResource(view), context, CREATE, "CreateView")) {
            denyCreateView(view.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!hasPermission(createResource(view), context, DROP, "DropView")) {
            denyDropView(view.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        if (!hasPermission(createResource(view), context, ALTER, "RenameView")) {
            denyRenameView(view.toString(), newView.toString());
        }
    }

    @Override
    public void checkCanSetViewAuthorization(SystemSecurityContext context, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        if (!hasPermission(createResource(view), context, ALTER, "SetViewAuthorization")) {
            denySetViewAuthorization(view.toString(), principal);
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        try {
            checkCanCreateView(context, table);
        }
        catch (AccessDeniedException ade) {
            denyCreateViewWithSelect(table.getSchemaTableName().getTableName(), context.getIdentity());
        }
    }

    @Override
    public void checkCanSetViewComment(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!hasPermission(createResource(view), context, ALTER, "SetViewComment")) {
            denyCommentView(view.toString());
        }
    }

    @Override
    public void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Object> properties)
    {
        if (!hasPermission(createResource(materializedView), context, CREATE, "CreateMaterializedView")) {
            denyCreateMaterializedView(materializedView.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRefreshMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        if (!hasPermission(createResource(materializedView), context, ALTER, "RefreshMaterializedView")) {
            denyRefreshMaterializedView(materializedView.toString());
        }
    }

    @Override
    public void checkCanSetMaterializedViewProperties(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Optional<Object>> properties)
    {
        if (!hasPermission(createResource(materializedView), context, ALTER, "SetMaterializedViewProperties")) {
            denySetMaterializedViewProperties(materializedView.toString());
        }
    }

    @Override
    public void checkCanDropMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        if (!hasPermission(createResource(materializedView), context, DROP, "DropMaterializedView")) {
            denyDropMaterializedView(materializedView.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRenameMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, CatalogSchemaTableName newView)
    {
        if (!hasPermission(createResource(materializedView), context, DROP, "RenameMaterializedView")) {
            denyRenameMaterializedView(materializedView.toString(), newView.toString());
        }
    }

    @Override
    public void checkCanGrantSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee, boolean grantOption)
    {
        if (!hasPermission(createResource(schema), context, GRANT, "GrantSchemaPrivilege")) {
            denyGrantSchemaPrivilege(privilege.toString(), schema.toString());
        }
    }

    @Override
    public void checkCanDenySchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee)
    {
        if (!hasPermission(createResource(schema), context, REVOKE, "DenySchemaPrivilege")) {
            denyDenySchemaPrivilege(privilege.toString(), schema.toString());
        }
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal revokee, boolean grantOption)
    {
        if (!hasPermission(createResource(schema), context, REVOKE, "RevokeSchemaPrivilege")) {
            denyRevokeSchemaPrivilege(privilege.toString(), schema.toString());
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee, boolean withGrantOption)
    {
        if (!hasPermission(createResource(table), context, GRANT, "GrantTablePrivilege")) {
            denyGrantTablePrivilege(privilege.toString(), table.toString());
        }
    }

    @Override
    public void checkCanDenyTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee)
    {
        if (!hasPermission(createResource(table), context, REVOKE, "DenyTablePrivilege")) {
            denyDenyTablePrivilege(privilege.toString(), table.toString());
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal revokee, boolean grantOptionFor)
    {
        if (!hasPermission(createResource(table), context, REVOKE, "RevokeTablePrivilege")) {
            denyRevokeTablePrivilege(privilege.toString(), table.toString());
        }
    }

    @Override
    public void checkCanGrantEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal grantee, boolean grantOption)
    {
        if (!hasPermission(createResource(entity), context, GRANT, "GrantEntityPrivilege")) {
            denyGrantEntityPrivilege(privilege.toString(), entity);
        }
    }

    @Override
    public void checkCanDenyEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal grantee)
    {
        if (!hasPermission(createResource(entity), context, REVOKE, "DenyEntityPrivilege")) {
            denyDenyEntityPrivilege(privilege.toString(), entity);
        }
    }

    @Override
    public void checkCanRevokeEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal revokee, boolean grantOption)
    {
        if (!hasPermission(createResource(entity), context, REVOKE, "RevokeEntityPrivilege")) {
            denyRevokeEntityPrivilege(privilege.toString(), entity);
        }
    }

    @Override
    public void checkCanCreateRole(SystemSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
        if (!hasPermission(createRoleResource(role), context, CREATE, "CreateRole")) {
            denyCreateRole(role);
        }
    }

    @Override
    public void checkCanDropRole(SystemSecurityContext context, String role)
    {
        if (!hasPermission(createRoleResource(role), context, DROP, "DropRole")) {
            denyDropRole(role);
        }
    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context)
    {
        if (!hasPermission(createRoleResource("*"), context, SHOW, "ShowRoles")) {
            denyShowRoles();
        }
    }

    @Override
    public void checkCanGrantRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        if (!hasPermission(createRoleResources(roles), context, GRANT, "GrantRoles")) {
            denyGrantRoles(roles, grantees);
        }
    }

    @Override
    public void checkCanRevokeRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        if (!hasPermission(createRoleResources(roles), context, REVOKE, "RevokeRoles")) {
            denyRevokeRoles(roles, grantees);
        }
    }

    @Override
    public void checkCanShowCurrentRoles(SystemSecurityContext context)
    {
        //allow
    }

    @Override
    public void checkCanShowRoleGrants(SystemSecurityContext context)
    {
        //allow
    }

    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext context, CatalogSchemaRoutineName procedure)
    {
        if (!hasPermission(createProcedureResource(procedure), context, EXECUTE, "ExecuteProcedure")) {
            denyExecuteProcedure(procedure.getSchemaRoutineName().getRoutineName());
        }
    }

    @Override
    public void checkCanExecuteTableProcedure(SystemSecurityContext context, CatalogSchemaTableName catalogSchemaTableName, String procedure)
    {
        if (!hasPermission(createResource(catalogSchemaTableName), context, ALTER, "ExecuteTableProcedure")) {
            denyExecuteTableProcedure(catalogSchemaTableName.toString(), procedure);
        }
    }

    @Override
    public void checkCanCreateFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
    {
        if (!hasPermission(createResource(functionName), context, CREATE, "CreateFunction")) {
            denyCreateFunction(functionName.toString());
        }
    }

    @Override
    public void checkCanDropFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
    {
        if (!hasPermission(createResource(functionName), context, DROP, "DropFunction")) {
            denyDropFunction(functionName.toString());
        }
    }

    @Override
    public void checkCanShowCreateFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
    {
        if (!hasPermission(createResource(functionName), context, SHOW, "ShowCreateFunction")) {
            denyShowCreateFunction(functionName.toString());
        }
    }

    @Override
    public void checkCanShowFunctions(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!hasPermission(createResource(schema), context, SHOW, "ShowFunctions")) {
            denyShowFunctions(schema.toString());
        }
    }

    @Override
    public boolean canExecuteFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
    {
        return hasPermission(createResource(functionName), context, EXECUTE, "ExecuteFunction");
    }

    @Override
    public boolean canCreateViewWithExecuteFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
    {
        return hasPermission(createResource(functionName), context, CREATE, "CreateViewWithExecuteFunction");
    }

    @Override
    public Set<SchemaFunctionName> filterFunctions(SystemSecurityContext context, String catalogName, Set<SchemaFunctionName> functionNames)
    {
        Set<SchemaFunctionName> toExclude = new HashSet<>();

        for (SchemaFunctionName functionName : functionNames) {
            RangerTrinoResource res = createResource(catalogName, functionName);

            if (!hasPermissionForFilter(res, context, _ANY, "filterFunctions")) {
                LOG.debug("filterFunctions(user=%s): skipping function %s.%s.%s", context.getIdentity(), catalogName, functionName.getSchemaName(), functionName.getFunctionName());

                toExclude.add(functionName);
            }
        }

        return toExclude.isEmpty() ? functionNames : functionNames.stream().filter(((Predicate<? super SchemaFunctionName>) toExclude::contains).negate()).collect(Collectors.toSet());
    }

    @Override
    public List<ViewExpression> getRowFilters(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        RangerAccessResult result = getRowFilterResult(createAccessRequest(createResource(tableName), context, SELECT, "getRowFilters"));

        if (!isRowFilterEnabled(result)) {
            return Collections.emptyList();
        }

        String filter = result.getFilterExpr();
        ViewExpression viewExpression = ViewExpression.builder().identity(context.getIdentity().getUser())
                .catalog(tableName.getCatalogName())
                .schema(tableName.getSchemaTableName().getSchemaName())
                .expression(filter).build();

        return Collections.singletonList(viewExpression);
    }

    @Override
    public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, CatalogSchemaTableName tableName, String columnName, Type type)
    {
        RangerAccessResult result = getDataMaskResult(createAccessRequest(createResource(tableName.getCatalogName(), tableName.getSchemaTableName().getSchemaName(), tableName.getSchemaTableName().getTableName(), columnName), context, SELECT, "getColumnMask"));

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

        if (transformer != null && !transformer.isEmpty()) {
            transformer = transformer.replace("{col}", columnName).replace("{type}", type.getDisplayName());
        }

        return Optional.ofNullable(ViewExpression.builder().identity(context.getIdentity().getUser())
                                           .catalog(tableName.getCatalogName())
                                           .schema(tableName.getSchemaTableName().getSchemaName())
                                           .expression(transformer).build());
    }

    @Override
    public Iterable<EventListener> getEventListeners()
    {
        return Collections.singletonList(eventListener);
    }

    @Override
    public void shutdown()
    {
        // nothing to do here
    }

    private RangerAccessResult getDataMaskResult(RangerTrinoAccessRequest request)
    {
        return rangerPlugin.evalDataMaskPolicies(request, null);
    }

    private RangerAccessResult getRowFilterResult(RangerTrinoAccessRequest request)
    {
        return rangerPlugin.evalRowFilterPolicies(request, null);
    }

    private boolean isDataMaskEnabled(RangerAccessResult result)
    {
        return result != null && result.isMaskEnabled();
    }

    private boolean isRowFilterEnabled(RangerAccessResult result)
    {
        return result != null && result.isRowFilterEnabled();
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

    private String getClientAddress(QueryId queryId)
    {
        return queryId != null ? eventListener.getClientAddress(queryId.getId()) : null;
    }

    private String getClientType(QueryId queryId)
    {
        return queryId != null ? eventListener.getClientType(queryId.getId()) : null;
    }

    private String getQueryText(QueryId queryId)
    {
        return queryId != null ? eventListener.getQueryText(queryId.getId()) : null;
    }

    private Instant getQueryTime(QueryId queryId)
    {
        return queryId != null ? eventListener.getQueryTime(queryId.getId()) : null;
    }

    private String getClientAddress(SystemSecurityContext context)
    {
        return context != null ? getClientAddress(context.getQueryId()) : null;
    }

    private String getClientType(SystemSecurityContext context)
    {
        return context != null ? getClientType(context.getQueryId()) : null;
    }

    private String getQueryText(SystemSecurityContext context)
    {
        return context != null ? getQueryText(context.getQueryId()) : null;
    }

    private Instant getQueryTime(SystemSecurityContext context)
    {
        return context != null ? getQueryTime(context.getQueryId()) : null;
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

    private boolean hasPermission(Collection<RangerTrinoResource> resources, SystemSecurityContext context, RangerTrinoAccessType accessType, String action)
    {
        boolean ret = true;

        for (RangerTrinoResource resource : resources) {
            RangerAccessResult result = rangerPlugin.isAccessAllowed(createAccessRequest(resource, context, accessType, action));

            ret = result != null && result.getIsAllowed();

            if (!ret) {
                break;
            }
        }

        return ret;
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

    private static RangerTrinoResource createUserResource(String userName)
    {
        RangerTrinoResource res = new RangerTrinoResource();

        res.setValue(RangerTrinoResource.KEY_USER, userName);

        return res;
    }

    private static RangerTrinoResource createProcedureResource(CatalogSchemaRoutineName procedure)
    {
        RangerTrinoResource res = new RangerTrinoResource();

        res.setValue(RangerTrinoResource.KEY_CATALOG, procedure.getCatalogName());
        res.setValue(RangerTrinoResource.KEY_SCHEMA, procedure.getSchemaRoutineName().getSchemaName());
        res.setValue(RangerTrinoResource.KEY_PROCEDURE, procedure.getSchemaRoutineName().getRoutineName());

        return res;
    }

    private static RangerTrinoResource createCatalogSessionResource(String catalogName, String propertyName)
    {
        RangerTrinoResource res = new RangerTrinoResource();

        res.setValue(RangerTrinoResource.KEY_CATALOG, catalogName);
        res.setValue(RangerTrinoResource.KEY_SESSION_PROPERTY, propertyName);

        return res;
    }

    private static RangerTrinoResource createResource(CatalogSchemaRoutineName procedure)
    {
        RangerTrinoResource res = new RangerTrinoResource();

        res.setValue(RangerTrinoResource.KEY_CATALOG, procedure.getCatalogName());
        res.setValue(RangerTrinoResource.KEY_SCHEMA, procedure.getSchemaRoutineName().getSchemaName());
        res.setValue(RangerTrinoResource.KEY_SCHEMA_FUNCTION, procedure.getSchemaRoutineName().getRoutineName());

        return res;
    }

    private static RangerTrinoResource createSystemPropertyResource(String property)
    {
        RangerTrinoResource res = new RangerTrinoResource();

        res.setValue(RangerTrinoResource.KEY_SYSTEM_PROPERTY, property);

        return res;
    }

    private static RangerTrinoResource createSystemInformation()
    {
        RangerTrinoResource res = new RangerTrinoResource();

        res.setValue(RangerTrinoResource.KEY_SYSINFO, "*");

        return res;
    }

    private static RangerTrinoResource createResource(CatalogSchemaName catalogSchemaName)
    {
        return createResource(catalogSchemaName.getCatalogName(), catalogSchemaName.getSchemaName());
    }

    private static RangerTrinoResource createResource(CatalogSchemaTableName catalogSchemaTableName)
    {
        return createResource(catalogSchemaTableName.getCatalogName(), catalogSchemaTableName.getSchemaTableName().getSchemaName(), catalogSchemaTableName.getSchemaTableName().getTableName());
    }

    private static RangerTrinoResource createResource(String catalogName)
    {
        return new RangerTrinoResource(catalogName, null, null);
    }

    private static RangerTrinoResource createResource(String catalogName, String schemaName)
    {
        return new RangerTrinoResource(catalogName, schemaName, null);
    }

    private static RangerTrinoResource createResource(String catalogName, String schemaName, final String tableName)
    {
        return new RangerTrinoResource(catalogName, schemaName, tableName);
    }

    private static RangerTrinoResource createResource(String catalogName, String schemaName, final String tableName, final String column)
    {
        return new RangerTrinoResource(catalogName, schemaName, tableName, column);
    }

    private static List<RangerTrinoResource> createResource(CatalogSchemaTableName table, Set<String> columns)
    {
        List<RangerTrinoResource> colRequests = new ArrayList<>();

        if (!columns.isEmpty()) {
            for (String column : columns) {
                RangerTrinoResource rangerTrinoResource = createResource(table.getCatalogName(), table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName(), column);

                colRequests.add(rangerTrinoResource);
            }
        }
        else {
            colRequests.add(createResource(table.getCatalogName(), table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName(), null));
        }

        return colRequests;
    }

    private static RangerTrinoResource createResource(String catalogName, SchemaFunctionName functionName)
    {
        RangerTrinoResource res = new RangerTrinoResource();

        res.setValue(RangerTrinoResource.KEY_CATALOG, catalogName);
        res.setValue(RangerTrinoResource.KEY_SCHEMA, functionName.getSchemaName());
        res.setValue(RangerTrinoResource.KEY_SCHEMA_FUNCTION, functionName.getFunctionName());

        return res;
    }

    private static RangerTrinoResource createResource(EntityKindAndName entity)
    {
        RangerTrinoResource ret = new RangerTrinoResource();

        switch (entity.entityKind().toUpperCase(ENGLISH)) {
            case "SCHEMA":
                ret.setValue(RangerTrinoResource.KEY_CATALOG, entity.name().getFirst());
                ret.setValue(RangerTrinoResource.KEY_SCHEMA, entity.name().get(1));
                break;

            case "TABLE":
            case "VIEW":
            case "MATERIALIZED VIEW":
                ret.setValue(RangerTrinoResource.KEY_CATALOG, entity.name().getFirst());
                ret.setValue(RangerTrinoResource.KEY_SCHEMA, entity.name().get(1));
                ret.setValue(RangerTrinoResource.KEY_TABLE, entity.name().get(2));
                break;
        }

        return ret;
    }

    private static RangerTrinoResource createRoleResource(String roleName)
    {
        RangerTrinoResource res = new RangerTrinoResource();

        res.setValue(RangerTrinoResource.KEY_ROLE, roleName);

        return res;
    }

    private static Set<RangerTrinoResource> createRoleResources(Set<String> roleNames)
    {
        Set<RangerTrinoResource> ret = new HashSet<>(roleNames.size());

        for (String rolName : roleNames) {
            ret.add(createRoleResource(rolName));
        }

        return ret;
    }

    private static RangerTrinoResource createResource(QueryId queryId)
    {
        RangerTrinoResource res = new RangerTrinoResource();

        res.setValue(RangerTrinoResource.KEY_QUERY_ID, queryId.getId());

        return res;
    }

    private static Identity toIdentity(Optional<Principal> principal)
    {
        return principal.isPresent() ? Identity.ofUser(principal.get().getName()) : Identity.ofUser("");
    }
}
