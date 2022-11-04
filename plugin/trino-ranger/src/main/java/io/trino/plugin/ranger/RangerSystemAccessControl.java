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

import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;

import javax.inject.Inject;

import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class RangerSystemAccessControl
        implements SystemAccessControl
{
    private static final String RANGER_PLUGIN_TYPE = "trino";
    // Apache Ranger will be the default authorizer if no authorizers match your catalog.
    private static final String RANGER_TRINO_AUTHORIZER_IMPL_CLASSNAME = "io.trino.plugin.ranger.RangerSystemAccessControlImpl";

    private final RangerPluginClassLoader rangerPluginClassLoader;
    private final SystemAccessControl systemAccessControlImpl;

    @Inject
    public RangerSystemAccessControl(RangerConfig config)
    {
        try {
            rangerPluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());

            @SuppressWarnings("unchecked")
            Class<SystemAccessControl> cls = (Class<SystemAccessControl>) Class.forName(RANGER_TRINO_AUTHORIZER_IMPL_CLASSNAME, true, rangerPluginClassLoader);

            activatePluginClassLoader();

            Map<String, String> configMap = new HashMap<>();
            if (config.getKeytab() != null && config.getPrincipal() != null) {
                configMap.put("ranger.keytab", config.getKeytab());
                configMap.put("ranger.principal", config.getPrincipal());
            }

            configMap.put("ranger.use_ugi", Boolean.toString(config.isUseUgi()));

            if (config.getHadoopConfigPath() != null) {
                configMap.put("ranger.hadoop_config", config.getHadoopConfigPath());
            }

            if (config.getAuditResourcesConfigPath() != null) {
                configMap.put("ranger.audit_resource", config.getAuditResourcesConfigPath());
            }

            if (config.getSecurityResourcesConfigPath() != null) {
                configMap.put("ranger.security_resource", config.getSecurityResourcesConfigPath());
            }

            if (config.getPolicyManagerSSLConfigPath() != null) {
                configMap.put("ranger.policy_manager_ssl_resource", config.getPolicyManagerSSLConfigPath());
            }

            if (config.getServiceName() != null) {
                configMap.put("ranger.service_name", config.getServiceName());
            }

            systemAccessControlImpl = cls.getDeclaredConstructor(Map.class).newInstance(configMap);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanSetSystemSessionProperty(context, propertyName);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanAccessCatalog(context, catalogName);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        Set<String> filteredCatalogs;
        try {
            activatePluginClassLoader();
            filteredCatalogs = systemAccessControlImpl.filterCatalogs(context, catalogs);
        }
        finally {
            deactivatePluginClassLoader();
        }
        return filteredCatalogs;
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanCreateSchema(context, schema);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanDropSchema(context, schema);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanRenameSchema(context, schema, newSchemaName);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanShowSchemas(context, catalogName);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        Set<String> filteredSchemas;
        try {
            activatePluginClassLoader();
            filteredSchemas = systemAccessControlImpl.filterSchemas(context, catalogName, schemaNames);
        }
        finally {
            deactivatePluginClassLoader();
        }
        return filteredSchemas;
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Object> properties)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanCreateTable(context, table, properties);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanDropTable(context, table);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView,
            Map<String, Object> properties)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanCreateMaterializedView(context, materializedView, properties);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanDropMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanDropMaterializedView(context, materializedView);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanRefreshMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanRefreshMaterializedView(context, materializedView);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanRenameMaterializedView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanRenameMaterializedView(context, view, newView);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanRenameTable(context, table, newTable);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        Set<SchemaTableName> filteredTableNames;
        try {
            activatePluginClassLoader();
            filteredTableNames = systemAccessControlImpl.filterTables(context, catalogName, tableNames);
        }
        finally {
            deactivatePluginClassLoader();
        }
        return filteredTableNames;
    }

    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanAddColumn(context, table);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanDropColumn(context, table);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanRenameColumn(context, table);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanSelectFromColumns(context, table, columns);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanInsertIntoTable(context, table);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanDeleteFromTable(context, table);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanCreateView(context, view);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanDropView(context, view);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanCreateViewWithSelectFromColumns(context, table, columns);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanSetCatalogSessionProperty(context, catalogName, propertyName);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanImpersonateUser(SystemSecurityContext context, String userName)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanImpersonateUser(context, userName);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanExecuteQuery(SystemSecurityContext context)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanExecuteQuery(context);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanViewQueryOwnedBy(SystemSecurityContext context, String queryOwner)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanViewQueryOwnedBy(context, queryOwner);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public Set<String> filterViewQueryOwnedBy(SystemSecurityContext context, Set<String> queryOwners)
    {
        Set<String> filteredQueryOwners;
        try {
            activatePluginClassLoader();
            filteredQueryOwners = systemAccessControlImpl.filterViewQueryOwnedBy(context, queryOwners);
        }
        finally {
            deactivatePluginClassLoader();
        }
        return filteredQueryOwners;
    }

    @Override
    public void checkCanKillQueryOwnedBy(SystemSecurityContext context, String queryOwner)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanKillQueryOwnedBy(context, queryOwner);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanShowCreateTable(context, table);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanSetTableComment(context, table);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanExecuteTableProcedure(SystemSecurityContext context, CatalogSchemaTableName table,
                                               String procedure)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanExecuteTableProcedure(context, table, procedure);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanShowTables(context, schema);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanShowColumns(context, table);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public Set<String> filterColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        Set<String> filteredColumns;
        try {
            activatePluginClassLoader();
            filteredColumns = systemAccessControlImpl.filterColumns(context, table, columns);
        }
        finally {
            deactivatePluginClassLoader();
        }
        return filteredColumns;
    }

    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanRenameView(context, view, newView);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee, boolean withGrantOption)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanGrantTablePrivilege(context, privilege, table, grantee, withGrantOption);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal revokee, boolean grantOptionFor)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanRevokeTablePrivilege(context, privilege, table, revokee, grantOptionFor);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

/*  Erik Anderson, commented out because interface is currently broken
  @Override
  public void checkCanShowRoles(SystemSecurityContext context, String catalogName) {
    try {
      activatePluginClassLoader();
      systemAccessControlImpl.checkCanShowRoles(context, catalogName);
    } finally {
      deactivatePluginClassLoader();
    }
  }
*/

    @Override
    public List<ViewExpression> getRowFilters(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        List<ViewExpression> viewExpressions;
        try {
            activatePluginClassLoader();
            viewExpressions = systemAccessControlImpl.getRowFilters(context, tableName);
        }
        finally {
            deactivatePluginClassLoader();
        }
        return viewExpressions;
    }

    @Override
    public List<ViewExpression> getColumnMasks(SystemSecurityContext context, CatalogSchemaTableName tableName, String columnName, Type type)
    {
        List<ViewExpression> viewExpressions;
        try {
            activatePluginClassLoader();
            viewExpressions = systemAccessControlImpl.getColumnMasks(context, tableName, columnName, type);
        }
        finally {
            deactivatePluginClassLoader();
        }
        return viewExpressions;
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanSetUser(principal, userName);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SystemSecurityContext context, String functionName, TrinoPrincipal grantee, boolean grantOption)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanGrantExecuteFunctionPrivilege(context, functionName, grantee, grantOption);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, TrinoPrincipal principal)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanSetSchemaAuthorization(context, schema, principal);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schemaName)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanShowCreateSchema(context, schemaName);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName procedure)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanExecuteProcedure(systemSecurityContext, procedure);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void checkCanExecuteFunction(SystemSecurityContext systemSecurityContext, String functionName)
    {
        try {
            activatePluginClassLoader();
            systemAccessControlImpl.checkCanExecuteFunction(systemSecurityContext, functionName);
        }
        finally {
            deactivatePluginClassLoader();
        }
    }

    private void activatePluginClassLoader()
    {
        if (rangerPluginClassLoader != null) {
            rangerPluginClassLoader.activate();
        }
    }

    private void deactivatePluginClassLoader()
    {
        if (rangerPluginClassLoader != null) {
            rangerPluginClassLoader.deactivate();
        }
    }
}
