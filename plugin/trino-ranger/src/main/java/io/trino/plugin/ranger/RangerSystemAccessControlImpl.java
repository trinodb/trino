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

import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;

public class RangerSystemAccessControlImpl
        implements SystemAccessControl
{
    // User group information
    private boolean useUgi;
    // These strings hold the full path to the .xml configuration files
    private String auditConfig;
    private String hadoopConfig;
    private String policyManagerSSLConfig;
    private String securityConfig;

    public static final String RANGER_CONFIG_KEYTAB = "ranger.keytab";
    public static final String RANGER_CONFIG_PRINCIPAL = "ranger.principal";
    public static final String RANGER_CONFIG_USE_UGI = "ranger.use_ugi";
    public static final String RANGER_CONFIG_HADOOP_CONFIG = "ranger.hadoop_config";
    public static final String RANGER_AUDIT_CONFIG = "ranger.audit_resource";
    public static final String RANGER_SECURITY_CONFIG = "ranger.security_resource";
    public static final String RANGER_POLICY_MANAGER_SSL_CONFIG = "ranger.policy_manager_ssl_resource";
    public static final String RANGER_TRINO_DEFAULT_HADOOP_CONF = "trino-ranger-site.xml";
    public static final String RANGER_TRINO_SERVICETYPE = "trino";
    public static final String RANGER_TRINO_APPID = "trino";
    public static final String RANGER_SERVICE_NAME = "ranger.service_name";

    private static final Logger LOG = Logger.get(RangerSystemAccessControl.class);

    private final RangerBasePlugin rangerPlugin;

    public RangerSystemAccessControlImpl(Map<String, String> config)
    {
        super();
        useUgi = false;
        if (config.get(RANGER_SERVICE_NAME) != null) {
            rangerPlugin = new RangerBasePlugin(RANGER_TRINO_SERVICETYPE, config.get(RANGER_SERVICE_NAME), RANGER_TRINO_APPID);
        }
        else {
            throw invalidRangerConfigEntry(config, RANGER_SERVICE_NAME);
        }

        Configuration hadoopConf = newEmptyConfiguration();
        if (config.get(RANGER_CONFIG_HADOOP_CONFIG) != null) {
            hadoopConfig = config.get(RANGER_CONFIG_HADOOP_CONFIG);
            URL url = getFileLocation(config.get(RANGER_CONFIG_HADOOP_CONFIG), Optional.of(RANGER_CONFIG_HADOOP_CONFIG));
            if (url == null) {
                throw invalidRangerConfigFile(hadoopConfig);
            }
            hadoopConf.addResource(url);
            // This resource is more for Hadoop but lets add to the general book keeping config. Use false so parsing
            // isnt restricted to the directory and subdirectory where the .jar is installed.
            rangerPlugin.getConfig().addResource(url);
        }
        else {
            throw invalidRangerConfigEntry(config, RANGER_CONFIG_HADOOP_CONFIG);
        }

        UserGroupInformation.setConfiguration(hadoopConf);

        if (config.get(RANGER_CONFIG_KEYTAB) != null && config.get(RANGER_CONFIG_PRINCIPAL) != null) {
            String keytab = config.get(RANGER_CONFIG_KEYTAB);
            String principal = config.get(RANGER_CONFIG_PRINCIPAL);

            LOG.info("Performing kerberos login with principal " + principal + " and keytab " + keytab);

            try {
                UserGroupInformation.loginUserFromKeytab(principal, keytab);
            }
            catch (IOException ioe) {
                LOG.error(ioe, "Kerberos login failed");
                throw new RuntimeException(ioe);
            }
        }

        if (config.getOrDefault(RANGER_CONFIG_USE_UGI, "false").equalsIgnoreCase("true")) {
            useUgi = true;
        }

        if (config.get(RANGER_AUDIT_CONFIG) != null) {
            auditConfig = config.get(RANGER_AUDIT_CONFIG);
            URL url = getFileLocation(auditConfig, Optional.of(RANGER_AUDIT_CONFIG));
            if (url == null) {
                throw invalidRangerConfigFile(auditConfig);
            }
            // Add the resources. Make sure we use false or the parsing will be restricted to the .class directory.
            rangerPlugin.getConfig().addResource(url, false);
        }
        else {
            throw invalidRangerConfigEntry(config, RANGER_AUDIT_CONFIG);
        }

        if (config.get(RANGER_SECURITY_CONFIG) != null) {
            securityConfig = config.get(RANGER_SECURITY_CONFIG);
            URL url = getFileLocation(securityConfig, Optional.of(RANGER_SECURITY_CONFIG));
            if (url == null) {
                throw invalidRangerConfigFile(securityConfig);
            }
            // Add the resources. Make sure we use false or the parsing will be restricted to the .class install directory.
            rangerPlugin.getConfig().addResource(url, false);
        }
        else {
            throw invalidRangerConfigEntry(config, RANGER_SECURITY_CONFIG);
        }

        if (config.get(RANGER_POLICY_MANAGER_SSL_CONFIG) != null) {
            policyManagerSSLConfig = config.get(RANGER_POLICY_MANAGER_SSL_CONFIG);
            URL url = getFileLocation(policyManagerSSLConfig, Optional.of(RANGER_AUDIT_CONFIG));
            if (url == null) {
                throw invalidRangerConfigFile(policyManagerSSLConfig);
            }
            // Add the resources. Make sure we use false or the parsing will be restricted to the .class install directory.
            rangerPlugin.getConfig().addResource(url, false);
        }
        else {
            throw invalidRangerConfigEntry(config, RANGER_POLICY_MANAGER_SSL_CONFIG);
        }

        // init() will initialize the loading of the configurations from the above added resources
        // Also lauches the RangerRESTClient which reads policies from Ranger
        rangerPlugin.init();
        rangerPlugin.setResultProcessor(new RangerDefaultAuditHandler());
    }

    /**
     * FILTERING AND DATA MASKING
     **/
    private RangerAccessResult getDataMaskResult(RangerTrinoAccessRequest request)
    {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getDataMaskResult(request=" + request + ")");
        }

        RangerAccessResult ret = rangerPlugin.evalDataMaskPolicies(request, null);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getDataMaskResult(request=" + request + "): ret=" + ret);
        }

        return ret;
    }

    private RangerAccessResult getRowFilterResult(RangerTrinoAccessRequest request)
    {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getRowFilterResult(request=" + request + ")");
        }

        RangerAccessResult ret = rangerPlugin.evalRowFilterPolicies(request, null);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getRowFilterResult(request=" + request + "): ret=" + ret);
        }

        return ret;
    }

    private boolean isDataMaskEnabled(RangerAccessResult result)
    {
        return result != null && result.isMaskEnabled();
    }

    private boolean isRowFilterEnabled(RangerAccessResult result)
    {
        return result != null && result.isRowFilterEnabled();
    }

    @Override
    public List<ViewExpression> getRowFilters(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        RangerTrinoAccessRequest request = createAccessRequest(createResource(tableName), context, TrinoAccessType.SELECT);
        RangerAccessResult result = getRowFilterResult(request);

        ViewExpression viewExpression = null;
        if (isRowFilterEnabled(result)) {
            String filter = result.getFilterExpr();

            viewExpression = new ViewExpression(
                    context.getIdentity().getUser(),
                    Optional.of(tableName.getCatalogName()),
                    Optional.of(tableName.getSchemaTableName().getSchemaName()),
                    filter);
        }

        return viewExpression == null ? Collections.emptyList() : List.of(viewExpression);
    }

    @Override
    public List<ViewExpression> getColumnMasks(SystemSecurityContext context, CatalogSchemaTableName tableName, String columnName, Type type)
    {
        RangerTrinoAccessRequest request = createAccessRequest(
                createResource(tableName.getCatalogName(), tableName.getSchemaTableName().getSchemaName(),
                        tableName.getSchemaTableName().getTableName(), Optional.of(columnName)),
                context, TrinoAccessType.SELECT);
        RangerAccessResult result = getDataMaskResult(request);

        ViewExpression viewExpression = null;
        if (isDataMaskEnabled(result)) {
            String maskType = result.getMaskType();
            RangerServiceDef.RangerDataMaskTypeDef maskTypeDef = result.getMaskTypeDef();
            String transformer = null;

            if (maskTypeDef != null) {
                transformer = maskTypeDef.getTransformer();
            }

            if (StringUtils.equalsIgnoreCase(maskType, RangerPolicy.MASK_TYPE_NULL)) {
                transformer = "NULL";
            }
            else if (StringUtils.equalsIgnoreCase(maskType, RangerPolicy.MASK_TYPE_CUSTOM)) {
                String maskedValue = result.getMaskedValue();

                if (maskedValue == null) {
                    transformer = "NULL";
                }
                else {
                    transformer = maskedValue;
                }
            }

            if (StringUtils.isNotEmpty(transformer)) {
                transformer = transformer.replace("{col}", columnName).replace("{type}", type.getDisplayName());
            }

            viewExpression = new ViewExpression(
                    context.getIdentity().getUser(),
                    Optional.of(tableName.getCatalogName()),
                    Optional.of(tableName.getSchemaTableName().getSchemaName()),
                    transformer);

            if (LOG.isDebugEnabled()) {
                LOG.debug("getColumnMask: user: %s, catalog: %s, schema: %s, transformer: %s");
            }
        }

        return viewExpression == null ? Collections.emptyList() : List.of(viewExpression);
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        LOG.debug("==> RangerSystemAccessControl.filterCatalogs(" + catalogs + ")");
        Set<String> filteredCatalogs = new HashSet<>(catalogs.size());
        for (String catalog : catalogs) {
            if (hasPermission(createResource(catalog), context, TrinoAccessType.SELECT)) {
                filteredCatalogs.add(catalog);
            }
        }
        return filteredCatalogs;
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        LOG.debug("==> RangerSystemAccessControl.filterSchemas(" + catalogName + ")");
        Set<String> filteredSchemaNames = new HashSet<>(schemaNames.size());
        for (String schemaName : schemaNames) {
            if (hasPermission(createResource(catalogName, schemaName), context, TrinoAccessType.SELECT)) {
                filteredSchemaNames.add(schemaName);
            }
        }
        return filteredSchemaNames;
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        LOG.debug("==> RangerSystemAccessControl.filterTables(" + catalogName + ")");
        Set<SchemaTableName> filteredTableNames = new HashSet<>(tableNames.size());
        for (SchemaTableName tableName : tableNames) {
            RangerTrinoResource res = createResource(catalogName, tableName.getSchemaName(), tableName.getTableName());
            if (hasPermission(res, context, TrinoAccessType.SELECT)) {
                filteredTableNames.add(tableName);
            }
        }
        return filteredTableNames;
    }

    /** PERMISSION CHECKS ORDERED BY SYSTEM, CATALOG, SCHEMA, TABLE, VIEW, COLUMN, QUERY, FUNCTIONS, PROCEDURES **/

    /**
     * SYSTEM
     **/

    @Override
    public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName)
    {
        if (!hasPermission(createSystemPropertyResource(propertyName), context, TrinoAccessType.ALTER)) {
            LOG.debug("RangerSystemAccessControl.checkCanSetSystemSessionProperty denied");
            AccessDeniedException.denySetSystemSessionProperty(propertyName);
        }
    }

    @Override
    public void checkCanImpersonateUser(SystemSecurityContext context, String userName)
    {
        if (!hasPermission(createUserResource(userName), context, TrinoAccessType.IMPERSONATE)) {
            LOG.debug("RangerSystemAccessControl.checkCanImpersonateUser(" + userName + ") denied");
            AccessDeniedException.denyImpersonateUser(context.getIdentity().getUser(), userName);
        }
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        // pass as it is deprecated
    }

    /**
     * CATALOG
     **/
    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
        if (!hasPermission(createCatalogSessionResource(catalogName, propertyName), context, TrinoAccessType.ALTER)) {
            LOG.debug("RangerSystemAccessControl.checkCanSetCatalogSessionProperty(" + catalogName + ") denied");
            AccessDeniedException.denySetCatalogSessionProperty(catalogName, propertyName);
        }
    }

/*  Erik Anderson, commented out because interface is currently broken
  @Override
  public void checkCanShowRoles(SystemSecurityContext context, String catalogName) {
    if (!hasPermission(createResource(catalogName), context, TrinoAccessType.SHOW)) {
      LOG.debug("RangerSystemAccessControl.checkCanShowRoles(" + catalogName + ") denied");
      AccessDeniedException.denyShowRoles(catalogName);
    }
  }
*/

    @Override
    public void checkCanAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        if (!hasPermission(createResource(catalogName), context, TrinoAccessType.USE)) {
            LOG.debug("RangerSystemAccessControl.checkCanAccessCatalog(" + catalogName + ") denied");
            AccessDeniedException.denyCatalogAccess(catalogName);
        }
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
        if (!hasPermission(createResource(catalogName), context, TrinoAccessType.SHOW)) {
            LOG.debug("RangerSystemAccessControl.checkCanShowSchemas(" + catalogName + ") denied");
            AccessDeniedException.denyShowSchemas(catalogName);
        }
    }

    /**
     * SCHEMA
     **/

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, TrinoPrincipal principal)
    {
        if (!hasPermission(createResource(schema.getCatalogName(), schema.getSchemaName()), context, TrinoAccessType.GRANT)) {
            LOG.debug("RangerSystemAccessControl.checkCanSetSchemaAuthorization(" + schema.getSchemaName() + ") denied");
            AccessDeniedException.denySetSchemaAuthorization(schema.getSchemaName(), principal);
        }
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!hasPermission(createResource(schema.getCatalogName(), schema.getSchemaName()), context, TrinoAccessType.SHOW)) {
            LOG.debug("RangerSystemAccessControl.checkCanShowCreateSchema(" + schema.getSchemaName() + ") denied");
            AccessDeniedException.denyShowCreateSchema(schema.getSchemaName());
        }
    }

    /**
     * Create schema is evaluated on the level of the Catalog. This means that it is assumed you have permission
     * to create a schema when you have create rights on the catalog level
     */
    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!hasPermission(createResource(schema.getCatalogName()), context, TrinoAccessType.CREATE)) {
            LOG.debug("RangerSystemAccessControl.checkCanCreateSchema(" + schema.getSchemaName() + ") denied");
            AccessDeniedException.denyCreateSchema(schema.getSchemaName());
        }
    }

    /**
     * This is evaluated against the schema name as ownership information is not available
     */
    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!hasPermission(createResource(schema.getCatalogName(), schema.getSchemaName()), context, TrinoAccessType.DROP)) {
            LOG.debug("RangerSystemAccessControl.checkCanDropSchema(" + schema.getSchemaName() + ") denied");
            AccessDeniedException.denyDropSchema(schema.getSchemaName());
        }
    }

    /**
     * This is evaluated against the schema name as ownership information is not available
     */
    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        RangerTrinoResource res = createResource(schema.getCatalogName(), schema.getSchemaName());
        if (!hasPermission(res, context, TrinoAccessType.ALTER)) {
            LOG.debug("RangerSystemAccessControl.checkCanRenameSchema(" + schema.getSchemaName() + ") denied");
            AccessDeniedException.denyRenameSchema(schema.getSchemaName(), newSchemaName);
        }
    }

    /**
     * TABLE
     **/

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!hasPermission(createResource(schema), context, TrinoAccessType.SHOW)) {
            LOG.debug("RangerSystemAccessControl.checkCanShowTables(" + schema + ") denied");
            AccessDeniedException.denyShowTables(schema.toString());
        }
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createResource(table), context, TrinoAccessType.SHOW)) {
            LOG.debug("RangerSystemAccessControl.checkCanShowTables(" + table + ") denied");
            AccessDeniedException.denyShowCreateTable(table.toString());
        }
    }

    /**
     * check if materialized view can be created
     */
    @Override
    public void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView,
            Map<String, Object> properties)
    {
        if (!hasPermission(createResource(materializedView), context, TrinoAccessType.CREATE)) {
            LOG.debug("RangerSystemAccessControl.checkCanCreateMaterializedView( " + materializedView.getSchemaTableName().getTableName() + ") denied");
            AccessDeniedException.denyCreateMaterializedView(materializedView.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDropMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        if (!hasPermission(createResource(materializedView), context, TrinoAccessType.DROP)) {
            LOG.debug("RangerSystemAccessControl.checkCanDropMaterializedView(" + materializedView.getSchemaTableName().getTableName() + ") denied");
            AccessDeniedException.denyDropMaterializedView(materializedView.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRefreshMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        if (!hasPermission(createResource(materializedView), context, TrinoAccessType.ALTER)) {
            LOG.debug("RangerSystemAccessControl.checkCanRefreshMaterializedView\n(" + materializedView.getSchemaTableName().getTableName() + ") denied");
            AccessDeniedException.denyRefreshMaterializedView(materializedView.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRenameMaterializedView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        if (!hasPermission(createResource(view), context, TrinoAccessType.ALTER)) {
            LOG.debug("RangerSystemAccessControl.checkCanRenameMaterializedView\n(" + view.getSchemaTableName().getTableName() + ") denied");
            AccessDeniedException.denyRenameMaterializedView(view.getSchemaTableName().getTableName(),
                    newView.getSchemaTableName().getTableName());
        }
    }

    /**
     * Create table is verified on schema level
     */
    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Object> properties)
    {
        if (!hasPermission(createResource(table.getCatalogName(), table.getSchemaTableName().getSchemaName()), context, TrinoAccessType.CREATE)) {
            LOG.debug("RangerSystemAccessControl.checkCanCreateTable(" + table.getSchemaTableName().getTableName() + ") denied");
            AccessDeniedException.denyCreateTable(table.getSchemaTableName().getTableName());
        }
    }

    /**
     * This is evaluated against the table name as ownership information is not available
     */
    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createResource(table), context, TrinoAccessType.DROP)) {
            LOG.debug("RangerSystemAccessControl.checkCanDropTable(" + table.getSchemaTableName().getTableName() + ") denied");
            AccessDeniedException.denyDropTable(table.getSchemaTableName().getTableName());
        }
    }

    /**
     * This is evaluated against the table name as ownership information is not available
     */
    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        RangerTrinoResource res = createResource(table);
        if (!hasPermission(res, context, TrinoAccessType.ALTER)) {
            LOG.debug("RangerSystemAccessControl.checkCanRenameTable(" + table.getSchemaTableName().getTableName() + ") denied");
            AccessDeniedException.denyRenameTable(table.getSchemaTableName().getTableName(), newTable.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        RangerTrinoResource res = createResource(table);
        if (!hasPermission(res, context, TrinoAccessType.INSERT)) {
            LOG.debug("RangerSystemAccessControl.checkCanInsertIntoTable(" + table.getSchemaTableName().getTableName() + ") denied");
            AccessDeniedException.denyInsertTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createResource(table), context, TrinoAccessType.DELETE)) {
            LOG.debug("RangerSystemAccessControl.checkCanDeleteFromTable(" + table.getSchemaTableName().getTableName() + ") denied");
            AccessDeniedException.denyDeleteTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee, boolean withGrantOption)
    {
        if (!hasPermission(createResource(table), context, TrinoAccessType.GRANT)) {
            LOG.debug("RangerSystemAccessControl.checkCanGrantTablePrivilege(" + table + ") denied");
            AccessDeniedException.denyGrantTablePrivilege(privilege.toString(), table.toString());
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal revokee, boolean grantOptionFor)
    {
        if (!hasPermission(createResource(table), context, TrinoAccessType.REVOKE)) {
            LOG.debug("RangerSystemAccessControl.checkCanRevokeTablePrivilege(" + table + ") denied");
            AccessDeniedException.denyRevokeTablePrivilege(privilege.toString(), table.toString());
        }
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createResource(table), context, TrinoAccessType.ALTER)) {
            LOG.debug("RangerSystemAccessControl.checkCanSetTableComment(" + table + ") denied");
            AccessDeniedException.denyCommentTable(table.toString());
        }
    }

    @Override
    public void checkCanExecuteTableProcedure(SystemSecurityContext context, CatalogSchemaTableName table,
                                              String procedure)
    {
        if (!hasPermission(createResource(table), context, TrinoAccessType.EXECUTE)) {
            LOG.debug("RangerSystemAccessControl.checkCanExecuteTableProcedure(" + table + ") denied");
            AccessDeniedException.denyExecuteTableProcedure(table.toString(), procedure);
        }
    }

    /**
     * Create view is verified on schema level
     */
    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!hasPermission(createResource(view.getCatalogName(), view.getSchemaTableName().getSchemaName()), context, TrinoAccessType.CREATE)) {
            LOG.debug("RangerSystemAccessControl.checkCanCreateView(" + view.getSchemaTableName().getTableName() + ") denied");
            AccessDeniedException.denyCreateView(view.getSchemaTableName().getTableName());
        }
    }

    /**
     * This is evaluated against the table name as ownership information is not available
     */
    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!hasPermission(createResource(view), context, TrinoAccessType.DROP)) {
            LOG.debug("RangerSystemAccessControl.checkCanDropView(" + view.getSchemaTableName().getTableName() + ") denied");
            AccessDeniedException.denyDropView(view.getSchemaTableName().getTableName());
        }
    }

    /**
     * This check equals the check for checkCanCreateView
     */
    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        try {
            checkCanCreateView(context, table);
        }
        catch (AccessDeniedException ade) {
            LOG.debug("RangerSystemAccessControl.checkCanCreateViewWithSelectFromColumns(" + table.getSchemaTableName().getTableName() + ") denied");
            AccessDeniedException.denyCreateViewWithSelect(table.getSchemaTableName().getTableName(), context.getIdentity());
        }
    }

    /**
     * This is evaluated against the table name as ownership information is not available
     */
    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        if (!hasPermission(createResource(view), context, TrinoAccessType.ALTER)) {
            LOG.debug("RangerSystemAccessControl.checkCanRenameView(" + view + ") denied");
            AccessDeniedException.denyRenameView(view.toString(), newView.toString());
        }
    }

    /** COLUMN **/

    /**
     * This is evaluated on table level
     */
    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        RangerTrinoResource res = createResource(table);
        if (!hasPermission(res, context, TrinoAccessType.ALTER)) {
            AccessDeniedException.denyAddColumn(table.getSchemaTableName().getTableName());
        }
    }

    /**
     * This is evaluated on table level
     */
    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        RangerTrinoResource res = createResource(table);
        if (!hasPermission(res, context, TrinoAccessType.DROP)) {
            LOG.debug("RangerSystemAccessControl.checkCanDropColumn(" + table.getSchemaTableName().getTableName() + ") denied");
            AccessDeniedException.denyDropColumn(table.getSchemaTableName().getTableName());
        }
    }

    /**
     * This is evaluated on table level
     */
    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        RangerTrinoResource res = createResource(table);
        if (!hasPermission(res, context, TrinoAccessType.ALTER)) {
            LOG.debug("RangerSystemAccessControl.checkCanRenameColumn(" + table.getSchemaTableName().getTableName() + ") denied");
            AccessDeniedException.denyRenameColumn(table.getSchemaTableName().getTableName());
        }
    }

    /**
     * This is evaluated on table level
     */
    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!hasPermission(createResource(table), context, TrinoAccessType.SHOW)) {
            LOG.debug("RangerSystemAccessControl.checkCanShowTables(" + table + ") denied");
            AccessDeniedException.denyShowColumns(table.toString());
        }
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        for (RangerTrinoResource res : createResource(table, columns)) {
            if (!hasPermission(res, context, TrinoAccessType.SELECT)) {
                LOG.debug("RangerSystemAccessControl.checkCanSelectFromColumns(" + table.getSchemaTableName().getTableName() + ") denied");
                AccessDeniedException.denySelectColumns(table.getSchemaTableName().getTableName(), columns);
            }
        }
    }

    /**
     * This is a NOOP, no filtering is applied
     */
    @Override
    public Set<String> filterColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        return columns;
    }

    /** QUERY **/

    /**
     * This is a NOOP. Everyone can execute a query
     *
     * @param context
     */
    @Override
    public void checkCanExecuteQuery(SystemSecurityContext context)
    {
    }

    @Override
    public void checkCanViewQueryOwnedBy(SystemSecurityContext context, String queryOwner)
    {
        if (!hasPermission(createUserResource(queryOwner), context, TrinoAccessType.IMPERSONATE)) {
            LOG.debug("RangerSystemAccessControl.checkCanViewQueryOwnedBy(" + queryOwner + ") denied");
            AccessDeniedException.denyImpersonateUser(context.getIdentity().getUser(), queryOwner);
        }
    }

    /**
     * This is a NOOP, no filtering is applied
     */
    @Override
    public Set<String> filterViewQueryOwnedBy(SystemSecurityContext context, Set<String> queryOwners)
    {
        return queryOwners;
    }

    @Override
    public void checkCanKillQueryOwnedBy(SystemSecurityContext context, String queryOwner)
    {
        if (!hasPermission(createUserResource(queryOwner), context, TrinoAccessType.IMPERSONATE)) {
            LOG.debug("RangerSystemAccessControl.checkCanKillQueryOwnedBy(" + queryOwner + ") denied");
            AccessDeniedException.denyImpersonateUser(context.getIdentity().getUser(), queryOwner);
        }
    }

    /**
     * FUNCTIONS
     **/
    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SystemSecurityContext context, String function, TrinoPrincipal grantee, boolean grantOption)
    {
        if (!hasPermission(createFunctionResource(function), context, TrinoAccessType.GRANT)) {
            LOG.debug("RangerSystemAccessControl.checkCanGrantExecuteFunctionPrivilege(" + function + ") denied");
            AccessDeniedException.denyGrantExecuteFunctionPrivilege(function, context.getIdentity(), grantee.getName());
        }
    }

    @Override
    public void checkCanExecuteFunction(SystemSecurityContext context, String function)
    {
        if (!hasPermission(createFunctionResource(function), context, TrinoAccessType.EXECUTE)) {
            LOG.debug("RangerSystemAccessControl.checkCanExecuteFunction(" + function + ") denied");
            AccessDeniedException.denyExecuteFunction(function);
        }
    }

    /**
     * PROCEDURES
     **/
    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext context, CatalogSchemaRoutineName procedure)
    {
        if (!hasPermission(createProcedureResource(procedure), context, TrinoAccessType.EXECUTE)) {
            LOG.debug("RangerSystemAccessControl.checkCanExecuteFunction(" + procedure.getSchemaRoutineName().getRoutineName() + ") denied");
            AccessDeniedException.denyExecuteProcedure(procedure.getSchemaRoutineName().getRoutineName());
        }
    }

    /**
     * HELPER FUNCTIONS
     **/

    private RangerTrinoAccessRequest createAccessRequest(RangerTrinoResource resource, SystemSecurityContext context, TrinoAccessType accessType)
    {
        String userName = null;
        Set<String> userGroups = null;

        if (useUgi) {
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(context.getIdentity().getUser());

            userName = ugi.getShortUserName();
            String[] groups = ugi != null ? ugi.getGroupNames() : null;

            if (groups != null && groups.length > 0) {
                userGroups = new HashSet<>(Arrays.asList(groups));
            }
        }
        else {
            userName = context.getIdentity().getUser();
            userGroups = context.getIdentity().getGroups();
        }

        RangerTrinoAccessRequest request = new RangerTrinoAccessRequest(
                resource,
                userName,
                userGroups,
                accessType);

        return request;
    }

    private boolean hasPermission(RangerTrinoResource resource, SystemSecurityContext context, TrinoAccessType accessType)
    {
        boolean ret = false;

        RangerTrinoAccessRequest request = createAccessRequest(resource, context, accessType);

        RangerAccessResult result = rangerPlugin.isAccessAllowed(request);
        if (result != null && result.getIsAllowed()) {
            ret = true;
        }

        return ret;
    }

    private static RangerTrinoResource createUserResource(String userName)
    {
        RangerTrinoResource res = new RangerTrinoResource();
        res.setValue(RangerTrinoResource.KEY_USER, userName);

        return res;
    }

    private static RangerTrinoResource createFunctionResource(String function)
    {
        RangerTrinoResource res = new RangerTrinoResource();
        res.setValue(RangerTrinoResource.KEY_FUNCTION, function);

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

    private static RangerTrinoResource createSystemPropertyResource(String property)
    {
        RangerTrinoResource res = new RangerTrinoResource();
        res.setValue(RangerTrinoResource.KEY_SYSTEM_PROPERTY, property);

        return res;
    }

    private static RangerTrinoResource createResource(CatalogSchemaName catalogSchemaName)
    {
        return createResource(catalogSchemaName.getCatalogName(), catalogSchemaName.getSchemaName());
    }

    private static RangerTrinoResource createResource(CatalogSchemaTableName catalogSchemaTableName)
    {
        return createResource(catalogSchemaTableName.getCatalogName(),
                catalogSchemaTableName.getSchemaTableName().getSchemaName(),
                catalogSchemaTableName.getSchemaTableName().getTableName());
    }

    private static RangerTrinoResource createResource(String catalogName)
    {
        return new RangerTrinoResource(catalogName, Optional.empty(), Optional.empty());
    }

    private static RangerTrinoResource createResource(String catalogName, String schemaName)
    {
        return new RangerTrinoResource(catalogName, Optional.of(schemaName), Optional.empty());
    }

    private static RangerTrinoResource createResource(String catalogName, String schemaName, final String tableName)
    {
        return new RangerTrinoResource(catalogName, Optional.of(schemaName), Optional.of(tableName));
    }

    private static RangerTrinoResource createResource(String catalogName, String schemaName, final String tableName, final Optional<String> column)
    {
        return new RangerTrinoResource(catalogName, Optional.of(schemaName), Optional.of(tableName), column);
    }

    private static List<RangerTrinoResource> createResource(CatalogSchemaTableName table, Set<String> columns)
    {
        List<RangerTrinoResource> colRequests = new ArrayList<>();

        if (columns.size() > 0) {
            for (String column : columns) {
                RangerTrinoResource rangerTrinoResource = createResource(table.getCatalogName(),
                        table.getSchemaTableName().getSchemaName(),
                        table.getSchemaTableName().getTableName(), Optional.of(column));
                colRequests.add(rangerTrinoResource);
            }
        }
        else {
            colRequests.add(createResource(table.getCatalogName(),
                    table.getSchemaTableName().getSchemaName(),
                    table.getSchemaTableName().getTableName(), Optional.empty()));
        }
        return colRequests;
    }

    private URL getFileLocation(String fileName, Optional<String> configEntry)
    {
        URL lurl = null;

        if (LOG.isDebugEnabled()) {
            if (configEntry.isPresent()) {
                LOG.debug("Trying to load config(" + configEntry.get() + " from " + fileName + " (cannot be null)");
            }
            else {
                LOG.debug("Trying to load config from " + fileName + " (cannot be null)");
            }
        }

        if (!StringUtils.isEmpty(fileName)) {
            lurl = RangerConfiguration.class.getClassLoader().getResource(fileName);

            if (lurl == null) {
                lurl = RangerConfiguration.class.getClassLoader().getResource("/" + fileName);
            }

            if (lurl == null) {
                File f = new File(fileName);
                if (f.exists()) {
                    try {
                        lurl = f.toURI().toURL();
                    }
                    catch (MalformedURLException e) {
                        LOG.error("Unable to load the resource name [" + fileName + "]. Ignoring the resource:" + f.getPath());
                    }
                }
                else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Conf file path " + fileName + " does not exists");
                    }
                }
            }
        }
        return lurl;
    }

    private static TrinoException invalidRangerConfigFile(String fileName)
    {
        return new TrinoException(
            CONFIGURATION_INVALID,
            String.format("%s is not a valid file", fileName));
    }

    private static TrinoException invalidRangerConfigEntry(Map<String, String> config, String configEntry)
    {
        return new TrinoException(
                CONFIGURATION_INVALID,
                String.format("%s must be specified in the ranger configurations. Value was '%s'", configEntry, config.get(configEntry)));
    }
}
