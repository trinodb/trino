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
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.connector.EntityPrivilege;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.security.Principal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNullElse;

public class RangerSystemAccessControl
        implements SystemAccessControl
{
    private static final Logger LOG = LoggerFactory.getLogger(RangerSystemAccessControl.class);

    public static final String RANGER_TRINO_DEFAULT_HADOOP_CONF = "trino-ranger-site.xml";
    public static final String RANGER_TRINO_SERVICETYPE = "trino";
    public static final String RANGER_TRINO_APPID = "trino";
    public static final String RANGER_TRINO_DEFAULT_SERVICE_NAME = "dev_trino";
    public static final String RANGER_TRINO_DEFAULT_SECURITY_CONF = "ranger-trino-security.xml";
    public static final String RANGER_TRINO_DEFAULT_AUDIT_CONF = "ranger-trino-audit.xml";
    public static final String RANGER_TRINO_DEFAULT_POLICYMGR_SSL_CONF = "ranger-policymgr-ssl.xml";

    private final RangerBasePlugin rangerPlugin;
    private final boolean useUgi;
    private final RangerTrinoEventListener eventListener = new RangerTrinoEventListener();

    @Inject
    public RangerSystemAccessControl(RangerConfig config)
    {
        super();

        setDefaultConfig(config);

        Configuration hadoopConf = new Configuration();

        if (config.getHadoopConfigPath() != null) {
            URL url = hadoopConf.getResource(config.getHadoopConfigPath());

            if (url == null) {
                LOG.warn("Hadoop config {} not found", config.getHadoopConfigPath());
            }
            else {
                hadoopConf.addResource(url);
            }
        }
        else {
            URL url = hadoopConf.getResource(RANGER_TRINO_DEFAULT_HADOOP_CONF);

            LOG.info("Trying to load Hadoop config from {} (can be null)", url);

            if (url != null) {
                hadoopConf.addResource(url);
            }
        }

        UserGroupInformation.setConfiguration(hadoopConf);

        if (config.getKeytab() != null && config.getPrincipal() != null) {
            String keytab = config.getKeytab();
            String principal = config.getPrincipal();

            LOG.info("Performing kerberos login with principal {} and keytab {}", principal, keytab);

            try {
                UserGroupInformation.loginUserFromKeytab(principal, keytab);
            }
            catch (IOException ioe) {
                LOG.error("Kerberos login failed", ioe); // ERROR

                throw new RuntimeException(ioe);
            }
        }

        useUgi = config.isUseUgi();

        RangerPluginConfig pluginConfig = new RangerPluginConfig(RANGER_TRINO_SERVICETYPE, config.getServiceName(), RANGER_TRINO_APPID, null, null, null);

        pluginConfig.addResourceIfReadable(config.getAuditConfigPath());
        pluginConfig.addResourceIfReadable(config.getSecurityConfigPath());
        pluginConfig.addResourceIfReadable(config.getPolicyMgrSslConfigPath());

        rangerPlugin = new RangerBasePlugin(pluginConfig);

        try {
            rangerPlugin.init();
        }
        catch (Throwable t) {
            LOG.error("Ranger authorizer initialization failed", t); // ERROR
        }

        rangerPlugin.setResultProcessor(new RangerDefaultAuditHandler());
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        LOG.debug("==> checkCanImpersonateUser(identity={}, userName={})", identity, userName);

        if (!hasPermission(createUserResource(userName), identity, null, TrinoAccessType.IMPERSONATE, "ImpersonateUser")) {
            LOG.debug("<== checkCanImpersonateUser(identity={}, userName={}): denied", identity, userName);

            AccessDeniedException.denyImpersonateUser(identity.getUser(), userName);
        }

        LOG.debug("<== checkCanImpersonateUser(identity={}, userName={}): allowed", identity, userName);
    }

    @Deprecated
    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        LOG.debug("==> checkCanSetUser(principal={}, userName={})", principal, userName);

        if (!hasPermission(createUserResource(userName), principal, null, TrinoAccessType.IMPERSONATE, "SetUser")) {
            LOG.debug("<== checkCanSetUser(principal={}, userName={}): denied", principal, userName);

            AccessDeniedException.denySetUser(principal, userName);
        }

        LOG.debug("<== checkCanSetUser(principal={}, userName={}): allowed", principal, userName);
    }

    /** QUERY **/
    @Deprecated
    @Override
    public void checkCanExecuteQuery(Identity identity)
    {
        checkCanExecuteQuery(identity, null);
    }

    @Override
    public void checkCanExecuteQuery(Identity identity, QueryId queryId)
    {
        LOG.debug("==> checkCanExecuteQuery(identity={}, queryId={})", identity, queryId);

        if (!hasPermission(createResource(queryId), identity, queryId, TrinoAccessType.EXECUTE, "ExecuteQuery")) {
            LOG.debug("<== checkCanExecuteQuery(identity={}, queryId={}): denied", identity, queryId);

            AccessDeniedException.denyExecuteQuery();
        }

        LOG.debug("<== checkCanExecuteQuery(identity={}, queryId={}): allowed", identity, queryId);
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        LOG.debug("==> checkCanViewQueryOwnedBy(identity={}, queryOwner={})", identity, queryOwner);

        if (!hasPermission(createUserResource(queryOwner.getUser()), identity, null, TrinoAccessType.IMPERSONATE, "ViewQueryOwnedBy")) {
            LOG.debug("<== checkCanViewQueryOwnedBy(identity={}, queryOwner={}): denied", identity, queryOwner);

            AccessDeniedException.denyImpersonateUser(identity.getUser(), queryOwner.getUser());
        }

        LOG.debug("<== checkCanViewQueryOwnedBy(identity={}, queryOwner={}): allowed", identity, queryOwner);
    }

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(Identity identity, Collection<Identity> queryOwners)
    {
        LOG.debug("==> filterViewQueryOwnedBy(identity={}, queryOwners={})", identity, queryOwners);

        Set<Identity> toExclude = null;

        for (Identity queryOwner : queryOwners) {
            if (!hasPermissionForFilter(createUserResource(queryOwner.getUser()), identity, null, TrinoAccessType.IMPERSONATE, "filterViewQueryOwnedBy")) {
                LOG.debug("filterViewQueryOwnedBy(user={}): skipping queries owned by {}", identity, queryOwner);

                if (toExclude == null) {
                    toExclude = new HashSet<>();
                }

                toExclude.add(queryOwner);
            }
        }

        Collection<Identity> ret = (toExclude == null) ? queryOwners : queryOwners.stream().filter(((Predicate<? super Identity>) toExclude::contains).negate()).collect(Collectors.toList());

        LOG.debug("<== filterViewQueryOwnedBy(identity={}, queryOwners={}): ret={}", identity, queryOwners, ret);

        return ret;
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        LOG.debug("==> checkCanKillQueryOwnedBy(identity={}, queryOwner={})", identity, queryOwner);

        if (!hasPermission(createUserResource(queryOwner.getUser()), identity, null, TrinoAccessType.IMPERSONATE, "KillQueryOwnedBy")) {
            LOG.debug("<== checkCanKillQueryOwnedBy(identity={}, queryOwner={}): denied", identity, queryOwner);

            AccessDeniedException.denyImpersonateUser(identity.getUser(), queryOwner.getUser());
        }

        LOG.debug("<== checkCanKillQueryOwnedBy(identity={}, queryOwner={}): allowed", identity, queryOwner);
    }

    @Override
    public void checkCanReadSystemInformation(Identity identity)
    {
        LOG.debug("==> checkCanReadSystemInformation(identity={})", identity);

        if (!hasPermission(createSystemInformation(), identity, null, TrinoAccessType.READ_SYSINFO, "ReadSystemInformation")) {
            LOG.debug("<== checkCanReadSystemInformation(identity={}): denied", identity);

            AccessDeniedException.denyReadSystemInformationAccess();
        }

        LOG.debug("<== checkCanReadSystemInformation(identity={})", identity);
    }

    @Override
    public void checkCanWriteSystemInformation(Identity identity)
    {
        LOG.debug("==> checkCanWriteSystemInformation(identity={})", identity);

        if (!hasPermission(createSystemInformation(), identity, null, TrinoAccessType.WRITE_SYSINFO, "WriteSystemInformation")) {
            LOG.debug("<== checkCanWriteSystemInformation(identity={}): denied", identity);

            AccessDeniedException.denyWriteSystemInformationAccess();
        }

        LOG.debug("<== checkCanWriteSystemInformation(identity={})", identity);
    }

    @Deprecated
    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        LOG.debug("==> checkCanSetSystemSessionProperty(identity={}, propertyName={})", identity, propertyName);

        if (!hasPermission(createSystemPropertyResource(propertyName), identity, null, TrinoAccessType.ALTER, "SetSystemSessionProperty")) {
            LOG.debug("<== checkCanSetSystemSessionProperty(identity={}, propertyName={}): denied", identity, propertyName);

            AccessDeniedException.denySetSystemSessionProperty(propertyName);
        }

        LOG.debug("<== checkCanSetSystemSessionProperty(identity={}, propertyName={}): allowed", identity, propertyName);
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, QueryId queryId, String propertyName)
    {
        LOG.debug("==> checkCanSetSystemSessionProperty(identity={}, queryId={}, propertyName={})", identity, queryId, propertyName);

        if (!hasPermission(createSystemPropertyResource(propertyName), identity, queryId, TrinoAccessType.ALTER, "SetSystemSessionProperty")) {
            LOG.debug("<== checkCanSetSystemSessionProperty(identity={}, queryId={}, propertyName={}): denied", identity, queryId, propertyName);

            AccessDeniedException.denySetSystemSessionProperty(propertyName);
        }

        LOG.debug("<== checkCanSetSystemSessionProperty(identity={}, queryId={}, propertyName={}): allowed", identity, queryId, propertyName);
    }

    /** CATALOG **/
    @Override
    public boolean canAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        LOG.debug("==> canAccessCatalog(context={}, catalogName={})", context, catalogName);

        boolean ret = hasPermission(createResource(catalogName), context, TrinoAccessType.USE, "AccessCatalog");

        LOG.debug("<== canAccessCatalog(context={}, catalogName={}): ret={}", context, catalogName, ret);

        return ret;
    }

    @Override
    public void checkCanCreateCatalog(SystemSecurityContext context, String catalogName)
    {
        LOG.debug("==> checkCanCreateCatalog(context={}, catalogName={})", context, catalogName);

        if (!hasPermission(createResource(catalogName), context, TrinoAccessType.CREATE, "CreateCatalog")) {
            LOG.debug("<== checkCanCreateCatalog(context={}, catalogName={}): denied", context, catalogName);

            AccessDeniedException.denyCreateCatalog(catalogName);
        }

        LOG.debug("<== checkCanCreateCatalog(context={}, catalogName={}): allowed", context, catalogName);
    }

    @Override
    public void checkCanDropCatalog(SystemSecurityContext context, String catalogName)
    {
        LOG.debug("==> checkCanDropCatalog(context={}, catalogName={})", context, catalogName);

        if (!hasPermission(createResource(catalogName), context, TrinoAccessType.DROP, "DropCatalog")) {
            LOG.debug("<== checkCanDropCatalog(context={}, catalogName={}): denied", context, catalogName);

            AccessDeniedException.denyCreateCatalog(catalogName);
        }

        LOG.debug("<== checkCanDropCatalog(context={}, catalogName={}): allowed", context, catalogName);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
        LOG.debug("==> checkCanSetCatalogSessionProperty(context={}, catalogName={}, propertyName={})", context, catalogName, propertyName);

        if (!hasPermission(createCatalogSessionResource(catalogName, propertyName), context, TrinoAccessType.ALTER, "SetCatalogSessionProperty")) {
            LOG.debug("<== checkCanSetCatalogSessionProperty(context={}, catalogName={}, propertyName={}): denied", context, catalogName, propertyName);

            AccessDeniedException.denySetCatalogSessionProperty(catalogName, propertyName);
        }

        LOG.debug("<== checkCanSetCatalogSessionProperty(context={}, catalogName={}, propertyName={}): allowed", context, catalogName, propertyName);
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        LOG.debug("==> filterCatalogs(context={}, catalogs={})", context, catalogs);

        Set<String> toExclude = null;

        for (String catalog : catalogs) {
            if (!hasPermissionForFilter(createResource(catalog), context, TrinoAccessType._ANY, "filterCatalogs")) {
                LOG.debug("filterCatalogs(user={}): skipping catalog {}", context.getIdentity(), catalog);

                if (toExclude == null) {
                    toExclude = new HashSet<>();
                }

                toExclude.add(catalog);
            }
        }

        Set<String> ret = toExclude == null ? catalogs : catalogs.stream().filter(((Predicate<? super String>) toExclude::contains).negate()).collect(Collectors.toSet());

        LOG.debug("<== filterCatalogs(context={}, catalogs={}): ret={}", context, catalogs, ret);

        return ret;
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema, Map<String, Object> properties)
    {
        LOG.debug("==> checkCanCreateSchema(context={}, schema={}, properties={})", context, schema, properties);

        if (!hasPermission(createResource(schema.getCatalogName(), schema.getSchemaName()), context, TrinoAccessType.CREATE, "CreateSchema")) {
            LOG.debug("<== checkCanCreateSchema(context={}, schema={}, properties={}): denied", context, schema, properties);

            AccessDeniedException.denyCreateSchema(schema.getSchemaName());
        }

        LOG.debug("<== checkCanCreateSchema(context={}, schema={}, properties={}): allowed", context, schema, properties);
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        LOG.debug("==> checkCanDropSchema(context={}, schema={})", context, schema);

        if (!hasPermission(createResource(schema.getCatalogName(), schema.getSchemaName()), context, TrinoAccessType.DROP, "DropSchema")) {
            LOG.debug("<== checkCanDropSchema(context={}, schema={}): denied", context, schema);

            AccessDeniedException.denyDropSchema(schema.getSchemaName());
        }

        LOG.debug("<== checkCanDropSchema(context={}, schema={}): allowed", context, schema);
    }

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        LOG.debug("==> checkCanRenameSchema(context={}, schema={}, newSchemaName={})", context, schema, newSchemaName);

        if (!hasPermission(createResource(schema.getCatalogName(), schema.getSchemaName()), context, TrinoAccessType.ALTER, "RenameSchema")) {
            LOG.debug("<== checkCanRenameSchema(context={}, schema={}, newSchemaName={}): denied", context, schema, newSchemaName);

            AccessDeniedException.denyRenameSchema(schema.getSchemaName(), newSchemaName);
        }

        LOG.debug("<== checkCanRenameSchema(context={}, schema={}, newSchemaName={}): allowed", context, schema, newSchemaName);
    }

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, TrinoPrincipal principal)
    {
        LOG.debug("==> checkCanSetSchemaAuthorization(context={}, schema={}, principal={})", context, schema, principal);

        if (!hasPermission(createResource(schema.getCatalogName(), schema.getSchemaName()), context, TrinoAccessType.GRANT, "SetSchemaAuthorization")) {
            LOG.debug("<== checkCanSetSchemaAuthorization(context={}, schema={}, principal={}): denied", context, schema, principal);

            AccessDeniedException.denySetSchemaAuthorization(schema.getSchemaName(), principal);
        }

        LOG.debug("<== checkCanSetSchemaAuthorization(context={}, schema={}, principal={}): allowed", context, schema, principal);
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
        LOG.debug("==> checkCanShowSchemas(context={}, catalogName={})", context, catalogName);

        if (!hasPermission(createResource(catalogName), context, TrinoAccessType.SHOW, "ShowSchemas")) {
            LOG.debug("<== checkCanShowSchemas(context={}, catalogName={}): denied", context, catalogName);

            AccessDeniedException.denyShowSchemas(catalogName);
        }

        LOG.debug("<== checkCanShowSchemas(context={}, catalogName={}): allowed", context, catalogName);
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        LOG.debug("==> filterSchemas(context={}, catalogName={}, schemaNames={})", context, catalogName, schemaNames);

        Set<String> toExclude = null;

        for (String schemaName : schemaNames) {
            if (!hasPermissionForFilter(createResource(catalogName, schemaName), context, TrinoAccessType._ANY, "filterSchemas")) {
                LOG.debug("filterSchemas(user={}): skipping schema {}", context.getIdentity(), schemaName);

                if (toExclude == null) {
                    toExclude = new HashSet<>();
                }

                toExclude.add(schemaName);
            }
        }

        Set<String> ret = toExclude == null ? schemaNames : schemaNames.stream().filter(((Predicate<? super String>) toExclude::contains).negate()).collect(Collectors.toSet());

        LOG.debug("<== filterSchemas(context={}, catalogName={}, schemaNames={}): ret={}", context, catalogName, schemaNames, ret);

        return ret;
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        LOG.debug("==> checkCanShowCreateSchema(context={}, schema={})", context, schema);

        if (!hasPermission(createResource(schema.getCatalogName(), schema.getSchemaName()), context, TrinoAccessType.SHOW, "ShowCreateSchema")) {
            LOG.debug("<== checkCanShowCreateSchema(context={}, schema={}): denied", context, schema);

            AccessDeniedException.denyShowCreateSchema(schema.getSchemaName());
        }

        LOG.debug("<== checkCanShowCreateSchema(context={}, schema={}): allowed", context, schema);
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Object> properties)
    {
        LOG.debug("==> checkCanCreateTable(context={}, table={}, properties={})", context, table, properties);

        if (!hasPermission(createResource(table), context, TrinoAccessType.CREATE, "CreateTable")) {
            LOG.debug("<== checkCanCreateTable(context={}, table={}, properties={}): denied", context, table, properties);

            AccessDeniedException.denyCreateTable(table.getSchemaTableName().getTableName());
        }

        LOG.debug("<== checkCanCreateTable(context={}, table={}, properties={}): allowed", context, table, properties);
    }

    /**
     * This is evaluated against the table name as ownership information is not available
     */
    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        LOG.debug("==> checkCanDropTable(context={}, table={})", context, table);

        if (!hasPermission(createResource(table), context, TrinoAccessType.DROP, "DropTable")) {
            LOG.debug("<== checkCanDropTable(context={}, table={}): denied", context, table);

            AccessDeniedException.denyDropTable(table.getSchemaTableName().getTableName());
        }

        LOG.debug("<== checkCanDropTable(context={}, table={}): allowed", context, table);
    }

    /**
     * This is evaluated against the table name as ownership information is not available
     */
    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        LOG.debug("==> checkCanRenameTable(context={}, table={}, newTable={})", context, table, newTable);

        if (!hasPermission(createResource(table), context, TrinoAccessType.ALTER, "RenameTable")) {
            LOG.debug("<== checkCanRenameTable(context={}, table={}, newTable={}): denied", context, table, newTable);

            AccessDeniedException.denyRenameTable(table.getSchemaTableName().getTableName(), newTable.getSchemaTableName().getTableName());
        }

        LOG.debug("<== checkCanRenameTable(context={}, table={}, newTable={}): allowed", context, table, newTable);
    }

    @Override
    public void checkCanSetTableProperties(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Optional<Object>> properties)
    {
        LOG.debug("==> checkCanSetTableProperties(context={}, table={}, properties={})", context, table, properties);

        if (!hasPermission(createResource(table), context, TrinoAccessType.ALTER, "SetTableProperties")) {
            LOG.debug("<== checkCanSetTableProperties(context={}, table={}, properties={}): denied", context, table, properties);

            AccessDeniedException.denySetTableProperties(table.toString());
        }

        LOG.debug("<== checkCanSetTableProperties(context={}, table={}, properties={}): allowed", context, table, properties);
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        LOG.debug("==> checkCanSetTableComment(context={}, table={})", context, table);

        if (!hasPermission(createResource(table), context, TrinoAccessType.ALTER, "SetTableComment")) {
            LOG.debug("<== checkCanSetTableComment(context={}, table={}): denied", context, table);

            AccessDeniedException.denyCommentTable(table.toString());
        }

        LOG.debug("<== checkCanSetTableComment(context={}, table={}): allowed", context, table);
    }

    @Override
    public void checkCanSetTableAuthorization(SystemSecurityContext context, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        LOG.debug("==> checkCanSetTableAuthorization(context={}, table={}, principal={})", context, table, principal);

        if (!hasPermission(createResource(table), context, TrinoAccessType.GRANT, "SetTableAuthorization")) {
            LOG.debug("<== checkCanSetTableAuthorization(context={}, table={}, principal={}): denied", context, table, principal);

            AccessDeniedException.denySetTableAuthorization(table.toString(), principal);
        }

        LOG.debug("<== checkCanSetTableAuthorization(context={}, table={}, principal={}): allowed", context, table, principal);
    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema)
    {
        LOG.debug("==> checkCanShowTables(context={}, schema={})", context, schema);

        if (!hasPermission(createResource(schema), context, TrinoAccessType.SHOW, "ShowTables")) {
            LOG.debug("<== checkCanShowTables(context={}, schema={}): denied", context, schema);

            AccessDeniedException.denyShowTables(schema.toString());
        }

        LOG.debug("<== checkCanShowTables(context={}, schema={}): allowed", context, schema);
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        LOG.debug("==> checkCanShowCreateTable(context={}, table={})", context, table);

        if (!hasPermission(createResource(table), context, TrinoAccessType.SHOW, "ShowCreateTable")) {
            LOG.debug("<== checkCanShowCreateTable(context={}, table={}): denied", context, table);

            AccessDeniedException.denyShowCreateTable(table.toString());
        }

        LOG.debug("<== checkCanShowCreateTable(context={}, table={}): allowed", context, table);
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        LOG.debug("==> checkCanInsertIntoTable(context={}, table={})", context, table);

        RangerTrinoResource res = createResource(table);

        if (!hasPermission(res, context, TrinoAccessType.INSERT, "InsertIntoTable")) {
            LOG.debug("<== checkCanInsertIntoTable(context={}, table={}): denied", context, table);

            AccessDeniedException.denyInsertTable(table.getSchemaTableName().getTableName());
        }

        LOG.debug("<== checkCanInsertIntoTable(context={}, table={}): allowed", context, table);
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        LOG.debug("==> checkCanDeleteFromTable(context={}, table={})", context, table);

        if (!hasPermission(createResource(table), context, TrinoAccessType.DELETE, "DeleteFromTable")) {
            LOG.debug("<== checkCanDeleteFromTable(context={}, table={}): denied", context, table);

            AccessDeniedException.denyDeleteTable(table.getSchemaTableName().getTableName());
        }

        LOG.debug("<== checkCanDeleteFromTable(context={}, table={}): allowed", context, table);
    }

    @Override
    public void checkCanTruncateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        LOG.debug("==> checkCanTruncateTable(context={}, table={})", context, table);

        if (!hasPermission(createResource(table), context, TrinoAccessType.DELETE, "TruncateTable")) {
            LOG.debug("<== checkCanTruncateTable(context={}, table={}): denied", context, table);

            AccessDeniedException.denyTruncateTable(table.getSchemaTableName().getTableName());
        }

        LOG.debug("<== checkCanTruncateTable(context={}, table={}): allowed", context, table);
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        LOG.debug("==> filterTables(context={}, catalogName={}, tableNames={})", context, catalogName, tableNames);

        Set<SchemaTableName> toExclude = null;

        for (SchemaTableName tableName : tableNames) {
            RangerTrinoResource res = createResource(catalogName, tableName.getSchemaName(), tableName.getTableName());

            if (!hasPermissionForFilter(res, context, TrinoAccessType._ANY, "filterTables")) {
                LOG.debug("filterTables(user={}): skipping table {}.{}.{}", context.getIdentity(), catalogName, tableName.getSchemaName(), tableName.getTableName());

                if (toExclude == null) {
                    toExclude = new HashSet<>();
                }

                toExclude.add(tableName);
            }
        }

        Set<SchemaTableName> ret = toExclude == null ? tableNames : tableNames.stream().filter(((Predicate<? super SchemaTableName>) toExclude::contains).negate()).collect(Collectors.toSet());

        LOG.debug("<== filterTables(context={}, catalogName={}, tableNames={}): ret={}", context, catalogName, tableNames, ret);

        return ret;
    }

    /**
     * This is evaluated on table level
     */
    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        LOG.debug("==> checkCanAddColumn(context={}, table={})", context, table);

        RangerTrinoResource res = createResource(table);

        if (!hasPermission(res, context, TrinoAccessType.ALTER, "AddColumn")) {
            LOG.debug("<== checkCanAddColumn(context={}, table={}): denied", context, table);

            AccessDeniedException.denyAddColumn(table.getSchemaTableName().getTableName());
        }

        LOG.debug("<== checkCanAddColumn(context={}, table={}): allowed", context, table);
    }

    @Override
    public void checkCanAlterColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        LOG.debug("==> checkCanAlterColumn(context={}, table={})", context, table);

        RangerTrinoResource res = createResource(table);

        if (!hasPermission(res, context, TrinoAccessType.ALTER, "AlterColumn")) {
            LOG.debug("<== checkCanAlterColumn(context={}, table={}): denied", context, table);

            AccessDeniedException.denyAddColumn(table.getSchemaTableName().getTableName());
        }

        LOG.debug("<== checkCanAlterColumn(context={}, table={}): allowed", context, table);
    }

    /**
     * This is evaluated on table level
     */
    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        LOG.debug("==> checkCanDropColumn(context={}, table={})", context, table);

        if (!hasPermission(createResource(table), context, TrinoAccessType.ALTER, "DropColumn")) {
            LOG.debug("<== checkCanDropColumn(context={}, table={}): denied", context, table);

            AccessDeniedException.denyDropColumn(table.getSchemaTableName().getTableName());
        }

        LOG.debug("<== checkCanDropColumn(context={}, table={}): allowed", context, table);
    }

    /**
     * This is evaluated on table level
     */
    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        LOG.debug("==> checkCanRenameColumn(context={}, table={})", context, table);

        RangerTrinoResource res = createResource(table);

        if (!hasPermission(res, context, TrinoAccessType.ALTER, "RenameColumn")) {
            LOG.debug("<== checkCanRenameColumn(context={}, table={}): denied", context, table);

            AccessDeniedException.denyRenameColumn(table.getSchemaTableName().getTableName());
        }

        LOG.debug("<== checkCanRenameColumn(context={}, table={}): allowed", context, table);
    }

    @Override
    public void checkCanSetColumnComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        LOG.debug("==> checkCanSetColumnComment(context={}, table={})", context, table);

        if (!hasPermission(createResource(table), context, TrinoAccessType.ALTER, "SetColumnComment")) {
            LOG.debug("<== checkCanSetColumnComment(context={}, table={}): denied", context, table);

            AccessDeniedException.denyCommentColumn(table.toString());
        }

        LOG.debug("<== checkCanSetColumnComment(context={}, table={}): allowed", context, table);
    }

    /**
     * This is evaluated on table level
     */
    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        LOG.debug("==> checkCanShowColumns(context={}, table={})", context, table);

        if (!hasPermission(createResource(table), context, TrinoAccessType.SHOW, "ShowColumns")) {
            LOG.debug("==> checkCanShowColumns(context={}, table={}): denied", context, table);

            AccessDeniedException.denyShowColumns(table.toString());
        }

        LOG.debug("==> checkCanShowColumns(context={}, table={}): allowed", context, table);
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        LOG.debug("==> checkCanSelectFromColumns(context={}, table={}, columns={})", context, table, columns);

        for (RangerTrinoResource res : createResource(table, columns)) {
            if (!hasPermission(res, context, TrinoAccessType.SELECT, "SelectFromColumns")) {
                LOG.debug("<== checkCanSelectFromColumns(context={}, table={}, columns={}): denied", context, table, columns);

                AccessDeniedException.denySelectColumns(table.getSchemaTableName().getTableName(), columns);
            }
        }

        LOG.debug("<== checkCanSelectFromColumns(context={}, table={}, columns={}): allowed", context, table, columns);
    }

    @Override
    public void checkCanUpdateTableColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> updatedColumnNames)
    {
        LOG.debug("==> checkCanUpdateTableColumns(context={}, table={}, updatedColumnNames={})", context, table, updatedColumnNames);

        if (!hasPermission(createResource(table), context, TrinoAccessType.INSERT, "UpdateTableColumns")) {
            LOG.debug("<== checkCanUpdateTableColumns(context={}, table={}, updatedColumnNames={}): denied", context, table, updatedColumnNames);

            AccessDeniedException.denyUpdateTableColumns(table.getSchemaTableName().getTableName(), updatedColumnNames);
        }

        LOG.debug("<== checkCanUpdateTableColumns(context={}, table={}, updatedColumnNames={}): allowed", context, table, updatedColumnNames);
    }

    @Deprecated
    @Override
    public Set<String> filterColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        LOG.debug("==> filterColumns(context={}, table={}, columns={})", context, table, columns);

        Set<String> toExclude = null;
        String catalogName = table.getCatalogName();
        String schemaName = table.getSchemaTableName().getSchemaName();
        String tableName = table.getSchemaTableName().getTableName();

        for (String column : columns) {
            RangerTrinoResource res = createResource(catalogName, schemaName, tableName, column);

            if (!hasPermissionForFilter(res, context, TrinoAccessType._ANY, "filterColumns")) {
                if (toExclude == null) {
                    toExclude = new HashSet<>();
                }

                toExclude.add(column);
            }
        }

        Set<String> ret = toExclude == null ? columns : columns.stream().filter(((Predicate<? super String>) toExclude::contains).negate()).collect(Collectors.toSet());

        LOG.debug("<== filterColumns(context={}, table={}, columns={}): ret={}", context, table, columns, ret);

        return ret;
    }

    /**
     * Create view is verified on schema level
     */
    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        LOG.debug("==> checkCanCreateView(context={}, view={})", context, view);

        if (!hasPermission(createResource(view), context, TrinoAccessType.CREATE, "CreateView")) {
            LOG.debug("<== checkCanCreateView(context={}, view={}): denied", context, view);

            AccessDeniedException.denyCreateView(view.getSchemaTableName().getTableName());
        }

        LOG.debug("<== checkCanCreateView(context={}, view={}): allowed", context, view);
    }

    /**
     * This is evaluated against the table name as ownership information is not available
     */
    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        LOG.debug("==> checkCanDropView(context={}, view={})", context, view);

        if (!hasPermission(createResource(view), context, TrinoAccessType.DROP, "DropView")) {
            LOG.debug("<== checkCanDropView(context={}, view={}): denied", context, view);

            AccessDeniedException.denyDropView(view.getSchemaTableName().getTableName());
        }

        LOG.debug("<== checkCanDropView(context={}, view={}): allowed", context, view);
    }

    /**
     * This is evaluated against the table name as ownership information is not available
     */
    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        LOG.debug("==> checkCanRenameView(context={}, view={}, newView={})", context, view, newView);

        if (!hasPermission(createResource(view), context, TrinoAccessType.ALTER, "RenameView")) {
            LOG.debug("<== checkCanRenameView(context={}, view={}, newView={}): denied", context, view, newView);

            AccessDeniedException.denyRenameView(view.toString(), newView.toString());
        }

        LOG.debug("<== checkCanRenameView(context={}, view={}, newView={}): allowed", context, view, newView);
    }

    @Override
    public void checkCanSetViewAuthorization(SystemSecurityContext context, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        LOG.debug("==> checkCanSetViewAuthorization(context={}, view={}, principal={})", context, view, principal);

        if (!hasPermission(createResource(view), context, TrinoAccessType.ALTER, "SetViewAuthorization")) {
            LOG.debug("<== checkCanSetViewAuthorization(context={}, view={}, principal={}): denied", context, view, principal);

            AccessDeniedException.denySetViewAuthorization(view.toString(), principal);
        }

        LOG.debug("<== checkCanSetViewAuthorization(context={}, view={}, principal={}): allowed", context, view, principal);
    }

    /**
     * This check equals the check for checkCanCreateView
     */
    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        LOG.debug("==> checkCanCreateViewWithSelectFromColumns(context={}, table={}, columns={})", context, table, columns);

        try {
            checkCanCreateView(context, table);
        }
        catch (AccessDeniedException ade) {
            LOG.debug("<== checkCanCreateViewWithSelectFromColumns(context={}, table={}, columns={}): denied", context, table, columns);

            AccessDeniedException.denyCreateViewWithSelect(table.getSchemaTableName().getTableName(), context.getIdentity());
        }

        LOG.debug("<== checkCanCreateViewWithSelectFromColumns(context={}, table={}, columns={}): allowed", context, table, columns);
    }

    @Override
    public void checkCanSetViewComment(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        LOG.debug("==> checkCanSetViewComment(context={}, view={})", context, view);

        if (!hasPermission(createResource(view), context, TrinoAccessType.ALTER, "SetViewComment")) {
            LOG.debug("<== checkCanSetViewComment(context={}, view={}): denied", context, view);

            AccessDeniedException.denyCommentView(view.toString());
        }

        LOG.debug("<== checkCanSetViewComment(context={}, view={}): allowed", context, view);
    }

    /**
     *
     * check if materialized view can be created
     */
    @Override
    public void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Object> properties)
    {
        LOG.debug("==> checkCanCreateMaterializedView(context={}, materializedView={}, properties={})", context, materializedView, properties);

        if (!hasPermission(createResource(materializedView), context, TrinoAccessType.CREATE, "CreateMaterializedView")) {
            LOG.debug("<== checkCanCreateMaterializedView(context={}, materializedView={}, properties={}): denied", context, materializedView, properties);

            AccessDeniedException.denyCreateMaterializedView(materializedView.getSchemaTableName().getTableName());
        }

        LOG.debug("<== checkCanCreateMaterializedView(context={}, materializedView={}, properties={}): allowed", context, materializedView, properties);
    }

    @Override
    public void checkCanRefreshMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        LOG.debug("==> checkCanRefreshMaterializedView(context={}, materializedView={})", context, materializedView);

        if (!hasPermission(createResource(materializedView), context, TrinoAccessType.ALTER, "RefreshMaterializedView")) {
            AccessDeniedException.denyRefreshMaterializedView(materializedView.toString());
        }

        LOG.debug("<== checkCanRefreshMaterializedView(context={}, materializedView={}): allowed", context, materializedView);
    }

    @Override
    public void checkCanSetMaterializedViewProperties(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Optional<Object>> properties)
    {
        LOG.debug("==> checkCanSetMaterializedViewProperties(context={}, materializedView={}, properties={})", context, materializedView, properties);

        if (!hasPermission(createResource(materializedView), context, TrinoAccessType.ALTER, "SetMaterializedViewProperties")) {
            LOG.debug("<== checkCanSetMaterializedViewProperties(context={}, materializedView={}, properties={}): denied", context, materializedView, properties);

            AccessDeniedException.denyRefreshMaterializedView(materializedView.toString());
        }

        LOG.debug("<== checkCanSetMaterializedViewProperties(context={}, materializedView={}, properties={}): allowed", context, materializedView, properties);
    }

    @Override
    public void checkCanDropMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        LOG.debug("==> checkCanDropMaterializedView(context={}, materializedView={})", context, materializedView);

        if (!hasPermission(createResource(materializedView), context, TrinoAccessType.DROP, "DropMaterializedView")) {
            LOG.debug("<== checkCanDropMaterializedView(context={}, materializedView={}): denied", context, materializedView);

            AccessDeniedException.denyCreateView(materializedView.getSchemaTableName().getTableName());
        }

        LOG.debug("<== checkCanDropMaterializedView(context={}, materializedView={}): allowed", context, materializedView);
    }

    @Override
    public void checkCanRenameMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, CatalogSchemaTableName newView)
    {
        LOG.debug("==> checkCanRenameMaterializedView(context={}, materializedView={}, newView={})", context, materializedView, newView);

        if (!hasPermission(createResource(materializedView), context, TrinoAccessType.DROP, "RenameMaterializedView")) {
            LOG.debug("<== checkCanRenameMaterializedView(context={}, materializedView={}, newView={}): denied", context, materializedView, newView);

            AccessDeniedException.denyRenameMaterializedView(materializedView.toString(), newView.toString());
        }

        LOG.debug("<== checkCanRenameMaterializedView(context={}, materializedView={}, newView={}): allowed", context, materializedView, newView);
    }

    @Override
    public void checkCanGrantSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee, boolean grantOption)
    {
        LOG.debug("==> checkCanGrantSchemaPrivilege(context={}, privilege={}, schema={}, grantee={}, grantOption={})", context, privilege, schema, grantee, grantOption);

        if (!hasPermission(createResource(schema), context, TrinoAccessType.GRANT, "GrantSchemaPrivilege")) {
            LOG.debug("<== checkCanGrantSchemaPrivilege(context={}, privilege={}, schema={}, grantee={}, grantOption={}): denied", context, privilege, schema, grantee, grantOption);

            AccessDeniedException.denyGrantSchemaPrivilege(privilege.toString(), schema.toString());
        }

        LOG.debug("<== checkCanGrantSchemaPrivilege(context={}, privilege={}, schema={}, grantee={}, grantOption={}): allowed", context, privilege, schema, grantee, grantOption);
    }

    @Override
    public void checkCanDenySchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee)
    {
        LOG.debug("==> checkCanDenySchemaPrivilege(context={}, privilege={}, schema={}, grantee={})", context, privilege, schema, grantee);

        if (!hasPermission(createResource(schema), context, TrinoAccessType.REVOKE, "DenySchemaPrivilege")) {
            LOG.debug("<== checkCanDenySchemaPrivilege(context={}, privilege={}, schema={}, grantee={}): denied", context, privilege, schema, grantee);

            AccessDeniedException.denyDenySchemaPrivilege(privilege.toString(), schema.toString());
        }

        LOG.debug("<== checkCanDenySchemaPrivilege(context={}, privilege={}, schema={}, grantee={}): allowed", context, privilege, schema, grantee);
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal revokee, boolean grantOption)
    {
        LOG.debug("==> checkCanDenySchemaPrivilege(context={}, privilege={}, schema={}, revokee={}, grantOption={})", context, privilege, schema, revokee, grantOption);

        if (!hasPermission(createResource(schema), context, TrinoAccessType.REVOKE, "RevokeSchemaPrivilege")) {
            LOG.debug("<== checkCanRevokeSchemaPrivilege(context={}, privilege={}, schema={}, revokee={}, grantOption={}): denied", context, privilege, schema, revokee, grantOption);

            AccessDeniedException.denyRevokeSchemaPrivilege(privilege.toString(), schema.toString());
        }

        LOG.debug("<== checkCanRevokeSchemaPrivilege(context={}, privilege={}, schema={}, revokee={}, grantOption={}): allowed", context, privilege, schema, revokee, grantOption);
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee, boolean withGrantOption)
    {
        LOG.debug("==> checkCanGrantTablePrivilege(context={}, privilege={}, table={}, grantee={}, withGrantOption={})", context, privilege, table, grantee, withGrantOption);

        if (!hasPermission(createResource(table), context, TrinoAccessType.GRANT, "GrantTablePrivilege")) {
            LOG.debug("<== checkCanGrantTablePrivilege(context={}, privilege={}, table={}, grantee={}, withGrantOption={}): denied", context, privilege, table, grantee, withGrantOption);

            AccessDeniedException.denyGrantTablePrivilege(privilege.toString(), table.toString());
        }

        LOG.debug("<== checkCanGrantTablePrivilege(context={}, privilege={}, table={}, grantee={}, withGrantOption={}): allowed", context, privilege, table, grantee, withGrantOption);
    }

    @Override
    public void checkCanDenyTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee)
    {
        LOG.debug("==> checkCanDenyTablePrivilege(context={}, privilege={}, table={}, grantee={})", context, privilege, table, grantee);

        if (!hasPermission(createResource(table), context, TrinoAccessType.REVOKE, "DenyTablePrivilege")) {
            LOG.debug("<== checkCanDenyTablePrivilege(context={}, privilege={}, table={}, grantee={}): denied", context, privilege, table, grantee);

            AccessDeniedException.denyDenyTablePrivilege(privilege.toString(), table.toString());
        }

        LOG.debug("<== checkCanDenyTablePrivilege(context={}, privilege={}, table={}, grantee={}): allowed", context, privilege, table, grantee);
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal revokee, boolean grantOptionFor)
    {
        LOG.debug("==> checkCanRevokeTablePrivilege(context={}, privilege={}, table={}, revokee={}, grantOptionFor={})", context, privilege, table, revokee, grantOptionFor);

        if (!hasPermission(createResource(table), context, TrinoAccessType.REVOKE, "RevokeTablePrivilege")) {
            LOG.debug("<== checkCanRevokeTablePrivilege(context={}, privilege={}, table={}, revokee={}, grantOptionFor={}): denied", context, privilege, table, revokee, grantOptionFor);

            AccessDeniedException.denyRevokeTablePrivilege(privilege.toString(), table.toString());
        }

        LOG.debug("<== checkCanRevokeTablePrivilege(context={}, privilege={}, table={}, revokee={}, grantOptionFor={}): allowed", context, privilege, table, revokee, grantOptionFor);
    }

    @Override
    public void checkCanGrantEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal grantee, boolean grantOption)
    {
        LOG.debug("==> checkCanGrantEntityPrivilege(context={}, privilege={}, entity={}, grantee={}, grantOption={})", context, privilege, entity, grantee, grantOption);

        if (!hasPermission(createResource(entity), context, TrinoAccessType.GRANT, "GrantEntityPrivilege")) {
            LOG.debug("<== checkCanGrantEntityPrivilege(context={}, privilege={}, entity={}, grantee={}, grantOption={}): denied", context, privilege, entity, grantee, grantOption);

            AccessDeniedException.denyGrantEntityPrivilege(privilege.toString(), entity);
        }

        LOG.debug("<== checkCanGrantEntityPrivilege(context={}, privilege={}, entity={}, grantee={}, grantOption={}): allowed", context, privilege, entity, grantee, grantOption);
    }

    @Override
    public void checkCanDenyEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal grantee)
    {
        LOG.debug("==> checkCanDenyEntityPrivilege(context={}, privilege={}, entity={}, grantee={})", context, privilege, entity, grantee);

        if (!hasPermission(createResource(entity), context, TrinoAccessType.REVOKE, "DenyEntityPrivilege")) {
            LOG.debug("<== checkCanGrantEntityPrivilege(context={}, privilege={}, entity={}, grantee={}): denied", context, privilege, entity, grantee);

            AccessDeniedException.denyDenyEntityPrivilege(privilege.toString(), entity);
        }

        LOG.debug("<== checkCanGrantEntityPrivilege(context={}, privilege={}, entity={}, grantee={}): allowed", context, privilege, entity, grantee);
    }

    @Override
    public void checkCanRevokeEntityPrivilege(SystemSecurityContext context, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal revokee, boolean grantOption)
    {
        LOG.debug("==> checkCanRevokeEntityPrivilege(context={}, privilege={}, entity={}, grantee={}, grantOption={})", context, privilege, entity, revokee, grantOption);

        if (!hasPermission(createResource(entity), context, TrinoAccessType.REVOKE, "RevokeEntityPrivilege")) {
            LOG.debug("<== checkCanRevokeEntityPrivilege(context={}, privilege={}, entity={}, grantee={}, grantOption={}): denied", context, privilege, entity, revokee, grantOption);

            AccessDeniedException.denyRevokeEntityPrivilege(privilege.toString(), entity);
        }

        LOG.debug("<== checkCanRevokeEntityPrivilege(context={}, privilege={}, entity={}, grantee={}, grantOption={}): allowed", context, privilege, entity, revokee, grantOption);
    }

    @Override
    public void checkCanCreateRole(SystemSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
        LOG.debug("==> checkCanCreateRole(context={}, role={}, grantor={})", context, role, grantor);

        if (!hasPermission(createRoleResource(role), context, TrinoAccessType.CREATE, "CreateRole")) {
            LOG.debug("<== checkCanCreateRole(context={}, role={}, grantor={}): denied", context, role, grantor);

            AccessDeniedException.denyCreateRole(role);
        }

        LOG.debug("<== checkCanCreateRole(context={}): allowed", context);
    }

    @Override
    public void checkCanDropRole(SystemSecurityContext context, String role)
    {
        LOG.debug("==> checkCanDropRole(context={}, role={})", context, role);

        if (!hasPermission(createRoleResource(role), context, TrinoAccessType.DROP, "DropRole")) {
            LOG.debug("<== checkCanDropRole(context={}, role={}): denied", context, role);

            AccessDeniedException.denyDropRole(role);
        }

        LOG.debug("<== checkCanDropRole(context={}, role={}): allowed", context, role);
    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context)
    {
        LOG.debug("==> checkCanShowRoles(context={})", context);

        if (!hasPermission(createRoleResource("*"), context, TrinoAccessType.SHOW, "ShowRoles")) {
            LOG.debug("<== checkCanShowRoles(context={}): denied", context);

            AccessDeniedException.denyShowRoles();
        }

        LOG.debug("<== checkCanShowRoles(context={}): allowed", context);
    }

    @Override
    public void checkCanGrantRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        LOG.debug("==> checkCanGrantRoles(context={}, roles={}, grantees={}, adminOption={}, grantor={})", context, roles, grantees, adminOption, grantor);

        if (!hasPermission(createRoleResources(roles), context, TrinoAccessType.GRANT, "GrantRoles")) {
            LOG.debug("<== checkCanGrantRoles(context={}, roles={}, grantees={}, adminOption={}, grantor={}): denied", context, roles, grantees, adminOption, grantor);

            AccessDeniedException.denyGrantRoles(roles, grantees);
        }

        LOG.debug("<== checkCanGrantRoles(context={}, roles={}, grantees={}, adminOption={}, grantor={}): allowed", context, roles, grantees, adminOption, grantor);
    }

    @Override
    public void checkCanRevokeRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        LOG.debug("==> checkCanRevokeRoles(context={}, roles={}, grantees={}, adminOption={}, grantor={})", context, roles, grantees, adminOption, grantor);

        if (!hasPermission(createRoleResources(roles), context, TrinoAccessType.REVOKE, "RevokeRoles")) {
            LOG.debug("<== checkCanRevokeRoles(context={}, roles={}, grantees={}, adminOption={}, grantor={}): denied", context, roles, grantees, adminOption, grantor);

            AccessDeniedException.denyRevokeRoles(roles, grantees);
        }

        LOG.debug("<== checkCanRevokeRoles(context={}, roles={}, grantees={}, adminOption={}, grantor={}): allowed", context, roles, grantees, adminOption, grantor);
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

    /** PROCEDURES **/
    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext context, CatalogSchemaRoutineName procedure)
    {
        LOG.debug("==> checkCanExecuteProcedure(context={}, procedure={})", context, procedure);

        if (!hasPermission(createProcedureResource(procedure), context, TrinoAccessType.EXECUTE, "ExecuteProcedure")) {
            LOG.debug("<== checkCanExecuteProcedure(context={}, procedure={}): denied", context, procedure);

            AccessDeniedException.denyExecuteProcedure(procedure.getSchemaRoutineName().getRoutineName());
        }

        LOG.debug("<== checkCanExecuteProcedure(context={}, procedure={}): allowed", context, procedure);
    }

    @Override
    public void checkCanExecuteTableProcedure(SystemSecurityContext context, CatalogSchemaTableName catalogSchemaTableName, String procedure)
    {
        LOG.debug("==> checkCanExecuteTableProcedure(context={}, catalogSchemaTableName={}, procedure={})", context, catalogSchemaTableName, procedure);

        if (!hasPermission(createResource(catalogSchemaTableName), context, TrinoAccessType.ALTER, "ExecuteTableProcedure")) {
            LOG.debug("<== checkCanExecuteTableProcedure(context={}, catalogSchemaTableName={}, procedure={}): denied", context, catalogSchemaTableName, procedure);

            AccessDeniedException.denyExecuteTableProcedure(catalogSchemaTableName.toString(), procedure);
        }

        LOG.debug("<== checkCanExecuteTableProcedure(context={}, catalogSchemaTableName={}, procedure={}): allowed", context, catalogSchemaTableName, procedure);
    }

    @Override
    public void checkCanCreateFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
    {
        LOG.debug("==> checkCanCreateFunction(context={}, functionName={})", context, functionName);

        if (!hasPermission(createResource(functionName), context, TrinoAccessType.CREATE, "CreateFunction")) {
            LOG.debug("<== checkCanCreateFunction(context={}, functionName={}): denied", context, functionName);

            AccessDeniedException.denyCreateFunction(functionName.toString());
        }

        LOG.debug("<== checkCanCreateFunction(context={}, functionName={}): allowed", context, functionName);
    }

    @Override
    public void checkCanDropFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
    {
        LOG.debug("==> checkCanDropFunction(context={}, functionName={})", context, functionName);

        if (!hasPermission(createResource(functionName), context, TrinoAccessType.DROP, "DropFunction")) {
            LOG.debug("<== checkCanDropFunction(context={}, functionName={}): denied", context, functionName);

            AccessDeniedException.denyDropFunction(functionName.toString());
        }

        LOG.debug("<== checkCanDropFunction(context={}, functionName={}): allowed", context, functionName);
    }

    @Override
    public void checkCanShowCreateFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
    {
        LOG.debug("==> checkCanShowCreateFunction(context={}, functionName={})", context, functionName);

        if (!hasPermission(createResource(functionName), context, TrinoAccessType.SHOW, "ShowCreateFunction")) {
            LOG.debug("<== checkCanShowCreateFunction(context={}, functionName={}): denied", context, functionName);

            AccessDeniedException.denyShowCreateFunction(functionName.toString());
        }

        LOG.debug("<== checkCanShowCreateFunction(context={}, functionName={}): allowed", context, functionName);
    }

    @Override
    public void checkCanShowFunctions(SystemSecurityContext context, CatalogSchemaName schema)
    {
        LOG.debug("==> checkCanShowFunctions(context={}, schema={})", context, schema);

        if (!hasPermission(createResource(schema), context, TrinoAccessType.SHOW, "ShowFunctions")) {
            LOG.debug("<== checkCanShowFunctions(context={}, schema={}): denied", context, schema);

            AccessDeniedException.denyShowFunctions(schema.toString());
        }

        LOG.debug("<== checkCanShowFunctions(context={}, schema={}): allowed", context, schema);
    }

    @Override
    public boolean canExecuteFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
    {
        LOG.debug("==> canExecuteFunction(context={}, procedure={})", context, functionName);

        boolean ret = hasPermission(createResource(functionName), context, TrinoAccessType.EXECUTE, "ExecuteFunction");

        LOG.debug("<== canExecuteFunction(context={}, procedure={}): ret={}", context, functionName, ret);

        return ret;
    }

    @Override
    public boolean canCreateViewWithExecuteFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
    {
        LOG.debug("==> canCreateViewWithExecuteFunction(context={}, procedure={})", context, functionName);

        boolean ret = hasPermission(createResource(functionName), context, TrinoAccessType.CREATE, "CreateViewWithExecuteFunction");

        LOG.debug("<== canCreateViewWithExecuteFunction(context={}, procedure={}): ret={}", context, functionName, ret);

        return ret;
    }

    @Override
    public Set<SchemaFunctionName> filterFunctions(SystemSecurityContext context, String catalogName, Set<SchemaFunctionName> functionNames)
    {
        LOG.debug("==> filterFunctions(context={}, catalogName={}, functions={})", context, catalogName, functionNames);

        Set<SchemaFunctionName> toExclude = null;

        for (SchemaFunctionName functionName : functionNames) {
            RangerTrinoResource res = createResource(catalogName, functionName);

            if (!hasPermissionForFilter(res, context, TrinoAccessType._ANY, "filterFunctions")) {
                LOG.debug("filterFunctions(user={}): skipping function {}.{}.{}", context.getIdentity(), catalogName, functionName.getSchemaName(), functionName.getFunctionName());

                if (toExclude == null) {
                    toExclude = new HashSet<>();
                }

                toExclude.add(functionName);
            }
        }

        Set<SchemaFunctionName> ret = toExclude == null ? functionNames : functionNames.stream().filter(((Predicate<? super SchemaFunctionName>) toExclude::contains).negate()).collect(Collectors.toSet());

        LOG.debug("<== filterFunctions(context={}, catalogName={}, functions={}): ret={}", context, catalogName, functionNames, ret);

        return ret;
    }

    @Override
    public List<ViewExpression> getRowFilters(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        LOG.debug("==> getRowFilters(context={}, tableName={})", context, tableName);

        RangerAccessResult result = getRowFilterResult(createAccessRequest(createResource(tableName), context, TrinoAccessType.SELECT, "getRowFilters"));
        ViewExpression viewExpression = null;

        if (isRowFilterEnabled(result)) {
            String filter = result.getFilterExpr();

            viewExpression = ViewExpression.builder().identity(context.getIdentity().getUser())
                    .catalog(tableName.getCatalogName())
                    .schema(tableName.getSchemaTableName().getSchemaName())
                    .expression(filter).build();
        }

        List<ViewExpression> ret = Optional.ofNullable(viewExpression).map(ImmutableList::of).orElseGet(ImmutableList::of);

        LOG.debug("<== getRowFilters(context={}, tableName={}): ret={}", context, tableName, ret);

        return ret;
    }

    @Override
    public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, CatalogSchemaTableName tableName, String columnName, Type type)
    {
        LOG.debug("==> getColumnMask(context={}, tableName={}, columnName={}, type={})", context, tableName, columnName, type);

        RangerAccessResult result = getDataMaskResult(createAccessRequest(createResource(tableName.getCatalogName(), tableName.getSchemaTableName().getSchemaName(), tableName.getSchemaTableName().getTableName(), columnName), context, TrinoAccessType.SELECT, "getColumnMask"));
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

                transformer = requireNonNullElse(maskedValue, "NULL");
            }

            if (StringUtils.isNotEmpty(transformer)) {
                transformer = transformer.replace("{col}", columnName).replace("{type}", type.getDisplayName());
            }

            viewExpression = ViewExpression.builder().identity(context.getIdentity().getUser())
                    .catalog(tableName.getCatalogName())
                    .schema(tableName.getSchemaTableName().getSchemaName())
                    .expression(transformer).build();
        }

        Optional<ViewExpression> ret = Optional.ofNullable(viewExpression);

        LOG.debug("<== getColumnMask(context={}, tableName={}, columnName={}, type={}): ret={}", context, tableName, columnName, type, ret);

        return ret;
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

    /** HELPER FUNCTIONS **/

    private RangerAccessResult getDataMaskResult(RangerTrinoAccessRequest request)
    {
        LOG.debug("==> getDataMaskResult(request={})", request);

        RangerAccessResult ret = rangerPlugin.evalDataMaskPolicies(request, null);

        LOG.debug("<== getDataMaskResult(request={}): ret={}", request, ret);

        return ret;
    }

    private RangerAccessResult getRowFilterResult(RangerTrinoAccessRequest request)
    {
        LOG.debug("==> getRowFilterResult(request={})", request);

        RangerAccessResult ret = rangerPlugin.evalRowFilterPolicies(request, null);

        LOG.debug("<== getRowFilterResult(request={}): ret={}", request, ret);

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

    private RangerTrinoAccessRequest createAccessRequest(RangerTrinoResource resource, SystemSecurityContext context, TrinoAccessType accessType, String action)
    {
        Set<String> userGroups = null;

        if (useUgi) {
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(context.getIdentity().getUser());
            String[] groups = ugi != null ? ugi.getGroupNames() : null;

            if (groups != null && groups.length > 0) {
                userGroups = new HashSet<>(Arrays.asList(groups));
            }
        }
        else {
            userGroups = context.getIdentity().getGroups();
        }

        return new RangerTrinoAccessRequest(resource, context.getIdentity().getUser(), userGroups, getQueryTime(context), getClientAddress(context), getClientType(context), getQueryText(context), accessType, action);
    }

    private RangerTrinoAccessRequest createAccessRequest(RangerTrinoResource resource, Identity identity, QueryId queryId, TrinoAccessType accessType, String action)
    {
        Set<String> userGroups = null;

        if (useUgi) {
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(identity.getUser());
            String[] groups = ugi != null ? ugi.getGroupNames() : null;

            if (groups != null && groups.length > 0) {
                userGroups = new HashSet<>(Arrays.asList(groups));
            }
        }
        else {
            userGroups = identity.getGroups();
        }

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

    private boolean hasPermission(RangerTrinoResource resource, SystemSecurityContext context, TrinoAccessType accessType, String action)
    {
        RangerAccessResult result = rangerPlugin.isAccessAllowed(createAccessRequest(resource, context, accessType, action));

        return result != null && result.getIsAllowed();
    }

    private boolean hasPermissionForFilter(RangerTrinoResource resource, SystemSecurityContext context, TrinoAccessType accessType, String action)
    {
        RangerTrinoAccessRequest request = createAccessRequest(resource, context, accessType, action);

        request.setResourceMatchingScope(RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS);

        RangerAccessResult result = rangerPlugin.isAccessAllowed(request, null);

        return result != null && result.getIsAllowed();
    }

    private boolean hasPermission(Collection<RangerTrinoResource> resources, SystemSecurityContext context, TrinoAccessType accessType, String action)
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

    private boolean hasPermission(RangerTrinoResource resource, Identity identity, QueryId queryId, TrinoAccessType accessType, String action)
    {
        RangerAccessResult result = rangerPlugin.isAccessAllowed(createAccessRequest(resource, identity, queryId, accessType, action));

        return result != null && result.getIsAllowed();
    }

    private boolean hasPermissionForFilter(RangerTrinoResource resource, Identity identity, QueryId queryId, TrinoAccessType accessType, String action)
    {
        RangerTrinoAccessRequest request = createAccessRequest(resource, identity, queryId, accessType, action);

        request.setResourceMatchingScope(RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS);

        RangerAccessResult result = rangerPlugin.isAccessAllowed(request, null);

        return result != null && result.getIsAllowed();
    }

    private boolean hasPermission(RangerTrinoResource resource, Optional<Principal> principal, QueryId queryId, TrinoAccessType accessType, String action)
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

        res.setValue(RangerTrinoResource.KEY_QUERY_ID, queryId != null ? queryId.getId() : "*");

        return res;
    }

    private void setDefaultConfig(RangerConfig config)
    {
        if (StringUtils.isBlank(config.getServiceName())) {
            config.setServiceName(RANGER_TRINO_DEFAULT_SERVICE_NAME);
        }

        if (StringUtils.isBlank(config.getSecurityConfigPath())) {
            config.setSecurityConfigPath(RANGER_TRINO_DEFAULT_SECURITY_CONF);
        }

        if (StringUtils.isBlank(config.getAuditConfigPath())) {
            config.setAuditConfigPath(RANGER_TRINO_DEFAULT_AUDIT_CONF);
        }

        if (StringUtils.isBlank(config.getPolicyMgrSslConfigPath())) {
            config.setPolicyMgrSslConfigPath(RANGER_TRINO_DEFAULT_POLICYMGR_SSL_CONF);
        }
    }

    private static Identity toIdentity(Optional<Principal> principal)
    {
        return principal.isPresent() ? Identity.ofUser(principal.get().getName()) : Identity.ofUser("");
    }

    /*
     * Classes in following libraries are used by dependent libraries, hence need to be
     * packaged even though these classes are not directly referenced in the plugin sources.
     * Hence the references below to avoid following build errors:
    [ERROR] Unused declared dependencies found:
    [ERROR]    com.sun.jersey:jersey-client:jar:1.19.3:compile
    [ERROR]    com.sun.jersey:jersey-bundle:jar:1.19.3:compile
    [ERROR]    commons-collections:commons-collections:jar:3.2.2:compile
    [ERROR]    org.apache.ranger:ranger-plugins-audit:jar:2.4.0:compile
     */
    private static void referDependentClasses()
    {
        Class<?>[] ignored = {
                org.apache.commons.collections.CollectionUtils.class,
                org.apache.http.Header.class,
                org.apache.http.client.HttpClient.class,
                org.apache.ranger.audit.provider.AuditProviderFactory.class,
                org.apache.zookeeper.common.ZKConfig.class,
                org.opensearch.client.Response.class,
                com.amazonaws.services.logs.AWSLogs.class,
                com.google.gson.GsonBuilder.class,
                com.oracle.js.parser.Scanner.class,
                com.oracle.svm.core.annotate.Alias.class,
                com.oracle.truffle.js.scriptengine.GraalJSScriptEngine.class,
                com.oracle.truffle.tools.chromeinspector.util.LineSearch.class,
                com.oracle.truffle.tools.profiler.CPUTracer.class,
                com.sun.jersey.client.proxy.ViewProxyProvider.class,
                com.sun.jersey.core.impl.provider.entity.ByteArrayProvider.class
        };
    }

    private static class RangerTrinoEventListener
            implements EventListener
    {
        private final Map<String, QueryCreatedEvent> activeQueries = new HashMap<>();

        public String getClientAddress(String queryId)
        {
            QueryCreatedEvent qce = activeQueries.get(queryId);
            QueryContext qc = qce != null ? qce.getContext() : null;

            return qc != null && qc.getRemoteClientAddress().isPresent() ? qc.getRemoteClientAddress().get() : null;
        }

        public String getQueryText(String queryId)
        {
            QueryCreatedEvent qce = activeQueries.get(queryId);
            QueryMetadata qm = qce != null ? qce.getMetadata() : null;

            return qm != null ? qm.getQuery() : null;
        }

        public Instant getQueryTime(String queryId)
        {
            QueryCreatedEvent qce = activeQueries.get(queryId);

            return qce != null ? qce.getCreateTime() : null;
        }

        public String getClientType(String queryId)
        {
            QueryCreatedEvent qce = activeQueries.get(queryId);
            QueryContext qc = qce != null ? qce.getContext() : null;

            return qc != null && qc.getUserAgent().isPresent() ? qc.getUserAgent().get() : null;
        }

        @Override
        public void queryCreated(QueryCreatedEvent queryCreatedEvent)
        {
            QueryMetadata qm = queryCreatedEvent.getMetadata();

            if (qm != null && StringUtils.isNotBlank(qm.getQueryId())) {
                activeQueries.put(qm.getQueryId(), queryCreatedEvent);
            }
        }

        @Override
        public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
        {
            QueryMetadata qm = queryCompletedEvent.getMetadata();

            if (qm != null && StringUtils.isNotBlank(qm.getQueryId())) {
                activeQueries.remove(qm.getQueryId());
            }
        }
    }

    private static class RangerTrinoResource
            extends RangerAccessResourceImpl
    {
        public static final String KEY_CATALOG = "catalog";
        public static final String KEY_SCHEMA = "schema";
        public static final String KEY_TABLE = "table";
        public static final String KEY_COLUMN = "column";
        public static final String KEY_USER = "trinouser";
        public static final String KEY_PROCEDURE = "procedure";
        public static final String KEY_SYSTEM_PROPERTY = "systemproperty";
        public static final String KEY_SESSION_PROPERTY = "sessionproperty";
        public static final String KEY_SCHEMA_FUNCTION = "schemafunction";
        public static final String KEY_ROLE = "role";
        public static final String KEY_QUERY_ID = "queryid";
        public static final String KEY_SYSINFO = "sysinfo";

        public RangerTrinoResource()
        {
        }

        public RangerTrinoResource(String catalogName, String schema, String table)
        {
            setValue(KEY_CATALOG, catalogName);
            setValue(KEY_SCHEMA, schema);
            setValue(KEY_TABLE, table);
        }

        public RangerTrinoResource(String catalogName, String schema, String table, String column)
        {
            setValue(KEY_CATALOG, catalogName);
            setValue(KEY_SCHEMA, schema);
            setValue(KEY_TABLE, table);
            setValue(KEY_COLUMN, column);
        }
    }

    private static class RangerTrinoAccessRequest
            extends RangerAccessRequestImpl
    {
        public RangerTrinoAccessRequest(RangerTrinoResource resource, String user, Set<String> userGroups, Instant queryTime, String clientAddress, String clientType, String queryText, TrinoAccessType trinoAccessType, String action)
        {
            super(resource, trinoAccessType.name().toLowerCase(ENGLISH), user, userGroups, null);

            setAction(action);
            setAccessTime(queryTime != null ? new Date(queryTime.getEpochSecond() * 1000) : new Date());
            setClientIPAddress(clientAddress);
            setClientType(clientType);
            setRequestData(queryText);
        }
    }

    private enum TrinoAccessType {
        CREATE, DROP, SELECT, INSERT, DELETE, USE, ALTER, ALL, GRANT, REVOKE, SHOW, IMPERSONATE, EXECUTE, READ_SYSINFO, WRITE_SYSINFO, _ANY
    }
}
