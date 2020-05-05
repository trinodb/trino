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
package io.prestosql.plugin.base.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.plugin.base.security.CatalogAccessControlRule.AccessMode;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaRoutineName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemAccessControlFactory;
import io.prestosql.spi.security.SystemSecurityContext;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.Type;

import java.nio.file.Paths;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.plugin.base.security.CatalogAccessControlRule.AccessMode.ALL;
import static io.prestosql.plugin.base.security.CatalogAccessControlRule.AccessMode.READ_ONLY;
import static io.prestosql.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static io.prestosql.plugin.base.security.FileBasedAccessControlConfig.SECURITY_REFRESH_PERIOD;
import static io.prestosql.plugin.base.util.JsonUtils.parseJson;
import static io.prestosql.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.prestosql.spi.security.AccessDeniedException.denyAddColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyCatalogAccess;
import static io.prestosql.spi.security.AccessDeniedException.denyCommentTable;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateTable;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateView;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.prestosql.spi.security.AccessDeniedException.denyDeleteTable;
import static io.prestosql.spi.security.AccessDeniedException.denyDropColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyDropSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyDropTable;
import static io.prestosql.spi.security.AccessDeniedException.denyDropView;
import static io.prestosql.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denyImpersonateUser;
import static io.prestosql.spi.security.AccessDeniedException.denyInsertTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameView;
import static io.prestosql.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denySetSchemaAuthorization;
import static io.prestosql.spi.security.AccessDeniedException.denySetUser;
import static io.prestosql.spi.security.AccessDeniedException.denyShowCreateSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyShowCreateTable;
import static io.prestosql.spi.security.AccessDeniedException.denyViewQuery;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileBasedSystemAccessControl
        implements SystemAccessControl
{
    private static final Logger log = Logger.get(FileBasedSystemAccessControl.class);

    public static final String NAME = "file";

    private final List<CatalogAccessControlRule> catalogRules;
    private final Optional<List<QueryAccessRule>> queryAccessRules;
    private final Optional<List<ImpersonationRule>> impersonationRules;
    private final Optional<List<PrincipalUserMatchRule>> principalUserMatchRules;

    private FileBasedSystemAccessControl(
            List<CatalogAccessControlRule> catalogRules,
            Optional<List<QueryAccessRule>> queryAccessRules,
            Optional<List<ImpersonationRule>> impersonationRules,
            Optional<List<PrincipalUserMatchRule>> principalUserMatchRules)
    {
        this.catalogRules = catalogRules;
        this.queryAccessRules = queryAccessRules;
        this.impersonationRules = impersonationRules;
        this.principalUserMatchRules = principalUserMatchRules;
    }

    public static class Factory
            implements SystemAccessControlFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public SystemAccessControl create(Map<String, String> config)
        {
            requireNonNull(config, "config is null");

            String configFileName = config.get(SECURITY_CONFIG_FILE);
            checkState(configFileName != null, "Security configuration must contain the '%s' property", SECURITY_CONFIG_FILE);

            if (config.containsKey(SECURITY_REFRESH_PERIOD)) {
                Duration refreshPeriod;
                try {
                    refreshPeriod = Duration.valueOf(config.get(SECURITY_REFRESH_PERIOD));
                }
                catch (IllegalArgumentException e) {
                    throw invalidRefreshPeriodException(config, configFileName);
                }
                if (refreshPeriod.toMillis() == 0) {
                    throw invalidRefreshPeriodException(config, configFileName);
                }
                return ForwardingSystemAccessControl.of(memoizeWithExpiration(
                        () -> {
                            log.info("Refreshing system access control from %s", configFileName);
                            return create(configFileName);
                        },
                        refreshPeriod.toMillis(),
                        MILLISECONDS));
            }
            return create(configFileName);
        }

        private PrestoException invalidRefreshPeriodException(Map<String, String> config, String configFileName)
        {
            return new PrestoException(
                    CONFIGURATION_INVALID,
                    format("Invalid duration value '%s' for property '%s' in '%s'", config.get(SECURITY_REFRESH_PERIOD), SECURITY_REFRESH_PERIOD, configFileName));
        }

        private SystemAccessControl create(String configFileName)
        {
            FileBasedSystemAccessControlRules rules = parseJson(Paths.get(configFileName), FileBasedSystemAccessControlRules.class);

            ImmutableList.Builder<CatalogAccessControlRule> catalogRulesBuilder = ImmutableList.builder();
            catalogRulesBuilder.addAll(rules.getCatalogRules());

            // Hack to allow Presto Admin to access the "system" catalog for retrieving server status.
            // todo Change userRegex from ".*" to one particular user that Presto Admin will be restricted to run as
            catalogRulesBuilder.add(new CatalogAccessControlRule(
                    ALL,
                    Optional.of(Pattern.compile(".*")),
                    Optional.empty(),
                    Optional.of(Pattern.compile("system"))));

            return new FileBasedSystemAccessControl(
                    catalogRulesBuilder.build(),
                    rules.getQueryAccessRules(),
                    rules.getImpersonationRules(),
                    rules.getPrincipalUserMatchRules());
        }
    }

    @Override
    public void checkCanImpersonateUser(SystemSecurityContext context, String userName)
    {
        if (impersonationRules.isEmpty()) {
            // if there are principal user match rules, we assume that impersonation checks are
            // handled there; otherwise, impersonation must be manually configured
            if (principalUserMatchRules.isEmpty()) {
                denyImpersonateUser(context.getIdentity().getUser(), userName);
            }
            return;
        }

        for (ImpersonationRule rule : impersonationRules.get()) {
            Optional<Boolean> allowed = rule.match(context.getIdentity().getUser(), userName);
            if (allowed.isPresent()) {
                if (allowed.get()) {
                    return;
                }
                denyImpersonateUser(context.getIdentity().getUser(), userName);
            }
        }

        denyImpersonateUser(context.getIdentity().getUser(), userName);
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        requireNonNull(principal, "principal is null");
        requireNonNull(userName, "userName is null");

        if (principalUserMatchRules.isEmpty()) {
            return;
        }

        if (principal.isEmpty()) {
            denySetUser(principal, userName);
        }

        String principalName = principal.get().getName();

        for (PrincipalUserMatchRule rule : principalUserMatchRules.get()) {
            Optional<Boolean> allowed = rule.match(principalName, userName);
            if (allowed.isPresent()) {
                if (allowed.get()) {
                    return;
                }
                denySetUser(principal, userName);
            }
        }

        denySetUser(principal, userName);
    }

    @Override
    public void checkCanExecuteQuery(SystemSecurityContext context)
    {
        if (queryAccessRules.isEmpty()) {
            return;
        }
        if (!canAccessQuery(context.getIdentity(), QueryAccessRule.AccessMode.EXECUTE)) {
            denyViewQuery();
        }
    }

    @Override
    public void checkCanViewQueryOwnedBy(SystemSecurityContext context, String queryOwner)
    {
        if (queryAccessRules.isEmpty()) {
            return;
        }
        if (!canAccessQuery(context.getIdentity(), QueryAccessRule.AccessMode.VIEW)) {
            denyViewQuery();
        }
    }

    @Override
    public Set<String> filterViewQueryOwnedBy(SystemSecurityContext context, Set<String> queryOwners)
    {
        if (queryAccessRules.isEmpty()) {
            return queryOwners;
        }
        Identity identity = context.getIdentity();
        return queryOwners.stream()
                .filter(owner -> canAccessQuery(identity, QueryAccessRule.AccessMode.VIEW))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanKillQueryOwnedBy(SystemSecurityContext context, String queryOwner)
    {
        if (queryAccessRules.isEmpty()) {
            return;
        }
        if (!canAccessQuery(context.getIdentity(), QueryAccessRule.AccessMode.KILL)) {
            denyViewQuery();
        }
    }

    private boolean canAccessQuery(Identity identity, QueryAccessRule.AccessMode requiredAccess)
    {
        if (queryAccessRules.isPresent()) {
            for (QueryAccessRule rule : queryAccessRules.get()) {
                Optional<Set<QueryAccessRule.AccessMode>> accessMode = rule.match(identity.getUser());
                if (accessMode.isPresent()) {
                    return accessMode.get().contains(requiredAccess);
                }
            }
        }
        return false;
    }

    @Override
    public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName)
    {
    }

    @Override
    public void checkCanAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        if (!canAccessCatalog(context.getIdentity(), catalogName, READ_ONLY)) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        ImmutableSet.Builder<String> filteredCatalogs = ImmutableSet.builder();
        for (String catalog : catalogs) {
            if (canAccessCatalog(context.getIdentity(), catalog, READ_ONLY)) {
                filteredCatalogs.add(catalog);
            }
        }
        return filteredCatalogs.build();
    }

    private boolean canAccessCatalog(Identity identity, String catalogName, AccessMode requiredAccess)
    {
        for (CatalogAccessControlRule rule : catalogRules) {
            Optional<AccessMode> accessMode = rule.match(identity.getUser(), identity.getGroups(), catalogName);
            if (accessMode.isPresent()) {
                return accessMode.get().implies(requiredAccess);
            }
        }
        return false;
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!canAccessCatalog(context.getIdentity(), schema.getCatalogName(), ALL)) {
            denyCreateSchema(schema.toString());
        }
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!canAccessCatalog(context.getIdentity(), schema.getCatalogName(), ALL)) {
            denyDropSchema(schema.toString());
        }
    }

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        if (!canAccessCatalog(context.getIdentity(), schema.getCatalogName(), ALL)) {
            denyRenameSchema(schema.toString(), newSchemaName);
        }
    }

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, PrestoPrincipal principal)
    {
        if (!canAccessCatalog(context.getIdentity(), schema.getCatalogName(), ALL)) {
            denySetSchemaAuthorization(schema.toString(), principal);
        }
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        if (!canAccessCatalog(context.getIdentity(), catalogName, READ_ONLY)) {
            return ImmutableSet.of();
        }

        return schemaNames;
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyShowCreateTable(table.toString());
        }
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schemaName)
    {
        if (!canAccessCatalog(context.getIdentity(), schemaName.getCatalogName(), ALL)) {
            denyShowCreateSchema(schemaName.toString());
        }
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyCreateTable(table.toString());
        }
    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyDropTable(table.toString());
        }
    }

    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyRenameTable(table.toString(), newTable.toString());
        }
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyCommentTable(table.toString());
        }
    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        if (!canAccessCatalog(context.getIdentity(), catalogName, READ_ONLY)) {
            return ImmutableSet.of();
        }

        return tableNames;
    }

    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table)
    {
    }

    @Override
    public List<ColumnMetadata> filterColumns(SystemSecurityContext context, CatalogSchemaTableName tableName, List<ColumnMetadata> columns)
    {
        if (!canAccessCatalog(context.getIdentity(), tableName.getCatalogName(), READ_ONLY)) {
            return ImmutableList.of();
        }

        return columns;
    }

    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyAddColumn(table.toString());
        }
    }

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyDropColumn(table.toString());
        }
    }

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyRenameColumn(table.toString());
        }
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyInsertTable(table.toString());
        }
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyDeleteTable(table.toString());
        }
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!canAccessCatalog(context.getIdentity(), view.getCatalogName(), ALL)) {
            denyCreateView(view.toString());
        }
    }

    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        if (!canAccessCatalog(context.getIdentity(), view.getCatalogName(), ALL)) {
            denyRenameView(view.toString(), newView.toString());
        }
    }

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!canAccessCatalog(context.getIdentity(), view.getCatalogName(), ALL)) {
            denyDropView(view.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyCreateViewWithSelect(table.toString(), context.getIdentity());
        }
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SystemSecurityContext context, String functionName, PrestoPrincipal grantee, boolean grantOption)
    {
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal grantee, boolean grantOption)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyGrantTablePrivilege(privilege.toString(), table.toString());
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal revokee, boolean grantOption)
    {
        if (!canAccessCatalog(context.getIdentity(), table.getCatalogName(), ALL)) {
            denyRevokeTablePrivilege(privilege.toString(), table.toString());
        }
    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context, String catalogName)
    {
    }

    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName procedure)
    {
    }

    @Override
    public void checkCanExecuteFunction(SystemSecurityContext systemSecurityContext, String functionName)
    {
    }

    @Override
    public Iterable<EventListener> getEventListeners()
    {
        return ImmutableSet.of();
    }

    @Override
    public Optional<ViewExpression> getRowFilter(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        return Optional.empty();
    }

    @Override
    public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, CatalogSchemaTableName tableName, String columnName, Type type)
    {
        return Optional.empty();
    }
}
