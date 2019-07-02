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
package sg.bigo.plugin.ranger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.plugin.base.security.ForwardingSystemAccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemAccessControlFactory;

import java.nio.file.Paths;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static io.prestosql.plugin.base.JsonUtils.parseJson;
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
import static io.prestosql.spi.security.AccessDeniedException.denyInsertTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameTable;
import static io.prestosql.spi.security.AccessDeniedException.denySelectColumns;
import static io.prestosql.spi.security.AccessDeniedException.denySetUser;
import static io.prestosql.spi.security.AccessDeniedException.denyShowColumnsMetadata;
import static io.prestosql.spi.security.AccessDeniedException.denyShowSchemas;
import static io.prestosql.spi.security.AccessDeniedException.denyShowTablesMetadata;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static sg.bigo.plugin.ranger.RangerAccessControlConfig.SECURITY_CONFIG_FILE;
import static sg.bigo.plugin.ranger.RangerAccessControlConfig.SECURITY_REFRESH_PERIOD;

public class RangerSystemAccessControl
        implements SystemAccessControl
{
    private static final Logger log = Logger.get(RangerSystemAccessControl.class);

    public static final String NAME = "ranger";

    private final List<CatalogAccessControlRule> catalogRules;
    private final Optional<List<PrincipalUserMatchRule>> principalUserMatchRules;

    private RangerSystemAccessControl(List<CatalogAccessControlRule> catalogRules, Optional<List<PrincipalUserMatchRule>> principalUserMatchRules)
    {
        this.catalogRules = catalogRules;
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
            RangerSystemAccessControlRules rules = parseJson(Paths.get(configFileName), RangerSystemAccessControlRules.class);

            ImmutableList.Builder<CatalogAccessControlRule> catalogRulesBuilder = ImmutableList.builder();
            catalogRulesBuilder.addAll(rules.getCatalogRules());

            // Hack to allow Presto Admin to access the "system" catalog for retrieving server status.
            // todo Change userRegex from ".*" to one particular user that Presto Admin will be restricted to run as
            catalogRulesBuilder.add(new CatalogAccessControlRule(
                    true,
                    false,
                    Optional.of(Pattern.compile(".*")),
                    Optional.of(Pattern.compile("system"))));

            return new RangerSystemAccessControl(catalogRulesBuilder.build(), rules.getPrincipalUserMatchRules());
        }
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        requireNonNull(principal, "principal is null");
        requireNonNull(userName, "userName is null");

        if (!principalUserMatchRules.isPresent()) {
            return;
        }

        if (!principal.isPresent()) {
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
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        ImmutableSet.Builder<String> filteredCatalogs = ImmutableSet.builder();
        for (String catalog : catalogs) {
            if (canAccessCatalog(identity, catalog)) {
                filteredCatalogs.add(catalog);
            }
        }
        return filteredCatalogs.build();
    }

    private boolean canModifyCatalog(Identity identity, String catalogName)
    {
        for (CatalogAccessControlRule rule : catalogRules) {
            CatalogAccessControlRule.MatchResult matchResult = rule.match(identity.getUser(), catalogName);
            if (matchResult.isAllow().isPresent()) {
                return matchResult.isAllow().get();
            }
        }
        return false;
    }

    private boolean canAccessCatalog(Identity identity, String catalogName)
    {
        for (CatalogAccessControlRule rule : catalogRules) {
            CatalogAccessControlRule.MatchResult matchResult = rule.match(identity.getUser(), catalogName);
            if (matchResult.isAllow().isPresent()) {
                return matchResult.isAllow().get();
            }
            if (matchResult.isReadOnly().isPresent()) {
                return matchResult.isReadOnly().get();
            }
        }
        return false;
    }

    @Override
    public void checkCanCreateSchema(Identity identity, CatalogSchemaName schema)
    {
        if (!canModifyCatalog(identity, schema.getCatalogName())) {
            denyCreateSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanDropSchema(Identity identity, CatalogSchemaName schema)
    {
        if (!canModifyCatalog(identity, schema.getCatalogName())) {
            denyDropSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanRenameSchema(Identity identity, CatalogSchemaName schema, String newSchemaName)
    {
        if (!canModifyCatalog(identity, schema.getCatalogName())) {
            denyRenameSchema(schema.getSchemaName(), newSchemaName);
        }
    }

    @Override
    public void checkCanShowSchemas(Identity identity, String catalogName)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            denyShowSchemas();
        }
    }

    @Override
    public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            return ImmutableSet.of();
        }

        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!canModifyCatalog(identity, table.getCatalogName())) {
            denyCreateTable(table.toString());
        }
    }

    @Override
    public void checkCanDropTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!canModifyCatalog(identity, table.getCatalogName())) {
            denyDropTable(table.toString());
        }
    }

    @Override
    public void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        if (!canModifyCatalog(identity, table.getCatalogName())) {
            denyRenameTable(table.toString(), newTable.toString());
        }
    }

    @Override
    public void checkCanSetTableComment(Identity identity, CatalogSchemaTableName table)
    {
        if (!canModifyCatalog(identity, table.getCatalogName())) {
            denyCommentTable(table.toString());
        }
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, CatalogSchemaName schema)
    {
        if (!canAccessCatalog(identity, schema.getCatalogName())) {
            denyShowTablesMetadata(schema.toString());
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            return ImmutableSet.of();
        }

        return tableNames;
    }

    @Override
    public void checkCanShowColumnsMetadata(Identity identity, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName())) {
            denyShowColumnsMetadata(table.toString());
        }
    }

    @Override
    public List<ColumnMetadata> filterColumns(Identity identity, CatalogSchemaTableName tableName, List<ColumnMetadata> columns)
    {
        if (!canAccessCatalog(identity, tableName.getCatalogName())) {
            return ImmutableList.of();
        }

        return columns;
    }

    @Override
    public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
    {
        if (!canModifyCatalog(identity, table.getCatalogName())) {
            denyAddColumn(table.toString());
        }
    }

    @Override
    public void checkCanDropColumn(Identity identity, CatalogSchemaTableName table)
    {
        if (!canModifyCatalog(identity, table.getCatalogName())) {
            denyDropColumn(table.toString());
        }
    }

    @Override
    public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table)
    {
        if (!canModifyCatalog(identity, table.getCatalogName())) {
            denyRenameColumn(table.toString());
        }
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        if (!canAccessCatalog(identity, table.getCatalogName())) {
            denySelectColumns(table.toString(), columns);
        }
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!canModifyCatalog(identity, table.getCatalogName())) {
            denyInsertTable(table.toString());
        }
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!canModifyCatalog(identity, table.getCatalogName())) {
            denyDeleteTable(table.toString());
        }
    }

    @Override
    public void checkCanCreateView(Identity identity, CatalogSchemaTableName view)
    {
        if (!canModifyCatalog(identity, view.getCatalogName())) {
            denyCreateView(view.toString());
        }
    }

    @Override
    public void checkCanDropView(Identity identity, CatalogSchemaTableName view)
    {
        if (!canModifyCatalog(identity, view.getCatalogName())) {
            denyDropView(view.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        if (!canModifyCatalog(identity, table.getCatalogName())) {
            denyCreateViewWithSelect(table.toString(), identity);
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal grantee, boolean withGrantOption)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal revokee, boolean grantOptionFor)
    {
    }

    @Override
    public void checkCanShowRoles(Identity identity, String catalogName)
    {
    }
}
