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
package io.trino.plugin.base.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.security.TableProcedureAccessControlRule.TableProcedurePrivilege;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static io.trino.plugin.base.security.TableProcedureAccessControlRule.TableProcedurePrivilege.EXECUTE;
import static java.util.Objects.requireNonNull;

public class CatalogTableProcedureAccessControlRule
{
    public static final CatalogTableProcedureAccessControlRule ALLOW_BUILTIN = new CatalogTableProcedureAccessControlRule(
            ImmutableSet.of(EXECUTE),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(Pattern.compile("system")),
            Optional.empty());

    private final Optional<Pattern> catalogRegex;
    private final TableProcedureAccessControlRule tableProcedureAccessControlRule;

    @JsonCreator
    public CatalogTableProcedureAccessControlRule(
            @JsonProperty("privileges") Set<TableProcedurePrivilege> privileges,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("role") Optional<Pattern> roleRegex,
            @JsonProperty("group") Optional<Pattern> groupRegex,
            @JsonProperty("catalog") Optional<Pattern> catalogRegex,
            @JsonProperty("table_procedure") Optional<Pattern> tableProcedureRegex)
    {
        this(catalogRegex, new TableProcedureAccessControlRule(privileges, userRegex, roleRegex, groupRegex, tableProcedureRegex));
    }

    private CatalogTableProcedureAccessControlRule(Optional<Pattern> catalogRegex, TableProcedureAccessControlRule tableProcedureAccessControlRule)
    {
        this.catalogRegex = requireNonNull(catalogRegex, "catalogRegex is null");
        this.tableProcedureAccessControlRule = requireNonNull(tableProcedureAccessControlRule, "tableProcedureAccessControlRule is null");
    }

    public boolean matches(String user, Set<String> roles, Set<String> groups, String catalogName, String tableProcedure)
    {
        if (!catalogRegex.map(regex -> regex.matcher(catalogName).matches()).orElse(true)) {
            return false;
        }
        return tableProcedureAccessControlRule.matches(user, roles, groups, tableProcedure);
    }

    Optional<AnyCatalogPermissionsRule> toAnyCatalogPermissionsRule()
    {
        if (tableProcedureAccessControlRule.getPrivileges().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new AnyCatalogPermissionsRule(
                tableProcedureAccessControlRule.getUserRegex(),
                tableProcedureAccessControlRule.getRoleRegex(),
                tableProcedureAccessControlRule.getGroupRegex(),
                catalogRegex));
    }

    public boolean canExecuteTableProcedure()
    {
        return tableProcedureAccessControlRule.canExecuteTableProcedure();
    }
}
