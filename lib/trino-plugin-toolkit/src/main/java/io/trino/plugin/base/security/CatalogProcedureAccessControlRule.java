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
import io.trino.plugin.base.security.ProcedureAccessControlRule.ProcedurePrivilege;
import io.trino.spi.connector.CatalogSchemaRoutineName;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static io.trino.plugin.base.security.ProcedureAccessControlRule.ProcedurePrivilege.EXECUTE;
import static io.trino.plugin.base.security.ProcedureAccessControlRule.ProcedurePrivilege.GRANT_EXECUTE;
import static java.util.Objects.requireNonNull;

public class CatalogProcedureAccessControlRule
{
    public static final CatalogProcedureAccessControlRule ALLOW_BUILTIN = new CatalogProcedureAccessControlRule(
            ImmutableSet.of(EXECUTE, GRANT_EXECUTE),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(Pattern.compile("system")),
            Optional.of(Pattern.compile("builtin")),
            Optional.empty());

    private final Optional<Pattern> catalogRegex;
    private final ProcedureAccessControlRule procedureAccessControlRule;

    @JsonCreator
    public CatalogProcedureAccessControlRule(
            @JsonProperty("privileges") Set<ProcedurePrivilege> privileges,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("role") Optional<Pattern> roleRegex,
            @JsonProperty("group") Optional<Pattern> groupRegex,
            @JsonProperty("catalog") Optional<Pattern> catalogRegex,
            @JsonProperty("schema") Optional<Pattern> schemaRegex,
            @JsonProperty("procedure") Optional<Pattern> procedureRegex)
    {
        this(catalogRegex, new ProcedureAccessControlRule(privileges, userRegex, roleRegex, groupRegex, schemaRegex, procedureRegex));
    }

    private CatalogProcedureAccessControlRule(Optional<Pattern> catalogRegex, ProcedureAccessControlRule procedureAccessControlRule)
    {
        this.catalogRegex = requireNonNull(catalogRegex, "catalogRegex is null");
        this.procedureAccessControlRule = requireNonNull(procedureAccessControlRule, "procedureAccessControlRule is null");
    }

    public boolean matches(String user, Set<String> roles, Set<String> groups, CatalogSchemaRoutineName procedureName)
    {
        if (!catalogRegex.map(regex -> regex.matcher(procedureName.getCatalogName()).matches()).orElse(true)) {
            return false;
        }
        return procedureAccessControlRule.matches(user, roles, groups, procedureName.getSchemaRoutineName());
    }

    Optional<AnyCatalogPermissionsRule> toAnyCatalogPermissionsRule()
    {
        if (procedureAccessControlRule.getPrivileges().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new AnyCatalogPermissionsRule(
                procedureAccessControlRule.getUserRegex(),
                procedureAccessControlRule.getRoleRegex(),
                procedureAccessControlRule.getGroupRegex(),
                catalogRegex));
    }

    Optional<AnyCatalogSchemaPermissionsRule> toAnyCatalogSchemaPermissionsRule()
    {
        if (procedureAccessControlRule.getPrivileges().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new AnyCatalogSchemaPermissionsRule(
                procedureAccessControlRule.getUserRegex(),
                procedureAccessControlRule.getRoleRegex(),
                procedureAccessControlRule.getGroupRegex(),
                catalogRegex,
                procedureAccessControlRule.getSchemaRegex()));
    }

    public boolean canExecuteProcedure()
    {
        return procedureAccessControlRule.canExecuteProcedure();
    }

    public boolean canGrantExecuteProcedure()
    {
        return procedureAccessControlRule.canGrantExecuteProcedure();
    }
}
