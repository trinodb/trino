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
import io.trino.spi.connector.SchemaRoutineName;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static io.trino.plugin.base.security.ProcedureAccessControlRule.ProcedurePrivilege.EXECUTE;
import static io.trino.plugin.base.security.ProcedureAccessControlRule.ProcedurePrivilege.GRANT_EXECUTE;
import static java.util.Objects.requireNonNull;

public class ProcedureAccessControlRule
{
    private final Set<ProcedurePrivilege> privileges;
    private final IdentityMatcher identityMatcher;
    private final Optional<UserSubstitutingPattern> schemaPattern;
    private final Optional<Pattern> procedureRegex;

    @JsonCreator
    public ProcedureAccessControlRule(
            @JsonProperty("privileges") Set<ProcedurePrivilege> privileges,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("role") Optional<Pattern> roleRegex,
            @JsonProperty("group") Optional<Pattern> groupRegex,
            @JsonProperty("schema") Optional<UserSubstitutingPattern> schemaPattern,
            @JsonProperty("procedure") Optional<Pattern> procedureRegex)
    {
        this.privileges = ImmutableSet.copyOf(requireNonNull(privileges, "privileges is null"));
        this.identityMatcher = new IdentityMatcher(userRegex, roleRegex, groupRegex);
        this.schemaPattern = requireNonNull(schemaPattern, "schemaPattern is null");
        this.procedureRegex = requireNonNull(procedureRegex, "procedureRegex is null");
    }

    public boolean matches(String user, Set<String> roles, Set<String> groups, SchemaRoutineName procedureName)
    {
        return identityMatcher.matches(user, roles, groups) &&
                schemaPattern.map(pattern -> pattern.matches(user, procedureName.getSchemaName())).orElse(true) &&
                procedureRegex.map(regex -> regex.matcher(procedureName.getRoutineName()).matches()).orElse(true);
    }

    public boolean canExecuteProcedure()
    {
        return privileges.contains(EXECUTE) || canGrantExecuteProcedure();
    }

    public boolean canGrantExecuteProcedure()
    {
        return privileges.contains(GRANT_EXECUTE);
    }

    Optional<AnySchemaPermissionsRule> toAnySchemaPermissionsRule()
    {
        if (privileges.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new AnySchemaPermissionsRule(identityMatcher, schemaPattern));
    }

    Set<ProcedurePrivilege> getPrivileges()
    {
        return privileges;
    }

    IdentityMatcher getIdentityMatcher()
    {
        return identityMatcher;
    }

    Optional<UserSubstitutingPattern> getSchemaPattern()
    {
        return schemaPattern;
    }

    public enum ProcedurePrivilege
    {
        EXECUTE, GRANT_EXECUTE
    }
}
