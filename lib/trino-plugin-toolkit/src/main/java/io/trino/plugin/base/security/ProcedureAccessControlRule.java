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
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> roleRegex;
    private final Optional<Pattern> groupRegex;
    private final Optional<Pattern> schemaRegex;
    private final Optional<Pattern> procedureRegex;

    @JsonCreator
    public ProcedureAccessControlRule(
            @JsonProperty("privileges") Set<ProcedurePrivilege> privileges,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("role") Optional<Pattern> roleRegex,
            @JsonProperty("group") Optional<Pattern> groupRegex,
            @JsonProperty("schema") Optional<Pattern> schemaRegex,
            @JsonProperty("procedure") Optional<Pattern> procedureRegex)
    {
        this.privileges = ImmutableSet.copyOf(requireNonNull(privileges, "privileges is null"));
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.roleRegex = requireNonNull(roleRegex, "roleRegex is null");
        this.groupRegex = requireNonNull(groupRegex, "groupRegex is null");
        this.schemaRegex = requireNonNull(schemaRegex, "schemaRegex is null");
        this.procedureRegex = requireNonNull(procedureRegex, "procedureRegex is null");
    }

    public boolean matches(String user, Set<String> roles, Set<String> groups, SchemaRoutineName procedureName)
    {
        return userRegex.map(regex -> regex.matcher(user).matches()).orElse(true) &&
                roleRegex.map(regex -> roles.stream().anyMatch(role -> regex.matcher(role).matches())).orElse(true) &&
                groupRegex.map(regex -> groups.stream().anyMatch(group -> regex.matcher(group).matches())).orElse(true) &&
                schemaRegex.map(regex -> regex.matcher(procedureName.getSchemaName()).matches()).orElse(true) &&
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
        return Optional.of(new AnySchemaPermissionsRule(userRegex, roleRegex, groupRegex, schemaRegex));
    }

    Set<ProcedurePrivilege> getPrivileges()
    {
        return privileges;
    }

    Optional<Pattern> getUserRegex()
    {
        return userRegex;
    }

    Optional<Pattern> getRoleRegex()
    {
        return roleRegex;
    }

    Optional<Pattern> getGroupRegex()
    {
        return groupRegex;
    }

    Optional<Pattern> getSchemaRegex()
    {
        return schemaRegex;
    }

    public enum ProcedurePrivilege
    {
        EXECUTE, GRANT_EXECUTE
    }
}
