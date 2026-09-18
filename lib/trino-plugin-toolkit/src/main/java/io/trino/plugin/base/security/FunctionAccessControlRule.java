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

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.SchemaRoutineName;
import io.trino.spi.function.FunctionKind;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static io.trino.plugin.base.security.FunctionAccessControlRule.FunctionPrivilege.EXECUTE;
import static io.trino.plugin.base.security.FunctionAccessControlRule.FunctionPrivilege.GRANT_EXECUTE;
import static io.trino.plugin.base.security.FunctionAccessControlRule.FunctionPrivilege.OWNERSHIP;
import static java.util.Objects.requireNonNull;

public class FunctionAccessControlRule
{
    private final Set<FunctionPrivilege> privileges;
    private final IdentityMatcher identityMatcher;
    private final Optional<UserSubstitutingPattern> schemaPattern;
    private final Optional<Pattern> functionRegex;

    @JsonCreator
    public FunctionAccessControlRule(
            @JsonProperty("privileges") Set<FunctionPrivilege> privileges,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("role") Optional<Pattern> roleRegex,
            @JsonProperty("group") Optional<Pattern> groupRegex,
            @JsonProperty("schema") Optional<UserSubstitutingPattern> schemaPattern,
            @JsonProperty("function") Optional<Pattern> functionRegex,
            @JsonProperty("function_kinds") @JsonAlias("functionKinds") Set<FunctionKind> functionKinds)
    {
        this.privileges = ImmutableSet.copyOf(requireNonNull(privileges, "privileges is null"));
        this.identityMatcher = new IdentityMatcher(userRegex, roleRegex, groupRegex);
        this.schemaPattern = requireNonNull(schemaPattern, "schemaPattern is null");
        this.functionRegex = requireNonNull(functionRegex, "functionRegex is null");
        if (functionKinds != null && !functionKinds.isEmpty()) {
            throw new IllegalArgumentException("function_kind is no longer supported in security rules");
        }
    }

    public boolean matches(String user, Set<String> roles, Set<String> groups, SchemaRoutineName functionName)
    {
        return identityMatcher.matches(user, roles, groups) &&
                schemaPattern.map(pattern -> pattern.matches(user, functionName.getSchemaName())).orElse(true) &&
                functionRegex.map(regex -> regex.matcher(functionName.getRoutineName()).matches()).orElse(true);
    }

    public boolean canExecuteFunction()
    {
        return privileges.contains(EXECUTE) || canGrantExecuteFunction();
    }

    public boolean canGrantExecuteFunction()
    {
        return privileges.contains(GRANT_EXECUTE);
    }

    public boolean hasOwnership()
    {
        return privileges.contains(OWNERSHIP);
    }

    Optional<AnySchemaPermissionsRule> toAnySchemaPermissionsRule()
    {
        if (privileges.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new AnySchemaPermissionsRule(identityMatcher, schemaPattern));
    }

    Set<FunctionPrivilege> getPrivileges()
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

    public enum FunctionPrivilege
    {
        EXECUTE, GRANT_EXECUTE, OWNERSHIP
    }
}
