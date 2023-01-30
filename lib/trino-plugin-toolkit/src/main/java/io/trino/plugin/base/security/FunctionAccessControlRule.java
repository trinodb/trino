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

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.security.FunctionAccessControlRule.FunctionPrivilege.EXECUTE;
import static io.trino.plugin.base.security.FunctionAccessControlRule.FunctionPrivilege.GRANT_EXECUTE;
import static io.trino.spi.function.FunctionKind.AGGREGATE;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static io.trino.spi.function.FunctionKind.TABLE;
import static io.trino.spi.function.FunctionKind.WINDOW;
import static java.util.Objects.requireNonNull;

public class FunctionAccessControlRule
{
    public static final FunctionAccessControlRule ALLOW_ALL = new FunctionAccessControlRule(
            ImmutableSet.copyOf(FunctionPrivilege.values()),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            ImmutableSet.copyOf(FunctionKind.values()));

    private final Set<FunctionPrivilege> privileges;
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> roleRegex;
    private final Optional<Pattern> groupRegex;
    private final Optional<Pattern> schemaRegex;
    private final Optional<Pattern> functionRegex;
    private final Set<FunctionKind> functionKinds;
    private final boolean globalScopeFunctionKind;

    @JsonCreator
    public FunctionAccessControlRule(
            @JsonProperty("privileges") Set<FunctionPrivilege> privileges,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("role") Optional<Pattern> roleRegex,
            @JsonProperty("group") Optional<Pattern> groupRegex,
            @JsonProperty("schema") Optional<Pattern> schemaRegex,
            @JsonProperty("function") Optional<Pattern> functionRegex,
            @JsonProperty("function_kinds") @JsonAlias("functionKinds") Set<FunctionKind> functionKinds)
    {
        this.privileges = ImmutableSet.copyOf(requireNonNull(privileges, "privileges is null"));
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.roleRegex = requireNonNull(roleRegex, "roleRegex is null");
        this.groupRegex = requireNonNull(groupRegex, "groupRegex is null");
        this.schemaRegex = requireNonNull(schemaRegex, "schemaRegex is null");
        this.functionRegex = requireNonNull(functionRegex, "functionRegex is null");
        this.functionKinds = requireNonNull(functionKinds, "functionKinds is null");
        checkState(!functionKinds.isEmpty(), "functionKinds cannot be empty, provide at least one function kind " + Arrays.toString(FunctionKind.values()));
        globalScopeFunctionKind = functionKinds.contains(SCALAR) || functionKinds.contains(AGGREGATE) || functionKinds.contains(WINDOW);
        // TODO when every function is tied to connectors then remove this check
        checkState(functionKinds.equals(Set.of(TABLE)) || schemaRegex.isEmpty(), "Cannot define schema for others function kinds than TABLE");
    }

    public boolean matches(String user, Set<String> roles, Set<String> groups, String functionName)
    {
        return globalScopeFunctionKind &&
                userRegex.map(regex -> regex.matcher(user).matches()).orElse(true) &&
                roleRegex.map(regex -> roles.stream().anyMatch(role -> regex.matcher(role).matches())).orElse(true) &&
                groupRegex.map(regex -> groups.stream().anyMatch(group -> regex.matcher(group).matches())).orElse(true) &&
                functionRegex.map(regex -> regex.matcher(functionName).matches()).orElse(true);
    }

    public boolean matches(String user, Set<String> roles, Set<String> groups, FunctionKind functionKind, SchemaRoutineName functionName)
    {
        return this.functionKinds.contains(functionKind) &&
                userRegex.map(regex -> regex.matcher(user).matches()).orElse(true) &&
                roleRegex.map(regex -> roles.stream().anyMatch(role -> regex.matcher(role).matches())).orElse(true) &&
                groupRegex.map(regex -> groups.stream().anyMatch(group -> regex.matcher(group).matches())).orElse(true) &&
                schemaRegex.map(regex -> regex.matcher(functionName.getSchemaName()).matches()).orElse(true) &&
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

    Optional<AnySchemaPermissionsRule> toAnySchemaPermissionsRule()
    {
        if (privileges.isEmpty() ||
                // TODO when every function is tied to connectors then remove this check
                !functionKinds.contains(TABLE)) {
            return Optional.empty();
        }
        return Optional.of(new AnySchemaPermissionsRule(userRegex, roleRegex, groupRegex, schemaRegex));
    }

    Set<FunctionPrivilege> getPrivileges()
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

    Set<FunctionKind> getFunctionKinds()
    {
        return functionKinds;
    }

    public enum FunctionPrivilege
    {
        EXECUTE, GRANT_EXECUTE
    }
}
