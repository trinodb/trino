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
import io.trino.plugin.base.security.FunctionAccessControlRule.FunctionPrivilege;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.function.FunctionKind;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.function.FunctionKind.TABLE;
import static java.util.Objects.requireNonNull;

public class CatalogFunctionAccessControlRule
{
    public static final CatalogFunctionAccessControlRule ALLOW_ALL = new CatalogFunctionAccessControlRule(
            Optional.empty(),
            FunctionAccessControlRule.ALLOW_ALL);

    private final Optional<Pattern> catalogRegex;
    private final FunctionAccessControlRule functionAccessControlRule;

    @JsonCreator
    public CatalogFunctionAccessControlRule(
            @JsonProperty("privileges") Set<FunctionPrivilege> privileges,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("role") Optional<Pattern> roleRegex,
            @JsonProperty("group") Optional<Pattern> groupRegex,
            @JsonProperty("catalog") Optional<Pattern> catalogRegex,
            @JsonProperty("schema") Optional<Pattern> schemaRegex,
            @JsonProperty("function") Optional<Pattern> tableFunctionRegex,
            @JsonProperty("function_kinds") @JsonAlias("functionKinds") Set<FunctionKind> functionKinds)
    {
        this(catalogRegex, new FunctionAccessControlRule(privileges, userRegex, roleRegex, groupRegex, schemaRegex, tableFunctionRegex, functionKinds));
    }

    private CatalogFunctionAccessControlRule(Optional<Pattern> catalogRegex, FunctionAccessControlRule functionAccessControlRule)
    {
        this.catalogRegex = requireNonNull(catalogRegex, "catalogRegex is null");
        this.functionAccessControlRule = requireNonNull(functionAccessControlRule, "functionAccessControlRule is null");
        // TODO when every function is tied to connectors then remove this check
        checkState(functionAccessControlRule.getFunctionKinds().equals(Set.of(TABLE)) || catalogRegex.isEmpty(), "Cannot define catalog for others function kinds than TABLE");
    }

    public boolean matches(String user, Set<String> roles, Set<String> groups, String functionName)
    {
        return functionAccessControlRule.matches(user, roles, groups, functionName);
    }

    public boolean matches(String user, Set<String> roles, Set<String> groups, FunctionKind functionKind, CatalogSchemaRoutineName functionName)
    {
        if (!catalogRegex.map(regex -> regex.matcher(functionName.getCatalogName()).matches()).orElse(true)) {
            return false;
        }
        return functionAccessControlRule.matches(user, roles, groups, functionKind, functionName.getSchemaRoutineName());
    }

    Optional<AnyCatalogPermissionsRule> toAnyCatalogPermissionsRule()
    {
        if (functionAccessControlRule.getPrivileges().isEmpty() ||
                // TODO when every function is tied to connectors then remove this check
                !functionAccessControlRule.getFunctionKinds().contains(TABLE)) {
            return Optional.empty();
        }
        return Optional.of(new AnyCatalogPermissionsRule(
                functionAccessControlRule.getUserRegex(),
                functionAccessControlRule.getRoleRegex(),
                functionAccessControlRule.getGroupRegex(),
                catalogRegex));
    }

    Optional<AnyCatalogSchemaPermissionsRule> toAnyCatalogSchemaPermissionsRule()
    {
        if (functionAccessControlRule.getPrivileges().isEmpty() ||
                // TODO when every function is tied to connectors then remove this check
                !functionAccessControlRule.getFunctionKinds().contains(TABLE)) {
            return Optional.empty();
        }
        return Optional.of(new AnyCatalogSchemaPermissionsRule(
                functionAccessControlRule.getUserRegex(),
                functionAccessControlRule.getRoleRegex(),
                functionAccessControlRule.getGroupRegex(),
                catalogRegex,
                functionAccessControlRule.getSchemaRegex()));
    }

    public boolean canExecuteFunction()
    {
        return functionAccessControlRule.canExecuteFunction();
    }

    public boolean canGrantExecuteFunction()
    {
        return functionAccessControlRule.canGrantExecuteFunction();
    }
}
