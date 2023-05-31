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
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public class AccessControlRules
{
    private final List<SchemaAccessControlRule> schemaRules;
    private final List<TableAccessControlRule> tableRules;
    private final List<SessionPropertyAccessControlRule> sessionPropertyRules;
    private final List<FunctionAccessControlRule> functionRules;
    private final List<AuthorizationRule> authorizationRules;

    @JsonCreator
    public AccessControlRules(
            @JsonProperty("schemas") Optional<List<SchemaAccessControlRule>> schemaRules,
            @JsonProperty("tables") Optional<List<TableAccessControlRule>> tableRules,
            @JsonProperty("session_properties") @JsonAlias("sessionProperties") Optional<List<SessionPropertyAccessControlRule>> sessionPropertyRules,
            @JsonProperty("functions") Optional<List<FunctionAccessControlRule>> functionRules,
            @JsonProperty("authorization") Optional<List<AuthorizationRule>> authorizationRules)
    {
        this.schemaRules = schemaRules.orElse(ImmutableList.of(SchemaAccessControlRule.ALLOW_ALL));
        this.tableRules = tableRules.orElse(ImmutableList.of(TableAccessControlRule.ALLOW_ALL));
        this.sessionPropertyRules = sessionPropertyRules.orElse(ImmutableList.of(SessionPropertyAccessControlRule.ALLOW_ALL));
        this.functionRules = functionRules.orElse(ImmutableList.of(FunctionAccessControlRule.ALLOW_ALL));
        this.authorizationRules = authorizationRules.orElse(ImmutableList.of());
    }

    public List<SchemaAccessControlRule> getSchemaRules()
    {
        return schemaRules;
    }

    public List<TableAccessControlRule> getTableRules()
    {
        return tableRules;
    }

    public List<SessionPropertyAccessControlRule> getSessionPropertyRules()
    {
        return sessionPropertyRules;
    }

    public List<FunctionAccessControlRule> getFunctionRules()
    {
        return functionRules;
    }

    public List<AuthorizationRule> getAuthorizationRules()
    {
        return authorizationRules;
    }

    public boolean hasRoleRules()
    {
        return schemaRules.stream().anyMatch(rule -> rule.getRoleRegex().isPresent()) ||
                tableRules.stream().anyMatch(rule -> rule.getRoleRegex().isPresent()) ||
                sessionPropertyRules.stream().anyMatch(rule -> rule.getRoleRegex().isPresent()) ||
                functionRules.stream().anyMatch(rule -> rule.getRoleRegex().isPresent()) ||
                authorizationRules.stream().anyMatch(rule -> rule.getOriginalRolePattern().isPresent()) ||
                authorizationRules.stream().anyMatch(rule -> rule.getNewRolePattern().isPresent());
    }
}
