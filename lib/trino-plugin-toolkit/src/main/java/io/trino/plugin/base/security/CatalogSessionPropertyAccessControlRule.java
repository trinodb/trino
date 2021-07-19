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

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class CatalogSessionPropertyAccessControlRule
{
    public static final CatalogSessionPropertyAccessControlRule ALLOW_ALL = new CatalogSessionPropertyAccessControlRule(
            true,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    private final Optional<Pattern> catalogRegex;
    private final SessionPropertyAccessControlRule sessionPropertyAccessControlRule;

    @JsonCreator
    public CatalogSessionPropertyAccessControlRule(
            @JsonProperty("allow") boolean allow,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("role") Optional<Pattern> roleRegex,
            @JsonProperty("group") Optional<Pattern> groupRegex,
            @JsonProperty("property") Optional<Pattern> propertyRegex,
            @JsonProperty("catalog") Optional<Pattern> catalogRegex)
    {
        this.sessionPropertyAccessControlRule = new SessionPropertyAccessControlRule(allow, userRegex, roleRegex, groupRegex, propertyRegex);
        this.catalogRegex = requireNonNull(catalogRegex, "catalogRegex is null");
    }

    public Optional<Boolean> match(String user, Set<String> roles, Set<String> groups, String catalog, String property)
    {
        if (!catalogRegex.map(regex -> regex.matcher(catalog).matches()).orElse(true)) {
            return Optional.empty();
        }
        return sessionPropertyAccessControlRule.match(user, roles, groups, property);
    }

    Optional<AnyCatalogPermissionsRule> toAnyCatalogPermissionsRule()
    {
        if (!sessionPropertyAccessControlRule.isAllow()) {
            return Optional.empty();
        }
        return Optional.of(new AnyCatalogPermissionsRule(
                sessionPropertyAccessControlRule.getUserRegex(),
                sessionPropertyAccessControlRule.getRoleRegex(),
                sessionPropertyAccessControlRule.getGroupRegex(),
                catalogRegex));
    }
}
