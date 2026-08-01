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

public class SchemaAccessControlRule
{
    public static final SchemaAccessControlRule ALLOW_ALL = new SchemaAccessControlRule(
            true,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    private final boolean owner;
    private final IdentityMatcher identityMatcher;
    private final Optional<UserSubstitutingPattern> schemaPattern;

    @JsonCreator
    public SchemaAccessControlRule(
            @JsonProperty("owner") boolean owner,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("role") Optional<Pattern> roleRegex,
            @JsonProperty("group") Optional<Pattern> groupRegex,
            @JsonProperty("schema") Optional<UserSubstitutingPattern> schemaPattern)
    {
        this.owner = owner;
        this.identityMatcher = new IdentityMatcher(userRegex, roleRegex, groupRegex);
        this.schemaPattern = requireNonNull(schemaPattern, "schemaPattern is null");
    }

    public Optional<Boolean> match(String user, Set<String> roles, Set<String> groups, String schema)
    {
        if (identityMatcher.matches(user, roles, groups) &&
                schemaPattern.map(pattern -> pattern.matches(user, schema)).orElse(true)) {
            return Optional.of(owner);
        }
        return Optional.empty();
    }

    Optional<AnySchemaPermissionsRule> toAnySchemaPermissionsRule()
    {
        if (!owner) {
            return Optional.empty();
        }
        return Optional.of(new AnySchemaPermissionsRule(identityMatcher, schemaPattern));
    }

    boolean isOwner()
    {
        return owner;
    }

    IdentityMatcher getIdentityMatcher()
    {
        return identityMatcher;
    }

    Optional<UserSubstitutingPattern> getSchemaPattern()
    {
        return schemaPattern;
    }
}
