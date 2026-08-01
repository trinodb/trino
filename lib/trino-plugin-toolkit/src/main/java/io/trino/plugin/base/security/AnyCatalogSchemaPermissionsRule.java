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

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class AnyCatalogSchemaPermissionsRule
{
    private final IdentityMatcher identityMatcher;
    private final Optional<UserSubstitutingPattern> catalogPattern;
    private final Optional<UserSubstitutingPattern> schemaPattern;

    public AnyCatalogSchemaPermissionsRule(IdentityMatcher identityMatcher, Optional<UserSubstitutingPattern> catalogPattern, Optional<UserSubstitutingPattern> schemaPattern)
    {
        this.identityMatcher = identityMatcher;
        this.catalogPattern = catalogPattern;
        this.schemaPattern = schemaPattern;
    }

    public boolean match(String user, Set<String> roles, Set<String> groups, String catalogName, String schemaName)
    {
        return identityMatcher.matches(user, roles, groups) &&
                catalogPattern.map(pattern -> pattern.matches(user, catalogName)).orElse(true) &&
                schemaPattern.map(pattern -> pattern.matches(user, schemaName)).orElse(true);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AnyCatalogSchemaPermissionsRule that = (AnyCatalogSchemaPermissionsRule) o;
        return identityMatcher.equals(that.identityMatcher) &&
                Objects.equals(catalogPattern, that.catalogPattern) &&
                Objects.equals(schemaPattern, that.schemaPattern);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(identityMatcher, catalogPattern, schemaPattern);
    }
}
