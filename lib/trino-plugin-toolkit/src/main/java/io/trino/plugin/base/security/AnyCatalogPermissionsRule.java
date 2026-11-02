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

public class AnyCatalogPermissionsRule
{
    private final IdentityMatcher identityMatcher;
    private final Optional<UserSubstitutingPattern> catalogPattern;

    public AnyCatalogPermissionsRule(IdentityMatcher identityMatcher, Optional<UserSubstitutingPattern> catalogPattern)
    {
        this.identityMatcher = identityMatcher;
        this.catalogPattern = catalogPattern;
    }

    public boolean match(String user, Set<String> roles, Set<String> groups, String catalog)
    {
        return identityMatcher.matches(user, roles, groups) &&
                catalogPattern.map(pattern -> pattern.matches(user, catalog)).orElse(true);
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
        AnyCatalogPermissionsRule that = (AnyCatalogPermissionsRule) o;
        return identityMatcher.equals(that.identityMatcher) &&
                Objects.equals(catalogPattern, that.catalogPattern);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(identityMatcher, catalogPattern);
    }
}
