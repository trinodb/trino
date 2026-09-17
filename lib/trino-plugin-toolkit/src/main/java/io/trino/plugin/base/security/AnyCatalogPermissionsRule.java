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
import java.util.regex.Pattern;

public class AnyCatalogPermissionsRule
{
    private final IdentityMatcher identityMatcher;
    private final Optional<Pattern> catalogRegex;

    public AnyCatalogPermissionsRule(IdentityMatcher identityMatcher, Optional<Pattern> catalogRegex)
    {
        this.identityMatcher = identityMatcher;
        this.catalogRegex = catalogRegex;
    }

    public boolean match(String user, Set<String> roles, Set<String> groups, String catalog)
    {
        return identityMatcher.matches(user, roles, groups) &&
                catalogRegex.map(regex -> regex.matcher(catalog).matches()).orElse(true);
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
                patternEquals(catalogRegex, that.catalogRegex);
    }

    private static boolean patternEquals(Optional<Pattern> left, Optional<Pattern> right)
    {
        if (left.isEmpty() || right.isEmpty()) {
            return left.isEmpty() == right.isEmpty();
        }
        Pattern leftPattern = left.get();
        Pattern rightPattern = right.get();
        return leftPattern.pattern().equals(rightPattern.pattern()) && leftPattern.flags() == rightPattern.flags();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(identityMatcher, catalogRegex);
    }
}
