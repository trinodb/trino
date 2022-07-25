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
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> roleRegex;
    private final Optional<Pattern> groupRegex;
    private final Optional<Pattern> catalogRegex;

    public AnyCatalogPermissionsRule(Optional<Pattern> userRegex, Optional<Pattern> roleRegex, Optional<Pattern> groupRegex, Optional<Pattern> catalogRegex)
    {
        this.userRegex = userRegex;
        this.roleRegex = roleRegex;
        this.groupRegex = groupRegex;
        this.catalogRegex = catalogRegex;
    }

    public boolean match(String user, Set<String> roles, Set<String> groups, String catalog)
    {
        return userRegex.map(regex -> regex.matcher(user).matches()).orElse(true) &&
                roleRegex.map(regex -> roles.stream().anyMatch(role -> regex.matcher(role).matches())).orElse(true) &&
                groupRegex.map(regex -> groups.stream().anyMatch(group -> regex.matcher(group).matches())).orElse(true) &&
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
        return patternEquals(userRegex, that.userRegex) &&
                patternEquals(roleRegex, that.roleRegex) &&
                patternEquals(groupRegex, that.groupRegex) &&
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
        return Objects.hash(userRegex, roleRegex, groupRegex, catalogRegex);
    }
}
