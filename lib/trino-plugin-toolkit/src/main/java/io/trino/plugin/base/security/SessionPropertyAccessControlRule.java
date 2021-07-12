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

public class SessionPropertyAccessControlRule
{
    public static final SessionPropertyAccessControlRule ALLOW_ALL = new SessionPropertyAccessControlRule(
            true,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    private final boolean allow;
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> roleRegex;
    private final Optional<Pattern> groupRegex;
    private final Optional<Pattern> propertyRegex;

    @JsonCreator
    public SessionPropertyAccessControlRule(
            @JsonProperty("allow") boolean allow,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("role") Optional<Pattern> roleRegex,
            @JsonProperty("group") Optional<Pattern> groupRegex,
            @JsonProperty("property") Optional<Pattern> propertyRegex)
    {
        this.allow = allow;
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.roleRegex = requireNonNull(roleRegex, "roleRegex is null");
        this.groupRegex = requireNonNull(groupRegex, "groupRegex is null");
        this.propertyRegex = requireNonNull(propertyRegex, "propertyRegex is null");
    }

    boolean isAllow()
    {
        return allow;
    }

    Optional<Pattern> getUserRegex()
    {
        return userRegex;
    }

    public Optional<Pattern> getRoleRegex()
    {
        return roleRegex;
    }

    Optional<Pattern> getGroupRegex()
    {
        return groupRegex;
    }

    public Optional<Boolean> match(String user, Set<String> roles, Set<String> groups, String property)
    {
        if (userRegex.map(regex -> regex.matcher(user).matches()).orElse(true) &&
                roleRegex.map(regex -> roles.stream().anyMatch(role -> regex.matcher(role).matches())).orElse(true) &&
                groupRegex.map(regex -> groups.stream().anyMatch(group -> regex.matcher(group).matches())).orElse(true) &&
                propertyRegex.map(regex -> regex.matcher(property).matches()).orElse(true)) {
            return Optional.of(allow);
        }
        return Optional.empty();
    }
}
