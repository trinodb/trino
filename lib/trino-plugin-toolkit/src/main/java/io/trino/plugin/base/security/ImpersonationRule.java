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

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.lang.Boolean.TRUE;
import static java.util.Objects.requireNonNull;

public class ImpersonationRule
{
    private final Optional<Pattern> originalUserPattern;
    private final Optional<Pattern> originalRolePattern;
    private final Pattern newUserPattern;
    private final boolean allow;

    @JsonCreator
    public ImpersonationRule(
            @JsonProperty("original_user") @JsonAlias("originalUser") Optional<Pattern> originalUserPattern,
            @JsonProperty("original_role") Optional<Pattern> originalRolePattern,
            @JsonProperty("new_user") @JsonAlias("newUser") Pattern newUserPattern,
            @JsonProperty("allow") Boolean allow)
    {
        this.originalUserPattern = requireNonNull(originalUserPattern, "originalUserPattern is null");
        this.originalRolePattern = requireNonNull(originalRolePattern, "originalRolePattern is null");
        this.newUserPattern = requireNonNull(newUserPattern, "newUserPattern is null");
        this.allow = firstNonNull(allow, TRUE);
    }

    public Optional<Boolean> match(String originalUser, Set<String> originalRoles, String newUser)
    {
        if (originalUserPattern.map(regex -> regex.matcher(originalUser).matches()).orElse(true) &&
                originalRolePattern.map(regex -> originalRoles.stream().anyMatch(role -> regex.matcher(role).matches())).orElse(true) &&
                newUserPattern.matcher(newUser).matches()) {
            return Optional.of(allow);
        }
        return Optional.empty();
    }
}
