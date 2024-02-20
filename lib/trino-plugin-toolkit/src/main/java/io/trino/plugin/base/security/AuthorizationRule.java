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
import io.trino.spi.security.TrinoPrincipal;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Boolean.TRUE;
import static java.util.Objects.requireNonNull;

public class AuthorizationRule
{
    private final Optional<Pattern> originalUserPattern;
    private final Optional<Pattern> originalGroupPattern;
    private final Optional<Pattern> originalRolePattern;
    private final Optional<Pattern> newUserPattern;
    private final Optional<Pattern> newRolePattern;
    private final boolean allow;

    @JsonCreator
    public AuthorizationRule(
            @JsonProperty("original_user") @JsonAlias("originalUser") Optional<Pattern> originalUserPattern,
            @JsonProperty("original_group") @JsonAlias("originalGroup") Optional<Pattern> originalGroupPattern,
            @JsonProperty("original_role") @JsonAlias("originalRole") Optional<Pattern> originalRolePattern,
            @JsonProperty("new_user") @JsonAlias("newUser") Optional<Pattern> newUserPattern,
            @JsonProperty("new_role") @JsonAlias("newRole") Optional<Pattern> newRolePattern,
            @JsonProperty("allow") Boolean allow)
    {
        checkArgument(newUserPattern.isPresent() || newRolePattern.isPresent(), "At least one of new_use or new_role is required, none were provided");
        this.originalUserPattern = requireNonNull(originalUserPattern, "originalUserPattern is null");
        this.originalGroupPattern = requireNonNull(originalGroupPattern, "originalGroupPattern is null");
        this.originalRolePattern = requireNonNull(originalRolePattern, "originalRolePattern is null");
        this.newUserPattern = requireNonNull(newUserPattern, "newUserPattern is null");
        this.newRolePattern = requireNonNull(newRolePattern, "newRolePattern is null");
        this.allow = firstNonNull(allow, TRUE);
    }

    public Optional<Boolean> match(String user, Set<String> groups, Set<String> roles, TrinoPrincipal newPrincipal)
    {
        if (originalUserPattern.map(regex -> regex.matcher(user).matches()).orElse(true) &&
                (originalGroupPattern.isEmpty() || groups.stream().anyMatch(group -> originalGroupPattern.get().matcher(group).matches())) &&
                (originalRolePattern.isEmpty() || roles.stream().anyMatch(role -> originalRolePattern.get().matcher(role).matches())) &&
                matches(newPrincipal)) {
            return Optional.of(allow);
        }
        return Optional.empty();
    }

    private boolean matches(TrinoPrincipal newPrincipal)
    {
        return switch (newPrincipal.getType()) {
            case USER -> newUserPattern.map(regex -> regex.matcher(newPrincipal.getName()).matches()).orElse(false);
            case ROLE -> newRolePattern.map(regex -> regex.matcher(newPrincipal.getName()).matches()).orElse(false);
        };
    }

    public Optional<Pattern> getOriginalRolePattern()
    {
        return originalRolePattern;
    }

    public Optional<Pattern> getNewRolePattern()
    {
        return newRolePattern;
    }
}
