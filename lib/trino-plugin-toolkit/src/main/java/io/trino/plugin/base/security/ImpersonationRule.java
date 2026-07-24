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
import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.lang.Boolean.TRUE;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public class ImpersonationRule
{
    public enum Reason
    {
        IMPERSONATION,
        VIEW_OWNER,
        MATERIALIZED_VIEW_OWNER,
        FUNCTION_OWNER,
        ROW_FILTER,
        COLUMN_MASK,
    }

    private static final Set<Reason> DEFAULT_REASONS = ImmutableSet.of(Reason.IMPERSONATION);

    private final Optional<Pattern> originalUserPattern;
    private final Optional<Pattern> originalRolePattern;
    private final Pattern newUserPattern;
    private final Set<Reason> reasons;
    private final boolean allow;

    @JsonCreator
    public ImpersonationRule(
            @JsonProperty("original_user") @JsonAlias("originalUser") Optional<Pattern> originalUserPattern,
            @JsonProperty("original_role") Optional<Pattern> originalRolePattern,
            @JsonProperty("new_user") @JsonAlias("newUser") Pattern newUserPattern,
            @JsonProperty("reasons") Optional<Set<Reason>> reasons,
            @JsonProperty("allow") Boolean allow)
    {
        this.originalUserPattern = requireNonNull(originalUserPattern, "originalUserPattern is null");
        this.originalRolePattern = requireNonNull(originalRolePattern, "originalRolePattern is null");
        this.newUserPattern = requireNonNull(newUserPattern, "newUserPattern is null");
        this.reasons = ImmutableSet.copyOf(requireNonNull(reasons, "reasons is null").orElse(DEFAULT_REASONS));
        if (this.reasons.isEmpty()) {
            throw new TrinoException(CONFIGURATION_INVALID, "reasons in impersonation rule must not be empty");
        }
        this.allow = requireNonNullElse(allow, TRUE);
    }

    public Optional<Boolean> match(String originalUser, Set<String> originalRoles, String newUser, Reason reason)
    {
        if (!reasons.contains(reason)) {
            return Optional.empty();
        }
        Pattern replacedNewUserPattern = newUserPattern;
        if (originalUserPattern.isPresent()) {
            Matcher matcher = originalUserPattern.get().matcher(originalUser);
            if (matcher.matches()) {
                StringBuilder stringBuilder = new StringBuilder();
                try {
                    matcher.appendReplacement(stringBuilder, newUserPattern.pattern());
                }
                catch (IndexOutOfBoundsException e) {
                    throw new TrinoException(
                            CONFIGURATION_INVALID,
                            "new_user in impersonation rule refers to a capturing group that does not exist in original_user",
                            e);
                }

                replacedNewUserPattern = Pattern.compile(stringBuilder.toString());
            }
        }

        if (originalUserPattern.map(regex -> regex.matcher(originalUser).matches()).orElse(true) &&
                originalRolePattern.map(regex -> originalRoles.stream().anyMatch(role -> regex.matcher(role).matches())).orElse(true) &&
                replacedNewUserPattern.matcher(newUser).matches()) {
            return Optional.of(allow);
        }
        return Optional.empty();
    }
}
