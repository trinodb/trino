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
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableSet;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class QueryAccessRule
{
    private final Set<AccessMode> allow;
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> roleRegex;

    @JsonCreator
    public QueryAccessRule(
            @JsonProperty("allow") Set<AccessMode> allow,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("role") Optional<Pattern> roleRegex)
    {
        this.allow = ImmutableSet.copyOf(requireNonNull(allow, "allow is null"));
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.roleRegex = requireNonNull(roleRegex, "roleRegex is null");
    }

    public Optional<Set<AccessMode>> match(String user, Set<String> roles)
    {
        if (userRegex.map(regex -> regex.matcher(user).matches()).orElse(true) &&
                roleRegex.map(regex -> roles.stream().anyMatch(role -> regex.matcher(role).matches())).orElse(true)) {
            return Optional.of(allow);
        }
        return Optional.empty();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("allow", allow)
                .add("userRegex", userRegex.orElse(null))
                .add("roleRegex", roleRegex.orElse(null))
                .toString();
    }

    public enum AccessMode
    {
        EXECUTE("execute"),
        VIEW("view"),
        KILL("kill");

        private static final Map<String, AccessMode> modeByName = stream(AccessMode.values()).collect(toImmutableMap(AccessMode::toString, identity()));

        private final String stringValue;

        AccessMode(String stringValue)
        {
            this.stringValue = requireNonNull(stringValue, "stringValue is null");
        }

        @JsonValue
        @Override
        public String toString()
        {
            return stringValue;
        }

        @JsonCreator
        public static AccessMode fromJson(Object value)
        {
            if (value instanceof String) {
                AccessMode accessMode = modeByName.get(((String) value).toLowerCase(Locale.US));
                if (accessMode != null) {
                    return accessMode;
                }
            }

            throw new IllegalArgumentException("Unknown " + AccessMode.class.getSimpleName() + ": " + value);
        }
    }
}
