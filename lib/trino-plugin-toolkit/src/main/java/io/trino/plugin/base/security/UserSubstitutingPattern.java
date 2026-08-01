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
import com.google.common.cache.Cache;
import io.trino.cache.EvictableCacheBuilder;

import java.util.regex.Pattern;

import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static java.util.Objects.requireNonNull;

/**
 * A regular expression that may contain the {@code {user}} placeholder, which is replaced with the current user name (quoted as a literal) before matching.
 */
public final class UserSubstitutingPattern
{
    private static final String USER_PLACEHOLDER = "{user}";

    private final String rawPattern;
    private final PatternProvider patternProvider;

    @JsonCreator
    public static UserSubstitutingPattern of(String rawPattern)
    {
        requireNonNull(rawPattern, "rawPattern is null");
        if (rawPattern.contains(USER_PLACEHOLDER)) {
            return new UserSubstitutingPattern(rawPattern, new UserSubstitutingPatternProvider(rawPattern));
        }
        return new UserSubstitutingPattern(rawPattern, new FixedPatternProvider(rawPattern));
    }

    private UserSubstitutingPattern(String rawPattern, PatternProvider patternProvider)
    {
        this.rawPattern = requireNonNull(rawPattern, "rawPattern is null");
        this.patternProvider = requireNonNull(patternProvider, "patternProvider is null");
    }

    public boolean matches(String user, String value)
    {
        return patternProvider.pattern(user).matcher(value).matches();
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof UserSubstitutingPattern that &&
                rawPattern.equals(that.rawPattern);
    }

    @Override
    public int hashCode()
    {
        return rawPattern.hashCode();
    }

    @Override
    public String toString()
    {
        return rawPattern;
    }

    private interface PatternProvider
    {
        Pattern pattern(String user);
    }

    private record FixedPatternProvider(Pattern pattern)
            implements PatternProvider
    {
        public FixedPatternProvider(String rawPattern)
        {
            this(Pattern.compile(rawPattern));
        }

        @Override
        public Pattern pattern(String user)
        {
            return pattern;
        }
    }

    private static final class UserSubstitutingPatternProvider
            implements PatternProvider
    {
        private final String rawPattern;
        private final Cache<String, Pattern> patternsByUser = EvictableCacheBuilder.newBuilder()
                .maximumSize(1_000)
                .build();

        public UserSubstitutingPatternProvider(String rawPattern)
        {
            this.rawPattern = requireNonNull(rawPattern, "rawPattern is null");
            // fail fast on invalid patterns: the substituted pattern is structurally the same for every user name,
            // because the substituted value is always a self-contained \Q...\E literal
            compile("user");
        }

        @Override
        public Pattern pattern(String user)
        {
            return uncheckedCacheGet(patternsByUser, user, () -> compile(user));
        }

        private Pattern compile(String user)
        {
            return Pattern.compile(rawPattern.replace(USER_PLACEHOLDER, Pattern.quote(user)));
        }
    }
}
