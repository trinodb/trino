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
package io.trino.server.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static io.trino.server.security.UserMapping.Case.KEEP;
import static java.lang.Boolean.TRUE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class UserMapping
{
    private final List<Rule> rules;

    public static UserMapping createUserMapping(Optional<String> userMappingPattern, Optional<File> userMappingFile)
    {
        if (userMappingPattern.isPresent()) {
            checkArgument(userMappingFile.isEmpty(), "user mapping pattern and file can not both be set");
            return new UserMapping(ImmutableList.of(new Rule(userMappingPattern.get())));
        }
        if (userMappingFile.isPresent()) {
            List<Rule> rules = parseJson(userMappingFile.get().toPath(), UserMappingRules.class).getRules();
            return new UserMapping(rules);
        }
        return new UserMapping(ImmutableList.of(new Rule("(.*)")));
    }

    @VisibleForTesting
    UserMapping(List<Rule> rules)
    {
        requireNonNull(rules, "rules is null");
        checkArgument(!rules.isEmpty(), "rules list is empty");
        this.rules = ImmutableList.copyOf(rules);
    }

    public String mapUser(String principal)
            throws UserMappingException
    {
        for (Rule rule : rules) {
            Optional<String> user = rule.mapUser(principal);
            if (user.isPresent()) {
                return user.get();
            }
        }

        throw new UserMappingException("No user mapping patterns match the principal");
    }

    public static final class UserMappingRules
    {
        private final List<Rule> rules;

        @JsonCreator
        public UserMappingRules(
                @JsonProperty("rules") List<Rule> rules)
        {
            this.rules = ImmutableList.copyOf(requireNonNull(rules, "rules is null"));
        }

        public List<Rule> getRules()
        {
            return rules;
        }
    }

    enum Case
    {
        KEEP {
            @Override
            public String transform(String value)
            {
                return value;
            }
        },
        LOWER {
            @Override
            public String transform(String value)
            {
                return value.toLowerCase(ENGLISH);
            }
        },
        UPPER {
            @Override
            public String transform(String value)
            {
                return value.toUpperCase(ENGLISH);
            }
        };

        public abstract String transform(String value);
    }

    public static final class Rule
    {
        private final Pattern pattern;
        private final String user;
        private final boolean allow;
        private final Case userCase;

        public Rule(String pattern)
        {
            this(pattern, "$1", true, KEEP);
        }

        @JsonCreator
        public Rule(
                @JsonProperty("pattern") String pattern,
                @JsonProperty("user") Optional<String> user,
                @JsonProperty("allow") Optional<Boolean> allow,
                @JsonProperty("case") Optional<Case> userCase)
        {
            this(
                    pattern,
                    user.orElse("$1"),
                    allow.orElse(TRUE),
                    userCase.orElse(KEEP));
        }

        public Rule(String pattern, String user, boolean allow, Case userCase)
        {
            this.pattern = Pattern.compile(requireNonNull(pattern, "pattern is null"));
            this.user = requireNonNull(user, "user is null");
            this.allow = allow;
            this.userCase = requireNonNull(userCase, "userCase is null");
        }

        public Optional<String> mapUser(String principal)
                throws UserMappingException
        {
            Matcher matcher = pattern.matcher(principal);
            if (!matcher.matches()) {
                return Optional.empty();
            }
            if (!allow) {
                throw new UserMappingException("Principal is not allowed");
            }
            String result = matcher.replaceAll(user).trim();
            if (result.isEmpty()) {
                throw new UserMappingException("Principal matched, but mapped user is empty");
            }
            return Optional.of(userCase.transform(result));
        }
    }
}
