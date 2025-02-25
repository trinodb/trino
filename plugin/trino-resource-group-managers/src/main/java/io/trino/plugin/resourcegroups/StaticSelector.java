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
package io.trino.plugin.resourcegroups;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.resourcegroups.SelectionContext;
import io.trino.spi.resourcegroups.SelectionCriteria;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StaticSelector
        implements ResourceGroupSelector
{
    private static final Pattern NAMED_GROUPS_PATTERN = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");
    private static final String USER_VARIABLE = "USER";
    private static final String SOURCE_VARIABLE = "SOURCE";

    private final Optional<Pattern> userRegex;
    private final ResourceGroupIdTemplate group;
    private final List<SelectionMatcher> selectionMatchers;

    public StaticSelector(
            Optional<Pattern> userRegex,
            Optional<Pattern> userGroupRegex,
            Optional<Pattern> originalUserRegex,
            Optional<Pattern> authenticatedUserRegex,
            Optional<Pattern> sourceRegex,
            Optional<List<String>> clientTags,
            Optional<SelectorResourceEstimate> selectorResourceEstimate,
            Optional<String> queryType,
            ResourceGroupIdTemplate group)
    {
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        requireNonNull(userGroupRegex, "userGroupRegex is null");
        requireNonNull(originalUserRegex, "originalUserRegex is null");
        requireNonNull(authenticatedUserRegex, "authenticatedUserRegex is null");
        requireNonNull(sourceRegex, "sourceRegex is null");
        requireNonNull(clientTags, "clientTags is null");
        requireNonNull(selectorResourceEstimate, "selectorResourceEstimate is null");
        requireNonNull(queryType, "queryType is null");
        this.group = requireNonNull(group, "group is null");

        ImmutableList.Builder<SelectionMatcher> selectionMatcherBuilder = ImmutableList.builder();
        HashSet<String> variableNames = new HashSet<>(ImmutableList.of(USER_VARIABLE, SOURCE_VARIABLE));
        userRegex.ifPresent(u -> {
            addNamedGroups(u, variableNames);
            selectionMatcherBuilder.add(new PatternMatcher(variableNames, u, SelectionCriteria::getUser));
        });
        originalUserRegex.ifPresent(u -> {
            addNamedGroups(u, variableNames);
            selectionMatcherBuilder.add(new PatternMatcher(variableNames, u, SelectionCriteria::getOriginalUser));
        });
        authenticatedUserRegex.ifPresent(u -> {
            addNamedGroups(u, variableNames);
            selectionMatcherBuilder.add(new OptionalPatternMatcher(variableNames, u, SelectionCriteria::getAuthenticatedUser));
        });
        sourceRegex.ifPresent(s -> {
            addNamedGroups(s, variableNames);
            selectionMatcherBuilder.add(new OptionalPatternMatcher(variableNames, s, SelectionCriteria::getSource));
        });
        Set<String> unresolvedVariables = Sets.difference(group.getVariableNames(), variableNames);
        checkArgument(unresolvedVariables.isEmpty(), "unresolved variables %s in resource group ID '%s', available: %s\"", unresolvedVariables, group, variableNames);

        userGroupRegex.ifPresent(pattern -> selectionMatcherBuilder.add(new BasicMatcher(c -> c.getUserGroups().stream().anyMatch(g -> pattern.matcher(g).matches()))));
        queryType.ifPresent(type -> selectionMatcherBuilder.add(new BasicMatcher(c -> type.equalsIgnoreCase(c.getQueryType().orElse("")))));
        selectorResourceEstimate.ifPresent(estimate -> selectionMatcherBuilder.add(new BasicMatcher(c -> estimate.match(c.getResourceEstimates()))));
        clientTags.ifPresent(tags -> selectionMatcherBuilder.add(new BasicMatcher(c -> c.getTags().containsAll(tags))));

        this.selectionMatchers = selectionMatcherBuilder.build();
    }

    @Override
    public Optional<SelectionContext<ResourceGroupIdTemplate>> match(SelectionCriteria criteria)
    {
        if (!selectionMatchers.stream().allMatch(m -> m.matches(criteria))) {
            return Optional.empty();
        }
        Map<String, String> variables = new HashMap<>();
        for (SelectionMatcher matcher : selectionMatchers) {
            matcher.populateVariables(criteria, variables);
        }

        variables.putIfAbsent(USER_VARIABLE, criteria.getUser());

        // Special handling for source, which is an optional field that is part of the standard variables
        variables.putIfAbsent(SOURCE_VARIABLE, criteria.getSource().orElse(""));

        ResourceGroupId id = group.expandTemplate(new VariableMap(variables));
        return Optional.of(new SelectionContext<>(id, group));
    }

    private static void addNamedGroups(Pattern pattern, HashSet<String> variables)
    {
        Matcher matcher = NAMED_GROUPS_PATTERN.matcher(pattern.toString());
        while (matcher.find()) {
            String name = matcher.group(1);
            checkArgument(!variables.contains(name), "Multiple definitions found for variable ${" + name + "}");
            variables.add(name);
        }
    }

    private interface SelectionMatcher
    {
        boolean matches(SelectionCriteria criteria);

        void populateVariables(SelectionCriteria criteria, Map<String, String> variables);
    }

    private record BasicMatcher(Predicate<SelectionCriteria> matchFn)
            implements SelectionMatcher
    {
        @Override
        public boolean matches(SelectionCriteria criteria)
        {
            return matchFn.test(criteria);
        }

        @Override
        public void populateVariables(SelectionCriteria criteria, Map<String, String> variables)
        {
            // no-op
        }
    }

    private static class PatternMatcher
            implements SelectionMatcher
    {
        private final Set<String> variableNames;
        private final Pattern pattern;
        private final Function<SelectionCriteria, String> valueExtractor;

        PatternMatcher(Set<String> variableNames, Pattern pattern, Function<SelectionCriteria, String> valueExtractor)
        {
            this.variableNames = variableNames;
            this.pattern = pattern;
            this.valueExtractor = valueExtractor;
        }

        @Override
        public boolean matches(SelectionCriteria criteria)
        {
            return pattern.matcher(valueExtractor.apply(criteria)).matches();
        }

        @Override
        public void populateVariables(SelectionCriteria criteria, Map<String, String> variables)
        {
            Matcher matcher = pattern.matcher(valueExtractor.apply(criteria));
            if (!matcher.matches()) {
                return;
            }
            Map<String, Integer> namedGroups = matcher.namedGroups();
            for (String key : variableNames) {
                if (namedGroups.containsKey(key)) {
                    String value = matcher.group(namedGroups.get(key));
                    if (value != null) {
                        variables.put(key, value);
                    }
                }
            }
        }
    }

    private static class OptionalPatternMatcher
            extends PatternMatcher
    {
        private final Function<SelectionCriteria, Optional<String>> valueExtractor;

        OptionalPatternMatcher(Set<String> variableNames, Pattern pattern, Function<SelectionCriteria, Optional<String>> valueExtractor)
        {
            super(variableNames, pattern, c -> valueExtractor.apply(c).get());
            this.valueExtractor = valueExtractor;
        }

        @Override
        public boolean matches(SelectionCriteria criteria)
        {
            return valueExtractor.apply(criteria).isPresent() && super.matches(criteria);
        }

        @Override
        public void populateVariables(SelectionCriteria criteria, Map<String, String> variables)
        {
            if (valueExtractor.apply(criteria).isPresent()) {
                super.populateVariables(criteria, variables);
            }
        }
    }

    @VisibleForTesting
    public Optional<Pattern> getUserRegex()
    {
        return userRegex;
    }
}
