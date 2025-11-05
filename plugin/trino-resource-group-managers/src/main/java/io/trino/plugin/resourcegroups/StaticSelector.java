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

        HashSet<String> variableNames = new HashSet<>(ImmutableList.of(USER_VARIABLE, SOURCE_VARIABLE));
        this.selectionMatchers = ImmutableList.<Optional<SelectionMatcher>>builder()
                .add(userRegex.map(userRegexValue -> {
                    addNamedGroups(userRegexValue, variableNames);
                    return new PatternMatcher(variableNames, userRegexValue, SelectionCriteria::getUser);
                }))
                .add(originalUserRegex.map(originalUserRegexValue -> {
                    addNamedGroups(originalUserRegexValue, variableNames);
                    return new PatternMatcher(variableNames, originalUserRegexValue, SelectionCriteria::getOriginalUser);
                }))
                .add(authenticatedUserRegex.map(authenticatedUserRegexValue -> {
                    addNamedGroups(authenticatedUserRegexValue, variableNames);
                    return new PatternMatcher(variableNames, authenticatedUserRegexValue, criteria -> criteria.getAuthenticatedUser().orElse(""));
                }))
                .add(sourceRegex.map(sourceRegexValue -> {
                    addNamedGroups(sourceRegexValue, variableNames);
                    return new PatternMatcher(variableNames, sourceRegexValue, criteria -> criteria.getSource().orElse(""));
                }))
                .add(userGroupRegex.map(userGroupRegexValue ->
                            new BasicMatcher(criteria -> criteria.getUserGroups().stream().anyMatch(userGroup -> userGroupRegexValue.matcher(userGroup).matches()))))
                .add(queryType.map(queryTypeValue ->
                            new BasicMatcher(criteria -> queryTypeValue.equalsIgnoreCase(criteria.getQueryType().orElse("")))))
                .add(selectorResourceEstimate.map(selectorResourceEstimateValue ->
                            new BasicMatcher(criteria -> selectorResourceEstimateValue.match(criteria.getResourceEstimates()))))
                .add(clientTags.map(clientTagsValue ->
                            new BasicMatcher(criteria -> criteria.getTags().containsAll(clientTagsValue))))
                .build()
                .stream()
                .flatMap(Optional::stream) // remove any empty optionals
                .toList();

        Set<String> unresolvedVariables = Sets.difference(group.getVariableNames(), variableNames);
        checkArgument(unresolvedVariables.isEmpty(), "unresolved variables %s in resource group ID '%s', available: %s\"", unresolvedVariables, group, variableNames);
    }

    @Override
    public Optional<SelectionContext<ResourceGroupIdTemplate>> match(SelectionCriteria criteria)
    {
        if (!selectionMatchers.stream().allMatch(matcher -> matcher.matches(criteria))) {
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

    private record BasicMatcher(Predicate<SelectionCriteria> matchPredicate)
            implements SelectionMatcher
    {
        @Override
        public boolean matches(SelectionCriteria criteria)
        {
            return matchPredicate.test(criteria);
        }

        @Override
        public void populateVariables(SelectionCriteria criteria, Map<String, String> variables)
        {
            // no-op
        }
    }

    private record PatternMatcher(Set<String> variableNames, Pattern pattern,
                                  Function<SelectionCriteria, String> valueExtractor)
            implements SelectionMatcher
    {
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

    @VisibleForTesting
    public Optional<Pattern> getUserRegex()
    {
        return userRegex;
    }
}
