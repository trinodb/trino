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
import com.google.common.collect.ImmutableSet;
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
    private final Optional<Pattern> userGroupRegex;
    private final Optional<Pattern> originalUserRegex;
    private final Optional<Pattern> authenticatedUserRegex;
    private final Optional<Pattern> sourceRegex;
    private final Set<String> clientTags;
    private final Optional<SelectorResourceEstimate> selectorResourceEstimate;
    private final Optional<String> queryType;
    private final ResourceGroupIdTemplate group;
    private final Set<String> variableNames;

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
        this.userGroupRegex = requireNonNull(userGroupRegex, "userGroupRegex is null");
        this.originalUserRegex = requireNonNull(originalUserRegex, "originalUserRegex is null");
        this.authenticatedUserRegex = requireNonNull(authenticatedUserRegex, "authenticatedUserRegex is null");
        this.sourceRegex = requireNonNull(sourceRegex, "sourceRegex is null");
        requireNonNull(clientTags, "clientTags is null");
        this.clientTags = ImmutableSet.copyOf(clientTags.orElse(ImmutableList.of()));
        this.selectorResourceEstimate = requireNonNull(selectorResourceEstimate, "selectorResourceEstimate is null");
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.group = requireNonNull(group, "group is null");

        HashSet<String> variableNames = new HashSet<>(ImmutableList.of(USER_VARIABLE, SOURCE_VARIABLE));
        userRegex.ifPresent(u -> addNamedGroups(u, variableNames));
        originalUserRegex.ifPresent(u -> addNamedGroups(u, variableNames));
        authenticatedUserRegex.ifPresent(u -> addNamedGroups(u, variableNames));
        sourceRegex.ifPresent(s -> addNamedGroups(s, variableNames));
        this.variableNames = ImmutableSet.copyOf(variableNames);

        Set<String> unresolvedVariables = Sets.difference(group.getVariableNames(), variableNames);
        checkArgument(unresolvedVariables.isEmpty(), "unresolved variables %s in resource group ID '%s', available: %s\"", unresolvedVariables, group, variableNames);
    }

    @Override
    public Optional<SelectionContext<ResourceGroupIdTemplate>> match(SelectionCriteria criteria)
    {
        Map<String, String> variables = new HashMap<>();

        if (!addVariablesForRegexIfMatching(userRegex, criteria.getUser(), variables)) {
            return Optional.empty();
        }

        if (userGroupRegex.isPresent() && criteria.getUserGroups().stream().noneMatch(group -> userGroupRegex.get().matcher(group).matches())) {
            return Optional.empty();
        }

        if (!addVariablesForRegexIfMatching(originalUserRegex, criteria.getOriginalUser(), variables)) {
            return Optional.empty();
        }

        if (!addVariablesForRegexIfMatching(authenticatedUserRegex, criteria.getAuthenticatedUser().orElse(""), variables)) {
            return Optional.empty();
        }

        if (!addVariablesForRegexIfMatching(sourceRegex, criteria.getSource().orElse(""), variables)) {
            return Optional.empty();
        }

        if (!clientTags.isEmpty() && !criteria.getTags().containsAll(clientTags)) {
            return Optional.empty();
        }

        if (selectorResourceEstimate.isPresent() && !selectorResourceEstimate.get().match(criteria.getResourceEstimates())) {
            return Optional.empty();
        }

        if (queryType.isPresent()) {
            String contextQueryType = criteria.getQueryType().orElse("");
            if (!queryType.get().equalsIgnoreCase(contextQueryType)) {
                return Optional.empty();
            }
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

    /**
     * @param optionalRegex The optional regex to match against the input.
     * @param input The input to match against the regex.
     * @param variables Variables to populate with the values from the regex.
     * @return False iff the regex is present and the input does not match it, else true,
     *         indicating matching should continue.
     */
    private boolean addVariablesForRegexIfMatching(Optional<Pattern> optionalRegex, String input, Map<String, String> variables)
    {
        if (optionalRegex.isEmpty()) {
            return true;
        }
        Pattern pattern = optionalRegex.get();
        Matcher matcher = pattern.matcher(input);
        if (!matcher.matches()) {
            return false;
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
        return true;
    }

    @VisibleForTesting
    public Optional<Pattern> getUserRegex()
    {
        return userRegex;
    }
}
