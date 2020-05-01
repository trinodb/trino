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
package io.prestosql.plugin.resourcegroups;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

final class TestingResourceGroups
{
    private TestingResourceGroups() {}

    public static ManagerSpec managerSpec(List<ResourceGroupSpec> resourceGroupSpecs, List<SelectorSpecBuilder> selectors)
    {
        return new ManagerSpec(
                resourceGroupSpecs,
                selectors.stream()
                        .map(SelectorSpecBuilder::build)
                        .collect(toImmutableList()),
                Optional.empty());
    }

    public static ManagerSpec managerSpec(ResourceGroupSpec resourceGroupSpec, List<SelectorSpecBuilder> selectors)
    {
        return managerSpec(ImmutableList.of(resourceGroupSpec), selectors);
    }

    public static ResourceGroupSpec resourceGroupSpec(String segmentName)
    {
        return new ResourceGroupSpec(
                new ResourceGroupNameTemplate(segmentName),
                DataSize.of(100, MEGABYTE).toString(),
                10,
                Optional.empty(),
                Optional.of(10),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public static ResourceGroupIdTemplate groupIdTemplate(String groupIdTemplate)
    {
        return new ResourceGroupIdTemplate(groupIdTemplate);
    }

    public static SelectorSpecBuilder selectorSpec(ResourceGroupIdTemplate groupIdTemplate)
    {
        return new SelectorSpecBuilder(
                new SelectorSpec(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        groupIdTemplate));
    }

    public static class SelectorSpecBuilder
    {
        private final SelectorSpec spec;

        private SelectorSpecBuilder(SelectorSpec spec)
        {
            requireNonNull(spec, "spec is null");

            this.spec = spec;
        }

        public SelectorSpec build()
        {
            return spec;
        }

        public SelectorSpecBuilder users(String... users)
        {
            return new SelectorSpecBuilder(
                    new SelectorSpec(
                            Optional.of(matchLiterals(users)),
                            spec.getUserGroupRegex(),
                            spec.getSourceRegex(),
                            spec.getQueryType(),
                            spec.getClientTags(),
                            spec.getResourceEstimate(),
                            spec.getGroup()));
        }

        public SelectorSpecBuilder userGroups(String... groups)
        {
            return new SelectorSpecBuilder(
                    new SelectorSpec(
                            spec.getUserRegex(),
                            Optional.of(matchLiterals(groups)),
                            spec.getSourceRegex(),
                            spec.getQueryType(),
                            spec.getClientTags(),
                            spec.getResourceEstimate(),
                            spec.getGroup()));
        }
    }

    private static Pattern matchLiterals(String[] literals)
    {
        checkArgument(literals.length > 0, "literals is empty");
        return Pattern.compile(Stream.of(literals)
                .map(Pattern::quote)
                .collect(joining("|")));
    }
}
