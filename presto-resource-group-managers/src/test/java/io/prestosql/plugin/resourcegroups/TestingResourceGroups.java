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
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public final class TestingResourceGroups
{
    private TestingResourceGroups()
    {
    }

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

    static class SelectorSpecBuilder
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

        public SelectorSpecBuilder userGroups(String first, String... later)
        {
            Set<String> groups = ImmutableSet.<String>builder()
                    .add(first)
                    .addAll(Arrays.asList(later))
                    .build();
            return new SelectorSpecBuilder(
                    new SelectorSpec(
                            spec.getUserRegex(),
                            Optional.of(Pattern.compile(String.join("|", groups.stream().map(Pattern::quote).collect(Collectors.toList())))),
                            spec.getSourceRegex(),
                            spec.getQueryType(),
                            spec.getClientTags(),
                            spec.getResourceEstimate(),
                            spec.getGroup()));
        }

        public SelectorSpecBuilder users(String first, String... later)
        {
            Set<String> users = ImmutableSet.<String>builder()
                    .add(first)
                    .addAll(Arrays.asList(later))
                    .build();
            return new SelectorSpecBuilder(
                    new SelectorSpec(
                            Optional.of(Pattern.compile(String.join("|", users.stream().map(Pattern::quote).collect(Collectors.toList())))),
                            spec.getUserGroupRegex(),
                            spec.getSourceRegex(),
                            spec.getQueryType(),
                            spec.getClientTags(),
                            spec.getResourceEstimate(),
                            spec.getGroup()));
        }
    }
}
