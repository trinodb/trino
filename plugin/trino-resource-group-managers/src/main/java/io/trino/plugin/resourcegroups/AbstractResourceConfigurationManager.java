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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.memory.ClusterMemoryPoolManager;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroup;
import io.trino.spi.resourcegroups.ResourceGroupConfigurationManager;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.resourcegroups.SelectionContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verifyNotNull;
import static io.trino.spi.StandardErrorCode.INVALID_RESOURCE_GROUP;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.function.Predicate.isEqual;

public abstract class AbstractResourceConfigurationManager
        implements ResourceGroupConfigurationManager<ResourceGroupIdTemplate>
{
    @GuardedBy("memoryPoolFraction")
    private final Map<ResourceGroup, Double> memoryPoolFraction = new HashMap<>();
    @GuardedBy("memoryPoolFraction")
    private long memoryPoolBytes;

    protected abstract Optional<Duration> getCpuQuotaPeriod();

    protected abstract List<ResourceGroupSpec> getRootGroups();

    protected void validateRootGroups(ManagerSpec managerSpec)
    {
        Queue<ResourceGroupSpec> groups = new LinkedList<>(managerSpec.getRootGroups());
        while (!groups.isEmpty()) {
            ResourceGroupSpec group = groups.poll();
            List<ResourceGroupSpec> subGroups = group.getSubGroups();
            groups.addAll(subGroups);
            if (group.getSoftCpuLimit().isPresent() || group.getHardCpuLimit().isPresent()) {
                checkArgument(managerSpec.getCpuQuotaPeriod().isPresent(), "cpuQuotaPeriod must be specified to use CPU limits on group: %s", group.getName());
            }
            if (group.getSoftCpuLimit().isPresent()) {
                checkArgument(group.getHardCpuLimit().isPresent(), "Must specify hard CPU limit in addition to soft limit");
                checkArgument(group.getSoftCpuLimit().get().compareTo(group.getHardCpuLimit().get()) <= 0, "Soft CPU limit cannot be greater than hard CPU limit");
            }
            if (group.getSchedulingPolicy().isPresent()) {
                switch (group.getSchedulingPolicy().get()) {
                    case WEIGHTED:
                    case WEIGHTED_FAIR:
                        checkArgument(
                                subGroups.stream().allMatch(t -> t.getSchedulingWeight().isPresent()) || subGroups.stream().noneMatch(t -> t.getSchedulingWeight().isPresent()),
                                format("Must specify scheduling weight for all sub-groups of '%s' or none of them", group.getName()));
                        break;
                    case QUERY_PRIORITY:
                    case FAIR:
                        for (ResourceGroupSpec subGroup : subGroups) {
                            checkArgument(subGroup.getSchedulingWeight().isEmpty(), "Must use 'weighted' or 'weighted_fair' scheduling policy if specifying scheduling weight for '%s'", group.getName());
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
        }
    }

    protected List<ResourceGroupSelector> buildSelectors(ManagerSpec managerSpec)
    {
        ImmutableList.Builder<ResourceGroupSelector> selectors = ImmutableList.builder();
        for (SelectorSpec spec : managerSpec.getSelectors()) {
            validateSelectors(managerSpec.getRootGroups(), spec);
            selectors.add(new StaticSelector(
                    spec.getUserRegex(),
                    spec.getUserGroupRegex(),
                    spec.getSourceRegex(),
                    spec.getClientTags(),
                    spec.getResourceEstimate(),
                    spec.getQueryType(),
                    spec.getGroup()));
        }
        return selectors.build();
    }

    private void validateSelectors(List<ResourceGroupSpec> groups, SelectorSpec spec)
    {
        spec.getQueryType().ifPresent(this::validateQueryType);
        StringBuilder fullyQualifiedGroupName = new StringBuilder();
        for (ResourceGroupNameTemplate groupName : spec.getGroup().getSegments()) {
            fullyQualifiedGroupName.append(groupName);
            ResourceGroupSpec match = groups
                    .stream()
                    .filter(groupSpec -> groupSpec.getName().equals(groupName))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(format("Selector refers to nonexistent group: %s", fullyQualifiedGroupName)));
            fullyQualifiedGroupName.append(".");
            groups = match.getSubGroups();
        }
    }

    private void validateQueryType(String queryType)
    {
        try {
            QueryType.valueOf(queryType.toUpperCase(ENGLISH));
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(format("Selector specifies an invalid query type: %s", queryType));
        }
    }

    protected AbstractResourceConfigurationManager(ClusterMemoryPoolManager memoryPoolManager)
    {
        memoryPoolManager.addChangeListener(poolInfo -> {
            Map<ResourceGroup, DataSize> memoryLimits = new HashMap<>();
            synchronized (memoryPoolFraction) {
                for (Map.Entry<ResourceGroup, Double> entry : memoryPoolFraction.entrySet()) {
                    long bytes = Math.round(poolInfo.getMaxBytes() * entry.getValue());
                    // setSoftMemoryLimit() acquires a lock on the root group of its tree, which could cause a deadlock if done while holding the "memoryPoolFraction" lock
                    memoryLimits.put(entry.getKey(), DataSize.ofBytes(bytes));
                }
                memoryPoolBytes = poolInfo.getMaxBytes();
            }
            memoryLimits.forEach((group, limit) ->
                    group.setSoftMemoryLimitBytes(limit.toBytes()));
        });
    }

    @Override
    public SelectionContext<ResourceGroupIdTemplate> parentGroupContext(SelectionContext<ResourceGroupIdTemplate> context)
    {
        ResourceGroupId parentGroupId = context.getResourceGroupId().getParent().orElseThrow(() -> new IllegalArgumentException("Group has no parent group: " + context.getResourceGroupId()));
        List<ResourceGroupNameTemplate> parentGroupIdTemplate = new ArrayList<>(context.getContext().getSegments());
        parentGroupIdTemplate.remove(parentGroupIdTemplate.size() - 1);
        return new SelectionContext<>(parentGroupId, ResourceGroupIdTemplate.fromSegments(parentGroupIdTemplate));
    }

    protected ResourceGroupSpec getMatchingSpec(ResourceGroup group, SelectionContext<ResourceGroupIdTemplate> context)
    {
        List<ResourceGroupSpec> candidates = getRootGroups();
        ResourceGroupIdTemplate groupIdTemplate = context.getContext();
        ResourceGroupSpec match = null;

        for (ResourceGroupNameTemplate segment : groupIdTemplate.getSegments()) {
            match = null;
            for (ResourceGroupSpec candidate : candidates) {
                if (candidate.getName().equals(segment)) {
                    if (match != null) {
                        throw new TrinoException(INVALID_RESOURCE_GROUP, format(
                                "Ambiguous configuration for [%s] using [%s]. Matches [%s] and [%s]",
                                group.getId(),
                                groupIdTemplate,
                                match.getName(),
                                candidate.getName()));
                    }
                    match = candidate;
                }
            }

            checkState(match != null, "No matching configuration found for [%s] using [%s]", group.getId(), groupIdTemplate);
            candidates = match.getSubGroups();
        }

        verifyNotNull(match, "match is null");
        return match;
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    protected void configureGroup(ResourceGroup group, ResourceGroupSpec match)
    {
        if (match.getSoftMemoryLimit().isPresent()) {
            group.setSoftMemoryLimitBytes(match.getSoftMemoryLimit().get().toBytes());
        }
        else {
            synchronized (memoryPoolFraction) {
                double fraction = match.getSoftMemoryLimitFraction().get();
                memoryPoolFraction.put(group, fraction);
                group.setSoftMemoryLimitBytes((long) (memoryPoolBytes * fraction));
            }
        }
        group.setMaxQueuedQueries(match.getMaxQueued());
        group.setSoftConcurrencyLimit(match.getSoftConcurrencyLimit().orElse(match.getHardConcurrencyLimit()));
        group.setHardConcurrencyLimit(match.getHardConcurrencyLimit());
        match.getSchedulingPolicy().ifPresent(group::setSchedulingPolicy);
        match.getSchedulingWeight().ifPresent(group::setSchedulingWeight);
        match.getJmxExport().filter(isEqual(group.getJmxExport()).negate()).ifPresent(group::setJmxExport);
        match.getSoftCpuLimit().map(Duration::toMillis).map(java.time.Duration::ofMillis).ifPresent(group::setSoftCpuLimit);
        match.getHardCpuLimit().map(Duration::toMillis).map(java.time.Duration::ofMillis).ifPresent(group::setHardCpuLimit);
        if (match.getSoftCpuLimit().isPresent() || match.getHardCpuLimit().isPresent()) {
            // This will never throw an exception if the validateRootGroups method succeeds
            checkState(getCpuQuotaPeriod().isPresent(), "cpuQuotaPeriod must be specified to use CPU limits on group: %s", group.getId());
            Duration limit;
            if (match.getHardCpuLimit().isPresent()) {
                limit = match.getHardCpuLimit().get();
            }
            else {
                limit = match.getSoftCpuLimit().get();
            }
            long rate = (long) Math.min(1000.0 * limit.toMillis() / (double) getCpuQuotaPeriod().get().toMillis(), Long.MAX_VALUE);
            rate = Math.max(1, rate);
            group.setCpuQuotaGenerationMillisPerSecond(rate);
        }
    }
}
