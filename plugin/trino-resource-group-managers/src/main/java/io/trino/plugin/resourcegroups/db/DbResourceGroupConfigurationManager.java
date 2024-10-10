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
package io.trino.plugin.resourcegroups.db;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.Duration;
import io.trino.plugin.resourcegroups.AbstractResourceConfigurationManager;
import io.trino.plugin.resourcegroups.ManagerSpec;
import io.trino.plugin.resourcegroups.ResourceGroupIdTemplate;
import io.trino.plugin.resourcegroups.ResourceGroupSelector;
import io.trino.plugin.resourcegroups.ResourceGroupSpec;
import io.trino.plugin.resourcegroups.SelectorSpec;
import io.trino.spi.TrinoException;
import io.trino.spi.memory.ClusterMemoryPoolManager;
import io.trino.spi.resourcegroups.ResourceGroup;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.resourcegroups.SelectionContext;
import io.trino.spi.resourcegroups.SelectionCriteria;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.Duration.succinctNanos;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_UNAVAILABLE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class DbResourceGroupConfigurationManager
        extends AbstractResourceConfigurationManager
{
    private static final Logger log = Logger.get(DbResourceGroupConfigurationManager.class);

    private final Optional<LifeCycleManager> lifeCycleManager;
    private final ResourceGroupsDao dao;
    private final Map<ResourceGroupId, ResourceGroupSpec> specsUsedToConfigureGroups = new ConcurrentHashMap<>();
    private final ResourceGroupToTemplateMap configuredGroups = new ResourceGroupToTemplateMap();
    private final AtomicReference<List<ResourceGroupSpec>> rootGroups = new AtomicReference<>(ImmutableList.of());
    private final AtomicReference<List<ResourceGroupSelector>> selectors = new AtomicReference<>();
    private final AtomicReference<Optional<Duration>> cpuQuotaPeriod = new AtomicReference<>(Optional.empty());
    private final ScheduledExecutorService configExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("DbResourceGroupConfigurationManager"));
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicLong lastRefresh = new AtomicLong();
    private final String environment;
    private final Duration maxRefreshInterval;
    private final Duration refreshInterval;
    private final boolean exactMatchSelectorEnabled;

    private final CounterStat refreshFailures = new CounterStat();

    @Inject
    public DbResourceGroupConfigurationManager(
            LifeCycleManager lifeCycleManager,
            ClusterMemoryPoolManager memoryPoolManager,
            DbResourceGroupConfig config,
            ResourceGroupsDao dao,
            @ForEnvironment String environment)
    {
        this(
                Optional.of(lifeCycleManager),
                memoryPoolManager,
                config,
                dao,
                environment);
    }

    @VisibleForTesting
    DbResourceGroupConfigurationManager(
            ClusterMemoryPoolManager memoryPoolManager,
            DbResourceGroupConfig config,
            ResourceGroupsDao dao,
            String environment)
    {
        this(
                Optional.empty(),
                memoryPoolManager,
                config,
                dao,
                environment);
    }

    private DbResourceGroupConfigurationManager(
            Optional<LifeCycleManager> lifeCycleManager,
            ClusterMemoryPoolManager memoryPoolManager,
            DbResourceGroupConfig config,
            ResourceGroupsDao dao,
            String environment)
    {
        super(memoryPoolManager);
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        requireNonNull(dao, "daoProvider is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.maxRefreshInterval = config.getMaxRefreshInterval();
        this.refreshInterval = config.getRefreshInterval();
        this.exactMatchSelectorEnabled = config.getExactMatchSelectorEnabled();
        this.dao = dao;
        load();
    }

    @Override
    protected Optional<Duration> getCpuQuotaPeriod()
    {
        return cpuQuotaPeriod.get();
    }

    @Override
    protected List<ResourceGroupSpec> getRootGroups()
    {
        if (lastRefresh.get() == 0) {
            throw new TrinoException(CONFIGURATION_UNAVAILABLE, "Root groups cannot be fetched from database");
        }
        if (this.selectors.get().isEmpty()) {
            throw new TrinoException(CONFIGURATION_INVALID, "No root groups are configured");
        }

        return rootGroups.get();
    }

    @Override
    protected void configureGroup(ResourceGroup group, ResourceGroupSpec groupSpec)
    {
        super.configureGroup(group, groupSpec);
        specsUsedToConfigureGroups.put(group.getId(), groupSpec);
    }

    @PreDestroy
    public void destroy()
    {
        configExecutor.shutdownNow();
    }

    @PostConstruct
    public void start()
    {
        if (started.compareAndSet(false, true)) {
            configExecutor.scheduleWithFixedDelay(this::load, 1000, refreshInterval.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void configure(ResourceGroup group, SelectionContext<ResourceGroupIdTemplate> criteria)
    {
        ResourceGroupSpec groupSpec = getMatchingSpec(group, criteria);
        configuredGroups.put(criteria.getContext(), group);
        synchronized (getRootGroup(group.getId())) {
            configureGroup(group, groupSpec);
        }
    }

    @Override
    public Optional<SelectionContext<ResourceGroupIdTemplate>> match(SelectionCriteria criteria)
    {
        if (lastRefresh.get() == 0) {
            throw new TrinoException(CONFIGURATION_UNAVAILABLE, "Selectors cannot be fetched from database");
        }
        if (selectors.get().isEmpty()) {
            throw new TrinoException(CONFIGURATION_INVALID, "No selectors are configured");
        }

        return selectors.get().stream()
                .map(s -> s.match(criteria))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    @VisibleForTesting
    public List<ResourceGroupSelector> getSelectors()
    {
        if (lastRefresh.get() == 0) {
            throw new TrinoException(CONFIGURATION_UNAVAILABLE, "Selectors cannot be fetched from database");
        }
        if (selectors.get().isEmpty()) {
            throw new TrinoException(CONFIGURATION_INVALID, "No selectors are configured");
        }
        return selectors.get();
    }

    private synchronized Optional<Duration> getCpuQuotaPeriodFromDb()
    {
        List<ResourceGroupGlobalProperties> globalProperties = dao.getResourceGroupGlobalProperties();
        checkState(globalProperties.size() <= 1, "There is more than one cpu_quota_period");
        return !globalProperties.isEmpty() ? globalProperties.get(0).getCpuQuotaPeriod() : Optional.empty();
    }

    @VisibleForTesting
    public synchronized void load()
    {
        try {
            Map.Entry<ManagerSpec, Map<ResourceGroupIdTemplate, ResourceGroupSpec>> specsFromDb = buildSpecsFromDb();
            ManagerSpec managerSpec = specsFromDb.getKey();
            Map<ResourceGroupIdTemplate, ResourceGroupSpec> newResourceGroupSpecs = specsFromDb.getValue();
            Map<ResourceGroupIdTemplate, Set<ResourceGroup>> templateToGroup = configuredGroups.getAllTemplateToGroupsMappings();
            Map<ResourceGroup, ResourceGroupSpec> changedGroups = findChangedGroups(templateToGroup, newResourceGroupSpecs);
            Set<ResourceGroup> deletedGroups = findDeletedGroups(templateToGroup, newResourceGroupSpecs);

            this.cpuQuotaPeriod.set(managerSpec.getCpuQuotaPeriod());
            this.rootGroups.set(managerSpec.getRootGroups());
            List<ResourceGroupSelector> selectors = buildSelectors(managerSpec);
            if (exactMatchSelectorEnabled) {
                ImmutableList.Builder<ResourceGroupSelector> builder = ImmutableList.builder();
                builder.add(new DbSourceExactMatchSelector(environment, dao));
                builder.addAll(selectors);
                this.selectors.set(builder.build());
            }
            else {
                this.selectors.set(selectors);
            }

            configureChangedGroups(changedGroups);
            disableDeletedGroups(deletedGroups);

            if (lastRefresh.get() > 0) {
                for (ResourceGroup deleted : deletedGroups) {
                    log.info("Resource group deleted '%s'", deleted.getId());
                }
                for (Map.Entry<ResourceGroup, ResourceGroupSpec> entry : changedGroups.entrySet()) {
                    log.info("Resource group '%s' changed to %s", entry.getKey().getId(), entry.getValue());
                }
            }
            else {
                log.info("Loaded %s selectors and %s resource groups from database", this.selectors.get().size(), this.specsUsedToConfigureGroups.size());
            }

            lastRefresh.set(System.nanoTime());
        }
        catch (Throwable e) {
            if (succinctNanos(System.nanoTime() - lastRefresh.get()).compareTo(maxRefreshInterval) > 0) {
                lastRefresh.set(0);
            }
            refreshFailures.update(1);
            log.error(e, "Error loading configuration from db");
        }
    }

    private Map<ResourceGroup, ResourceGroupSpec> findChangedGroups(
            Map<ResourceGroupIdTemplate, Set<ResourceGroup>> templateToGroups,
            Map<ResourceGroupIdTemplate, ResourceGroupSpec> newResourceGroupSpecs)
    {
        ImmutableMap.Builder<ResourceGroup, ResourceGroupSpec> changedGroups = ImmutableMap.builder();
        for (Map.Entry<ResourceGroupIdTemplate, Set<ResourceGroup>> entry : templateToGroups.entrySet()) {
            ResourceGroupSpec newSpec = newResourceGroupSpecs.get(entry.getKey());
            if (newSpec != null) {
                Set<ResourceGroup> changedGroupsForCurrentTemplate = entry.getValue().stream()
                        .filter(resourceGroupId -> {
                            ResourceGroupSpec previousSpec = specsUsedToConfigureGroups.get(resourceGroupId.getId());
                            return previousSpec == null || !previousSpec.sameConfig(newSpec);
                        })
                        .collect(toImmutableSet());
                for (ResourceGroup group : changedGroupsForCurrentTemplate) {
                    changedGroups.put(group, newSpec);
                }
            }
        }
        return changedGroups.buildOrThrow();
    }

    private Set<ResourceGroup> findDeletedGroups(
            Map<ResourceGroupIdTemplate, Set<ResourceGroup>> templateToGroups,
            Map<ResourceGroupIdTemplate, ResourceGroupSpec> newResourceGroupSpecs)
    {
        return templateToGroups.entrySet().stream()
                .filter(entry -> !newResourceGroupSpecs.containsKey(entry.getKey()))
                .flatMap(entry -> entry.getValue().stream())
                .filter(resourceGroup -> !resourceGroup.isDisabled())
                .collect(toImmutableSet());
    }

    // Populate temporary data structures to build resource group specs and selectors from db
    private synchronized void populateFromDbHelper(Map<Long, ResourceGroupSpecBuilder> recordMap,
            Set<Long> rootGroupIds,
            Map<Long, ResourceGroupIdTemplate> resourceGroupIdTemplateMap,
            Map<Long, Set<Long>> subGroupIdsToBuild)
    {
        List<ResourceGroupSpecBuilder> records = dao.getResourceGroups(environment);
        for (ResourceGroupSpecBuilder record : records) {
            recordMap.put(record.getId(), record);
            if (record.getParentId().isEmpty()) {
                rootGroupIds.add(record.getId());
                resourceGroupIdTemplateMap.put(record.getId(), new ResourceGroupIdTemplate(record.getNameTemplate().toString()));
            }
            else {
                subGroupIdsToBuild.computeIfAbsent(record.getParentId().get(), k -> new HashSet<>()).add(record.getId());
            }
        }
    }

    private synchronized Map.Entry<ManagerSpec, Map<ResourceGroupIdTemplate, ResourceGroupSpec>> buildSpecsFromDb()
    {
        // New resource group spec map
        Map<ResourceGroupIdTemplate, ResourceGroupSpec> resourceGroupSpecs = new HashMap<>();
        // Set of root group db ids
        Set<Long> rootGroupIds = new HashSet<>();
        // Map of id from db to resource group spec
        Map<Long, ResourceGroupSpec> resourceGroupSpecMap = new HashMap<>();
        // Map of id from db to resource group template id
        Map<Long, ResourceGroupIdTemplate> resourceGroupIdTemplateMap = new HashMap<>();
        // Map of id from db to resource group spec builder
        Map<Long, ResourceGroupSpecBuilder> recordMap = new HashMap<>();
        // Map of subgroup id's not yet built
        Map<Long, Set<Long>> subGroupIdsToBuild = new HashMap<>();
        populateFromDbHelper(recordMap, rootGroupIds, resourceGroupIdTemplateMap, subGroupIdsToBuild);
        // Build up resource group specs from leaf to root
        for (LinkedList<Long> queue = new LinkedList<>(rootGroupIds); !queue.isEmpty(); ) {
            Long id = queue.pollFirst();
            resourceGroupIdTemplateMap.computeIfAbsent(id, k -> {
                ResourceGroupSpecBuilder builder = recordMap.get(id);
                return ResourceGroupIdTemplate.forSubGroupNamed(
                        resourceGroupIdTemplateMap.get(builder.getParentId().get()),
                        builder.getNameTemplate().toString());
            });
            Set<Long> childrenToBuild = subGroupIdsToBuild.getOrDefault(id, ImmutableSet.of());
            // Add to resource group specs if no more child resource groups are left to build
            if (childrenToBuild.isEmpty()) {
                ResourceGroupSpecBuilder builder = recordMap.get(id);
                ResourceGroupSpec resourceGroupSpec = builder.build();
                resourceGroupSpecMap.put(id, resourceGroupSpec);
                // Add newly built spec to spec map
                resourceGroupSpecs.put(resourceGroupIdTemplateMap.get(id), resourceGroupSpec);
                // Add this resource group spec to parent subgroups and remove id from subgroup ids to build
                builder.getParentId().ifPresent(parentId -> {
                    recordMap.get(parentId).addSubGroup(resourceGroupSpec);
                    subGroupIdsToBuild.get(parentId).remove(id);
                });
            }
            else {
                // Add this group back to queue since it still has subgroups to build
                queue.addFirst(id);
                // Add this group's subgroups to the queue so that when this id is dequeued again childrenToBuild will be empty
                queue.addAll(0, childrenToBuild);
            }
        }

        // Specs are built from db records, validate and return manager spec
        List<ResourceGroupSpec> rootGroups = rootGroupIds.stream().map(resourceGroupSpecMap::get).collect(Collectors.toList());

        List<SelectorSpec> selectors = dao.getSelectors(environment)
                .stream()
                .map(selectorRecord ->
                        new SelectorSpec(
                                selectorRecord.getUserRegex(),
                                selectorRecord.getUserGroupRegex(),
                                selectorRecord.getSourceRegex(),
                                selectorRecord.getQueryType(),
                                selectorRecord.getClientTags(),
                                selectorRecord.getSelectorResourceEstimate(),
                                resourceGroupIdTemplateMap.get(selectorRecord.getResourceGroupId()))
                ).collect(Collectors.toList());
        ManagerSpec managerSpec = new ManagerSpec(rootGroups, selectors, getCpuQuotaPeriodFromDb());
        validateRootGroups(managerSpec);
        return new AbstractMap.SimpleImmutableEntry<>(managerSpec, resourceGroupSpecs);
    }

    private synchronized void configureChangedGroups(Map<ResourceGroup, ResourceGroupSpec> changedGroups)
    {
        for (Map.Entry<ResourceGroup, ResourceGroupSpec> entry : changedGroups.entrySet()) {
            ResourceGroup group = entry.getKey();
            ResourceGroupSpec groupSpec = entry.getValue();
            synchronized (getRootGroup(group.getId())) {
                configureGroup(group, groupSpec);
            }
        }
    }

    private synchronized void disableDeletedGroups(Set<ResourceGroup> deletedGroups)
    {
        for (ResourceGroup group : deletedGroups) {
            group.setDisabled(true);
        }
    }

    private ResourceGroup getRootGroup(ResourceGroupId groupId)
    {
        Optional<ResourceGroupId> parent = groupId.getParent();
        while (parent.isPresent()) {
            groupId = parent.get();
            parent = groupId.getParent();
        }
        // GroupId is guaranteed to be in groups: it is added before the first call to this method in configure()
        return configuredGroups.get(groupId);
    }

    @Managed
    @Nested
    public CounterStat getRefreshFailures()
    {
        return refreshFailures;
    }

    @Override
    public void shutdown()
    {
        lifeCycleManager.ifPresent(LifeCycleManager::stop);
    }

    /**
     * Stores mappings between group ID templates and groups expanded from them.
     * A group, throughout its lifecycle, can be expanded from different templates.
     * For example, 'admin' can be expanded from 'admin' and '${USER}'.
     * This data structure stores the most recent mapping for a group registered by
     * {@link ResourceGroupToTemplateMap#put(ResourceGroupIdTemplate, ResourceGroup)}.
     */
    private static class ResourceGroupToTemplateMap
    {
        private final Map<ResourceGroupId, ResourceGroup> groups = new ConcurrentHashMap<>();
        private final Map<ResourceGroupId, ResourceGroupIdTemplate> groupIdToTemplate = new HashMap<>();
        private final Map<ResourceGroupIdTemplate, Set<ResourceGroup>> templateToGroups = new HashMap<>();

        /**
         * Registers a mapping between a template and a group.
         * If a mapping for the group already exists, it is replaced by the new one.
         */
        synchronized void put(ResourceGroupIdTemplate newTemplate, ResourceGroup group)
        {
            ResourceGroup previousGroup = groups.putIfAbsent(group.getId(), group);
            checkState(previousGroup == null || previousGroup == group, "Unexpected resource group instance");

            ResourceGroupIdTemplate previousTemplate = groupIdToTemplate.put(group.getId(), newTemplate);
            if (previousTemplate != null) {
                templateToGroups.get(previousTemplate)
                        .remove(group);
            }
            templateToGroups.computeIfAbsent(newTemplate, _ -> new HashSet<>())
                    .add(group);
        }

        ResourceGroup get(ResourceGroupId groupId)
        {
            return groups.get(groupId);
        }

        synchronized Map<ResourceGroupIdTemplate, Set<ResourceGroup>> getAllTemplateToGroupsMappings()
        {
            return templateToGroups.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableSet.copyOf(entry.getValue())));
        }
    }
}
