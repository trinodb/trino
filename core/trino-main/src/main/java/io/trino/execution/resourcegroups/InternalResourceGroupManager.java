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
package io.trino.execution.resourcegroups;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.trino.execution.ManagedQueryExecution;
import io.trino.memory.ClusterMemoryManager;
import io.trino.server.ResourceGroupInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.resourcegroups.ResourceGroupConfigurationManager;
import io.trino.spi.resourcegroups.ResourceGroupConfigurationManagerContext;
import io.trino.spi.resourcegroups.ResourceGroupConfigurationManagerFactory;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.resourcegroups.SelectionContext;
import io.trino.spi.resourcegroups.SelectionCriteria;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public final class InternalResourceGroupManager<C>
        implements ResourceGroupManager<C>
{
    private static final Logger log = Logger.get(InternalResourceGroupManager.class);

    private static final File CONFIG_FILE = new File("etc/resource-groups.properties");
    private static final String NAME_PROPERTY = "resource-groups.configuration-manager";

    private final ScheduledExecutorService refreshExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("ResourceGroupManager"));
    private final List<InternalResourceGroup> rootGroups = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<ResourceGroupId, InternalResourceGroup> groups = new ConcurrentHashMap<>();
    private final AtomicReference<ResourceGroupConfigurationManager<C>> configurationManager;
    private final ResourceGroupConfigurationManagerContext configurationManagerContext;
    private final ResourceGroupConfigurationManager<?> legacyManager;
    private final MBeanExporter exporter;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicLong lastCpuQuotaGenerationNanos = new AtomicLong(System.nanoTime());
    private final Map<String, ResourceGroupConfigurationManagerFactory> configurationManagerFactories = new ConcurrentHashMap<>();
    private final SecretsResolver secretsResolver;

    @Inject
    public InternalResourceGroupManager(
            LegacyResourceGroupConfigurationManager legacyManager,
            ClusterMemoryManager memoryPoolManager,
            NodeInfo nodeInfo,
            MBeanExporter exporter,
            SecretsResolver secretsResolver)
    {
        this.exporter = requireNonNull(exporter, "exporter is null");
        this.configurationManagerContext = new ResourceGroupConfigurationManagerContextInstance(memoryPoolManager::addChangeListener, nodeInfo.getEnvironment());
        this.legacyManager = requireNonNull(legacyManager, "legacyManager is null");
        this.configurationManager = new AtomicReference<>(cast(legacyManager));
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
    }

    @Override
    public Optional<ResourceGroupInfo> tryGetResourceGroupInfo(ResourceGroupId id)
    {
        InternalResourceGroup resourceGroup = groups.get(id);
        return Optional.ofNullable(resourceGroup)
                .map(InternalResourceGroup::getFullInfo);
    }

    @Override
    public Optional<List<ResourceGroupInfo>> tryGetPathToRoot(ResourceGroupId id)
    {
        InternalResourceGroup resourceGroup = groups.get(id);
        return Optional.ofNullable(resourceGroup)
                .map(InternalResourceGroup::getPathToRoot);
    }

    @Override
    public void submit(ManagedQueryExecution queryExecution, SelectionContext<C> selectionContext, Executor executor)
    {
        checkState(configurationManager.get() != null, "configurationManager not set");
        createGroupIfNecessary(selectionContext, executor);
        groups.get(selectionContext.getResourceGroupId()).run(queryExecution);
    }

    @Override
    public SelectionContext<C> selectGroup(SelectionCriteria criteria)
    {
        return configurationManager.get().match(criteria)
                .orElseThrow(() -> new TrinoException(QUERY_REJECTED, "No matching resource group found with the configured selection rules"));
    }

    @Override
    public void addConfigurationManagerFactory(ResourceGroupConfigurationManagerFactory factory)
    {
        if (configurationManagerFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("Resource group configuration manager '%s' is already registered", factory.getName()));
        }
    }

    @Override
    public void loadConfigurationManager()
            throws Exception
    {
        File configFile = CONFIG_FILE.getAbsoluteFile();
        if (!configFile.exists()) {
            return;
        }

        Map<String, String> properties = new HashMap<>(loadPropertiesFrom(configFile.getPath()));

        String name = properties.remove(NAME_PROPERTY);
        checkState(!isNullOrEmpty(name), "Resource groups configuration %s does not contain '%s'", configFile, NAME_PROPERTY);

        setConfigurationManager(name, properties);
    }

    @VisibleForTesting
    public void setConfigurationManager(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading resource group configuration manager --");

        ResourceGroupConfigurationManagerFactory factory = configurationManagerFactories.get(name);
        checkState(factory != null, "Resource group configuration manager '%s' is not registered", name);

        ResourceGroupConfigurationManager<C> configurationManager;
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            configurationManager = cast(factory.create(ImmutableMap.copyOf(secretsResolver.getResolvedConfiguration(properties)), configurationManagerContext));
        }

        checkState(this.configurationManager.compareAndSet(cast(legacyManager), configurationManager), "configurationManager already set");

        log.info("-- Loaded resource group configuration manager %s --", name);
    }

    @SuppressWarnings("ObjectEquality")
    @VisibleForTesting
    public ResourceGroupConfigurationManager<C> getConfigurationManager()
    {
        ResourceGroupConfigurationManager<C> manager = configurationManager.get();
        checkState(manager != legacyManager, "cannot fetch legacy manager");
        return manager;
    }

    @PreDestroy
    public void destroy()
    {
        configurationManager.get().shutdown();
        refreshExecutor.shutdownNow();
    }

    @PostConstruct
    public void start()
    {
        if (started.compareAndSet(false, true)) {
            refreshExecutor.scheduleWithFixedDelay(this::refreshAndStartQueries, 1, 100, TimeUnit.MILLISECONDS);
        }
    }

    private void refreshAndStartQueries()
    {
        long nanoTime = System.nanoTime();
        long elapsedSeconds = NANOSECONDS.toSeconds(nanoTime - lastCpuQuotaGenerationNanos.get());
        if (elapsedSeconds > 0) {
            // Only advance our clock on second boundaries to avoid calling generateCpuQuota() too frequently, and because it would be a no-op for zero seconds.
            lastCpuQuotaGenerationNanos.addAndGet(elapsedSeconds * 1_000_000_000L);
        }
        else if (elapsedSeconds < 0) {
            // nano time has overflowed
            lastCpuQuotaGenerationNanos.set(nanoTime);
        }
        for (InternalResourceGroup group : rootGroups) {
            try {
                if (elapsedSeconds > 0) {
                    group.generateCpuQuota(elapsedSeconds);
                }
            }
            catch (RuntimeException e) {
                log.error(e, "Exception while generation cpu quota for %s", group);
            }
            try {
                group.updateGroupsAndProcessQueuedQueries();
            }
            catch (RuntimeException e) {
                log.error(e, "Exception while processing queued queries for %s", group);
            }
        }
    }

    private synchronized void createGroupIfNecessary(SelectionContext<C> context, Executor executor)
    {
        ResourceGroupId id = context.getResourceGroupId();
        InternalResourceGroup currentGroup = groups.get(id);
        if (currentGroup == null) {
            InternalResourceGroup group;
            if (id.getParent().isPresent()) {
                createGroupIfNecessary(configurationManager.get().parentGroupContext(context), executor);
                InternalResourceGroup parent = groups.get(id.getParent().get());
                requireNonNull(parent, "parent is null");
                group = parent.getOrCreateSubGroup(id.getLastSegment());
            }
            else {
                InternalResourceGroup root = new InternalResourceGroup(id.getSegments().get(0), this::exportGroup, executor);
                group = root;
                rootGroups.add(root);
            }
            configurationManager.get().configure(group, context);
            checkState(groups.put(id, group) == null, "Unexpected existing resource group");
        }
        else if (currentGroup.isDisabled()) {
            if (id.getParent().isPresent()) {
                createGroupIfNecessary(configurationManager.get().parentGroupContext(context), executor);
                InternalResourceGroup parent = groups.get(id.getParent().get());
                requireNonNull(parent, "parent is null");
                InternalResourceGroup group = parent.getOrCreateSubGroup(id.getLastSegment());
                checkState(group == currentGroup, "Unexpected resource group instance");
            }
            configurationManager.get().configure(currentGroup, context);
            currentGroup.setDisabled(false);
        }
    }

    private void exportGroup(InternalResourceGroup group, Boolean export)
    {
        try {
            if (export) {
                exporter.exportWithGeneratedName(group, InternalResourceGroup.class, group.getId().toString());
            }
            else {
                exporter.unexportWithGeneratedName(InternalResourceGroup.class, group.getId().toString());
            }
        }
        catch (JmxException e) {
            log.error(e, "Error %s resource group %s", export ? "exporting" : "unexporting", group.getId());
        }
    }

    @Managed
    public int getQueriesQueuedOnInternal()
    {
        int queriesQueuedInternal = 0;
        for (InternalResourceGroup rootGroup : rootGroups) {
            queriesQueuedInternal += rootGroup.getQueriesQueuedOnInternal();
        }
        return queriesQueuedInternal;
    }

    @SuppressWarnings("unchecked")
    private static <C> ResourceGroupConfigurationManager<C> cast(ResourceGroupConfigurationManager<?> manager)
    {
        return (ResourceGroupConfigurationManager<C>) manager;
    }
}
