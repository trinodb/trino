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
package io.trino.plugin.varada;

import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.di.VaradaInitializedServiceRegistry;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.node.CoordinatorInitializedEvent;
import io.trino.plugin.varada.util.VaradaInitializedServiceMarker;
import io.trino.plugin.warp.gen.stats.VaradaStatsCachePredicates;
import io.trino.plugin.warp.gen.stats.VaradaStatsDictionary;
import io.trino.plugin.warp.gen.stats.VaradaStatsDispatcherPageSource;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmupDemoter;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmupExportService;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmupImportService;
import io.trino.plugin.warp.gen.stats.VaradaStatsWorkerTaskExecutorService;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.varada.dictionary.DictionaryCacheService.DICTIONARY_STAT_GROUP;
import static io.trino.plugin.varada.dispatcher.DispatcherPageSourceFactory.STATS_DISPATCHER_KEY;
import static io.trino.plugin.varada.dispatcher.warmup.WorkerTaskExecutorService.WORKER_TASK_EXECUTOR_STAT_GROUP;
import static io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService.WARMUP_DEMOTER_STAT_GROUP;
import static io.trino.plugin.varada.dispatcher.warmup.export.WarmupExportingService.WARMUP_EXPORTER_STAT_GROUP;
import static io.trino.plugin.varada.dispatcher.warmup.warmers.WeGroupWarmer.WARMUP_IMPORTER_STAT_GROUP;
import static io.trino.plugin.varada.juffer.PredicatesCacheService.STATS_CACHE_PREDICATE_KEY;
import static java.util.Objects.requireNonNull;

@Singleton
public class CoordinatorNodeManager
        implements VaradaInitializedServiceMarker
{
    private static final Logger logger = Logger.get(CoordinatorNodeManager.class);

    private final NodeManager nodeManager;
    private final GlobalConfiguration globalConfiguration;
    private final EventBus eventBus; // required for @Subscribe methods
    private final MetricsManager metricsManager;
    private boolean coordinatorInitialized; // no need to set, default is false

    private Node coordinatorNode;

    @Inject
    public CoordinatorNodeManager(NodeManager nodeManager,
            GlobalConfiguration globalConfiguration,
            EventBus eventBus,
            MetricsManager metricsManager,
            VaradaInitializedServiceRegistry varadaInitializedServiceRegistry)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.globalConfiguration = requireNonNull(globalConfiguration, "globalConfiguration is null");
        this.eventBus = requireNonNull(eventBus, "EventBus is null");
        this.metricsManager = requireNonNull(metricsManager);
        varadaInitializedServiceRegistry.addService(this);
    }

    @Override
    public void init()
    {
        logger.debug("coordinator node [%s] is initialising", getCoordinatorNode().getNodeIdentifier());

        coordinatorInitialized = true;
        metricsManager.registerMetric(VaradaStatsDispatcherPageSource.create(STATS_DISPATCHER_KEY));
        metricsManager.registerMetric(VaradaStatsWarmingService.create(WARMING_SERVICE_STAT_GROUP));
        metricsManager.registerMetric(VaradaStatsWarmupDemoter.create(WARMUP_DEMOTER_STAT_GROUP));
        metricsManager.registerMetric(VaradaStatsWarmupImportService.create(WARMUP_IMPORTER_STAT_GROUP));
        metricsManager.registerMetric(VaradaStatsWarmupExportService.create(WARMUP_EXPORTER_STAT_GROUP));
        metricsManager.registerMetric(VaradaStatsWorkerTaskExecutorService.create(WORKER_TASK_EXECUTOR_STAT_GROUP));
        metricsManager.registerMetric(VaradaStatsDictionary.create(DICTIONARY_STAT_GROUP));
        metricsManager.registerMetric(VaradaStatsCachePredicates.create(STATS_CACHE_PREDICATE_KEY));
        eventBus.post(new CoordinatorInitializedEvent(getCoordinatorNode()));
    }

    public Node getCoordinatorNode()
    {
        if (Objects.isNull(coordinatorNode)) {
            coordinatorNode = nodeManager.getCurrentNode();
        }
        return coordinatorNode;
    }

    public List<Node> getWorkerNodes()
    {
        List<Node> workers;
        if (globalConfiguration.getIsSingle()) {
            workers = List.of(nodeManager.getCurrentNode());
        }
        else {
            workers = nodeManager
                    .getAllNodes()
                    .stream()
                    .filter(node -> !node.isCoordinator())
                    .sorted(Comparator.comparing(Node::getNodeIdentifier))
                    .collect(toImmutableList());
        }
        return workers;
    }

    public boolean isCoordinatorReady()
    {
        return coordinatorInitialized;
    }

    public boolean isClusterReady()
    {
        logger.debug("workerNodes=%s", nodeManager.getWorkerNodes());
        logger.debug("isClusterReady::isCoordinatorReady=%b workerNodes=%s",
                isCoordinatorReady(),
                nodeManager.getWorkerNodes());
        return isCoordinatorReady() &&
                (nodeManager.getWorkerNodes().size() != 0 || globalConfiguration.getIsSingle());
    }
}
