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
package io.trino.connector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.metadata.CatalogManager;
import io.trino.node.AllNodes;
import io.trino.node.InternalNode;
import io.trino.node.InternalNodeManager;
import io.trino.spi.connector.CatalogHandle;
import io.trino.transaction.TransactionManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CatalogPruneTask
{
    private static final Logger log = Logger.get(CatalogPruneTask.class);
    private static final JsonCodec<List<CatalogHandle>> CATALOG_HANDLES_CODEC = listJsonCodec(CatalogHandle.class);

    private final TransactionManager transactionManager;
    private final CatalogManager catalogManager;
    private final ConnectorServicesProvider connectorServicesProvider;
    private final InternalNode currentNode;
    private final InternalNodeManager internalNodeManager;
    private final HttpClient httpClient;

    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, daemonThreadsNamed("catalog-prune"));
    private final ThreadPoolExecutorMBean executorMBean = new ThreadPoolExecutorMBean(executor);

    private final boolean enabled;
    private final Duration updateInterval;

    private final AtomicBoolean started = new AtomicBoolean();

    @Inject
    public CatalogPruneTask(
            TransactionManager transactionManager,
            CatalogManager catalogManager,
            ConnectorServicesProvider connectorServicesProvider,
            InternalNode currentNode,
            InternalNodeManager internalNodeManager,
            @ForCatalogPrune HttpClient httpClient,
            CatalogPruneTaskConfig catalogPruneTaskConfig)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.connectorServicesProvider = requireNonNull(connectorServicesProvider, "connectorServicesProvider is null");
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        this.enabled = catalogPruneTaskConfig.isEnabled();
        updateInterval = catalogPruneTaskConfig.getUpdateInterval();
    }

    @PostConstruct
    public void start()
    {
        if (enabled && !started.getAndSet(true)) {
            executor.scheduleWithFixedDelay(() -> {
                try {
                    pruneWorkerCatalogs();
                }
                catch (Throwable e) {
                    // ignore to avoid getting unscheduled
                    log.warn(e, "Error pruning catalogs");
                }
            }, updateInterval.toMillis(), updateInterval.toMillis(), MILLISECONDS);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return executorMBean;
    }

    @VisibleForTesting
    public void pruneWorkerCatalogs()
    {
        AllNodes allNodes = internalNodeManager.getAllNodes();
        Set<URI> online = Stream.of(allNodes.activeNodes(), allNodes.inactiveNodes(), allNodes.drainingNodes(), allNodes.drainedNodes(), allNodes.shuttingDownNodes())
                .flatMap(Set::stream)
                .map(InternalNode::getInternalUri)
                .filter(uri -> !uri.equals(currentNode.getInternalUri()))
                .collect(toImmutableSet());

        // send message to workers to trigger prune
        List<CatalogHandle> activeCatalogs = getActiveCatalogs();
        pruneWorkerCatalogs(online, activeCatalogs);

        // prune all inactive catalogs - we pass an empty set here because manager always retains active catalogs
        connectorServicesProvider.pruneCatalogs(ImmutableSet.of());
    }

    void pruneWorkerCatalogs(Set<URI> online, List<CatalogHandle> activeCatalogs)
    {
        for (URI uri : online) {
            uri = uriBuilderFrom(uri).appendPath("/v1/task/pruneCatalogs").build();
            Request request = preparePost()
                    .setUri(uri)
                    .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                    .setBodyGenerator(jsonBodyGenerator(CATALOG_HANDLES_CODEC, activeCatalogs))
                    .build();
            httpClient.executeAsync(request, new ResponseHandler<>()
            {
                @Override
                public Exception handleException(Request request, Exception exception)
                {
                    log.debug(exception, "Error pruning catalogs on server: %s", request.getUri());
                    return exception;
                }

                @Override
                public Object handle(Request request, Response response)
                {
                    log.debug("Pruned catalogs on server: %s", request.getUri());
                    return null;
                }
            });
        }
    }

    private List<CatalogHandle> getActiveCatalogs()
    {
        ImmutableSet.Builder<CatalogHandle> activeCatalogs = ImmutableSet.builder();
        // all catalogs in an active transaction
        transactionManager.getAllTransactionInfos().forEach(info -> activeCatalogs.addAll(info.getActiveCatalogs()));
        // all catalogs currently associated with a name
        activeCatalogs.addAll(catalogManager.getActiveCatalogs());
        return ImmutableList.copyOf(activeCatalogs.build());
    }
}
