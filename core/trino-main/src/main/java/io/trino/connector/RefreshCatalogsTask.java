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
import com.google.inject.Inject;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.metadata.CatalogManager;
import io.trino.node.AllNodes;
import io.trino.node.InternalNode;
import io.trino.node.InternalNodeManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.net.URI;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.preparePost;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RefreshCatalogsTask
{
    private static final Logger log = Logger.get(RefreshCatalogsTask.class);

    private final CatalogManager catalogManager;
    private final InternalNode currentNode;
    private final InternalNodeManager internalNodeManager;
    private final HttpClient httpClient;

    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, daemonThreadsNamed("catalog-refresh"));
    private final ThreadPoolExecutorMBean executorMBean = new ThreadPoolExecutorMBean(executor);

    private final boolean enabled;
    private final Duration refreshInterval;

    private final AtomicBoolean started = new AtomicBoolean();

    @Inject
    public RefreshCatalogsTask(
            CatalogManager catalogManager,
            InternalNode currentNode,
            InternalNodeManager internalNodeManager,
            @ForCatalogRefresh HttpClient httpClient,
            RefreshCatalogsTaskConfig refreshCatalogsTaskConfig)
    {
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        this.enabled = refreshCatalogsTaskConfig.isEnabled();
        this.refreshInterval = refreshCatalogsTaskConfig.getRefreshInterval();
    }

    @PostConstruct
    public void start()
    {
        if (enabled && !started.getAndSet(true)) {
            executor.scheduleWithFixedDelay(() -> {
                try {
                    refreshCatalogs();
                }
                catch (Throwable e) {
                    // ignore to avoid getting unscheduled
                    log.warn(e, "Error refreshing catalogs");
                }
            }, refreshInterval.toMillis(), refreshInterval.toMillis(), MILLISECONDS);
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
    public void refreshCatalogs()
    {
        // First refresh the coordinator's catalogs
        if (catalogManager instanceof CoordinatorDynamicCatalogManager dynamicCatalogManager) {
            try {
                log.debug("Refreshing catalogs on coordinator");
                dynamicCatalogManager.refreshCatalogs();
                log.info("Successfully refreshed catalogs on coordinator");
            }
            catch (Exception e) {
                log.warn(e, "Failed to refresh catalogs on coordinator");
            }
        }
        else {
            log.debug("Catalog manager does not support dynamic refresh: %s", catalogManager.getClass().getSimpleName());
        }

        // Send refresh signal to all workers
        AllNodes allNodes = internalNodeManager.getAllNodes();
        Set<URI> workers = Stream.of(allNodes.activeNodes(), allNodes.inactiveNodes(), allNodes.drainingNodes(), allNodes.drainedNodes(), allNodes.shuttingDownNodes())
                .flatMap(Set::stream)
                .map(InternalNode::getInternalUri)
                .filter(uri -> !uri.equals(currentNode.getInternalUri()))
                .collect(toImmutableSet());

        refreshWorkerCatalogs(workers);
    }

    @VisibleForTesting
    void refreshWorkerCatalogs(Set<URI> workers)
    {
        for (URI uri : workers) {
            uri = uriBuilderFrom(uri).appendPath("/v1/task/refreshCatalogs").build();
            Request request = preparePost()
                    .setUri(uri)
                    .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                    .build();
            httpClient.executeAsync(request, new ResponseHandler<>()
            {
                @Override
                public Exception handleException(Request request, Exception exception)
                {
                    log.debug(exception, "Error refreshing catalogs on server: %s", request.getUri());
                    return exception;
                }

                @Override
                public Object handle(Request request, Response response)
                {
                    log.debug("Refreshed catalogs on server: %s", request.getUri());
                    return null;
                }
            });
        }
    }
}
