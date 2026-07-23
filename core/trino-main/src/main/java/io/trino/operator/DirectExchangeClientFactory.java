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
package io.trino.operator;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.trino.FeaturesConfig;
import io.trino.FeaturesConfig.DataIntegrityVerification;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.exchange.ExchangeMetricsCollector;
import io.trino.execution.FailureInjector;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.TaskFailureListener;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.node.InternalNode;
import io.trino.spi.HostAddress;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.ExchangeId;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class DirectExchangeClientFactory
        implements DirectExchangeClientSupplier
{
    private final NodeInfo nodeInfo;
    private final DataIntegrityVerification dataIntegrityVerification;
    private final DataSize maxBufferedBytes;
    private final DataSize deduplicationBufferSize;
    private final int concurrentRequestMultiplier;
    private final Duration maxErrorDuration;
    private final HttpClient httpClient;
    private final DataSize maxResponseSize;
    private final boolean acknowledgePages;
    private final ScheduledExecutorService scheduler;
    private final ThreadPoolExecutorMBean executorMBean;
    private final ExecutorService pageBufferClientCallbackExecutor;
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private final Optional<ExchangeMetricsCollector> exchangeMetricsCollector;
    private final Optional<HostAddress> localNodeAddress;
    private final Optional<Provider<SqlTaskManager>> localTaskManager;

    @Inject
    public DirectExchangeClientFactory(
            InternalNode currentNode,
            // Provider is used to break the dependency cycle:
            // SqlTaskManager -> LocalExecutionPlanner -> DirectExchangeClientSupplier
            Provider<SqlTaskManager> taskManager,
            FailureInjector failureInjector,
            NodeInfo nodeInfo,
            FeaturesConfig featuresConfig,
            DirectExchangeClientConfig config,
            @ForExchange HttpClient httpClient,
            @ForExchange HttpClientConfig httpClientConfig,
            @ForExchange ScheduledExecutorService scheduler,
            ExchangeManagerRegistry exchangeManagerRegistry,
            Optional<ExchangeMetricsCollector> exchangeMetricsCollector)
    {
        this(nodeInfo,
                featuresConfig.getExchangeDataIntegrityVerification(),
                config.getMaxBufferSize(),
                config.getDeduplicationBufferSize(),
                config.getMaxResponseSize(),
                httpClientConfig.getMaxResponseContentLength(),
                config.getConcurrentRequestMultiplier(),
                config.getMaxErrorDuration(),
                config.isAcknowledgePages(),
                config.getPageBufferClientMaxCallbackThreads(),
                httpClient,
                scheduler,
                exchangeManagerRegistry,
                exchangeMetricsCollector,
                // Fetching results directly from local output buffers bypasses TaskResource and would
                // skip its failure injection, so keep the HTTP path when an injector is installed (tests)
                failureInjector.canInjectFailures() ? Optional.empty() : Optional.of(currentNode.getHostAndPort()),
                failureInjector.canInjectFailures() ? Optional.empty() : Optional.of(taskManager));
    }

    public DirectExchangeClientFactory(
            NodeInfo nodeInfo,
            DataIntegrityVerification dataIntegrityVerification,
            DataSize maxBufferedBytes,
            DataSize deduplicationBufferSize,
            DataSize maxResponseSize,
            DataSize maxClientResponseSize,
            int concurrentRequestMultiplier,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            int pageBufferClientMaxCallbackThreads,
            HttpClient httpClient,
            ScheduledExecutorService scheduler,
            ExchangeManagerRegistry exchangeManagerRegistry,
            Optional<ExchangeMetricsCollector> exchangeMetricsCollector,
            Optional<HostAddress> localNodeAddress,
            Optional<Provider<SqlTaskManager>> localTaskManager)
    {
        this.nodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
        this.dataIntegrityVerification = requireNonNull(dataIntegrityVerification, "dataIntegrityVerification is null");
        this.maxBufferedBytes = requireNonNull(maxBufferedBytes, "maxBufferedBytes is null");
        this.deduplicationBufferSize = requireNonNull(deduplicationBufferSize, "deduplicationBufferSize is null");
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.maxErrorDuration = requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        this.acknowledgePages = acknowledgePages;
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        // Use only 0.75 of the maxResponseSize to leave room for additional bytes from the encoding
        // TODO figure out a better way to compute the size of data that will be transferred over the network
        requireNonNull(maxResponseSize, "maxResponseSize is null");
        long maxResponseSizeBytes = (long) (Math.min(maxClientResponseSize.toBytes(), maxResponseSize.toBytes()) * 0.75);
        this.maxResponseSize = DataSize.ofBytes(maxResponseSizeBytes);

        this.scheduler = requireNonNull(scheduler, "scheduler is null");

        this.pageBufferClientCallbackExecutor = newFixedThreadPool(pageBufferClientMaxCallbackThreads, daemonThreadsNamed("page-buffer-client-callback-%s"));
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) pageBufferClientCallbackExecutor);

        checkArgument(maxBufferedBytes.toBytes() > 0, "maxBufferSize must be at least 1 byte: %s", maxBufferedBytes);
        checkArgument(maxResponseSize.toBytes() > 0, "maxResponseSize must be at least 1 byte: %s", maxResponseSize);
        checkArgument(concurrentRequestMultiplier > 0, "concurrentRequestMultiplier must be at least 1: %s", concurrentRequestMultiplier);
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
        this.exchangeMetricsCollector = requireNonNull(exchangeMetricsCollector, "exchangeMetricsCollector is null");
        this.localNodeAddress = requireNonNull(localNodeAddress, "localNodeAddress is null");
        this.localTaskManager = requireNonNull(localTaskManager, "localTaskManager is null");
    }

    @PreDestroy
    public void stop()
    {
        pageBufferClientCallbackExecutor.shutdownNow();
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return executorMBean;
    }

    @Override
    public DirectExchangeClient get(
            QueryId queryId,
            ExchangeId exchangeId,
            Span parentSpan,
            LocalMemoryContext memoryContext,
            TaskFailureListener taskFailureListener,
            RetryPolicy retryPolicy)
    {
        @SuppressWarnings("resource")
        DirectExchangeBuffer buffer = switch (retryPolicy) {
            case TASK -> throw new UnsupportedOperationException();
            case QUERY -> new DeduplicatingDirectExchangeBuffer(
                    scheduler,
                    deduplicationBufferSize,
                    retryPolicy,
                    exchangeManagerRegistry,
                    exchangeMetricsCollector,
                    queryId,
                    parentSpan,
                    exchangeId);
            case NONE -> new StreamingDirectExchangeBuffer(scheduler, maxBufferedBytes);
        };

        return new DirectExchangeClient(
                nodeInfo.getExternalAddress(),
                dataIntegrityVerification,
                buffer,
                maxResponseSize,
                concurrentRequestMultiplier,
                maxErrorDuration,
                acknowledgePages,
                httpClient,
                scheduler,
                memoryContext,
                pageBufferClientCallbackExecutor,
                taskFailureListener,
                localNodeAddress,
                localTaskManager.map(Provider::get));
    }
}
