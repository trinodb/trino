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
package io.trino.split;

import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.QueryManagerConfig;
import io.trino.metadata.TableFunctionHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.tracing.TrinoAttributes;
import jakarta.annotation.PreDestroy;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.tracing.ScopedSpan.scopedSpan;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class SplitManager
{
    private final CatalogServiceProvider<ConnectorSplitManager> splitManagerProvider;
    private final Tracer tracer;
    private final int minScheduleSplitBatchSize;
    private final ExecutorService executorService;
    private final Executor executor;

    @Inject
    public SplitManager(CatalogServiceProvider<ConnectorSplitManager> splitManagerProvider, Tracer tracer, QueryManagerConfig config)
    {
        this.splitManagerProvider = requireNonNull(splitManagerProvider, "splitManagerProvider is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.minScheduleSplitBatchSize = config.getMinScheduleSplitBatchSize();
        this.executorService = newCachedThreadPool(daemonThreadsNamed("splits-manager-callback-%s"));
        this.executor = new BoundedExecutor(executorService, config.getMaxSplitManagerCallbackThreads());
    }

    @PreDestroy
    public void shutdown()
    {
        executorService.shutdown();
    }

    public SplitSource getSplits(
            Session session,
            Span parentSpan,
            TableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        CatalogHandle catalogHandle = table.getCatalogHandle();
        ConnectorSplitManager splitManager = splitManagerProvider.getService(catalogHandle);
        if (!isAllowPushdownIntoConnectors(session)) {
            dynamicFilter = DynamicFilter.EMPTY;
        }

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);

        ConnectorSplitSource source;
        try (var ignore = scopedSpan(tracer.spanBuilder("SplitManager.getSplits")
                .setParent(Context.current().with(parentSpan))
                .setAttribute(TrinoAttributes.TABLE, table.getConnectorHandle().toString())
                .startSpan())) {
            source = splitManager.getSplits(
                    table.getTransaction(),
                    connectorSession,
                    table.getConnectorHandle(),
                    dynamicFilter,
                    constraint);
        }

        SplitSource splitSource = new ConnectorAwareSplitSource(catalogHandle, source);

        Span span = splitSourceSpan(parentSpan, catalogHandle);

        if (minScheduleSplitBatchSize > 1) {
            splitSource = new TracingSplitSource(splitSource, tracer, Optional.empty(), "split-batch");
            splitSource = new BufferingSplitSource(splitSource, executor, minScheduleSplitBatchSize);
            splitSource = new TracingSplitSource(splitSource, tracer, Optional.of(span), "split-buffer");
        }
        else {
            splitSource = new TracingSplitSource(splitSource, tracer, Optional.of(span), "split-batch");
        }

        return splitSource;
    }

    public SplitSource getSplits(Session session, Span parentSpan, TableFunctionHandle function)
    {
        CatalogHandle catalogHandle = function.getCatalogHandle();
        ConnectorSplitManager splitManager = splitManagerProvider.getService(catalogHandle);

        ConnectorSplitSource source;
        try (var ignore = scopedSpan(tracer.spanBuilder("SplitManager.getSplits")
                .setParent(Context.current().with(parentSpan))
                .setAttribute(TrinoAttributes.FUNCTION, function.getFunctionHandle().toString())
                .startSpan())) {
            source = splitManager.getSplits(
                    function.getTransactionHandle(),
                    session.toConnectorSession(catalogHandle),
                    function.getFunctionHandle());
        }

        SplitSource splitSource = new ConnectorAwareSplitSource(catalogHandle, source);

        Span span = splitSourceSpan(parentSpan, catalogHandle);
        return new TracingSplitSource(splitSource, tracer, Optional.of(span), "split-buffer");
    }

    private Span splitSourceSpan(Span parentSpan, CatalogHandle catalogHandle)
    {
        return tracer.spanBuilder("split-source")
                .setParent(Context.current().with(parentSpan))
                .setAttribute(TrinoAttributes.CATALOG, catalogHandle.getCatalogName())
                .startSpan();
    }
}
