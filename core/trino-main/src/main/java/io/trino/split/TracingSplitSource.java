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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.trino.spi.connector.CatalogHandle;
import io.trino.tracing.TrinoAttributes;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class TracingSplitSource
        implements SplitSource
{
    private final SplitSource source;
    private final Tracer tracer;
    private final Optional<Span> parentSpan;
    private final String spanName;

    public TracingSplitSource(SplitSource source, Tracer tracer, Optional<Span> parentSpan, String spanName)
    {
        this.source = requireNonNull(source, "source is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.parentSpan = requireNonNull(parentSpan, "parentSpan is null");
        this.spanName = requireNonNull(spanName, "spanName is null");
    }

    @Override
    public CatalogHandle getCatalogHandle()
    {
        return source.getCatalogHandle();
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(int maxSize)
    {
        Span span = tracer.spanBuilder(spanName)
                .setParent(parentSpan.map(Context.current()::with).orElse(Context.current()))
                .setAttribute(TrinoAttributes.SPLIT_BATCH_MAX_SIZE, (long) maxSize)
                .startSpan();

        ListenableFuture<SplitBatch> future;
        try (var ignored = span.makeCurrent()) {
            future = source.getNextBatch(maxSize);
        }
        catch (Throwable t) {
            span.end();
            throw t;
        }

        Futures.addCallback(future, new FutureCallback<>()
        {
            @Override
            public void onSuccess(SplitBatch batch)
            {
                span.setAttribute(TrinoAttributes.SPLIT_BATCH_RESULT_SIZE, batch.getSplits().size());
                span.end();
            }

            @Override
            public void onFailure(Throwable t)
            {
                span.end();
            }
        }, directExecutor());

        return future;
    }

    @Override
    public void close()
    {
        try (source) {
            parentSpan.ifPresent(Span::end);
        }
    }

    @Override
    public boolean isFinished()
    {
        return source.isFinished();
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        return source.getTableExecuteSplitsInfo();
    }
}
