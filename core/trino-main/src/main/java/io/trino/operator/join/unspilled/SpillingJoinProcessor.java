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
package io.trino.operator.join.unspilled;

import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.WorkProcessor;
import io.trino.operator.join.unspilled.PageJoiner.PageJoinerFactory;
import io.trino.spi.Page;

import java.io.IOException;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class SpillingJoinProcessor
        implements WorkProcessor.Process<WorkProcessor<Page>>
{
    private final Runnable afterClose;
    private final boolean waitForBuild;
    private final ListenableFuture<LookupSourceProvider> lookupSourceProvider;
    private final PageJoiner sourcePagesJoiner;
    private final WorkProcessor<Page> joinedSourcePages;

    private boolean closed;

    public SpillingJoinProcessor(
            Runnable afterClose,
            boolean waitForBuild,
            ListenableFuture<LookupSourceProvider> lookupSourceProvider,
            PageJoinerFactory pageJoinerFactory,
            WorkProcessor<Page> sourcePages)
    {
        this.afterClose = requireNonNull(afterClose, "afterClose is null");
        this.waitForBuild = waitForBuild;
        this.lookupSourceProvider = requireNonNull(lookupSourceProvider, "lookupSourceProvider is null");
        sourcePagesJoiner = pageJoinerFactory.getPageJoiner(lookupSourceProvider);
        joinedSourcePages = sourcePages.transform(sourcePagesJoiner);
    }

    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try (Closer closer = Closer.create()) {
            // `afterClose` must be run last.
            // Closer is documented to mimic try-with-resource, which implies close will happen in reverse order.
            closer.register(afterClose::run);

            closer.register(sourcePagesJoiner);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public WorkProcessor.ProcessState<WorkProcessor<Page>> process()
    {
        // wait for build side to be completed before fetching any probe data
        // TODO: fix support for probe short-circuit: https://github.com/trinodb/trino/issues/3957
        if (waitForBuild && !lookupSourceProvider.isDone()) {
            return WorkProcessor.ProcessState.blocked(asVoid(lookupSourceProvider));
        }

        if (!joinedSourcePages.isFinished()) {
            return WorkProcessor.ProcessState.ofResult(joinedSourcePages);
        }

        close();
        return WorkProcessor.ProcessState.finished();
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }
}
