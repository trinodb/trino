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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.util.Objects.requireNonNull;

public class PageSourceOperator
        implements Operator
{
    private final ConnectorPageSource pageSource;
    private final OperatorContext operatorContext;
    private long completedBytes;
    private long readTimeNanos;

    public PageSourceOperator(ConnectorPageSource pageSource, OperatorContext operatorContext)
    {
        this.pageSource = requireNonNull(pageSource, "pageSource is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        try {
            pageSource.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isFinished()
    {
        return pageSource.isFinished();
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        CompletableFuture<?> pageSourceBlocked = pageSource.isBlocked();
        return pageSourceBlocked.isDone() ? NOT_BLOCKED : asVoid(toListenableFuture(pageSourceBlocked));
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        Page page = pageSource.getNextPage();
        if (page == null) {
            return null;
        }

        // update operator stats
        long endCompletedBytes = pageSource.getCompletedBytes();
        long endReadTimeNanos = pageSource.getReadTimeNanos();
        operatorContext.recordPhysicalInputWithTiming(endCompletedBytes - completedBytes, page.getPositionCount(), endReadTimeNanos - readTimeNanos);
        operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());
        completedBytes = endCompletedBytes;
        readTimeNanos = endReadTimeNanos;

        // assure the page is in memory before handing to another operator
        page = page.getLoadedPage();

        return page;
    }

    @Override
    public void close()
            throws IOException
    {
        pageSource.close();
    }
}
