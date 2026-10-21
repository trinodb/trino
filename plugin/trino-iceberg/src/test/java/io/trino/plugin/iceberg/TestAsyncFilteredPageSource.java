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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.plugin.iceberg.delete.PageFilter;
import io.trino.plugin.iceberg.delete.Positions;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAsyncFilteredPageSource
{
    @Test
    void testLoadStartsWhenOpened()
    {
        AtomicInteger loaderCallCount = new AtomicInteger();
        SettableFuture<Optional<PageFilter>> future = SettableFuture.create();
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(pageWithRows(3))),
                () -> {
                    loaderCallCount.incrementAndGet();
                    return future;
                });

        assertThat(loaderCallCount.get()).isEqualTo(1);
        // nothing needs the filter yet, so the page source does not wait for it
        assertThat(pageSource.isBlocked().isDone()).isTrue();
    }

    @Test
    void testEmptyPagesCancelLoadWhenFinished()
    {
        SettableFuture<Optional<PageFilter>> future = SettableFuture.create();
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(emptyPage(), emptyPage())),
                () -> future);

        assertThat(pageSource.getNextSourcePage().getPositionCount()).isEqualTo(0);
        assertThat(future.isCancelled()).isFalse();
        assertThat(pageSource.getNextSourcePage().getPositionCount()).isEqualTo(0);
        assertThat(pageSource.isFinished()).isTrue();
        assertThat(future.isCancelled()).isTrue();
    }

    @Test
    void testNoRowsCancelsLoad()
    {
        SettableFuture<Optional<PageFilter>> future = SettableFuture.create();
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of()),
                () -> future);

        assertThat(pageSource.getNextSourcePage()).isNull();
        assertThat(pageSource.isFinished()).isTrue();
        assertThat(future.isCancelled()).isTrue();
    }

    @Test
    void testNoRowsIgnoresFailedLoad()
    {
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(emptyPage())),
                () -> immediateFailedFuture(new RuntimeException("load failed")));

        assertThat(pageSource.getNextSourcePage().getPositionCount()).isEqualTo(0);
        assertThat(pageSource.isFinished()).isTrue();
    }

    @Test
    void testBlockedWhileLoading()
    {
        SettableFuture<Optional<PageFilter>> future = SettableFuture.create();
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(pageWithRows(3))),
                () -> future);

        // first non-empty page waits for the load and returns null
        assertThat(pageSource.getNextSourcePage()).isNull();
        assertThat(pageSource.isBlocked().isDone()).isFalse();
        assertThat(pageSource.isFinished()).isFalse();

        future.set(Optional.empty());
        assertThat(pageSource.isBlocked().isDone()).isTrue();
        assertThat(pageSource.getNextSourcePage()).isNotNull()
                .matches(page -> page.getPositionCount() == 3);
        assertThat(pageSource.isFinished()).isTrue();
        assertThat(pageSource.getNextSourcePage()).isNull();
    }

    @Test
    void testFirstNonEmptyPageBufferedAndReturnedAfterLoading()
    {
        SettableFuture<Optional<PageFilter>> future = SettableFuture.create();
        AtomicInteger appliedCount = new AtomicInteger();
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(pageWithRows(3), pageWithRows(5))),
                () -> future);

        // buffers the 3-row page until the load completes
        assertThat(pageSource.getNextSourcePage()).isNull();

        future.set(Optional.of(countingFilter(appliedCount)));

        // buffered pages returned
        SourcePage first = pageSource.getNextSourcePage();
        assertThat(first.getPositionCount()).isEqualTo(3);
        assertThat(appliedCount.get()).isEqualTo(1);

        SourcePage second = pageSource.getNextSourcePage();
        assertThat(second.getPositionCount()).isEqualTo(5);
        assertThat(appliedCount.get()).isEqualTo(2);
    }

    @Test
    void testLoaderCalledExactlyOnce()
    {
        AtomicInteger loaderCallCount = new AtomicInteger();
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(pageWithRows(1), pageWithRows(1), pageWithRows(1))),
                () -> {
                    loaderCallCount.incrementAndGet();
                    return immediateFuture(Optional.empty());
                });

        drainPageSource(pageSource);

        assertThat(loaderCallCount.get()).isEqualTo(1);
    }

    @Test
    void testEmptyPagesPassThroughWhileLoading()
    {
        SettableFuture<Optional<PageFilter>> future = SettableFuture.create();
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(emptyPage(), emptyPage(), pageWithRows(2))),
                () -> future);

        // empty pages do not need the filter, so they are returned before the load completes
        assertThat(pageSource.getNextSourcePage().getPositionCount()).isEqualTo(0);
        assertThat(pageSource.getNextSourcePage().getPositionCount()).isEqualTo(0);
        assertThat(pageSource.isBlocked().isDone()).isTrue();

        // the non-empty page waits for the filter
        assertThat(pageSource.getNextSourcePage()).isNull();
        assertThat(pageSource.isBlocked().isDone()).isFalse();

        future.set(Optional.empty());
        assertThat(pageSource.getNextSourcePage().getPositionCount()).isEqualTo(2);
        assertThat(future.isCancelled()).isFalse();
    }

    @Test
    void testFailedLoaderPropagatesException()
    {
        SettableFuture<Optional<PageFilter>> future = SettableFuture.create();
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(pageWithRows(3))),
                () -> future);

        assertThat(pageSource.getNextSourcePage()).isNull();
        future.setException(new RuntimeException("load failed"));
        assertThatThrownBy(pageSource::getNextSourcePage)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("load failed");
    }

    @Test
    void testFailedLoaderImmediatelyPropagatesException()
    {
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(pageWithRows(3))),
                () -> immediateFailedFuture(new RuntimeException("load failed")));

        assertThatThrownBy(pageSource::getNextSourcePage)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("load failed");
    }

    @Test
    void testFilterExceptionWrappedAsTrinoException()
    {
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(pageWithRows(3))),
                () -> immediateFuture(Optional.of(throwingFilter(new RuntimeException("apply failed")))));

        assertThatThrownBy(pageSource::getNextSourcePage)
                .isInstanceOf(TrinoException.class)
                .hasMessage("apply failed")
                .satisfies(e -> assertThat(((TrinoException) e).getErrorCode()).isEqualTo(ICEBERG_BAD_DATA.toErrorCode()));
    }

    @Test
    void testFilterTrinoExceptionPropagatesUnwrapped()
    {
        TrinoException original = new TrinoException(ICEBERG_BAD_DATA, "already a TrinoException");
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(pageWithRows(3))),
                () -> immediateFuture(Optional.of(throwingFilter(original))));

        assertThatThrownBy(pageSource::getNextSourcePage).isSameAs(original);
    }

    @Test
    void testLoaderThrowingSynchronouslyWrappedAsTrinoException()
    {
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(pageWithRows(3))),
                () -> { throw new RuntimeException("could not start the load"); });

        assertThatThrownBy(pageSource::getNextSourcePage)
                .isInstanceOf(TrinoException.class)
                .hasMessage("could not start the load")
                .satisfies(e -> assertThat(((TrinoException) e).getErrorCode()).isEqualTo(ICEBERG_BAD_DATA.toErrorCode()));
    }

    @Test
    void testLoaderThrowingTrinoExceptionSynchronouslyPropagatesUnwrapped()
    {
        TrinoException original = new TrinoException(ICEBERG_BAD_DATA, "already a TrinoException");
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(pageWithRows(3))),
                () -> { throw original; });

        assertThatThrownBy(pageSource::getNextSourcePage).isSameAs(original);
    }

    @Test
    void testCloseCancelsPendingLoadAndReleasesBufferedPage()
            throws IOException
    {
        SettableFuture<Optional<PageFilter>> future = SettableFuture.create();
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(pageWithRows(3))),
                () -> future);

        // buffers the page until the load completes
        assertThat(pageSource.getNextSourcePage()).isNull();
        assertThat(pageSource.isFinished()).isFalse();

        pageSource.close();

        assertThat(future.isCancelled()).isTrue();
        assertThat(pageSource.isFinished()).isTrue();
    }

    private static Page emptyPage()
    {
        return new Page(0);
    }

    private static Page pageWithRows(int rows)
    {
        return new Page(rows);
    }

    private static PageFilter countingFilter(AtomicInteger callCount)
    {
        return new PageFilter()
        {
            @Override
            public Positions filterPositions(SourcePage page, Positions positions)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void applyFilter(SourcePage page)
            {
                callCount.incrementAndGet();
            }
        };
    }

    private static PageFilter throwingFilter(RuntimeException exception)
    {
        return new PageFilter()
        {
            @Override
            public Positions filterPositions(SourcePage page, Positions positions)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void applyFilter(SourcePage page)
            {
                throw exception;
            }
        };
    }

    private static void drainPageSource(AsyncFilteredPageSource pageSource)
    {
        while (!pageSource.isFinished()) {
            pageSource.getNextSourcePage();
        }
    }
}
