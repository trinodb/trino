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
    void testEmptyPagesDoesNotCallLoader()
    {
        AtomicInteger loaderCallCount = new AtomicInteger();
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(emptyPage(), emptyPage())),
                () -> {
                    loaderCallCount.incrementAndGet();
                    return immediateFuture(Optional.empty());
                });

        assertThat(pageSource.getNextSourcePage().getPositionCount()).isEqualTo(0);
        assertThat(pageSource.getNextSourcePage().getPositionCount()).isEqualTo(0);
        assertThat(pageSource.getNextSourcePage()).isNull();
        assertThat(loaderCallCount.get()).isEqualTo(0);
    }

    @Test
    void testNoRowsDoesNotCallLoader()
    {
        AtomicInteger loaderCallCount = new AtomicInteger();
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of()),
                () -> {
                    loaderCallCount.incrementAndGet();
                    return immediateFuture(Optional.empty());
                });

        assertThat(pageSource.getNextSourcePage()).isNull();
        assertThat(pageSource.isFinished()).isTrue();
        assertThat(loaderCallCount.get()).isEqualTo(0);
    }

    @Test
    void testBlockedWhileLoading()
    {
        SettableFuture<Optional<PageFilter>> future = SettableFuture.create();
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(pageWithRows(3))),
                () -> future);

        // first non-empty page triggers the load but returns null
        assertThat(pageSource.getNextSourcePage()).isNull();
        assertThat(pageSource.isBlocked().isDone()).isFalse();
        assertThat(pageSource.isFinished()).isFalse();

        future.set(Optional.empty());
        assertThat(pageSource.isBlocked().isDone()).isTrue();
        assertThat(pageSource.getNextSourcePage()).isNotNull()
                .matches(page -> page.getPositionCount() == 3);
        assertThat(pageSource.isFinished()).isTrue();
    }

    @Test
    void testFirstNonEmptyPageBufferedAndReturnedAfterLoading()
    {
        SettableFuture<Optional<PageFilter>> future = SettableFuture.create();
        AtomicInteger appliedCount = new AtomicInteger();
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(pageWithRows(3), pageWithRows(5))),
                () -> future);

        // triggers load, buffers the 3-row page
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
    void testEmptyPagesBeforeFirstNonEmptyPageDontTriggerLoader()
    {
        AtomicInteger loaderCallCount = new AtomicInteger();
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(emptyPage(), emptyPage(), pageWithRows(2))),
                () -> {
                    loaderCallCount.incrementAndGet();
                    return immediateFuture(Optional.empty());
                });

        // two empty pages pass through without triggering
        assertThat(pageSource.getNextSourcePage().getPositionCount()).isEqualTo(0);
        assertThat(loaderCallCount.get()).isEqualTo(0);
        assertThat(pageSource.getNextSourcePage().getPositionCount()).isEqualTo(0);
        assertThat(loaderCallCount.get()).isEqualTo(0);

        // non-empty page triggers load and is returned
        SourcePage page = pageSource.getNextSourcePage();
        assertThat(page.getPositionCount()).isEqualTo(2);
        assertThat(loaderCallCount.get()).isEqualTo(1);
    }

    @Test
    void testIsFinishedAccountsForBufferedPage()
    {
        SettableFuture<Optional<PageFilter>> future = SettableFuture.create();
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(pageWithRows(1))),
                () -> future);

        // delegate is exhausted after the trigger page, but we still have it buffered
        assertThat(pageSource.getNextSourcePage()).isNull();
        assertThat(pageSource.isFinished()).isFalse();

        future.set(Optional.empty());

        // buffered page drains
        assertThat(pageSource.getNextSourcePage()).isNotNull();
        assertThat(pageSource.isFinished()).isTrue();
        assertThat(pageSource.getNextSourcePage()).isNull();
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

    /**
     * A loader that fails to start must not leave the first page buffered, or isFinished() never returns true.
     */
    @Test
    void testLoaderThrowingSynchronouslyLeavesNoBufferedPage()
    {
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(pageWithRows(3))),
                () -> { throw new RuntimeException("could not start the load"); });

        assertThatThrownBy(pageSource::getNextSourcePage).isInstanceOf(TrinoException.class);
        assertThat(pageSource.isFinished()).isTrue();
    }

    @Test
    void testCloseCancelsPendingLoadAndReleasesBufferedPage()
            throws IOException
    {
        SettableFuture<Optional<PageFilter>> future = SettableFuture.create();
        AsyncFilteredPageSource pageSource = new AsyncFilteredPageSource(
                new FixedPageSource(ImmutableList.of(pageWithRows(3))),
                () -> future);

        // triggers the load and buffers the page
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
