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

import io.trino.Session;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.Thread.State.BLOCKED;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestPageSourceProviderMemoryTracking
{
    @Test
    void testSharedMemoryUsageReportedIntoAtMostOneContext()
    {
        AtomicLong memoryUsage = new AtomicLong(1000);
        AtomicInteger polls = new AtomicInteger();
        PageSourceProvider provider = createPageSourceProvider(() -> {
            polls.incrementAndGet();
            return memoryUsage.get();
        });

        RecordingMemoryContext first = new RecordingMemoryContext();
        RecordingMemoryContext second = new RecordingMemoryContext();
        provider.trackMemoryUsage(first);
        provider.trackMemoryUsage(second);

        // The calling context claims the reporting role.
        provider.updateMemoryUsage(first);
        assertThat(first.bytes).isEqualTo(1000);
        assertThat(second.bytes).isEqualTo(0);
        assertThat(polls).hasValue(1);

        // Other contexts do not poll the provider or mutate the reporting context.
        memoryUsage.set(2000);
        provider.updateMemoryUsage(second);
        assertThat(first.bytes).isEqualTo(1000);
        assertThat(second.bytes).isEqualTo(0);
        assertThat(polls).hasValue(1);

        provider.updateMemoryUsage(first);
        assertThat(first.bytes).isEqualTo(2000);
        assertThat(second.bytes).isEqualTo(0);
        assertThat(polls).hasValue(2);
    }

    @Test
    void testReportingPassesOnUntrack()
    {
        AtomicLong memoryUsage = new AtomicLong(1000);
        PageSourceProvider provider = createPageSourceProvider(memoryUsage);

        RecordingMemoryContext first = new RecordingMemoryContext();
        RecordingMemoryContext second = new RecordingMemoryContext();
        provider.trackMemoryUsage(first);
        provider.trackMemoryUsage(second);

        provider.updateMemoryUsage(first);

        // untracking the reporting context resets it and passes the role on the next update
        provider.untrackMemoryUsage(first);
        assertThat(first.bytes).isEqualTo(0);
        provider.updateMemoryUsage(second);
        assertThat(second.bytes).isEqualTo(1000);

        // an untracked context is never reported into again
        memoryUsage.set(3000);
        provider.updateMemoryUsage(first);
        assertThat(first.bytes).isEqualTo(0);
        assertThat(second.bytes).isEqualTo(1000);
        provider.updateMemoryUsage(second);
        assertThat(second.bytes).isEqualTo(3000);
    }

    @Test
    void testUntrackNonReportingContext()
    {
        AtomicLong memoryUsage = new AtomicLong(1000);
        PageSourceProvider provider = createPageSourceProvider(memoryUsage);

        RecordingMemoryContext first = new RecordingMemoryContext();
        RecordingMemoryContext second = new RecordingMemoryContext();
        provider.trackMemoryUsage(first);
        provider.trackMemoryUsage(second);

        provider.updateMemoryUsage(first);

        // untracking a non-reporting context does not affect the reporting one
        provider.untrackMemoryUsage(second);
        provider.updateMemoryUsage(first);
        assertThat(first.bytes).isEqualTo(1000);
        assertThat(second.bytes).isEqualTo(0);

        // untracking is idempotent, and untracking the last context stops reporting
        provider.untrackMemoryUsage(second);
        provider.untrackMemoryUsage(first);
        assertThat(first.bytes).isEqualTo(0);
        provider.updateMemoryUsage(first);
        assertThat(first.bytes).isEqualTo(0);
    }

    @Test
    void testNonReportingContextCannotCauseStaleOverwrite()
            throws Exception
    {
        AtomicLong memoryUsage = new AtomicLong(1000);
        AtomicInteger polls = new AtomicInteger();
        CountDownLatch firstSampled = new CountDownLatch(1);
        CountDownLatch allowFirstPollToFinish = new CountDownLatch(1);
        PageSourceProvider provider = createPageSourceProvider(() -> {
            long sampled = memoryUsage.get();
            if (polls.incrementAndGet() == 1) {
                firstSampled.countDown();
                assertThat(awaitUninterruptibly(allowFirstPollToFinish, 10, SECONDS)).isTrue();
            }
            return sampled;
        });
        RecordingMemoryContext first = new RecordingMemoryContext();
        RecordingMemoryContext second = new RecordingMemoryContext();
        provider.trackMemoryUsage(first);
        provider.trackMemoryUsage(second);

        try (ExecutorService executor = newFixedThreadPool(2)) {
            Future<?> firstUpdate = executor.submit(() -> provider.updateMemoryUsage(first));
            try {
                assertThat(firstSampled.await(10, SECONDS)).isTrue();

                memoryUsage.set(2000);
                Future<?> secondUpdate = executor.submit(() -> provider.updateMemoryUsage(second));
                // A non-reporting driver must return without polling or writing into the owner's context.
                secondUpdate.get(10, SECONDS);
                assertThat(polls).hasValue(1);
            }
            finally {
                allowFirstPollToFinish.countDown();
            }
            firstUpdate.get(10, SECONDS);
        }

        assertThat(first.updates).containsExactly(1000L);
        assertThat(second.updates).isEmpty();

        provider.updateMemoryUsage(first);
        assertThat(first.updates).containsExactly(1000L, 2000L);
        assertThat(polls).hasValue(2);
    }

    @Test
    void testUpdateAndUntrackAreSerialized()
            throws Exception
    {
        AtomicInteger polls = new AtomicInteger();
        PageSourceProvider provider = createPageSourceProvider(() -> {
            polls.incrementAndGet();
            return 1000;
        });
        CountDownLatch reportingStarted = new CountDownLatch(1);
        CountDownLatch allowReportToFinish = new CountDownLatch(1);
        RecordingMemoryContext first = new RecordingMemoryContext()
        {
            @Override
            public void setBytes(long currentBytes)
            {
                if (currentBytes > 0) {
                    reportingStarted.countDown();
                    assertThat(awaitUninterruptibly(allowReportToFinish, 10, SECONDS)).isTrue();
                }
                super.setBytes(currentBytes);
            }
        };
        RecordingMemoryContext second = new RecordingMemoryContext();
        provider.trackMemoryUsage(first);
        provider.trackMemoryUsage(second);

        try (ExecutorService executor = newFixedThreadPool(2)) {
            Future<?> update = executor.submit(() -> provider.updateMemoryUsage(first));
            Future<?> untrack;
            try {
                assertThat(reportingStarted.await(10, SECONDS)).isTrue();

                AtomicReference<Thread> untrackThread = new AtomicReference<>();
                untrack = executor.submit(() -> {
                    untrackThread.set(Thread.currentThread());
                    provider.untrackMemoryUsage(first);
                });
                // Ensure untrack reached the reporter monitor before allowing the update to finish.
                assertEventually(() -> assertThat(untrackThread.get())
                        .isNotNull()
                        .extracting(Thread::getState)
                        .isEqualTo(BLOCKED));
            }
            finally {
                allowReportToFinish.countDown();
            }

            update.get(10, SECONDS);
            untrack.get(10, SECONDS);
        }

        assertThat(first.bytes).isEqualTo(0);
        assertThat(polls).hasValue(1);

        // The old context cannot report after untracking, and a live context can take over.
        provider.updateMemoryUsage(first);
        assertThat(polls).hasValue(1);
        provider.updateMemoryUsage(second);
        assertThat(second.bytes).isEqualTo(1000);
        assertThat(polls).hasValue(2);
    }

    @Test
    void testConcurrentRetrackWaitsForPreviousReset()
            throws Exception
    {
        PageSourceProvider provider = createPageSourceProvider(new AtomicLong(1000));
        CountDownLatch resetStarted = new CountDownLatch(1);
        CountDownLatch allowResetToFinish = new CountDownLatch(1);
        RecordingMemoryContext active = new RecordingMemoryContext();
        RecordingMemoryContext reused = new RecordingMemoryContext()
        {
            @Override
            public void setBytes(long currentBytes)
            {
                if (currentBytes == 0) {
                    resetStarted.countDown();
                    assertThat(awaitUninterruptibly(allowResetToFinish, 10, SECONDS)).isTrue();
                }
                super.setBytes(currentBytes);
            }
        };
        provider.trackMemoryUsage(active);
        provider.trackMemoryUsage(reused);
        provider.updateMemoryUsage(active);

        try (ExecutorService executor = newFixedThreadPool(2)) {
            Future<?> untrack = executor.submit(() -> provider.untrackMemoryUsage(reused));
            Future<?> retrack;
            try {
                assertThat(resetStarted.await(10, SECONDS)).isTrue();

                AtomicReference<Thread> retrackThread = new AtomicReference<>();
                retrack = executor.submit(() -> {
                    retrackThread.set(Thread.currentThread());
                    provider.trackMemoryUsage(reused);
                });
                // Re-registration must wait on the lifecycle monitor until the old reset completes.
                assertEventually(() -> assertThat(retrackThread.get())
                        .isNotNull()
                        .extracting(Thread::getState)
                        .isEqualTo(BLOCKED));
            }
            finally {
                allowResetToFinish.countDown();
            }

            untrack.get(10, SECONDS);
            retrack.get(10, SECONDS);
        }

        provider.untrackMemoryUsage(active);
        provider.updateMemoryUsage(reused);
        assertThat(reused.updates).containsExactly(0L, 1000L);
    }

    @Test
    void testResetFailureStillReleasesReportingRole()
    {
        PageSourceProvider provider = createPageSourceProvider(new AtomicLong(1000));
        RecordingMemoryContext first = new RecordingMemoryContext()
        {
            @Override
            public void setBytes(long currentBytes)
            {
                if (currentBytes == 0) {
                    throw new RuntimeException("reset failed");
                }
                super.setBytes(currentBytes);
            }
        };
        RecordingMemoryContext second = new RecordingMemoryContext();
        provider.trackMemoryUsage(first);
        provider.trackMemoryUsage(second);

        provider.updateMemoryUsage(first);
        assertThatThrownBy(() -> provider.untrackMemoryUsage(first))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("reset failed");

        provider.updateMemoryUsage(second);
        assertThat(second.bytes).isEqualTo(1000);
    }

    @Test
    void testUpdateWithoutTrackedContexts()
    {
        PageSourceProvider provider = createPageSourceProvider(new AtomicLong(1000));
        RecordingMemoryContext memoryContext = new RecordingMemoryContext();

        provider.updateMemoryUsage(memoryContext);
        provider.untrackMemoryUsage(memoryContext);
    }

    @Test
    void testDefaultImplementationsAreNoop()
    {
        PageSourceProvider provider = new PageSourceProvider()
        {
            @Override
            public ConnectorPageSource createPageSource(
                    Session session,
                    Split split,
                    TableHandle table,
                    Optional<ConnectorTableCredentials> tableCredentials,
                    List<ColumnHandle> columns,
                    DynamicFilter dynamicFilter,
                    MemoryContext memoryContext)
            {
                throw new UnsupportedOperationException();
            }
        };

        RecordingMemoryContext memoryContext = new RecordingMemoryContext();
        provider.trackMemoryUsage(memoryContext);
        provider.updateMemoryUsage(memoryContext);
        provider.untrackMemoryUsage(memoryContext);
        assertThat(memoryContext.bytes).isEqualTo(0);
    }

    private static PageSourceProvider createPageSourceProvider(AtomicLong memoryUsage)
    {
        return createPageSourceProvider(memoryUsage::get);
    }

    private static PageSourceProvider createPageSourceProvider(LongSupplier memoryUsage)
    {
        ConnectorPageSourceProvider connectorPageSourceProvider = new ConnectorPageSourceProvider()
        {
            @Override
            public long getMemoryUsage()
            {
                return memoryUsage.getAsLong();
            }
        };
        return new PageSourceManager(_ -> () -> connectorPageSourceProvider)
                .createPageSourceProvider(TEST_CATALOG_HANDLE);
    }

    private static class RecordingMemoryContext
            implements MemoryContext
    {
        private final List<Long> updates = new CopyOnWriteArrayList<>();
        private volatile long bytes;

        @Override
        public void setBytes(long currentBytes)
        {
            this.bytes = currentBytes;
            updates.add(currentBytes);
        }
    }
}
