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

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

final class TestCopyOnWriteStats
{
    @Test
    void allCountersStartAtZero()
    {
        CopyOnWriteStats stats = new CopyOnWriteStats();

        assertThat(stats.getCommits().getTotalCount()).isZero();
        assertThat(stats.getRewrittenFiles().getTotalCount()).isZero();
        assertThat(stats.getRewrittenOldBytes().getTotalCount()).isZero();
        assertThat(stats.getRewrittenNewBytes().getTotalCount()).isZero();
        assertThat(stats.getDanglingDeletesRemoved().getTotalCount()).isZero();
        assertThat(stats.getCommitStateUnknown().getTotalCount()).isZero();
        assertThat(stats.getOrphanFilesCleanedUp().getTotalCount()).isZero();
        assertThat(stats.getUniqueRewriteTaskInvariantViolations().getTotalCount()).isZero();
    }

    @Test
    void recordCommitAccumulatesAcrossInvocations()
    {
        CopyOnWriteStats stats = new CopyOnWriteStats();

        stats.recordCommit(3, 1_000L, 800L);
        stats.recordCommit(2, 500L, 300L);

        assertThat(stats.getCommits().getTotalCount()).isEqualTo(2);
        assertThat(stats.getRewrittenFiles().getTotalCount()).isEqualTo(5);
        assertThat(stats.getRewrittenOldBytes().getTotalCount()).isEqualTo(1_500L);
        assertThat(stats.getRewrittenNewBytes().getTotalCount()).isEqualTo(1_100L);
    }

    @Test
    void recordCommitWithZeroFilesStillIncrementsCommitsCounter()
    {
        // Insert-only MERGE produces no rewrites but is still committed via the CoW path; we
        // must observe the commit even though the byte/file deltas are zero.
        CopyOnWriteStats stats = new CopyOnWriteStats();

        stats.recordCommit(0, 0L, 0L);

        assertThat(stats.getCommits().getTotalCount()).isEqualTo(1);
        assertThat(stats.getRewrittenFiles().getTotalCount()).isZero();
        assertThat(stats.getRewrittenOldBytes().getTotalCount()).isZero();
        assertThat(stats.getRewrittenNewBytes().getTotalCount()).isZero();
    }

    @Test
    void negativeOrZeroByteDeltasAreIgnoredButDoNotCorruptCounter()
    {
        // Defensive: a buggy caller passing negative deltas must not silently subtract from a
        // monotonic counter (which would mislead operators reading the JMX value).
        CopyOnWriteStats stats = new CopyOnWriteStats();

        stats.recordCommit(0, -1L, -1L);
        stats.recordCommit(0, 0L, 0L);

        assertThat(stats.getRewrittenOldBytes().getTotalCount()).isZero();
        assertThat(stats.getRewrittenNewBytes().getTotalCount()).isZero();
        assertThat(stats.getCommits().getTotalCount()).isEqualTo(2);
    }

    @Test
    void recordsCommitStateUnknownAndOrphanCleanupIndependently()
    {
        CopyOnWriteStats stats = new CopyOnWriteStats();

        stats.recordCommitStateUnknown();
        stats.recordCommitStateUnknown();
        stats.recordOrphanFilesCleanedUp(7);

        assertThat(stats.getCommitStateUnknown().getTotalCount()).isEqualTo(2);
        assertThat(stats.getOrphanFilesCleanedUp().getTotalCount()).isEqualTo(7);
    }

    @Test
    void recordsDanglingDeletesAndInvariantViolationsIndependently()
    {
        CopyOnWriteStats stats = new CopyOnWriteStats();

        stats.recordDanglingDeletesRemoved(4);
        stats.recordDanglingDeletesRemoved(0);
        stats.recordUniqueRewriteTaskInvariantViolation();

        assertThat(stats.getDanglingDeletesRemoved().getTotalCount()).isEqualTo(4);
        assertThat(stats.getUniqueRewriteTaskInvariantViolations().getTotalCount()).isEqualTo(1);
    }

    @Test
    void counterIncrementsAreThreadSafe()
            throws Exception
    {
        // Smoke-test the underlying CounterStat: 16 threads each record 10_000 commits should
        // sum to exactly 160_000. A naive non-atomic counter would lose updates under contention.
        int threads = 16;
        int incrementsPerThread = 10_000;
        CopyOnWriteStats stats = new CopyOnWriteStats();
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            for (int worker = 0; worker < threads; worker++) {
                executor.execute(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < incrementsPerThread; i++) {
                            stats.recordCommit(1, 1L, 1L);
                        }
                    }
                    catch (Throwable error) {
                        failure.compareAndSet(null, error);
                    }
                    finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isNull();

            long expected = (long) threads * incrementsPerThread;
            assertThat(stats.getCommits().getTotalCount()).isEqualTo(expected);
            assertThat(stats.getRewrittenFiles().getTotalCount()).isEqualTo(expected);
            assertThat(stats.getRewrittenOldBytes().getTotalCount()).isEqualTo(expected);
            assertThat(stats.getRewrittenNewBytes().getTotalCount()).isEqualTo(expected);
        }
        finally {
            executor.shutdownNow();
        }
    }
}
