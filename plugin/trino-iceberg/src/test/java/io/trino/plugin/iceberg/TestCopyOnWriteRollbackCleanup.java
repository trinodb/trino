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
import io.trino.plugin.hive.RollbackAction;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestCopyOnWriteRollbackCleanup
{
    private ExecutorService executor;

    @BeforeEach
    void setup()
    {
        executor = Executors.newFixedThreadPool(4);
    }

    @AfterEach
    void teardown()
            throws InterruptedException
    {
        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void emptyPathListDoesNotSubmitAnyTasks()
    {
        RecordingFileIo fileIo = new RecordingFileIo();
        ExecutorServiceSpy spy = new ExecutorServiceSpy(executor);

        IcebergMetadata.deleteOrphanFilesInParallel(fileIo, List.of(), spy);

        assertThat(spy.submitCount.get()).isZero();
        assertThat(fileIo.deletedPaths).isEmpty();
    }

    @Test
    void deletesEveryPathConcurrently()
    {
        RecordingFileIo fileIo = new RecordingFileIo();
        List<String> paths = List.of(
                "s3://bucket/a.parquet",
                "s3://bucket/b.parquet",
                "s3://bucket/c.parquet");

        IcebergMetadata.deleteOrphanFilesInParallel(fileIo, paths, executor);

        assertThat(fileIo.deletedPaths).containsExactlyInAnyOrderElementsOf(paths);
    }

    @Test
    void individualDeleteFailuresAreLoggedAndDoNotBlockOthers()
    {
        RecordingFileIo fileIo = new RecordingFileIo()
                .failingWhen(path -> path.contains("bad"));
        List<String> paths = List.of(
                "s3://bucket/good-1.parquet",
                "s3://bucket/bad.parquet",
                "s3://bucket/good-2.parquet");

        // Must not throw; failures are best-effort and logged internally.
        IcebergMetadata.deleteOrphanFilesInParallel(fileIo, paths, executor);

        // All paths were attempted; only the non-failing ones are in deletedPaths.
        assertThat(fileIo.attemptedPaths).containsExactlyInAnyOrderElementsOf(paths);
        assertThat(fileIo.deletedPaths).containsExactlyInAnyOrder(
                "s3://bucket/good-1.parquet",
                "s3://bucket/good-2.parquet");
    }

    @Test
    void usesBulkDeleteWhenFileIoSupportsBulkOperations()
    {
        BulkRecordingFileIo bulkFileIo = new BulkRecordingFileIo();
        ExecutorServiceSpy spy = new ExecutorServiceSpy(executor);

        List<String> paths = ImmutableList.of(
                "s3://bucket/a.parquet",
                "s3://bucket/b.parquet",
                "s3://bucket/c.parquet");

        IcebergMetadata.deleteOrphanFilesInParallel(bulkFileIo, paths, spy);

        // The FileIO implements SupportsBulkOperations, so the single-delete fallback
        // executor must be bypassed entirely. Cloud bulk-delete handles fan-out internally.
        assertThat(spy.submitCount.get()).isZero();
        assertThat(bulkFileIo.bulkDeleteCalls.get()).isEqualTo(1);
        assertThat(bulkFileIo.lastBulkPaths).containsExactlyElementsOf(paths);
        // Per-file deleteFile path must not be used when bulk is available.
        assertThat(bulkFileIo.singleDeleteCalls.get()).isZero();
    }

    @Test
    void bulkDeleteFailureIsLoggedButDoesNotThrow()
    {
        BulkRecordingFileIo bulkFileIo = new BulkRecordingFileIo();
        bulkFileIo.throwOnBulkDelete = new RuntimeException("cloud bulk API unavailable");

        // Must not throw; rollback cleanup is best-effort and must never mask the query failure.
        IcebergMetadata.deleteOrphanFilesInParallel(
                bulkFileIo, ImmutableList.of("a", "b"), executor);

        assertThat(bulkFileIo.bulkDeleteCalls.get()).isEqualTo(1);
    }

    @Test
    void cleanupInFlightRewritesIsNoOpWhenRegistryIsEmpty()
    {
        BulkRecordingFileIo fileIo = new BulkRecordingFileIo();
        Set<String> registry = ConcurrentHashMap.newKeySet();

        IcebergMergeSink.cleanupInFlightRewrites(registry, fileIo, executor);

        assertThat(fileIo.bulkDeleteCalls.get()).isZero();
        assertThat(fileIo.singleDeleteCalls.get()).isZero();
        assertThat(registry).isEmpty();
    }

    @Test
    void cleanupInFlightRewritesBulkDeletesAndClearsRegistry()
    {
        BulkRecordingFileIo fileIo = new BulkRecordingFileIo();
        Set<String> registry = ConcurrentHashMap.newKeySet();
        registry.add("s3://bucket/pending-1.parquet");
        registry.add("s3://bucket/pending-2.parquet");

        IcebergMergeSink.cleanupInFlightRewrites(registry, fileIo, executor);

        assertThat(fileIo.bulkDeleteCalls.get()).isEqualTo(1);
        assertThat(fileIo.lastBulkPaths).containsExactlyInAnyOrder(
                "s3://bucket/pending-1.parquet",
                "s3://bucket/pending-2.parquet");
        // Registry must be drained so a second abort() is a no-op and never re-deletes paths.
        assertThat(registry).isEmpty();
    }

    @Test
    void cleanupInFlightRewritesIsIdempotentAcrossRepeatedCalls()
    {
        BulkRecordingFileIo fileIo = new BulkRecordingFileIo();
        Set<String> registry = ConcurrentHashMap.newKeySet();
        registry.add("s3://bucket/pending.parquet");

        IcebergMergeSink.cleanupInFlightRewrites(registry, fileIo, executor);
        IcebergMergeSink.cleanupInFlightRewrites(registry, fileIo, executor);

        // Second call finds an empty registry and must not re-trigger a bulk delete.
        assertThat(fileIo.bulkDeleteCalls.get()).isEqualTo(1);
        assertThat(registry).isEmpty();
    }

    @Test
    void waitsForAllSubmittedTasksBeforeReturning()
            throws InterruptedException
    {
        // Use a single-threaded executor so tasks run serialised, and SlowFileIo(50ms) to keep
        // tasks 2..N still pending when the call returns if the implementation ever regressed
        // to not awaiting. The completedDeletes count is then proof that the method waited:
        // a non-waiting return would leave completedDeletes < paths.size() at the assertion.
        ExecutorService serialExecutor = Executors.newSingleThreadExecutor();
        try {
            SlowFileIo fileIo = new SlowFileIo(/* perDeleteMillis */ 50);
            List<String> paths = List.of("a", "b", "c", "d");

            IcebergMetadata.deleteOrphanFilesInParallel(fileIo, paths, serialExecutor);

            assertThat(fileIo.completedDeletes.get()).isEqualTo(paths.size());
        }
        finally {
            serialExecutor.shutdown();
            assertThat(serialExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void awaitAllRewritesOrRollbackReturnsResultsWhenAllSucceed()
    {
        AtomicInteger rollbackCount = new AtomicInteger();
        CompletableFuture<CopyOnWriteFileRewriter.RewriteResult> a = CompletableFuture.completedFuture(
                stubRewriteResult(() -> rollbackCount.incrementAndGet()));
        CompletableFuture<CopyOnWriteFileRewriter.RewriteResult> b = CompletableFuture.completedFuture(
                stubRewriteResult(() -> rollbackCount.incrementAndGet()));

        List<CopyOnWriteFileRewriter.RewriteResult> results =
                IcebergMergeSink.awaitAllRewritesOrRollback(List.of(a, b));

        assertThat(results).hasSize(2);
        // Success path never invokes rollback; those handles are passed on to the caller.
        assertThat(rollbackCount).hasValue(0);
    }

    @Test
    void awaitAllRewritesOrRollbackRollsBackEverySuccessWhenAnyRewriteFails()
    {
        AtomicInteger rollbackCount = new AtomicInteger();
        CompletableFuture<CopyOnWriteFileRewriter.RewriteResult> successA = CompletableFuture.completedFuture(
                stubRewriteResult(() -> rollbackCount.incrementAndGet()));
        CompletableFuture<CopyOnWriteFileRewriter.RewriteResult> failed = CompletableFuture.failedFuture(
                new RuntimeException("rewrite failed"));
        CompletableFuture<CopyOnWriteFileRewriter.RewriteResult> successB = CompletableFuture.completedFuture(
                stubRewriteResult(() -> rollbackCount.incrementAndGet()));

        assertThatThrownBy(() -> IcebergMergeSink.awaitAllRewritesOrRollback(List.of(successA, failed, successB)))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("rewrite failed");

        // Both successful rewrites must be rolled back, including successB whose future was
        // visited after the failure was observed.
        assertThat(rollbackCount).hasValue(2);
    }

    @Test
    void awaitAllRewritesOrRollbackWaitsForLateCompletingFutureBeforeRollingBack()
            throws Exception
    {
        AtomicInteger rollbackCount = new AtomicInteger();
        CompletableFuture<CopyOnWriteFileRewriter.RewriteResult> failed = CompletableFuture.failedFuture(
                new RuntimeException("boom"));
        CompletableFuture<CopyOnWriteFileRewriter.RewriteResult> pending = new CompletableFuture<>();

        // Run the call on a worker thread so we can observe that it blocks on `pending`.
        CompletableFuture<Throwable> asyncCall = CompletableFuture.supplyAsync(() -> {
            try {
                IcebergMergeSink.awaitAllRewritesOrRollback(List.of(failed, pending));
                return null;
            }
            catch (Throwable t) {
                return t;
            }
        });

        // The method MUST NOT return before `pending` settles; this assertion would regress
        // if we ever reintroduce the eager-cancel pattern.
        assertThat(asyncCall.isDone()).isFalse();

        pending.complete(stubRewriteResult(() -> rollbackCount.incrementAndGet()));

        Throwable thrown = asyncCall.get(5, TimeUnit.SECONDS);
        assertThat(thrown).isInstanceOf(RuntimeException.class).hasMessage("boom");
        // The late success must be rolled back so its committed file is not orphaned.
        assertThat(rollbackCount).hasValue(1);
    }

    @Test
    void awaitAllRewritesOrRollbackAggregatesMultipleFailuresViaSuppression()
    {
        RuntimeException first = new RuntimeException("first failure");
        RuntimeException second = new RuntimeException("second failure");
        CompletableFuture<CopyOnWriteFileRewriter.RewriteResult> failedA = CompletableFuture.failedFuture(first);
        CompletableFuture<CopyOnWriteFileRewriter.RewriteResult> failedB = CompletableFuture.failedFuture(second);

        assertThatThrownBy(() -> IcebergMergeSink.awaitAllRewritesOrRollback(List.of(failedA, failedB)))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("first failure")
                .satisfies(t -> assertThat(t.getSuppressed()).contains(second));
    }

    @Test
    void awaitAllRewritesOrRollbackSuppressesRollbackCloseFailures()
    {
        RuntimeException primary = new RuntimeException("rewrite failed");
        IOException rollbackFailure = new IOException("rollback failed");
        CompletableFuture<CopyOnWriteFileRewriter.RewriteResult> failed = CompletableFuture.failedFuture(primary);
        CompletableFuture<CopyOnWriteFileRewriter.RewriteResult> success = CompletableFuture.completedFuture(
                stubRewriteResult(() -> {
                    throw rollbackFailure;
                }));

        assertThatThrownBy(() -> IcebergMergeSink.awaitAllRewritesOrRollback(List.of(failed, success)))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("rewrite failed")
                .satisfies(t -> assertThat(t.getSuppressed()).contains(rollbackFailure));
    }

    @Test
    void detectsBareCommitStateUnknownException()
    {
        // When the commit state is unknown the catalog may have accepted the snapshot;
        // rollback must NOT delete the just-written files. The detector is the gate for that.
        CommitStateUnknownException unknown = new CommitStateUnknownException(new RuntimeException("HTTP 502"));

        assertThat(IcebergMetadata.hasCommitStateUnknownException(unknown)).isTrue();
    }

    @Test
    void detectsCommitStateUnknownWrappedInRuntimeException()
    {
        // Iceberg's commit path wraps the underlying network/IO failure in a RuntimeException
        // before it surfaces to Trino. The detector must still match.
        RuntimeException wrapper = new RuntimeException(
                "commit failed",
                new CommitStateUnknownException(new RuntimeException("HTTP 502")));

        assertThat(IcebergMetadata.hasCommitStateUnknownException(wrapper)).isTrue();
    }

    @Test
    void detectsDeeplyNestedCommitStateUnknown()
    {
        Throwable deepest = new CommitStateUnknownException(new RuntimeException("timeout"));
        Throwable cause3 = new RuntimeException("layer 3", deepest);
        Throwable cause2 = new RuntimeException("layer 2", cause3);
        Throwable top = new RuntimeException("layer 1", cause2);

        assertThat(IcebergMetadata.hasCommitStateUnknownException(top)).isTrue();
    }

    @Test
    void doesNotMisclassifyRecoverableCommitFailure()
    {
        // CommitFailedException means the commit definitively did not happen and rollback
        // SHOULD clean up; it must not be confused with the unknown-state case.
        CommitFailedException recoverable = new CommitFailedException("snapshot conflict");

        assertThat(IcebergMetadata.hasCommitStateUnknownException(recoverable)).isFalse();
    }

    @Test
    void doesNotMisclassifyArbitraryRuntimeException()
    {
        assertThat(IcebergMetadata.hasCommitStateUnknownException(new RuntimeException("disk full"))).isFalse();
        assertThat(IcebergMetadata.hasCommitStateUnknownException(new IllegalStateException())).isFalse();
    }

    @Test
    void returnsFalseForNullThrowable()
    {
        assertThat(IcebergMetadata.hasCommitStateUnknownException(null)).isFalse();
    }

    @Test
    void terminatesOnTwoNodeCauseCycle()
    {
        // Two-node cycle (a -> b -> a) is the smallest cycle the JDK actually allows
        // (Throwable.initCause forbids self-causation). The detector must terminate via the
        // tortoise-and-hare loop rather than spinning forever.
        CycleNode a = new CycleNode("a");
        CycleNode b = new CycleNode("b");
        a.linkTo(b);
        b.linkTo(a);

        assertThat(IcebergMetadata.hasCommitStateUnknownException(a)).isFalse();
    }

    private static final class CycleNode
            extends RuntimeException
    {
        CycleNode(String message)
        {
            super(message);
        }

        void linkTo(Throwable cause)
        {
            initCause(cause);
        }
    }

    private static CopyOnWriteFileRewriter.RewriteResult stubRewriteResult(RollbackAction rollbackAction)
    {
        CopyOnWriteFileRewriter.RewriteMetrics metrics = new CopyOnWriteFileRewriter.RewriteMetrics(0, 0, 0, 0, 0, 0);
        return new CopyOnWriteFileRewriter.RewriteResult(Optional.empty(), rollbackAction, metrics, new Metrics(0L, null, null, null, null));
    }

    private static final class RecordingFileIo
            implements FileIO
    {
        final Set<String> attemptedPaths = ConcurrentHashMap.newKeySet();
        final Set<String> deletedPaths = ConcurrentHashMap.newKeySet();
        private Predicate<String> failurePredicate = _ -> false;

        RecordingFileIo failingWhen(Predicate<String> predicate)
        {
            this.failurePredicate = predicate;
            return this;
        }

        @Override
        public void deleteFile(String path)
        {
            attemptedPaths.add(path);
            if (failurePredicate.test(path)) {
                throw new RuntimeException("simulated delete failure: " + path);
            }
            deletedPaths.add(path);
        }

        @Override
        public InputFile newInputFile(String path)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputFile newOutputFile(String path)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static final class BulkRecordingFileIo
            implements SupportsBulkOperations
    {
        final AtomicInteger bulkDeleteCalls = new AtomicInteger();
        final AtomicInteger singleDeleteCalls = new AtomicInteger();
        volatile List<String> lastBulkPaths = List.of();
        RuntimeException throwOnBulkDelete;

        @Override
        public void deleteFiles(Iterable<String> pathsToDelete)
        {
            bulkDeleteCalls.incrementAndGet();
            lastBulkPaths = ImmutableList.copyOf(pathsToDelete);
            if (throwOnBulkDelete != null) {
                throw throwOnBulkDelete;
            }
        }

        @Override
        public void deleteFile(String path)
        {
            singleDeleteCalls.incrementAndGet();
        }

        @Override
        public InputFile newInputFile(String path)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputFile newOutputFile(String path)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static final class SlowFileIo
            implements FileIO
    {
        final AtomicInteger completedDeletes = new AtomicInteger();
        private final long perDeleteMillis;

        SlowFileIo(long perDeleteMillis)
        {
            this.perDeleteMillis = perDeleteMillis;
        }

        @Override
        public void deleteFile(String path)
        {
            try {
                Thread.sleep(perDeleteMillis);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            completedDeletes.incrementAndGet();
        }

        @Override
        public InputFile newInputFile(String path)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputFile newOutputFile(String path)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static final class ExecutorServiceSpy
            extends AbstractExecutorService
    {
        final AtomicInteger submitCount = new AtomicInteger();
        private final ExecutorService delegate;

        ExecutorServiceSpy(ExecutorService delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public void execute(Runnable command)
        {
            submitCount.incrementAndGet();
            delegate.execute(command);
        }

        @Override
        public void shutdown()
        {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown()
        {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated()
        {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit)
                throws InterruptedException
        {
            return delegate.awaitTermination(timeout, unit);
        }
    }
}
