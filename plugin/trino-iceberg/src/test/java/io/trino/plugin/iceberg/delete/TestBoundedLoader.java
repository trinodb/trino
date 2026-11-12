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
package io.trino.plugin.iceberg.delete;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.google.common.util.concurrent.Futures.getDone;
import static io.trino.plugin.iceberg.delete.BoundedLoader.MAX_CONCURRENT_LOADS;
import static io.trino.plugin.iceberg.delete.BoundedLoader.MAX_TOTAL_LOAD_BYTES;
import static io.trino.plugin.iceberg.delete.BoundedLoader.MIN_CONCURRENT_LOADS;
import static io.trino.plugin.iceberg.delete.BoundedLoader.ONE_MEGABYTE;
import static io.trino.plugin.iceberg.delete.BoundedLoader.loadAll;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestBoundedLoader
{
    private static final long LARGE_FILE = 1024 * ONE_MEGABYTE;

    @Test
    void testEmpty()
    {
        RecordingLoads loads = new RecordingLoads();
        assertThat(loadAll(ImmutableList.of(), loads).isDone()).isTrue();
        assertThat(loads.started()).isEmpty();
    }

    @Test
    void testTinyFilesStartMaximumWindow()
    {
        // tiny files are bounded by the minimum cost of an opened file
        RecordingLoads loads = new RecordingLoads();
        loadAll(files(MAX_CONCURRENT_LOADS * 2, 1), loads);

        assertThat(loads.started()).containsExactlyElementsOf(range(0, MAX_CONCURRENT_LOADS));
    }

    @Test
    void testFileCountBelowMinimum()
    {
        RecordingLoads loads = new RecordingLoads();
        loadAll(files(3, ONE_MEGABYTE), loads);

        assertThat(loads.started()).containsExactly(0, 1, 2);
    }

    @Test
    void testWindowIsTotalBudgetDividedByFileSize()
    {
        // 1 MB files: the byte budget allows MAX_TOTAL_LOAD_BYTES / 1 MB loads
        int expected = toIntExact(MAX_TOTAL_LOAD_BYTES / ONE_MEGABYTE);
        assertThat(expected).isEqualTo(24);

        RecordingLoads loads = new RecordingLoads();
        loadAll(files(expected * 2, ONE_MEGABYTE), loads);

        assertThat(loads.started()).containsExactlyElementsOf(range(0, expected));
    }

    @Test
    void testWindowUsesLargestFile()
    {
        List<DeleteFile> files = new ArrayList<>(files(MAX_CONCURRENT_LOADS * 2, 1));
        files.set(7, file(7, MAX_TOTAL_LOAD_BYTES / MIN_CONCURRENT_LOADS));
        RecordingLoads loads = new RecordingLoads();
        loadAll(ImmutableList.copyOf(files), loads);

        assertThat(loads.started()).containsExactlyElementsOf(range(0, MIN_CONCURRENT_LOADS));
    }

    @Test
    void testLargeFilesUseMinimumWindow()
    {
        RecordingLoads loads = new RecordingLoads();
        loadAll(files(MAX_CONCURRENT_LOADS * 2, LARGE_FILE), loads);

        assertThat(loads.started()).containsExactlyElementsOf(range(0, MIN_CONCURRENT_LOADS));
    }

    @Test
    void testZeroSizedFiles()
    {
        RecordingLoads loads = new RecordingLoads();
        loadAll(files(MAX_CONCURRENT_LOADS * 2, 0), loads);

        assertThat(loads.started()).containsExactlyElementsOf(range(0, MAX_CONCURRENT_LOADS));
    }

    @Test
    void testNextLoadStartsWhenAnyLoadInProgressCompletes()
    {
        RecordingLoads loads = new RecordingLoads();
        loadAll(files(MIN_CONCURRENT_LOADS * 3, LARGE_FILE), loads);
        assertThat(loads.started()).containsExactlyElementsOf(range(0, MIN_CONCURRENT_LOADS));

        // a slow load does not hold up the others, completing any load starts the next one in order
        loads.complete(3);
        assertThat(loads.started()).containsExactlyElementsOf(range(0, MIN_CONCURRENT_LOADS + 1));

        loads.complete(5);
        assertThat(loads.started()).containsExactlyElementsOf(range(0, MIN_CONCURRENT_LOADS + 2));

        // the load that was started later also frees its slot
        loads.complete(MIN_CONCURRENT_LOADS);
        assertThat(loads.started()).containsExactlyElementsOf(range(0, MIN_CONCURRENT_LOADS + 3));
    }

    @Test
    void testEveryLoadRunsOnceAndAllFuturesComplete()
            throws Exception
    {
        int count = MIN_CONCURRENT_LOADS * 3 + 2;
        RecordingLoads loads = new RecordingLoads();
        ListenableFuture<Void> all = loadAll(files(count, LARGE_FILE), loads);

        // complete in start order until nothing is left to start
        for (int next = 0; next < count; next++) {
            assertThat(loads.started()).contains(next);
            assertThat(all.isDone()).isFalse();
            loads.complete(next);
        }

        assertThat(all.isDone()).isTrue();
        getDone(all);
        assertThat(loads.started()).containsExactlyElementsOf(range(0, count));
    }

    @Test
    void testFailureCancelsRunningLoadsAndStopsLaterLoads()
    {
        RecordingLoads loads = new RecordingLoads();
        ListenableFuture<Void> all = loadAll(files(MIN_CONCURRENT_LOADS * 2, LARGE_FILE), loads);

        loads.fail(2, new IllegalStateException("open failed"));

        assertThat(all.isDone()).isTrue();
        assertThatThrownBy(() -> getDone(all))
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseMessage("open failed");
        // the other running loads are canceled
        for (int index = 0; index < MIN_CONCURRENT_LOADS; index++) {
            if (index != 2) {
                assertThat(loads.future(index).isCancelled()).as("load %s is canceled", index).isTrue();
            }
        }
        // nothing that waited for a load in a chain starts
        assertThat(loads.started()).containsExactlyElementsOf(range(0, MIN_CONCURRENT_LOADS));
    }

    @Test
    void testFailureOfLoadStartedLaterCancelsRunningLoadsAndStopsLaterLoads()
    {
        RecordingLoads loads = new RecordingLoads();
        int count = MIN_CONCURRENT_LOADS * 3;
        ListenableFuture<Void> all = loadAll(files(count, LARGE_FILE), loads);

        // each completed load lets the next queued load start
        loads.complete(1);
        loads.complete(4);
        assertThat(loads.started()).containsExactlyElementsOf(range(0, MIN_CONCURRENT_LOADS + 2));
        int laterLoad = MIN_CONCURRENT_LOADS + 1;

        loads.fail(laterLoad, new IllegalStateException("open failed"));

        assertThat(all.isDone()).isTrue();
        assertThatThrownBy(() -> getDone(all))
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseMessage("open failed");
        for (int index = 0; index < MIN_CONCURRENT_LOADS + 2; index++) {
            if (index == 1 || index == 4) {
                // finished before the failure
                assertThat(loads.future(index).isCancelled()).as("completed load %s is not canceled", index).isFalse();
            }
            else if (index != laterLoad) {
                assertThat(loads.future(index).isCancelled()).as("load %s is canceled", index).isTrue();
            }
        }
        // no queued load starts, not even the ones that freed slots by being canceled
        assertThat(loads.started()).containsExactlyElementsOf(range(0, MIN_CONCURRENT_LOADS + 2));
    }

    @Test
    void testFailureOfLoadIsTheCauseOfTheResult()
    {
        RecordingLoads loads = new RecordingLoads();
        ListenableFuture<Void> all = loadAll(files(MIN_CONCURRENT_LOADS * 3, LARGE_FILE), loads);

        // the loads that are queued are skipped when this fails, and they must not hide why
        IllegalStateException failure = new IllegalStateException("open failed");
        loads.fail(2, failure);

        assertThat(all.isDone()).isTrue();
        assertThatThrownBy(() -> getDone(all))
                .isInstanceOf(ExecutionException.class)
                .hasCause(failure);
        assertThat(loads.started()).containsExactlyElementsOf(range(0, MIN_CONCURRENT_LOADS));
    }

    @Test
    void testCancelCancelsRunningLoadsAndStopsLaterLoads()
    {
        RecordingLoads loads = new RecordingLoads();
        ListenableFuture<Void> all = loadAll(files(MIN_CONCURRENT_LOADS * 2, LARGE_FILE), loads);

        all.cancel(true);

        assertThatThrownBy(() -> getDone(all)).isInstanceOf(CancellationException.class);
        for (int index = 0; index < MIN_CONCURRENT_LOADS; index++) {
            assertThat(loads.future(index).isCancelled()).as("load %s is canceled", index).isTrue();
        }
        assertThat(loads.started()).containsExactlyElementsOf(range(0, MIN_CONCURRENT_LOADS));
    }

    @Test
    void testCancelWithManyQueuedLoads()
    {
        RecordingLoads loads = new RecordingLoads();
        ListenableFuture<Void> all = loadAll(files(100_000, LARGE_FILE), loads);

        all.cancel(true);

        assertThatThrownBy(() -> getDone(all)).isInstanceOf(CancellationException.class);
        assertThat(loads.started()).containsExactlyElementsOf(range(0, MIN_CONCURRENT_LOADS));
    }

    @Test
    void testFailureWithManyQueuedLoads()
    {
        RecordingLoads loads = new RecordingLoads();
        ListenableFuture<Void> all = loadAll(files(100_000, LARGE_FILE), loads);

        loads.fail(0, new IllegalStateException("open failed"));

        assertThatThrownBy(() -> getDone(all))
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseMessage("open failed");
        assertThat(loads.started()).containsExactlyElementsOf(range(0, MIN_CONCURRENT_LOADS));
    }

    @Test
    void testCompletedLoadsAreNotCanceledByLaterFailure()
    {
        RecordingLoads loads = new RecordingLoads();
        ListenableFuture<Void> all = loadAll(files(MIN_CONCURRENT_LOADS * 2, LARGE_FILE), loads);

        loads.complete(0);
        loads.fail(1, new IllegalStateException("open failed"));

        assertThat(all.isDone()).isTrue();
        assertThat(loads.future(0).isCancelled()).isFalse();
        // the load that followed the completed one did start before the failure
        assertThat(loads.started()).contains(MIN_CONCURRENT_LOADS);
    }

    private static List<DeleteFile> files(int count, long sizeInBytes)
    {
        ImmutableList.Builder<DeleteFile> files = ImmutableList.builder();
        for (int i = 0; i < count; i++) {
            files.add(file(i, sizeInBytes));
        }
        return files.build();
    }

    private static DeleteFile file(int index, long sizeInBytes)
    {
        return new DeleteFile(
                FileContent.POSITION_DELETES,
                "s3://bucket/delete-" + index + ".parquet",
                FileFormat.PARQUET,
                1,
                sizeInBytes,
                ImmutableList.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                1,
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static int indexOf(DeleteFile file)
    {
        // paths are built by file(index, size) as "s3://bucket/delete-<index>.parquet"
        String path = file.path();
        return Integer.parseInt(path.substring(path.lastIndexOf('-') + 1, path.lastIndexOf('.')));
    }

    private static List<Integer> range(int startInclusive, int endExclusive)
    {
        ImmutableList.Builder<Integer> range = ImmutableList.builder();
        for (int i = startInclusive; i < endExclusive; i++) {
            range.add(i);
        }
        return range.build();
    }

    /**
     * Records which loads started and lets the test decide when each one finishes.
     */
    private static class RecordingLoads
            implements Function<DeleteFile, ListenableFuture<?>>
    {
        private final List<Integer> started = new ArrayList<>();
        private final Map<Integer, SettableFuture<Void>> futuresByIndex = new HashMap<>();

        @Override
        public synchronized ListenableFuture<?> apply(DeleteFile file)
        {
            int index = indexOf(file);
            assertThat(futuresByIndex).doesNotContainKey(index);
            started.add(index);
            SettableFuture<Void> future = SettableFuture.create();
            futuresByIndex.put(index, future);
            return future;
        }

        public synchronized List<Integer> started()
        {
            return ImmutableList.copyOf(started);
        }

        public synchronized SettableFuture<Void> future(int index)
        {
            SettableFuture<Void> future = futuresByIndex.get(index);
            assertThat(future).as("load %s was started", index).isNotNull();
            return future;
        }

        public void complete(int index)
        {
            future(index).set(null);
        }

        public void fail(int index, Throwable failure)
        {
            future(index).setException(failure);
        }
    }
}
