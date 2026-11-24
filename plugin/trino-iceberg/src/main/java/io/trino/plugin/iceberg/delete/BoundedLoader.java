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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.AsyncSemaphore;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newSequentialExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static java.lang.Math.clamp;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;

final class BoundedLoader
{
    static final long ONE_MEGABYTE = 1024 * 1024;
    // Lower bound of the file size used to size the window, so that tiny files do not allow an unbounded number of loads in progress
    static final long MIN_LOAD_BYTES = ONE_MEGABYTE / 2;
    // Upper bound of the total size of the files loading at once, unless that would allow fewer than MIN_CONCURRENT_LOADS loads in progress.
    static final long MAX_TOTAL_LOAD_BYTES = 24 * ONE_MEGABYTE;
    static final int MIN_CONCURRENT_LOADS = 8;
    static final int MAX_CONCURRENT_LOADS = toIntExact(MAX_TOTAL_LOAD_BYTES / MIN_LOAD_BYTES);

    private BoundedLoader() {}

    /**
     * Starts a load of each file in order such that the number of loads in progress is bounded.
     * The bound is derived so that the sizes of the files loading at once add up to about
     * {@link #MAX_TOTAL_LOAD_BYTES} based on the largest {@link DeleteFile#fileSizeInBytes}.
     * The returned future completes when all loads finished. If a load fails or the returned future is canceled,
     * the futures of the loads in progress are canceled and the loads that have not yet started never start.
     *
     * @param load starts the load of a file, and returns a future that completes when the load is finished
     */
    static ListenableFuture<Void> loadAll(List<DeleteFile> files, Function<DeleteFile, ListenableFuture<?>> load)
    {
        if (files.isEmpty()) {
            return immediateVoidFuture();
        }
        long largestFileSize = files.stream()
                .mapToLong(DeleteFile::fileSizeInBytes)
                .max()
                .orElseThrow();
        int maxConcurrentLoads = clamp(MAX_TOTAL_LOAD_BYTES / max(largestFileSize, 1), MIN_CONCURRENT_LOADS, MAX_CONCURRENT_LOADS);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ListenableFuture<List<Void>> result = AsyncSemaphore.processAll(
                files,
                file -> {
                    Throwable recordedFailure = failure.get();
                    if (recordedFailure != null) {
                        return immediateFailedFuture(recordedFailure);
                    }
                    ListenableFuture<Void> started = Futures.transform(load.apply(file), _ -> null, directExecutor());
                    addExceptionCallback(started, throwable -> failure.compareAndSet(null, throwable));
                    return started;
                },
                maxConcurrentLoads,
                // Avoid directExecutor usage with AsyncSemaphore, risks stack overflow due to listener chaining.
                newSequentialExecutor(directExecutor()));
        return Futures.transform(result, _ -> null, directExecutor());
    }
}
