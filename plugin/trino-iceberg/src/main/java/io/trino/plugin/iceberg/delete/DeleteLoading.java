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

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;

final class DeleteLoading
{
    // Load files with at least this many records inline on the split thread instead of in the pool.
    // Loading a large equality or position delete file is CPU bound, but the pool is designed for IO bound tasks.
    // DVs are always loaded in the pool: deserializing the bitmap is extremely cheap so the load is IO bound.
    static final long INLINE_LOAD_SIZE = 200_000L;

    private DeleteLoading() {}

    /**
     * Same as {@link Futures#submit(Callable, Executor)} but reports a rejected task through the returned future
     */
    static <T> ListenableFuture<T> submitTask(Callable<T> task, Executor executor)
    {
        try {
            return Futures.submit(task, executor);
        }
        catch (RejectedExecutionException e) {
            return immediateFailedFuture(e);
        }
    }
}
