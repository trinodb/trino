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
package io.trino.plugin.exchange.filesystem;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ExecutionStats
{
    private final TimeStat finished = new TimeStat(MILLISECONDS);
    private final TimeStat failed = new TimeStat(MILLISECONDS);

    public <T> T record(Supplier<T> call)
    {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            T result = call.get();
            finished.add(stopwatch.elapsed(MILLISECONDS), MILLISECONDS);
            return result;
        }
        catch (Throwable t) {
            failed.add(stopwatch.elapsed(MILLISECONDS), MILLISECONDS);
            throw t;
        }
    }

    public <T> CompletableFuture<T> record(CompletableFuture<T> future)
    {
        Stopwatch stopwatch = Stopwatch.createStarted();
        future.whenComplete((value, failure) -> {
            if (failure == null) {
                finished.add(stopwatch.elapsed(MILLISECONDS), MILLISECONDS);
            }
            else {
                failed.add(stopwatch.elapsed(MILLISECONDS), MILLISECONDS);
            }
        });
        return future;
    }

    public <T> ListenableFuture<T> record(ListenableFuture<T> future)
    {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Futures.addCallback(future, new FutureCallback<T>()
        {
            @Override
            public void onSuccess(T result)
            {
                finished.add(stopwatch.elapsed(MILLISECONDS), MILLISECONDS);
            }

            @Override
            public void onFailure(Throwable t)
            {
                failed.add(stopwatch.elapsed(MILLISECONDS), MILLISECONDS);
            }
        }, directExecutor());
        return future;
    }

    @Managed
    @Nested
    public TimeStat getFinished()
    {
        return finished;
    }

    @Managed
    @Nested
    public TimeStat getFailed()
    {
        return failed;
    }
}
