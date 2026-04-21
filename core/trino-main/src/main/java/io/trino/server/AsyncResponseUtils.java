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
package io.trino.server;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.Futures.catching;
import static com.google.common.util.concurrent.Futures.withTimeout;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class AsyncResponseUtils
{
    private AsyncResponseUtils() {}

    public static <V> ListenableFuture<V> withFallbackAfterTimeout(ListenableFuture<V> future, Duration timeout, Supplier<V> fallback, ScheduledExecutorService timeoutExecutor)
    {
        return catching(withTimeout(future, timeout.toMillis(), MILLISECONDS, timeoutExecutor), TimeoutException.class, _ -> fallback.get(), directExecutor());
    }
}
