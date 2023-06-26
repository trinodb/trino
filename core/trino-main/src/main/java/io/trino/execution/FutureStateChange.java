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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class FutureStateChange<T>
{
    // Use a separate future for each listener so canceled listeners can be removed
    @GuardedBy("listeners")
    private final Set<SettableFuture<T>> listeners = new HashSet<>();

    public ListenableFuture<T> createNewListener()
    {
        SettableFuture<T> listener = SettableFuture.create();
        synchronized (listeners) {
            listeners.add(listener);
        }

        // remove the listener when the future completes
        listener.addListener(
                () -> {
                    // Only listeners that are canceled before being notified need individual removal from the listeners set,
                    // since all futures are cleared from the listeners set before being notified
                    if (listener.isCancelled()) {
                        synchronized (listeners) {
                            listeners.remove(listener);
                        }
                    }
                },
                directExecutor());

        return listener;
    }

    public void complete(T newState)
    {
        fireStateChange(newState, directExecutor());
    }

    public void complete(T newState, Executor executor)
    {
        fireStateChange(newState, executor);
    }

    private void fireStateChange(T newState, Executor executor)
    {
        requireNonNull(executor, "executor is null");
        List<SettableFuture<T>> futures;
        synchronized (listeners) {
            futures = ImmutableList.copyOf(listeners);
            listeners.clear();
        }

        for (SettableFuture<T> future : futures) {
            executor.execute(() -> future.set(newState));
        }
    }
}
