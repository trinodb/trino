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
package io.prestosql.plugin.hive.metastore.cache;

import io.airlift.concurrent.BoundedExecutor;

import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

/**
 * Extension of {@link BoundedExecutor} that will skip task queue when recursive tasks are scheduled
 * from within currently executed task.
 */
public class ReentrantBoundedExecutor
        implements Executor
{
    private final ThreadLocal<Boolean> executorThreadMarkers = ThreadLocal.withInitial(() -> false);
    private final Executor boundedExecutor;
    private final Executor coreExecutor;

    ReentrantBoundedExecutor(Executor coreExecutor, int maxThreads)
    {
        this.boundedExecutor = new BoundedExecutor(requireNonNull(coreExecutor, "coreExecutor is null"), maxThreads);
        this.coreExecutor = coreExecutor;
    }

    @Override
    public void execute(Runnable task)
    {
        if (executorThreadMarkers.get()) {
            // schedule recursive task immediately as it's being scheduled from currently executed task
            coreExecutor.execute(task);
            return;
        }

        boundedExecutor.execute(() -> {
            executorThreadMarkers.set(true);
            try {
                task.run();
            }
            finally {
                executorThreadMarkers.remove();
            }
        });
    }
}
