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
package io.trino.util;

import com.google.common.util.concurrent.ListeningExecutorService;
import io.trino.spi.VersionEmbedder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.MoreFutures.getDone;

public final class Executors
{
    private Executors() {}

    /**
     * Run all tasks on executor returning as soon as all complete or any task fails.
     * Upon task execution failure, other tasks are cancelled and interrupted, but not waited
     * for.
     */
    public static <T> void executeUntilFailure(Executor executor, Collection<Callable<T>> tasks)
    {
        CompletionService<T> completionService = new ExecutorCompletionService<>(executor);
        List<Future<T>> futures = new ArrayList<>(tasks.size());
        for (Callable<T> task : tasks) {
            futures.add(completionService.submit(task));
        }
        try {
            for (int i = 0; i < futures.size(); i++) {
                getDone(take(completionService));
            }
        }
        catch (Exception failure) {
            try {
                futures.forEach(future -> future.cancel(true));
            }
            catch (RuntimeException e) {
                failure.addSuppressed(e);
            }
            throw failure;
        }
    }

    private static <T> Future<T> take(CompletionService<T> completionService)
    {
        try {
            return completionService.take();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted", e);
        }
    }

    public static ListeningExecutorService decorateWithVersion(ExecutorService executorService, VersionEmbedder versionEmbedder)
    {
        return decorateWithVersion(listeningDecorator(executorService), versionEmbedder);
    }

    public static ListeningExecutorService decorateWithVersion(ListeningExecutorService executorService, VersionEmbedder versionEmbedder)
    {
        return new DecoratingListeningExecutorService(
                executorService,
                new DecoratingListeningExecutorService.TaskDecorator()
                {
                    @Override
                    public Runnable decorate(Runnable command)
                    {
                        return versionEmbedder.embedVersion(command);
                    }

                    @Override
                    public <T> Callable<T> decorate(Callable<T> task)
                    {
                        return versionEmbedder.embedVersion(task);
                    }
                });
    }
}
