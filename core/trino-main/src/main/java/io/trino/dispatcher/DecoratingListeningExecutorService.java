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
package io.trino.dispatcher;

import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DecoratingListeningExecutorService
        extends ForwardingListeningExecutorService
        implements ListeningExecutorService
{
    // TODO remove after requiring Java 19+ for runtime.
    private static final @Nullable MethodHandle CLOSE_METHOD;

    static {
        Method closeMethod;
        try {
            closeMethod = ExecutorService.class.getMethod("close");
        }
        catch (NoSuchMethodException e) {
            closeMethod = null;
        }
        try {
            CLOSE_METHOD = closeMethod != null
                    ? MethodHandles.lookup().unreflect(closeMethod)
                    : null;
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private final ListeningExecutorService delegate;
    private final TaskDecorator decorator;

    public DecoratingListeningExecutorService(ListeningExecutorService delegate, TaskDecorator decorator)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.decorator = requireNonNull(decorator, "decorator is null");
    }

    @Override
    protected ListeningExecutorService delegate()
    {
        return delegate;
    }

    @Override
    public void execute(Runnable command)
    {
        delegate.execute(decorator.decorate(command));
    }

    @Override
    public <T> ListenableFuture<T> submit(Callable<T> task)
    {
        return delegate.submit(decorator.decorate(task));
    }

    @Override
    public ListenableFuture<?> submit(Runnable task)
    {
        return delegate.submit(decorator.decorate(task));
    }

    @Override
    public <T> ListenableFuture<T> submit(Runnable task, T result)
    {
        return delegate.submit(decorator.decorate(task), result);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException
    {
        return delegate.invokeAll(tasks.stream()
                .map(decorator::decorate)
                .collect(toImmutableList()));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException
    {
        return delegate.invokeAll(
                tasks.stream()
                        .map(decorator::decorate)
                        .collect(toImmutableList()),
                timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException
    {
        return delegate.invokeAny(tasks.stream()
                .map(decorator::decorate)
                .collect(toImmutableList()));
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException
    {
        return delegate.invokeAny(
                tasks.stream()
                        .map(decorator::decorate)
                        .collect(toImmutableList()),
                timeout, unit);
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        return super.shutdownNow();
    }

    @Override
    public boolean isShutdown()
    {
        return super.isShutdown();
    }

    @Override
    public boolean isTerminated()
    {
        return super.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException
    {
        return super.awaitTermination(timeout, unit);
    }

    // TODO This is temporary, until Guava's ForwardingExecutorService has the method in their interface. See https://github.com/google/guava/issues/6296
    //@Override
    public void close()
    {
        if (CLOSE_METHOD == null) {
            throw new UnsupportedOperationException("ExecutorService.close has close() method since Java 19. " +
                    "The DecoratingListeningExecutorService supports the method only when run with Java 19 runtime.");
        }
        try {
            CLOSE_METHOD.invoke(delegate());
        }
        catch (Throwable e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public interface TaskDecorator
    {
        Runnable decorate(Runnable command);

        <T> Callable<T> decorate(Callable<T> task);
    }
}
