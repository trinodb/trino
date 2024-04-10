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
package io.trino.plugin.base;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.multibindings.Multibinder;
import jakarta.annotation.PreDestroy;

import java.io.Closeable;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public class ClosingBinder
{
    public static ClosingBinder closingBinder(Binder binder)
    {
        return new ClosingBinder(binder);
    }

    private final Multibinder<ExecutorService> executors;
    private final Multibinder<Closeable> closeables;

    private ClosingBinder(Binder binder)
    {
        executors = newSetBinder(binder, ExecutorService.class, ForCleanup.class);
        closeables = newSetBinder(binder, Closeable.class, ForCleanup.class);
        binder.bind(Cleanup.class).asEagerSingleton();
    }

    public void registerExecutor(Class<? extends ExecutorService> type)
    {
        registerExecutor(Key.get(type));
    }

    public void registerExecutor(Key<? extends ExecutorService> key)
    {
        executors.addBinding().to(requireNonNull(key, "key is null"));
    }

    public void registerCloseable(Class<? extends Closeable> type)
    {
        registerCloseable(Key.get(type));
    }

    public void registerCloseable(Key<? extends Closeable> key)
    {
        closeables.addBinding().to(key);
    }

    private record Cleanup(
            @ForCleanup Set<ExecutorService> executors,
            @ForCleanup Set<Closeable> closeables)
    {
        @Inject
        private Cleanup
        {
            executors = ImmutableSet.copyOf(executors);
            closeables = ImmutableSet.copyOf(closeables);
        }

        @PreDestroy
        public void shutdown()
                throws IOException
        {
            try (Closer closer = Closer.create()) {
                executors.forEach(executor -> closer.register(executor::shutdownNow));
                closeables.forEach(closer::register);
            }
        }
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    private @interface ForCleanup {}
}
