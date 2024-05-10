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
import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.multibindings.Multibinder;
import io.trino.plugin.base.util.AutoCloseableCloser;
import jakarta.annotation.PreDestroy;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static com.google.common.base.Verify.verifyNotNull;
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
    private final Multibinder<AutoCloseable> closeables;

    private ClosingBinder(Binder binder)
    {
        executors = newSetBinder(binder, ExecutorService.class, ForCleanup.class);
        closeables = newSetBinder(binder, AutoCloseable.class, ForCleanup.class);
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

    public void registerCloseable(Class<? extends AutoCloseable> type)
    {
        registerCloseable(Key.get(type));
    }

    public void registerCloseable(Key<? extends AutoCloseable> key)
    {
        closeables.addBinding().to(key);
    }

    public <T> void registerResource(Class<T> type, Consumer<? super T> close)
    {
        registerResource(Key.get(type), close);
    }

    public <T> void registerResource(Key<T> key, Consumer<? super T> close)
    {
        closeables.addBinding().toProvider(new ResourceCloser<T>(key, close));
    }

    private static class ResourceCloser<T>
            implements Provider<AutoCloseable>
    {
        private final Key<T> key;
        private final Consumer<? super T> close;
        private Injector injector;

        private ResourceCloser(Key<T> key, Consumer<? super T> close)
        {
            this.key = requireNonNull(key, "key is null");
            this.close = requireNonNull(close, "close is null");
        }

        @Inject
        public void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Override
        public AutoCloseable get()
        {
            T object = injector.getInstance(key);
            verifyNotNull(object, "null at key %s", key);
            Consumer<? super T> close = this.close;
            return () -> close.accept(object);
        }
    }

    private record Cleanup(
            @ForCleanup Set<ExecutorService> executors,
            @ForCleanup Set<AutoCloseable> closeables)
    {
        @Inject
        private Cleanup
        {
            executors = ImmutableSet.copyOf(executors);
            closeables = ImmutableSet.copyOf(closeables);
        }

        @PreDestroy
        public void shutdown()
                throws Exception
        {
            try (var closer = AutoCloseableCloser.create()) {
                // TODO should this await termination?
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
