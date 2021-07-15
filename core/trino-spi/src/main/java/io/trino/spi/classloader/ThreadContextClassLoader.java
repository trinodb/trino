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
package io.trino.spi.classloader;

import java.util.function.Supplier;

public class ThreadContextClassLoader
{
    private ThreadContextClassLoader() {}

    public static void withClassLoader(ClassLoader newThreadContextClassLoader, Runnable runnable)
    {
        withClassLoader(newThreadContextClassLoader, () -> {
            runnable.run();
            return null;
        });
    }

    public static <T> T withClassLoader(ClassLoader newThreadContextClassLoader, Supplier<T> supplier)
    {
        final ClassLoader originalThreadContextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(newThreadContextClassLoader);
        try {
            return supplier.get();
        }
        finally {
            Thread.currentThread().setContextClassLoader(originalThreadContextClassLoader);
        }
    }

    public static <T, E extends Exception> T withClassLoader(ClassLoader newThreadContextClassLoader, ThrowingSupplier<T, E> supplier)
    {
        final ClassLoader originalThreadContextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(newThreadContextClassLoader);
        try {
            return supplier.get();
        }
        finally {
            Thread.currentThread().setContextClassLoader(originalThreadContextClassLoader);
        }
    }

    @FunctionalInterface
    public interface ThrowingSupplier<T, E extends Exception>
            extends Supplier<T>
    {
        @Override
        default T get()
        {
            try {
                return supplyOrThrow();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        T supplyOrThrow() throws E;
    }
}
