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

import java.io.Closeable;

public class ThreadContextClassLoader
        implements Closeable
{
    private final ClassLoader originalThreadContextClassLoader;

    /**
     * @deprecated Please use {@link #withClassLoader} or {@link #withClassLoaderOf} instead
     */
    @Deprecated
    public ThreadContextClassLoader(ClassLoader newThreadContextClassLoader)
    {
        this.originalThreadContextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(newThreadContextClassLoader);
    }

    @Override
    public void close()
    {
        Thread.currentThread().setContextClassLoader(originalThreadContextClassLoader);
    }

    public static void withClassLoaderOf(Class<?> clazz, Runnable runnable)
    {
        withClassLoader(clazz.getClassLoader(), runnable);
    }

    public static <T, E extends Exception> T withClassLoaderOf(Class<?> clazz, ThrowingSupplier<T, E> supplier)
    {
        return withClassLoader(clazz.getClassLoader(), supplier);
    }

    public static void withClassLoader(ClassLoader newThreadContextClassLoader, Runnable runnable)
    {
        withClassLoader(newThreadContextClassLoader, () -> {
            runnable.run();
            return null;
        });
    }

    public static <T, E extends Exception> T withClassLoader(ClassLoader newThreadContextClassLoader, ThrowingSupplier<T, E> supplier)
    {
        final ClassLoader originalThreadContextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(newThreadContextClassLoader);
        try {
            return supplier.get();
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            Thread.currentThread().setContextClassLoader(originalThreadContextClassLoader);
        }
    }

    @FunctionalInterface
    public interface ThrowingSupplier<T, E extends Exception>
    {
        T get() throws E;
    }
}
