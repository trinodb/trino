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
package io.trino.testing;

import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import jakarta.annotation.Nullable;

import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class SharedResource<T extends AutoCloseable>
{
    private final Callable<T> factory;
    @GuardedBy("this")
    private long counter;
    @GuardedBy("this")
    @Nullable
    private T instance;

    public SharedResource(Callable<T> factory)
    {
        this.factory = requireNonNull(factory, "factory is null");
    }

    public synchronized Lease<T> getInstanceLease()
            throws Exception
    {
        if (instance == null) {
            checkState(counter == 0, "Expected counter to be 0, but was: %s", counter);
            instance = requireNonNull(factory.call(), "factory.call() is null");
        }
        counter++;
        return new Lease<>(instance, this::returnLease);
    }

    private synchronized Void returnLease()
            throws Exception
    {
        counter--;
        checkState(counter >= 0, "Counter got negative");
        if (counter == 0) {
            requireNonNull(instance, "instance is null")
                    .close();
            instance = null;
        }
        return null;
    }

    public static class Lease<T>
            implements AutoCloseable, Supplier<T>
    {
        private final T instance;
        private final Callable<Void> onClose;

        public Lease(T instance, Callable<Void> onClose)
        {
            this.instance = requireNonNull(instance, "instance is null");
            this.onClose = requireNonNull(onClose, "onClose is null");
        }

        @Override
        public T get()
        {
            return instance;
        }

        @Override
        public void close()
                throws Exception
        {
            onClose.call();
        }
    }
}
