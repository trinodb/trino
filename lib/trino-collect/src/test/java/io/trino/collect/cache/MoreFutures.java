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
package io.trino.collect.cache;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.google.common.base.Throwables.propagateIfPossible;
import static java.util.Objects.requireNonNull;

final class MoreFutures
{
    private MoreFutures() {}

    /**
     * Copy of {@code io.airlift.concurrent.MoreFutures#getFutureValue}, see there for documentation.
     */
    public static <V> V getFutureValue(Future<V> future)
    {
        return getFutureValue(future, RuntimeException.class);
    }

    /**
     * Copy of {@code io.airlift.concurrent.MoreFutures#getFutureValue}, see there for documentation.
     */
    public static <V, E extends Exception> V getFutureValue(Future<V> future, Class<E> exceptionType)
            throws E
    {
        requireNonNull(future, "future is null");
        requireNonNull(exceptionType, "exceptionType is null");

        try {
            return future.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted", e);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            propagateIfPossible(cause, exceptionType);
            throw new RuntimeException(cause);
        }
    }
}
