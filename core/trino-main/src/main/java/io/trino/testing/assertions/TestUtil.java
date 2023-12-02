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
package io.trino.testing.assertions;

import io.trino.client.FailureException;
import io.trino.client.FailureInfo;
import io.trino.execution.Failure;
import io.trino.spi.TrinoException;
import io.trino.testing.QueryFailedException;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.trino.util.Failures.toFailure;
import static java.util.Objects.requireNonNull;

public final class TestUtil
{
    private TestUtil() {}

    public static Optional<FailureInfo> getFailureInfo(Throwable throwable)
    {
        if (throwable instanceof QueryFailedException queryFailedException) {
            return getFailureInfo(queryFailedException.getCause());
        }
        if (throwable instanceof Failure failure) {
            return Optional.of(failure.getFailureInfo().toFailureInfo());
        }
        if (throwable instanceof FailureException failureException) {
            return Optional.of(failureException.getFailureInfo());
        }
        if (throwable instanceof TrinoException trinoException) {
            return Optional.of(toFailure(trinoException).toFailureInfo());
        }
        return Optional.empty();
    }

    public static <T> void verifyResultOrFailure(Supplier<T> callback, Consumer<T> verifyResults, Consumer<Throwable> verifyFailure)
    {
        requireNonNull(callback, "callback is null");
        requireNonNull(verifyResults, "verifyResults is null");
        requireNonNull(verifyFailure, "verifyFailure is null");

        T result;
        try {
            result = callback.get();
        }
        catch (Throwable t) {
            verifyFailure.accept(t);
            return;
        }
        verifyResults.accept(result);
    }
}
