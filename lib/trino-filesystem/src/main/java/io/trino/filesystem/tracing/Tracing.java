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
package io.trino.filesystem.tracing;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;

import java.util.Optional;

import static io.opentelemetry.api.common.AttributeKey.booleanKey;

public final class Tracing
{
    // This attribute was deprecated and removed from stable attributes
    public static final AttributeKey<Boolean> EXCEPTION_ESCAPED = booleanKey("exception.escaped");

    private Tracing() {}

    public static <T> Attributes attribute(AttributeKey<T> key, Optional<T> optionalValue)
    {
        return optionalValue.map(value -> Attributes.of(key, value))
                .orElseGet(Attributes::empty);
    }

    public static <E extends Exception> void withTracing(Span span, CheckedRunnable<E> runnable)
            throws E
    {
        withTracing(span, () -> {
            runnable.run();
            return null;
        });
    }

    public static <T, E extends Exception> T withTracing(Span span, CheckedSupplier<T, E> supplier)
            throws E
    {
        try (var _ = span.makeCurrent()) {
            return supplier.get();
        }
        catch (Throwable t) {
            span.setStatus(StatusCode.ERROR, t.getMessage());
            span.recordException(t, Attributes.of(EXCEPTION_ESCAPED, true));
            throw t;
        }
        finally {
            span.end();
        }
    }

    public interface CheckedRunnable<E extends Exception>
    {
        void run()
                throws E;
    }

    public interface CheckedSupplier<T, E extends Exception>
    {
        T get()
                throws E;
    }
}
