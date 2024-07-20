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
package io.trino.metastore.tracing;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;

import java.util.function.Supplier;

import static io.opentelemetry.api.trace.StatusCode.ERROR;
import static io.opentelemetry.semconv.ExceptionAttributes.EXCEPTION_ESCAPED;

final class Tracing
{
    private Tracing() {}

    public static void withTracing(Span span, Runnable runnable)
    {
        withTracing(span, () -> {
            runnable.run();
            return null;
        });
    }

    public static <T> T withTracing(Span span, Supplier<T> supplier)
    {
        try (var _ = span.makeCurrent()) {
            return supplier.get();
        }
        catch (Throwable t) {
            span.setStatus(ERROR, t.getMessage());
            span.recordException(t, Attributes.of(EXCEPTION_ESCAPED, true));
            throw t;
        }
        finally {
            span.end();
        }
    }
}
