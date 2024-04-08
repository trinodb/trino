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
package io.trino.spi;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToLongFunction;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;

public final class MoreSizeOf
{
    private static final long INSTANT_INSTANCE_SIZE = instanceSize(Instant.class);
    private static final long DURATION_INSTANCE_SIZE = instanceSize(Duration.class);
    private static final long URI_INSTANCE_SIZE = instanceSize(URI.class);

    public static final int ATOMIC_INTEGER_INSTANCE_SIZE = instanceSize(AtomicInteger.class);
    public static final int ATOMIC_LONG_INSTANCE_SIZE = instanceSize(AtomicLong.class);
    public static final int ATOMIC_BOOLEAN_INSTANCE_SIZE = instanceSize(AtomicBoolean.class);

    private static final int ATOMIC_REFERENCE_INSTANCE_SIZE = instanceSize(AtomicReference.class);

    private MoreSizeOf() {}

    public static long sizeOf(Instant instant)
    {
        return INSTANT_INSTANCE_SIZE;
    }

    public static long sizeOf(Duration duration)
    {
        return DURATION_INSTANCE_SIZE;
    }

    public static long sizeOf(URI uri)
    {
        return URI_INSTANCE_SIZE + estimatedSizeOf(uri.toString()) * 2; // rough estimate
    }

    public static <T> long sizeOf(AtomicReference<T> atomicReference, ToLongFunction<T> valueSize)
    {
        if (atomicReference == null) {
            return 0;
        }
        T value = atomicReference.get();
        return ATOMIC_REFERENCE_INSTANCE_SIZE + (value == null ? 0 : valueSize.applyAsLong(value));
    }
}
