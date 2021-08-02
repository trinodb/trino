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
package io.trino.plugin.base.util;

import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.booleans.BooleanOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleHash;
import it.unimi.dsi.fastutil.doubles.DoubleOpenCustomHashSet;
import it.unimi.dsi.fastutil.longs.LongHash;
import it.unimi.dsi.fastutil.longs.LongOpenCustomHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;

import java.lang.invoke.MethodHandle;
import java.util.Collection;
import java.util.Set;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verifyNotNull;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Boolean.TRUE;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Objects.requireNonNull;

public final class FastutilSetHelper
{
    private FastutilSetHelper() {}

    @SuppressWarnings("unchecked")
    public static Set<?> toFastutilHashSet(Set<?> set, Type type, MethodHandle hashCodeHandle, MethodHandle equalsHandle)
    {
        requireNonNull(set, "set is null");
        requireNonNull(type, "type is null");

        // 0.25 as the load factor is chosen because the argument set is assumed to be small (<10000),
        // and the return set is assumed to be read-heavy.
        // The performance of InCodeGenerator heavily depends on the load factor being small.
        Class<?> javaElementType = type.getJavaType();
        if (javaElementType == long.class) {
            return new LongOpenCustomHashSet((Collection<Long>) set, 0.25f, new LongStrategy(hashCodeHandle, equalsHandle));
        }
        if (javaElementType == double.class) {
            return new DoubleOpenCustomHashSet((Collection<Double>) set, 0.25f, new DoubleStrategy(hashCodeHandle, equalsHandle));
        }
        if (javaElementType == boolean.class) {
            return new BooleanOpenHashSet((Collection<Boolean>) set, 0.25f);
        }
        else if (!type.getJavaType().isPrimitive()) {
            return new ObjectOpenCustomHashSet<>(set, 0.25f, new ObjectStrategy(hashCodeHandle, equalsHandle));
        }
        else {
            throw new UnsupportedOperationException("Unsupported native type in set: " + type.getJavaType() + " with type " + type.getTypeSignature());
        }
    }

    public static boolean in(boolean booleanValue, BooleanOpenHashSet set)
    {
        return set.contains(booleanValue);
    }

    public static boolean in(double doubleValue, DoubleOpenCustomHashSet set)
    {
        return set.contains(doubleValue);
    }

    public static boolean in(long longValue, LongOpenCustomHashSet set)
    {
        return set.contains(longValue);
    }

    public static boolean in(Object objectValue, ObjectOpenCustomHashSet<?> set)
    {
        return set.contains(objectValue);
    }

    private static final class LongStrategy
            implements LongHash.Strategy
    {
        private final MethodHandle hashCodeHandle;
        private final MethodHandle equalsHandle;

        public LongStrategy(MethodHandle hashCodeHandle, MethodHandle equalsHandle)
        {
            this.hashCodeHandle = requireNonNull(hashCodeHandle, "hashCodeHandle is null");
            this.equalsHandle = requireNonNull(equalsHandle, "equalsHandle is null");
        }

        @Override
        public int hashCode(long value)
        {
            try {
                return Long.hashCode((long) hashCodeHandle.invokeExact(value));
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, TrinoException.class);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, t);
            }
        }

        @Override
        public boolean equals(long a, long b)
        {
            try {
                Boolean result = (Boolean) equalsHandle.invokeExact(a, b);
                // FastutilHashSet is not intended be used for indeterminate values lookup
                verifyNotNull(result, "result is null");
                return TRUE.equals(result);
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, TrinoException.class);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
    }

    private static final class DoubleStrategy
            implements DoubleHash.Strategy
    {
        private final MethodHandle hashCodeHandle;
        private final MethodHandle equalsHandle;

        public DoubleStrategy(MethodHandle hashCodeHandle, MethodHandle equalsHandle)
        {
            this.hashCodeHandle = requireNonNull(hashCodeHandle, "hashCodeHandle is null");
            this.equalsHandle = requireNonNull(equalsHandle, "equalsHandle is null");
        }

        @Override
        public int hashCode(double value)
        {
            try {
                return Long.hashCode((long) hashCodeHandle.invokeExact(value));
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, TrinoException.class);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, t);
            }
        }

        @Override
        public boolean equals(double a, double b)
        {
            try {
                Boolean result = (Boolean) equalsHandle.invokeExact(a, b);
                // FastutilHashSet is not intended be used for indeterminate values lookup
                verifyNotNull(result, "result is null");
                return TRUE.equals(result);
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, TrinoException.class);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
    }

    private static final class ObjectStrategy
            implements Hash.Strategy<Object>
    {
        private final MethodHandle hashCodeHandle;
        private final MethodHandle equalsHandle;

        public ObjectStrategy(MethodHandle hashCodeHandle, MethodHandle equalsHandle)
        {
            this.hashCodeHandle = requireNonNull(hashCodeHandle, "hashCodeHandle is null").asType(methodType(long.class, Object.class));
            this.equalsHandle = requireNonNull(equalsHandle, "equalsHandle is null").asType(methodType(Boolean.class, Object.class, Object.class));
        }

        @Override
        public int hashCode(Object value)
        {
            if (value == null) {
                return 0;
            }
            try {
                return Long.hashCode((long) hashCodeHandle.invokeExact(value));
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, TrinoException.class);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, t);
            }
        }

        @Override
        public boolean equals(Object a, Object b)
        {
            if (b == null || a == null) {
                return a == null && b == null;
            }
            try {
                Boolean result = (Boolean) equalsHandle.invokeExact(a, b);
                // FastutilHashSet is not intended be used for indeterminate values lookup
                verifyNotNull(result, "result is null");
                return TRUE.equals(result);
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, TrinoException.class);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
    }
}
