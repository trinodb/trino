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
package io.prestosql.util;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.FunctionInvoker;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.booleans.BooleanOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleHash;
import it.unimi.dsi.fastutil.doubles.DoubleOpenCustomHashSet;
import it.unimi.dsi.fastutil.longs.LongHash;
import it.unimi.dsi.fastutil.longs.LongOpenCustomHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verifyNotNull;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static java.lang.Boolean.TRUE;
import static java.util.Objects.requireNonNull;

public final class FastutilSetHelper
{
    private FastutilSetHelper() {}

    public static Set<?> toFastutilHashSet(Set<?> set, Type type, Metadata metadata)
    {
        return toFastutilHashSet(
                set,
                type,
                metadata.getScalarFunctionInvoker(metadata.resolveOperator(HASH_CODE, ImmutableList.of(type)), Optional.empty()),
                metadata.getScalarFunctionInvoker(metadata.resolveOperator(EQUAL, ImmutableList.of(type, type)), Optional.empty()));
    }

    @SuppressWarnings("unchecked")
    public static Set<?> toFastutilHashSet(Set<?> set, Type type, FunctionInvoker hashCode, FunctionInvoker equals)
    {
        requireNonNull(set, "set is null");
        requireNonNull(type, "type is null");
        requireNonNull(hashCode, "hashCode is null");
        checkArgument(hashCode.getInstanceFactory().isEmpty(), "hashCode method has instance factory");
        requireNonNull(equals, "equals is null");
        checkArgument(equals.getInstanceFactory().isEmpty(), "equals method has instance factory");

        // 0.25 as the load factor is chosen because the argument set is assumed to be small (<10000),
        // and the return set is assumed to be read-heavy.
        // The performance of InCodeGenerator heavily depends on the load factor being small.
        Class<?> javaElementType = type.getJavaType();
        if (javaElementType == long.class) {
            return new LongOpenCustomHashSet((Collection<Long>) set, 0.25f, new LongStrategy(hashCode, equals));
        }
        if (javaElementType == double.class) {
            return new DoubleOpenCustomHashSet((Collection<Double>) set, 0.25f, new DoubleStrategy(hashCode, equals));
        }
        if (javaElementType == boolean.class) {
            return new BooleanOpenHashSet((Collection<Boolean>) set, 0.25f);
        }
        else if (!type.getJavaType().isPrimitive()) {
            return new ObjectOpenCustomHashSet<>(set, 0.25f, new ObjectStrategy(hashCode, equals));
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

        private LongStrategy(FunctionInvoker hashCode, FunctionInvoker equals)
        {
            hashCodeHandle = hashCode.getMethodHandle();
            equalsHandle = equals.getMethodHandle();
        }

        @Override
        public int hashCode(long value)
        {
            try {
                return Long.hashCode((long) hashCodeHandle.invokeExact(value));
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
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
                throwIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
    }

    private static final class DoubleStrategy
            implements DoubleHash.Strategy
    {
        private final MethodHandle hashCodeHandle;
        private final MethodHandle equalsHandle;

        private DoubleStrategy(FunctionInvoker hashCode, FunctionInvoker equals)
        {
            hashCodeHandle = hashCode.getMethodHandle();
            equalsHandle = equals.getMethodHandle();
        }

        @Override
        public int hashCode(double value)
        {
            try {
                return Long.hashCode((long) hashCodeHandle.invokeExact(value));
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
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
                throwIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
    }

    private static final class ObjectStrategy
            implements Hash.Strategy<Object>
    {
        private final MethodHandle hashCodeHandle;
        private final MethodHandle equalsHandle;

        private ObjectStrategy(FunctionInvoker hashCode, FunctionInvoker equals)
        {
            hashCodeHandle = hashCode.getMethodHandle().asType(MethodType.methodType(long.class, Object.class));
            equalsHandle = equals.getMethodHandle().asType(MethodType.methodType(Boolean.class, Object.class, Object.class));
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
                throwIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
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
                throwIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
    }
}
