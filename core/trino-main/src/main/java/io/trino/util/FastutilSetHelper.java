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
package io.trino.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.trino.collect.cache.NonEvictableCache;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.booleans.BooleanOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleHash;
import it.unimi.dsi.fastutil.doubles.DoubleOpenCustomHashSet;
import it.unimi.dsi.fastutil.longs.LongHash;
import it.unimi.dsi.fastutil.longs.LongOpenCustomHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;

import javax.annotation.concurrent.GuardedBy;

import java.lang.invoke.MethodHandle;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verifyNotNull;
import static io.trino.collect.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.util.SingleAccessMethodCompiler.compileSingleAccessMethod;
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
            return new LongOpenCustomHashSet((Collection<Long>) set, 0.25f, new LongStrategy(hashCodeHandle, equalsHandle, type));
        }
        if (javaElementType == double.class) {
            return new DoubleOpenCustomHashSet((Collection<Double>) set, 0.25f, new DoubleStrategy(hashCodeHandle, equalsHandle, type));
        }
        if (javaElementType == boolean.class) {
            return new BooleanOpenHashSet((Collection<Boolean>) set, 0.25f);
        }
        if (!type.getJavaType().isPrimitive()) {
            return new ObjectOpenCustomHashSet<>(set, 0.25f, new ObjectStrategy(hashCodeHandle, equalsHandle, type));
        }
        throw new UnsupportedOperationException("Unsupported native type in set: " + type.getJavaType() + " with type " + type.getTypeSignature());
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
        private final LongHashCode longHashCode;
        private final LongEquals longEquals;

        public LongStrategy(MethodHandle hashCodeHandle, MethodHandle equalsHandle, Type type)
        {
            requireNonNull(hashCodeHandle, "hashCodeHandle is null");
            requireNonNull(equalsHandle, "equalsHandle is null");
            requireNonNull(type, "type is null");

            this.longHashCode = MethodGenerator.getGeneratedMethod(type, LongHashCode.class, hashCodeHandle);
            this.longEquals = MethodGenerator.getGeneratedMethod(type, LongEquals.class, equalsHandle);
        }

        // Needs to be public for compileSingleAccessMethod
        public interface LongHashCode
        {
            long hashCode(long value);
        }

        // Needs to be public for compileSingleAccessMethod
        public interface LongEquals
        {
            Boolean equals(long a, long b);
        }

        @Override
        public int hashCode(long value)
        {
            return Long.hashCode(longHashCode.hashCode(value));
        }

        @Override
        public boolean equals(long a, long b)
        {
            Boolean result = longEquals.equals(a, b);
            // FastutilHashSet is not intended be used for indeterminate values lookup
            verifyNotNull(result, "result is null");
            return TRUE.equals(result);
        }
    }

    private static final class DoubleStrategy
            implements DoubleHash.Strategy
    {
        private final DoubleHashCode doubleHashCode;
        private final DoubleEquals doubleEquals;

        public DoubleStrategy(MethodHandle hashCodeHandle, MethodHandle equalsHandle, Type type)
        {
            requireNonNull(hashCodeHandle, "hashCodeHandle is null");
            requireNonNull(equalsHandle, "equalsHandle is null");
            requireNonNull(type, "type is null");

            this.doubleHashCode = MethodGenerator.getGeneratedMethod(type, DoubleHashCode.class, hashCodeHandle);
            this.doubleEquals = MethodGenerator.getGeneratedMethod(type, DoubleEquals.class, equalsHandle);
        }

        // Needs to be public for compileSingleAccessMethod
        public interface DoubleHashCode
        {
            long hashCode(double value);
        }

        // Needs to be public for compileSingleAccessMethod
        public interface DoubleEquals
        {
            Boolean equals(double a, double b);
        }

        @Override
        public int hashCode(double value)
        {
            return Long.hashCode(doubleHashCode.hashCode(value));
        }

        @Override
        public boolean equals(double a, double b)
        {
            Boolean result = doubleEquals.equals(a, b);
            // FastutilHashSet is not intended be used for indeterminate values lookup
            verifyNotNull(result, "result is null");
            return TRUE.equals(result);
        }
    }

    private static final class ObjectStrategy
            implements Hash.Strategy<Object>
    {
        private final ObjectHashCode objectHashCode;
        private final ObjectEquals objectEquals;

        public ObjectStrategy(MethodHandle hashCodeHandle, MethodHandle equalsHandle, Type type)
        {
            requireNonNull(hashCodeHandle, "hashCodeHandle is null");
            requireNonNull(equalsHandle, "equalsHandle is null");
            requireNonNull(type, "type is null");

            this.objectHashCode = MethodGenerator.getGeneratedMethod(
                    type,
                    ObjectHashCode.class,
                    hashCodeHandle.asType(methodType(long.class, Object.class)));
            this.objectEquals = MethodGenerator.getGeneratedMethod(
                    type,
                    ObjectEquals.class,
                    equalsHandle.asType(methodType(Boolean.class, Object.class, Object.class)));
        }

        // Needs to be public for compileSingleAccessMethod
        public interface ObjectHashCode
        {
            long hashCode(Object value);
        }

        // Needs to be public for compileSingleAccessMethod
        public interface ObjectEquals
        {
            Boolean equals(Object a, Object b);
        }

        @Override
        public int hashCode(Object value)
        {
            if (value == null) {
                return 0;
            }
            return Long.hashCode(objectHashCode.hashCode(value));
        }

        @Override
        public boolean equals(Object a, Object b)
        {
            if (b == null || a == null) {
                return a == null && b == null;
            }
            Boolean result = objectEquals.equals(a, b);
            // FastutilHashSet is not intended be used for indeterminate values lookup
            verifyNotNull(result, "result is null");
            return TRUE.equals(result);
        }
    }

    private static class MethodGenerator
    {
        private static final NonEvictableCache<MethodKey<?>, GeneratedMethod<?>> generatedMethodCache = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .maximumSize(1_000)
                        .expireAfterWrite(2, TimeUnit.HOURS));

        private static <T> T getGeneratedMethod(Type type, Class<T> operatorInterface, MethodHandle methodHandle)
        {
            try {
                @SuppressWarnings("unchecked")
                GeneratedMethod<T> generatedMethod = (GeneratedMethod<T>) uncheckedCacheGet(
                        generatedMethodCache,
                        new MethodKey<>(type, operatorInterface),
                        () -> new GeneratedMethod<>(type, operatorInterface, methodHandle));
                return generatedMethod.get();
            }
            catch (UncheckedExecutionException e) {
                throwIfUnchecked(e.getCause());
                throw new RuntimeException(e.getCause());
            }
        }

        private static class MethodKey<T>
        {
            private final Type type;
            private final Class<T> singleMethodInterface;

            public MethodKey(Type type, Class<T> singleMethodInterface)
            {
                this.type = requireNonNull(type, "type is null");
                this.singleMethodInterface = requireNonNull(singleMethodInterface, "singleMethodInterface is null");
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                MethodKey<?> that = (MethodKey<?>) o;
                return type.equals(that.type) && singleMethodInterface.equals(that.singleMethodInterface);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(type, singleMethodInterface);
            }
        }

        private static class GeneratedMethod<T>
        {
            private Type type;
            private Class<T> singleMethodInterface;
            private MethodHandle methodHandle;
            @GuardedBy("this")
            private T operator;

            public GeneratedMethod(Type type, Class<T> singleMethodInterface, MethodHandle methodHandle)
            {
                this.type = requireNonNull(type, "type is null");
                this.singleMethodInterface = requireNonNull(singleMethodInterface, "singleMethodInterface is null");
                this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
            }

            public synchronized T get()
            {
                if (operator != null) {
                    return operator;
                }
                String suggestedClassName = singleMethodInterface.getSimpleName() + "_" + type.getDisplayName();
                operator = compileSingleAccessMethod(suggestedClassName, singleMethodInterface, methodHandle);
                // Free up memory by removing references to the objects that we won't need anymore
                type = null;
                singleMethodInterface = null;
                methodHandle = null;
                return operator;
            }
        }
    }
}
