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
package io.trino.jdbc;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import io.trino.client.ClientTypeSignature;

import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

final class TypeConversions
{
    private final Table<String, Class<?>, TypeConversion<Object, Object>> conversions;

    private TypeConversions(Table<String, Class<?>, TypeConversion<Object, Object>> conversions)
    {
        this.conversions = ImmutableTable.copyOf(requireNonNull(conversions, "conversions is null"));
    }

    /**
     * @throws NoConversionRegisteredException when conversion is not registered
     */
    public <T> T convert(ClientTypeSignature type, Object value, Class<T> target)
            throws SQLException
    {
        if (value == null) {
            return null;
        }

        TypeConversion<Object, Object> conversion = conversions.get(type.getRawType(), target);
        if (conversion == null) {
            throw new NoConversionRegisteredException();
        }

        @SuppressWarnings("unchecked")
        T converted = (T) conversion.apply(type, value);
        return converted;
    }

    public boolean hasConversion(String sourceRawType, Class<?> target)
    {
        return conversions.contains(sourceRawType, target);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final ImmutableTable.Builder<String, Class<?>, TypeConversion<Object, Object>> conversions = ImmutableTable.builder();

        private Builder() {}

        public <S, T> Builder add(String sourceRawType, Class<S> sourceClass, Class<T> target, SimpleTypeConversion<S, T> conversion)
        {
            requireNonNull(conversion, "conversion is null");
            return add(sourceRawType, sourceClass, target, ((type, value) -> conversion.apply(value)));
        }

        public <S, T> Builder add(String sourceRawType, Class<S> sourceClass, Class<T> target, TypeConversion<S, T> conversion)
        {
            requireNonNull(conversion, "conversion is null");
            conversions.put(sourceRawType, target, (type, value) -> {
                S valueAsSourceType = sourceClass.cast(value);
                T result = conversion.apply(type, valueAsSourceType);
                return target.cast(result);
            });
            return this;
        }

        public TypeConversions build()
        {
            return new TypeConversions(conversions.buildOrThrow());
        }
    }

    @FunctionalInterface
    public interface TypeConversion<S, T>
    {
        /**
         * Convert a non-null value.
         */
        T apply(ClientTypeSignature type, S value)
                throws SQLException;
    }

    @FunctionalInterface
    public interface SimpleTypeConversion<S, T>
    {
        /**
         * Convert a non-null value.
         */
        T apply(S value)
                throws SQLException;
    }

    public static class NoConversionRegisteredException
            extends RuntimeException {}
}
