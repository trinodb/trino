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
package io.prestosql.jdbc;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

final class TypeConversions
{
    private final Table<Class<?>, Class<?>, TypeConversion<Object, Object>> conversions;

    private TypeConversions(Table<Class<?>, Class<?>, TypeConversion<Object, Object>> conversions)
    {
        this.conversions = ImmutableTable.copyOf(requireNonNull(conversions, "conversions is null"));
    }

    /**
     * @throws NoConversionRegisteredException when conversion is not registered
     */
    public <T> T convert(Object value, Class<T> target)
            throws SQLException
    {
        if (value == null) {
            return null;
        }

        TypeConversion<Object, Object> conversion = conversions.get(value.getClass(), target);
        if (conversion == null) {
            throw new NoConversionRegisteredException();
        }

        @SuppressWarnings("unchecked")
        T converted = (T) conversion.apply(value);
        return converted;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final ImmutableTable.Builder<Class<?>, Class<?>, TypeConversion<Object, Object>> conversions = ImmutableTable.builder();

        private Builder() {}

        @SuppressWarnings("unchecked")
        public <S, T> Builder add(Class<S> source, Class<T> target, TypeConversion<S, T> conversion)
        {
            conversions.put(source, target, (TypeConversion<Object, Object>) conversion);
            return this;
        }

        public TypeConversions build()
        {
            return new TypeConversions(conversions.build());
        }
    }

    @FunctionalInterface
    public interface TypeConversion<S, T>
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
