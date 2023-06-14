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
package io.trino.spi.function.table;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

// Note: this code was forked from com.google.common.primitives.Primitives
// Copyright (C) 2007 The Guava Authors
final class Primitives
{
    private Primitives() {}

    private static final Map<Class<?>, Class<?>> WRAPPERS = new HashMap<>();

    static {
        WRAPPERS.put(boolean.class, Boolean.class);
        WRAPPERS.put(byte.class, Byte.class);
        WRAPPERS.put(char.class, Character.class);
        WRAPPERS.put(double.class, Double.class);
        WRAPPERS.put(float.class, Float.class);
        WRAPPERS.put(int.class, Integer.class);
        WRAPPERS.put(long.class, Long.class);
        WRAPPERS.put(short.class, Short.class);
    }

    public static <T> Class<T> wrap(Class<T> type)
    {
        requireNonNull(type);

        // cast is safe: long.class and Long.class are both of type Class<Long>
        @SuppressWarnings("unchecked")
        Class<T> wrapped = (Class<T>) WRAPPERS.get(type);
        return (wrapped == null) ? type : wrapped;
    }
}
