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
package io.trino.plugin.jdbc;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Suppliers.memoize;

public final class LazyMaps
{
    private LazyMaps() {}

    public static <K, V> Map<K, V> of(Supplier<Set<Map.Entry<K, V>>> supplier)
    {
        Supplier<Set<Map.Entry<K, V>>> cachedSupplier = memoize(supplier::get);
        return new AbstractMap<>()
        {
            @Override
            public Set<Entry<K, V>> entrySet()
            {
                return cachedSupplier.get();
            }
        };
    }
}
