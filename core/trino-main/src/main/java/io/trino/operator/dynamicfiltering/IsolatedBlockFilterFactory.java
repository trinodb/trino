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
package io.trino.operator.dynamicfiltering;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import io.airlift.bytecode.DynamicClassLoader;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.spi.type.Type;

import java.lang.reflect.Constructor;
import java.util.Objects;

import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.sql.gen.IsolatedClass.isolateClass;
import static java.util.Objects.requireNonNull;

public class IsolatedBlockFilterFactory
{
    private final NonEvictableLoadingCache<CacheKey, Constructor<? extends DynamicPageFilter.BlockFilter>> cache;

    public IsolatedBlockFilterFactory()
    {
        this.cache = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .maximumSize(1000),
                CacheLoader.from(key -> {
                    Class<? extends DynamicPageFilter.BlockFilter> isolatedBlockFilter = isolateClass(
                            new DynamicClassLoader(DynamicPageFilter.class.getClassLoader()),
                            DynamicPageFilter.BlockFilter.class,
                            DynamicPageFilter.InternalBlockFilter.class);
                    try {
                        return isolatedBlockFilter.getConstructor(TupleDomainFilter.class, int.class);
                    }
                    catch (NoSuchMethodException e) {
                        throw new RuntimeException(e);
                    }
                }));
    }

    public DynamicPageFilter.BlockFilter createIsolatedBlockFilter(TupleDomainFilter filter, int channelIndex)
    {
        try {
            return cache.getUnchecked(new CacheKey(filter.getType().getClass(), filter.getClass()))
                    .newInstance(filter, channelIndex);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static class CacheKey
    {
        private final Class<? extends Type> type;
        private final Class<? extends TupleDomainFilter> filter;

        CacheKey(Class<? extends Type> type, Class<? extends TupleDomainFilter> filter)
        {
            this.type = requireNonNull(type, "type is null");
            this.filter = requireNonNull(filter, "filter is null");
        }

        public Class<? extends Type> getType()
        {
            return type;
        }

        public Class<? extends TupleDomainFilter> getFilter()
        {
            return filter;
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
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(type, cacheKey.type)
                    && Objects.equals(filter, cacheKey.filter);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, filter);
        }
    }
}
