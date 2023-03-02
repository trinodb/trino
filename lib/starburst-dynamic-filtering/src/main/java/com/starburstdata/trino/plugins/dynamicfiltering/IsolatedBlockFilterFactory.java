/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamicfiltering;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import io.airlift.bytecode.DynamicClassLoader;
import io.trino.collect.cache.NonEvictableLoadingCache;
import io.trino.spi.type.Type;

import java.lang.reflect.Constructor;
import java.util.Objects;

import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
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
                    Class<? extends DynamicPageFilter.BlockFilter> isolatedBlockFilter = IsolatedClass.isolateClass(
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
