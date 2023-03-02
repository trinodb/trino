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
import io.airlift.jmx.CacheStatsMBean;
import io.trino.collect.cache.NonEvictableCache;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;

public class DynamicPageFilterCache
{
    private static final int COLLECTION_THREADS = 4;

    private final TypeManager typeManager;
    private final ExecutorService executor;
    private final NonEvictableCache<DynamicFilter, DynamicPageFilter> cache;
    private final IsolatedBlockFilterFactory isolatedBlockFilterFactory;

    @Inject
    public DynamicPageFilterCache(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.executor = newFixedThreadPool(COLLECTION_THREADS, daemonThreadsNamed("dynamic-page-filter-collector-%s"));
        this.cache = buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterAccess(2, MINUTES)
                .recordStats());
        this.isolatedBlockFilterFactory = new IsolatedBlockFilterFactory();
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Managed
    @Nested
    public CacheStatsMBean getDynamicPageFilterCacheStats()
    {
        return new CacheStatsMBean(cache);
    }

    public DynamicPageFilter getDynamicPageFilter(DynamicFilter dynamicFilter, List<ColumnHandle> columns)
    {
        try {
            return cache.get(dynamicFilter, () -> new DynamicPageFilter(
                    dynamicFilter,
                    columns,
                    typeManager.getTypeOperators(),
                    isolatedBlockFilterFactory,
                    executor));
        }
        catch (ExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new RuntimeException(e.getCause());
        }
    }
}
