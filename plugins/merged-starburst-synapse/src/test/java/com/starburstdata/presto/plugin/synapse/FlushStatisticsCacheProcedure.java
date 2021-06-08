/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.CachingJdbcClient;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;

import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.trino.spi.block.MethodHandleUtil.methodHandle;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class FlushStatisticsCacheProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle FLUSH_STATISTICS_CACHE = methodHandle(
            FlushStatisticsCacheProcedure.class,
            "flushStatisticsCache",
            String.class,
            String.class);

    private final CachingJdbcClient cachingJdbcClient;

    @Inject
    public FlushStatisticsCacheProcedure(CachingJdbcClient cachingJdbcClient)
    {
        this.cachingJdbcClient = requireNonNull(cachingJdbcClient, "cachingJdbcClient is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "flush_statistics_cache",
                ImmutableList.of(
                        new Procedure.Argument("schema_name", VARCHAR),
                        new Procedure.Argument("table_name", VARCHAR)),
                FLUSH_STATISTICS_CACHE.bindTo(this));
    }

    public void flushStatisticsCache(String schemaName, String tableName)
    {
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);

        Stream.of(cachingJdbcClient.getClass().getMethods())
                .filter(method -> method.getName().equals("onDataChanged"))
                .filter(method -> !ImmutableList.copyOf(method.getParameterTypes()).equals(List.of(JdbcTableHandle.class)))
                .findAny()
                .ifPresent(method -> {
                    // TODO The code below is written the way it is written because CachingJdbcClient.onDataChanged(JdbcTableHandle) is not
                    //  a suitable method for flushing table statistics cache (https://github.com/trinodb/trino/pull/7565/files#r648035883).
                    //  It should be updated once a better method is present, e.g. as part of https://github.com/trinodb/trino/pull/8237
                    throw new RuntimeException("Hacky code below can perhaps be simplified using the following method. " +
                            "If not, talk to the code author. " +
                            "Candidate method: " + method);
                });

        try {
            Field statisticsCache = cachingJdbcClient.getClass().getDeclaredField("statisticsCache");
            statisticsCache.setAccessible(true);
            Method invalidateCache = cachingJdbcClient.getClass().getDeclaredMethod("invalidateCache", Cache.class, Predicate.class);
            invalidateCache.setAccessible(true);
            Field keyTableHandle = Class.forName("io.trino.plugin.jdbc.CachingJdbcClient$TableStatisticsCacheKey").getDeclaredField("tableHandle");
            keyTableHandle.setAccessible(true);
            Method references = JdbcTableHandle.class.getDeclaredMethod("references", SchemaTableName.class);
            references.setAccessible(true);

            invalidateCache.invoke(
                    null,
                    statisticsCache.get(cachingJdbcClient),
                    (Predicate<Object>) key -> {
                        try {
                            JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) keyTableHandle.get(key);
                            return ((Boolean) references.invoke(jdbcTableHandle, schemaTableName));
                        }
                        catch (ReflectiveOperationException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
