/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.synapse;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.jdbc.CachingJdbcClient;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class FlushStatisticsCacheProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle FLUSH_STATISTICS_CACHE;

    private final CachingJdbcClient cachingJdbcClient;

    static {
        try {
            FLUSH_STATISTICS_CACHE = lookup().unreflect(FlushStatisticsCacheProcedure.class.getMethod("flushStatisticsCache", String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

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
                        new Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        new Procedure.Argument("TABLE_NAME", VARCHAR)),
                FLUSH_STATISTICS_CACHE.bindTo(this));
    }

    public void flushStatisticsCache(String schemaName, String tableName)
    {
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        cachingJdbcClient.onDataChanged(schemaTableName);
    }
}
