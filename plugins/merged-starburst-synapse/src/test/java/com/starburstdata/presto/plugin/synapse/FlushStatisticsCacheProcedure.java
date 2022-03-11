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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.CachingJdbcClient;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;

import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

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
