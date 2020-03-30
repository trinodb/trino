/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.distributed;

import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.parquet.ParquetPageSourceFactory;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.DecimalType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;

public class SnowflakePageSourceFactory
        implements HivePageSourceFactory
{
    private final ParquetPageSourceFactory parquetPageSourceFactory;

    public SnowflakePageSourceFactory(ParquetPageSourceFactory parquetPageSourceFactory)
    {
        this.parquetPageSourceFactory = parquetPageSourceFactory;
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            Optional<DeleteDeltaLocations> deleteDeltaLocations)
    {
        List<HiveColumnHandle> transformedColumns = columns.stream()
                .map(column -> {
                    if (TIMESTAMP_WITH_TIME_ZONE.equals(column.getType())) {
                        return new HiveColumnHandle(
                                column.getName(),
                                column.getHiveType(),
                                DecimalType.createDecimalType(19),
                                column.getHiveColumnIndex(),
                                column.getColumnType(),
                                column.getComment());
                    }
                    else {
                        return column;
                    }
                }).collect(toImmutableList());
        return parquetPageSourceFactory.createPageSource(
                configuration,
                session,
                path,
                start,
                length,
                fileSize,
                schema,
                transformedColumns,
                effectivePredicate,
                hiveStorageTimeZone,
                deleteDeltaLocations).map(pageSource -> new TranslatingPageSource(pageSource, columns, session));
    }
}
