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
