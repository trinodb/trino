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

import io.prestosql.plugin.jdbc.DefaultSqlCustomization;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.type.DateTimeEncoding.MILLIS_SHIFT;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.lang.String.format;

/**
 * Customizes the SQL generation to return timestamp with timezone columns as numeric values.
 * We need this to work around the fact that Snowflake cannot export timestamp with time zone
 * columns to Parquet.
 *
 * The builder generates an expression that encodes the information storing the milliseconds
 * in the upper 52 bits and the offset (in minutes) in the lower 12 bits (as 2048 + number of
 * offset minutes, to make it easier to deal with negative numbers).  Note that the millisecond
 * representation is compatible with Presto's encoding of {@link io.prestosql.spi.type.TimestampWithTimeZoneType},
 * so this encoding has the same precision as Presto's type (the least significant 12 bits
 * have a different meaning there - they represent a timezone id, so a translation is necessary).
 */
public class SnowflakeSqlCustomization
        extends DefaultSqlCustomization
{
    public SnowflakeSqlCustomization(String quoteString)
    {
        super(quoteString);
    }

    @Override
    public List<String> buildColumnNames(List<JdbcColumnHandle> jdbcColumnHandles)
    {
        return jdbcColumnHandles.stream()
                .map(columnHandle ->
                        // milliseconds shifted left by 12 ORed with 2048 +
                        // offset hours * 60 + offset minutes
                        TIMESTAMP_WITH_TIME_ZONE.equals(columnHandle.getColumnType()) ?
                                format(
                                        "BITOR(" +
                                                // using TO_DECIMAL to prevent Snowflake from losing precision on the shift result
                                                "TO_DECIMAL(BITSHIFTLEFT(EXTRACT('EPOCH_MILLISECOND', %1$s), %2$s), 38, 0), " +
                                                "%3$s + EXTRACT('TZH', %1$s) * 60 + EXTRACT('TZM', %1$s)) %1$s",
                                        columnHandle.getColumnName(),
                                        MILLIS_SHIFT,
                                        2048) :
                                quote(columnHandle.getColumnName()))
                .collect(toImmutableList());
    }
}
