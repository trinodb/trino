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

import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.QueryBuilder;

import java.util.List;

import static com.starburstdata.presto.plugin.snowflake.jdbc.SnowflakeClient.IDENTIFIER_QUOTE;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

/**
 * Customizes the SQL generation to return timestamp with timezone columns as numeric values.
 * We need this to work around the fact that Snowflake cannot export timestamp with time zone
 * columns to Parquet.
 * <p>
 * The builder generates an expression that encodes the information storing the milliseconds
 * in the upper 52 bits and the offset (in minutes) in the lower 12 bits (as 2048 + number of
 * offset minutes, to make it easier to deal with negative numbers).  Note that the millisecond
 * representation is compatible with Presto's encoding of {@link io.prestosql.spi.type.TimestampWithTimeZoneType},
 * so this encoding has the same precision as Presto's type (the least significant 12 bits
 * have a different meaning there - they represent a timezone id, so a translation is necessary).
 */
public class SnowflakeQueryBuilder
        extends QueryBuilder
{
    // from io.prestosql.spi.type.DateTimeEncoding
    private static final int MILLIS_SHIFT = 12;

    public SnowflakeQueryBuilder()
    {
        super(IDENTIFIER_QUOTE);
    }

    @Override
    protected String getProjection(List<JdbcColumnHandle> columns)
    {
        if (columns.isEmpty()) {
            return "null";
        }
        return columns.stream()
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
                .collect(joining(", "));
    }
}
