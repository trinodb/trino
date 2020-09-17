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

import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.QueryBuilder;
import io.prestosql.spi.type.TimestampWithTimeZoneType;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
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
    // Similar to io.prestosql.spi.type.DateTimeEncoding#MILLIS_SHIFT
    public static final int TIMESTAMP_WITH_TIME_ZONE_MILLIS_SHIFT = 12;
    public static final int TIMESTAMP_WITH_TIME_ZONE_ZONE_MASK = 0xFFF;
    public static final int ZONE_OFFSET_MINUTES_BIAS = 2048;

    private final JdbcClient jdbcClient;

    public SnowflakeQueryBuilder(JdbcClient jdbcClient)
    {
        super(jdbcClient);
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
    }

    @Override
    protected String getProjection(List<JdbcColumnHandle> columns)
    {
        if (columns.isEmpty()) {
            return "null";
        }

        return columns.stream()
                .map(columnHandle ->
                {
                    String expression;
                    if (columnHandle.getColumnType() instanceof TimestampWithTimeZoneType) {
                        // milliseconds shifted left by 12 ORed with 2048 +
                        // offset hours * 60 + offset minutes
                        expression = format("BITOR(" +
                                // using TO_DECIMAL to prevent Snowflake from losing precision on the shift result
                                "TO_DECIMAL(BITSHIFTLEFT(EXTRACT('EPOCH_MILLISECOND', %1$s), %2$s), 38, 0), " +
                                "%3$s + EXTRACT('TZH', %1$s) * 60 + EXTRACT('TZM', %1$s))",
                                columnHandle.toSqlExpression(jdbcClient::quoted),
                                TIMESTAMP_WITH_TIME_ZONE_MILLIS_SHIFT,
                                ZONE_OFFSET_MINUTES_BIAS);
                    }
                    else {
                        expression = columnHandle.toSqlExpression(jdbcClient::quoted);
                    }

                    return format("%s AS %s", expression, jdbcClient.quoted(columnHandle.getColumnName()));
                })
                .collect(joining(", "));
    }
}
