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

import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;

import java.util.List;
import java.util.Map;

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
 * representation is compatible with Presto's encoding of {@link io.trino.spi.type.TimestampWithTimeZoneType},
 * so this encoding has the same precision as Presto's type (the least significant 12 bits
 * have a different meaning there - they represent a timezone id, so a translation is necessary).
 */
public class SnowflakeQueryBuilder
        extends DefaultQueryBuilder
{
    // Similar to io.trino.spi.type.DateTimeEncoding#MILLIS_SHIFT
    public static final int TIMESTAMP_WITH_TIME_ZONE_MILLIS_SHIFT = 12;
    public static final int TIMESTAMP_WITH_TIME_ZONE_ZONE_MASK = 0xFFF;
    public static final int ZONE_OFFSET_MINUTES_BIAS = 2048;

    @Override
    protected String getProjection(JdbcClient client, List<JdbcColumnHandle> columns, Map<String, String> columnExpressions)
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
                                getColumnExpression(client, columnHandle, columnExpressions),
                                TIMESTAMP_WITH_TIME_ZONE_MILLIS_SHIFT,
                                ZONE_OFFSET_MINUTES_BIAS);
                    }
                    else if (columnHandle.getColumnType() instanceof TimestampType) {
                        expression = format("CAST(%s as TIMESTAMP(3))", getColumnExpression(client, columnHandle, columnExpressions));
                    }
                    else if (columnHandle.getColumnType() instanceof TimeType) {
                        expression = format("CAST(%s as TIME(3))", getColumnExpression(client, columnHandle, columnExpressions));
                    }
                    else {
                        expression = getColumnExpression(client, columnHandle, columnExpressions);
                    }

                    return format("%s AS %s", expression, client.quoted(columnHandle.getColumnName()));
                })
                .collect(joining(", "));
    }

    private String getColumnExpression(JdbcClient client, JdbcColumnHandle jdbcColumnHandle, Map<String, String> columnExpressions)
    {
        String expression = columnExpressions.get(jdbcColumnHandle.getColumnName());
        if (expression == null) {
            return client.quoted(jdbcColumnHandle.getColumnName());
        }
        return expression;
    }
}
